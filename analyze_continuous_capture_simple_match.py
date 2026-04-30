#!/usr/bin/env python3
"""Analyze continuous file-sink captures and recover burst quality."""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
import csv
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib-codex")
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from check_bin import circular_phase_spread_rad, classify_signal, phase_cluster_stats


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_ROOT = PROJECT_ROOT / "1to1"
DEFAULT_PLOT_ROOT = PROJECT_ROOT / "output_analyze_continuous"
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "continuous_capture_config.json"
DEFAULT_CAPTURE_GROUPS = [
    {
        "label": "calibration",
        "distance_m": 2.0,
        "reflector_file": "data_reflector_rx_from_initiator_calibration",
        "initiator_file": "data_initiator_rx_from_reflector_calibration",
    },
    {
        "label": "measurement",
        "distance_m": 4.0,
        "reflector_file": "data_reflector_rx_from_initiator_measurement",
        "initiator_file": "data_initiator_rx_from_reflector_measurement",
    },
]
PAIR_FREQ_CSV_FIELDS = [
    "freq_index",
    "freq_hz",
    "pair_abs",
    "pair_angle_rad",
    "pair_phase_rad",
    "initiator_abs",
    "initiator_angle_rad",
    "reflector_abs",
    "reflector_angle_rad",
]
PAIR_ANGLE_CSV_FIELDS = [
    "freq_index",
    "freq_hz",
    "pair_angle_rad",
    "pair_phase_rad",
]

# Constants for simple match distance estimation
PROPAGATION_SPEED_MPS = 2.3e8  # Effective BLE channel sounder speed (0.767c)
DEFAULT_DISTANCE_GRID_START_M = 0.0
DEFAULT_DISTANCE_GRID_STOP_M = 30.0
DEFAULT_DISTANCE_GRID_STEP_M = 0.01


def wrap_to_pi(x: np.ndarray | float) -> np.ndarray | float:
    """Wrap angle to (-π, π]."""
    return (np.asarray(x) + np.pi) % (2.0 * np.pi) - np.pi


def estimate_distance_simple_match_from_pair_rows(
    pair_rows: list[dict[str, Any]],
    args: argparse.Namespace,
    *,
    source: str,
) -> dict[str, Any]:
    """
    Estimate distance via simple wrapped phase error minimization.
    
    Scans 0-30m range with 0.01m steps to find best match of phase model.
    Returns format compatible with estimate_distance_phase_match_from_pair_rows.
    """
    usable_pair_rows = sorted(pair_rows, key=lambda item: int(item["freq_index"]))
    if len(usable_pair_rows) < 2:
        raise SystemExit("有效公共频点少于 2 个，无法做相位匹配测距")
    
    freqs_hz = np.array([float(row["freq_hz"]) for row in usable_pair_rows], dtype=float)
    measured_wrapped = np.array([float(row["pair_phase_rad"]) for row in usable_pair_rows], dtype=float)
    measured_wrapped = wrap_to_pi(measured_wrapped)
    
    propagation_speed_mps = float(getattr(args, "propagation_speed_mps", PROPAGATION_SPEED_MPS))
    
    # Simple distance grid scan
    distance_grid = np.arange(
        DEFAULT_DISTANCE_GRID_START_M,
        DEFAULT_DISTANCE_GRID_STOP_M + 0.5 * DEFAULT_DISTANCE_GRID_STEP_M,
        DEFAULT_DISTANCE_GRID_STEP_M,
        dtype=float,
    )
    
    costs = []
    phase0_values = []
    
    for distance_m in distance_grid:
        # Model phase for this distance
        model_phase = -4.0 * np.pi * freqs_hz * distance_m / propagation_speed_mps
        
        # Find optimal phase0 to minimize wrapped error
        residual = measured_wrapped - model_phase
        coherent_sum = np.mean(np.exp(1j * residual))
        phase0 = float(np.angle(coherent_sum))
        
        # Calculate wrapped error
        wrapped_error = wrap_to_pi(measured_wrapped - (model_phase + phase0))
        cost = float(np.mean(wrapped_error ** 2))
        
        phase0_values.append(phase0)
        costs.append(cost)
    
    # Find best distance
    costs_array = np.array(costs)
    best_index = int(np.argmin(costs_array))
    best_distance_m = float(distance_grid[best_index])
    best_phase0 = float(phase0_values[best_index])
    best_cost = float(costs_array[best_index])
    
    # Compute best fit
    best_model_phase = -4.0 * np.pi * freqs_hz * best_distance_m / propagation_speed_mps
    best_wrapped_fit = wrap_to_pi(best_model_phase + best_phase0)
    best_wrapped_error = np.asarray(wrap_to_pi(measured_wrapped - best_wrapped_fit), dtype=float)
    
    # Build rows for diagnostic output
    rows: list[dict[str, Any]] = []
    for pair_row, fit_phase, phase_error in zip(usable_pair_rows, best_wrapped_fit, best_wrapped_error):
        rows.append(
            {
                "freq_index": int(pair_row["freq_index"]),
                "freq_hz": float(pair_row["freq_hz"]),
                "pair_abs": float(pair_row.get("pair_abs", 0.0)),
                "phase_wrapped_measured": float(pair_row["pair_phase_rad"]),
                "phase_wrapped_fit": float(fit_phase),
                "phase_wrapped_error": float(phase_error),
            }
        )
    
    return {
        "root": str(resolve_root(args.root)),
        "source": source,
        "pair_csv": str(args.pair_csv.resolve()) if args.pair_csv is not None else None,
        "reflector_file": None,
        "initiator_file": None,
        "reflector_invalid_burst_count": 0,
        "initiator_invalid_burst_count": 0,
        "valid_freq_count": int(len(rows)),
        "distance_m": best_distance_m,
        "propagation_speed_mps": float(propagation_speed_mps),
        "phase0_rad": best_phase0,
        "wrapped_phase_cost": best_cost,
        "wrapped_phase_rms_error": float(np.sqrt(np.mean(best_wrapped_error ** 2))),
        "wrapped_phase_max_abs_error": float(np.max(np.abs(best_wrapped_error))),
        "distance_grid_m": [float(x) for x in distance_grid],
        "cost_grid": [float(x) for x in costs],
        "rows": rows,
    }


def resolve_root(root: Path) -> Path:
    if root.is_absolute():
        return root
    return (PROJECT_ROOT / root).resolve()


def _pick_existing_path(candidates: list[Path]) -> Path:
    for path in candidates:
        if path.exists():
            return path
    return candidates[0]


def default_capture_paths(root: Path) -> tuple[Path, Path]:
    resolved_root = resolve_root(root)
    reflector = _pick_existing_path(
        [
            resolved_root / "data_reflector_rx_from_initiator_calibration",
            resolved_root / "data_reflector_rx_from_initiator2",
            resolved_root / "continuous_capture" / "data_reflector_rx_from_initiator.bin",
            resolved_root / "data_reflector_rx_from_initiator.bin",
        ]
    )
    initiator = _pick_existing_path(
        [
            resolved_root / "data_initiator_rx_from_reflector_calibration",
            resolved_root / "data_initiator_rx_from_reflector2",
            resolved_root / "continuous_capture" / "data_initiator_rx_from_reflector.bin",
            resolved_root / "data_initiator_rx_from_reflector.bin",
        ]
    )
    return reflector, initiator


def resolve_group_file(root: Path, value: str | Path) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return resolve_root(root) / path


def capture_group_specs(config: dict[str, Any]) -> list[dict[str, Any]]:
    raw_groups = config.get("capture_groups", DEFAULT_CAPTURE_GROUPS)
    if not isinstance(raw_groups, list):
        raise SystemExit("capture_groups 必须是 list")

    groups: list[dict[str, Any]] = []
    for idx, item in enumerate(raw_groups):
        if not isinstance(item, dict):
            raise SystemExit(f"capture_groups[{idx}] 必须是 JSON object")
        label = str(item.get("label", f"group{idx + 1}"))
        reflector_file = item.get("reflector_file")
        initiator_file = item.get("initiator_file")
        if reflector_file is None or initiator_file is None:
            raise SystemExit(f"capture_groups[{idx}] 缺少 reflector_file 或 initiator_file")
        groups.append(
            {
                "label": label,
                "distance_m": item.get("distance_m"),
                "reflector_file": str(reflector_file),
                "initiator_file": str(initiator_file),
            }
        )
    return groups


def load_gr_complex_bin(path: Path) -> np.ndarray:
    if not path.exists():
        raise FileNotFoundError(f"capture file not found: {path}")
    return np.fromfile(path, dtype=np.complex64)


def expected_burst_count(start_offset_hz: float, stop_offset_hz: float, step_hz: float, repeats: int) -> int:
    freq_count = int(round((float(stop_offset_hz) - float(start_offset_hz)) / float(step_hz))) + 1
    return freq_count * int(repeats)


def default_plot_dir(root: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return (DEFAULT_PLOT_ROOT / root.name / timestamp).resolve()


def load_config(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    resolved = path if path.is_absolute() else (PROJECT_ROOT / path).resolve()
    if not resolved.exists():
        return {}
    with resolved.open("r", encoding="utf-8") as fp:
        data = json.load(fp)
    if not isinstance(data, dict):
        raise SystemExit(f"配置文件必须是 JSON object: {resolved}")
    return data


def apply_config_defaults(parser: argparse.ArgumentParser, config: dict[str, Any]) -> None:
    defaults: dict[str, Any] = {}
    for action in parser._actions:
        if not action.option_strings or action.dest in {"help", "config"}:
            continue
        if action.dest not in config:
            continue
        value = config[action.dest]
        if action.type is Path and value is not None:
            value = Path(value)
        defaults[action.dest] = value
    parser.set_defaults(**defaults)


def moving_average(x: np.ndarray, width: int) -> np.ndarray:
    if width <= 1 or x.size == 0:
        return x.astype(float, copy=False)
    kernel = np.ones(int(width), dtype=float) / float(width)
    return np.convolve(x, kernel, mode="same")


def _merge_short_gaps(mask: np.ndarray, max_gap: int) -> np.ndarray:
    if max_gap <= 0 or mask.size == 0:
        return mask
    merged = mask.copy()
    idx = 0
    while idx < merged.size:
        if merged[idx]:
            idx += 1
            continue
        gap_start = idx
        while idx < merged.size and not merged[idx]:
            idx += 1
        gap_end = idx
        left_on = gap_start > 0 and merged[gap_start - 1]
        right_on = gap_end < merged.size and merged[gap_end]
        if left_on and right_on and gap_end - gap_start <= max_gap:
            merged[gap_start:gap_end] = True
    return merged


def _true_runs(mask: np.ndarray) -> list[tuple[int, int]]:
    runs: list[tuple[int, int]] = []
    idx = 0
    while idx < mask.size:
        if not mask[idx]:
            idx += 1
            continue
        start = idx
        while idx < mask.size and mask[idx]:
            idx += 1
        runs.append((start, idx))
    return runs


def robust_complex_mean(x: np.ndarray, outlier_mad_scale: float = 8.0) -> tuple[complex, int, int]:
    if x.size == 0:
        return 0j, 0, 0
    finite = x[np.isfinite(x.real) & np.isfinite(x.imag)]
    if finite.size == 0:
        return 0j, 0, int(x.size)
    center = np.median(finite.real) + 1j * np.median(finite.imag)
    radius = np.abs(finite - center)
    median_radius = float(np.median(radius))
    mad_radius = float(np.median(np.abs(radius - median_radius)))
    threshold = median_radius + (1e-9 if mad_radius <= 1e-12 else outlier_mad_scale * mad_radius)
    valid = finite[radius <= threshold]
    if valid.size == 0:
        valid = finite
    return complex(np.mean(valid)), int(valid.size), int(x.size - valid.size)


def _find_true_runs_from_flags(flags: list[bool]) -> list[tuple[int, int]]:
    runs: list[tuple[int, int]] = []
    idx = 0
    while idx < len(flags):
        if not flags[idx]:
            idx += 1
            continue
        start = idx
        while idx < len(flags) and flags[idx]:
            idx += 1
        runs.append((start, idx))
    return runs


def find_stable_core_segment(
    x: np.ndarray,
    *,
    min_segment_len: int,
) -> tuple[int, int, str]:
    n = int(x.size)
    if n < max(min_segment_len, 32):
        return 0, n, "full_segment_short"

    window = min(n, max(256, min(1024, n // 4 if n >= 1024 else n)))
    hop = max(32, window // 4)
    if window <= 0 or hop <= 0:
        return 0, n, "full_segment_fallback"

    amp = np.abs(x)
    window_rows: list[dict[str, float | int]] = []
    for start in range(0, n - window + 1, hop):
        stop = start + window
        chunk = x[start:stop]
        cluster = phase_cluster_stats(chunk)
        window_rows.append(
            {
                "start": int(start),
                "stop": int(stop),
                "mean_abs": float(np.mean(amp[start:stop])),
                "coherence": float(cluster["coherence"]),
                "phase_std": float(cluster["cluster_phase_std"]),
            }
        )

    if not window_rows:
        return 0, n, "full_segment_no_windows"

    mean_abs_values = np.array([float(row["mean_abs"]) for row in window_rows], dtype=float)
    amp_ref = float(np.percentile(mean_abs_values, 75)) if mean_abs_values.size else 0.0

    def pick_run(min_coherence: float, max_phase_std: float, amp_ratio: float, tag: str) -> tuple[int, int, str] | None:
        flags = [
            bool(
                float(row["mean_abs"]) >= amp_ref * amp_ratio
                and float(row["coherence"]) >= min_coherence
                and float(row["phase_std"]) <= max_phase_std
            )
            for row in window_rows
        ]
        runs = _find_true_runs_from_flags(flags)
        if not runs:
            return None
        best = max(
            runs,
            key=lambda item: (
                int(window_rows[item[1] - 1]["stop"]) - int(window_rows[item[0]]["start"]),
                -item[0],
            ),
        )
        start = int(window_rows[best[0]]["start"])
        stop = int(window_rows[best[1] - 1]["stop"])
        if stop - start < min_segment_len:
            return None
        return start, stop, tag

    for cfg in (
        (0.97, 0.18, 0.85, "stable_core_strict"),
        (0.94, 0.30, 0.80, "stable_core_relaxed"),
        (0.90, 0.45, 0.75, "stable_core_loose"),
    ):
        picked = pick_run(*cfg)
        if picked is not None:
            return picked

    return 0, n, "full_segment_untrimmed"


def summarize_segment(
    x: np.ndarray,
    *,
    min_segment_len: int,
    edge_trim_samples: int = 0,
) -> dict[str, Any]:
    raw_cluster = phase_cluster_stats(x)
    raw_classification = classify_signal(x)
    core_start, core_stop, selection = find_stable_core_segment(x, min_segment_len=min_segment_len)
    requested_edge_trim = max(0, int(edge_trim_samples))
    edge_trim_applied = 0
    core_len = int(core_stop - core_start)
    if requested_edge_trim > 0 and core_len > min_segment_len:
        max_trim_each_side = max(0, int((core_len - min_segment_len) // 2))
        edge_trim_applied = min(requested_edge_trim, max_trim_each_side)
        core_start += edge_trim_applied
        core_stop -= edge_trim_applied
    segment = x[core_start:core_stop]
    cluster = phase_cluster_stats(segment)
    classification = classify_signal(segment)
    z_mean, robust_samples, outlier_samples = robust_complex_mean(segment)
    return {
        "selection": selection,
        "edge_trim_requested_samples": int(requested_edge_trim),
        "edge_trim_applied_samples": int(edge_trim_applied),
        "core_offset_start": int(core_start),
        "core_offset_stop": int(core_stop),
        "raw_segment_len": int(x.size),
        "raw_segment_classification": raw_classification,
        "raw_segment_coherence": float(raw_cluster["coherence"]),
        "raw_segment_phase_std": float(raw_cluster["cluster_phase_std"]),
        "raw_segment_mean_abs": float(np.mean(np.abs(x))) if x.size else 0.0,
        "segment_start": int(core_start),
        "segment_stop": int(core_stop),
        "segment_len": int(segment.size),
        "segment_classification": classification,
        "segment_coherence": float(cluster["coherence"]),
        "segment_phase_std": float(cluster["cluster_phase_std"]),
        "segment_phase_p95_abs": float(cluster["cluster_phase_p95_abs"]),
        "segment_phase_max_abs": float(cluster["cluster_phase_max_abs"]),
        "segment_mean_abs": float(np.mean(np.abs(segment))) if segment.size else 0.0,
        "segment_max_abs": float(np.max(np.abs(segment))) if segment.size else 0.0,
        "robust_samples": int(robust_samples),
        "outlier_samples": int(outlier_samples),
        "robust_mean_i": float(np.real(z_mean)),
        "robust_mean_q": float(np.imag(z_mean)),
        "robust_mean_abs": float(abs(z_mean)),
        "robust_mean_phase": float(np.angle(z_mean)) if abs(z_mean) > 0 else 0.0,
    }


def _summarize_candidate_task(task: tuple[int, int, int, np.ndarray, int, int]) -> dict[str, Any]:
    candidate_index, start, stop, raw_segment, min_segment_len, edge_trim_samples = task
    summary = summarize_segment(
        raw_segment,
        min_segment_len=min_segment_len,
        edge_trim_samples=edge_trim_samples,
    )
    core_start = int(summary["core_offset_start"])
    core_stop = int(summary["core_offset_stop"])
    abs_start = int(start + core_start)
    abs_stop = int(start + core_stop)
    raw_len = int(stop - start)
    summary.update(
        {
            "candidate_index": int(candidate_index),
            "raw_segment_start": int(start),
            "raw_segment_stop": int(stop),
            "raw_segment_len": int(raw_len),
            "segment_start": int(abs_start),
            "segment_stop": int(abs_stop),
            "segment_len": int(max(0, abs_stop - abs_start)),
            "score": float(
                summary["robust_mean_abs"]
                * (0.25 + summary["segment_coherence"])
                * np.sqrt(max(1, abs_stop - abs_start))
            ),
        }
    )
    return summary


def detect_capture_bursts(
    capture: np.ndarray,
    *,
    repeats: int,
    start_offset_hz: float,
    step_hz: float,
    center_freq_hz: float,
    smooth_len: int,
    threshold_ratio: float,
    gap_tolerance: int,
    min_segment_len: int,
    expected_bursts: int,
    edge_trim_samples: int = 0,
    jobs: int = 1,
) -> list[dict[str, Any]]:
    amp = np.abs(capture)
    amp_smooth = moving_average(amp, smooth_len)
    noise_floor = float(np.percentile(amp_smooth, 20)) if amp_smooth.size else 0.0
    signal_level = float(np.percentile(amp_smooth, 99.5)) if amp_smooth.size else 0.0
    threshold = noise_floor + float(threshold_ratio) * max(0.0, signal_level - noise_floor)
    mask = _merge_short_gaps(amp_smooth >= threshold, gap_tolerance)

    tasks: list[tuple[int, int, int, np.ndarray, int, int]] = []
    for candidate_index, (start, stop) in enumerate(_true_runs(mask)):
        raw_len = stop - start
        if raw_len < min_segment_len:
            continue
        tasks.append(
            (
                int(candidate_index),
                int(start),
                int(stop),
                capture[start:stop],
                int(min_segment_len),
                int(edge_trim_samples),
            )
        )

    worker_count = max(1, int(jobs))
    if worker_count > 1 and len(tasks) > 1:
        worker_count = min(worker_count, len(tasks))
        chunksize = max(1, len(tasks) // (worker_count * 4))
        with ProcessPoolExecutor(max_workers=worker_count) as executor:
            candidates = list(executor.map(_summarize_candidate_task, tasks, chunksize=chunksize))
    else:
        candidates = [_summarize_candidate_task(task) for task in tasks]

    candidates = filter_sequence_candidates(
        candidates,
        expected_bursts=expected_bursts,
        min_segment_len=min_segment_len,
    )
    expected_freq_count = int(round(float(expected_bursts) / float(repeats))) if repeats > 0 else 0
    rows = assign_freq_groups(
        candidates,
        expected_freq_count=expected_freq_count,
        repeats=repeats,
        center_freq_hz=center_freq_hz,
        start_offset_hz=start_offset_hz,
        step_hz=step_hz,
    )
    for burst_index, row in enumerate(rows):
        row["burst_index"] = int(burst_index)
        row["amp_threshold"] = float(threshold)
        row["amp_noise_floor"] = float(noise_floor)
        row["amp_signal_level"] = float(signal_level)
    return rows


def filter_sequence_candidates(
    candidates: list[dict[str, Any]],
    *,
    expected_bursts: int,
    min_segment_len: int,
) -> list[dict[str, Any]]:
    if not candidates:
        return []

    lengths = np.array([int(row["segment_len"]) for row in candidates], dtype=float)
    nominal_len = float(np.median(lengths[lengths > 0])) if np.any(lengths > 0) else float(min_segment_len)
    min_raw_good_len = max(float(min_segment_len), 0.42 * nominal_len)
    suspicious_short_len = max(float(min_segment_len), 0.60 * nominal_len)
    cluster_gap = 1.35 * nominal_len

    slotted: list[dict[str, Any]] = []
    idx = 0
    while idx < len(candidates):
        row = candidates[idx]
        row["quality_flags"] = []
        if float(row["raw_segment_len"]) >= min_raw_good_len:
            slotted.append(row)
            idx += 1
            continue

        cluster = [row]
        next_idx = idx + 1
        while next_idx < len(candidates):
            prev = candidates[next_idx - 1]
            cur = candidates[next_idx]
            if float(cur["raw_segment_len"]) >= min_raw_good_len:
                break
            if int(cur["raw_segment_start"]) - int(prev["raw_segment_start"]) > cluster_gap:
                break
            cluster.append(cur)
            next_idx += 1

        rep = max(cluster, key=lambda item: float(item["score"]))
        rep["quality_flags"] = list(rep.get("quality_flags", [])) + ["short_burst"]
        for extra in cluster:
            if extra is rep:
                continue
            extra["sequence_ok"] = False
            extra["quality_flags"] = list(extra.get("quality_flags", [])) + ["extra_impulse_fragment"]
        slotted.append(rep)
        idx = next_idx

    for row in slotted:
        flags = list(row.get("quality_flags", []))
        if float(row["raw_segment_len"]) < suspicious_short_len:
            flags.append("suspicious_short")
        if float(row["segment_coherence"]) < 0.90:
            flags.append("low_coherence")
        if float(row["segment_phase_std"]) > 0.45:
            flags.append("wide_phase")
        if str(row["segment_classification"]) != "stable_cluster":
            flags.append(f"class_{row['segment_classification']}")
        if int(row["core_offset_start"]) > max(256, int(0.08 * max(1, int(row["raw_segment_len"])))):
            flags.append("trimmed_head")
        row["quality_flags"] = flags
        row["sequence_ok"] = not any(
            flag in {"short_burst", "suspicious_short", "low_coherence", "wide_phase"} for flag in flags
        )

    if expected_bursts > 0 and len(slotted) > expected_bursts:
        invalid = [row for row in slotted if not row["sequence_ok"]]
        valid = [row for row in slotted if row["sequence_ok"]]
        invalid_keep = sorted(invalid, key=lambda item: float(item["score"]), reverse=True)
        valid_keep = sorted(valid, key=lambda item: float(item["score"]), reverse=True)
        keep_budget = expected_bursts
        chosen: list[dict[str, Any]] = []
        for pool in (valid_keep, invalid_keep):
            for row in pool:
                if len(chosen) >= keep_budget:
                    break
                chosen.append(row)
        chosen_ids = {id(item) for item in chosen}
        slotted = [row for row in slotted if id(row) in chosen_ids]

    slotted.sort(key=lambda item: int(item["segment_start"]))
    if expected_bursts > 0 and len(slotted) > expected_bursts:
        slotted = slotted[:expected_bursts]
    return slotted


def _group_cost(rows: list[dict[str, Any]]) -> float:
    size = len(rows)
    if size <= 0:
        return 1e9

    phases = [float(row["robust_mean_phase"]) for row in rows]
    abs_values = np.array([float(row["robust_mean_abs"]) for row in rows], dtype=float)
    lengths = np.array([float(row["segment_len"]) for row in rows], dtype=float)
    starts = np.array([float(row["raw_segment_start"]) for row in rows], dtype=float)

    phase_spread = circular_phase_spread_rad(phases) if size > 1 else 0.0
    mean_abs = max(1e-9, float(np.mean(abs_values)))
    abs_spread_ratio = float((np.max(abs_values) - np.min(abs_values)) / mean_abs) if size > 1 else 0.0
    mean_len = max(1.0, float(np.mean(lengths)))
    len_spread_ratio = float((np.max(lengths) - np.min(lengths)) / mean_len) if size > 1 else 0.0
    gap_spread_ratio = 0.0
    if size > 2:
        gaps = np.diff(starts)
        gap_mean = max(1.0, float(np.mean(gaps)))
        gap_spread_ratio = float((np.max(gaps) - np.min(gaps)) / gap_mean)

    size_penalty = {1: 0.60, 2: 0.18, 3: 0.0}.get(size, 1.5)
    return (
        4.0 * phase_spread
        + 1.5 * abs_spread_ratio
        + 0.5 * len_spread_ratio
        + 0.2 * gap_spread_ratio
        + size_penalty
    )


def _partition_repeat_groups(rows: list[dict[str, Any]], repeats: int) -> list[list[dict[str, Any]]]:
    max_group_size = max(1, int(repeats))
    n = len(rows)
    if n <= 0:
        return []

    best_cost = [float("inf")] * (n + 1)
    best_size = [1] * (n + 1)
    best_cost[n] = 0.0

    for idx in range(n - 1, -1, -1):
        for size in range(1, max_group_size + 1):
            stop = idx + size
            if stop > n:
                break
            cost = _group_cost(rows[idx:stop]) + best_cost[stop]
            if cost < best_cost[idx]:
                best_cost[idx] = cost
                best_size[idx] = size

    groups: list[list[dict[str, Any]]] = []
    idx = 0
    while idx < n:
        size = max(1, min(max_group_size, best_size[idx]))
        groups.append(rows[idx : idx + size])
        idx += size
    return groups


def assign_freq_groups(
    candidates: list[dict[str, Any]],
    *,
    expected_freq_count: int,
    repeats: int,
    center_freq_hz: float,
    start_offset_hz: float,
    step_hz: float,
) -> list[dict[str, Any]]:
    rows = [dict(row) for row in candidates]
    for row in rows:
        row["assigned_to_freq"] = False
        row["freq_index"] = -1
        row["repeat_index"] = -1
        row["freq_hz"] = None
        row["slot_kind"] = "unassigned"

    if expected_freq_count <= 0 or not rows:
        return rows

    if repeats > 1:
        groups = _partition_repeat_groups(rows, repeats)
        for freq_index, group_rows in enumerate(groups):
            for repeat_index, row in enumerate(group_rows):
                if freq_index >= expected_freq_count:
                    row["slot_kind"] = "overflow"
                    row["quality_flags"] = list(row.get("quality_flags", [])) + ["overflow_after_expected_slots"]
                    continue

                row["assigned_to_freq"] = bool(row.get("sequence_ok", True))
                row["freq_index"] = int(freq_index)
                row["repeat_index"] = int(repeat_index)
                row["freq_hz"] = float(center_freq_hz + start_offset_hz + freq_index * step_hz)
                row["slot_kind"] = "valid_slot" if bool(row.get("sequence_ok", True)) else "invalid_slot"
                if len(group_rows) < repeats:
                    row["quality_flags"] = list(row.get("quality_flags", [])) + ["partial_repeat_group"]
        return rows

    freq_index = 0
    for row in rows:
        if freq_index >= expected_freq_count:
            row["slot_kind"] = "overflow"
            row["quality_flags"] = list(row.get("quality_flags", [])) + ["overflow_after_expected_slots"]
            continue

        freq_hz = float(center_freq_hz + start_offset_hz + freq_index * step_hz)
        row["assigned_to_freq"] = bool(row.get("sequence_ok", True))
        row["freq_index"] = int(freq_index)
        row["repeat_index"] = 0
        row["freq_hz"] = freq_hz
        row["slot_kind"] = "valid_slot" if bool(row.get("sequence_ok", True)) else "invalid_slot"
        freq_index += 1

    return rows


def average_rows_by_freq(rows: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    grouped: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        if not bool(row.get("sequence_ok", True)):
            continue
        if not bool(row.get("assigned_to_freq", False)):
            continue
        grouped.setdefault(int(row["freq_index"]), []).append(row)

    averaged: dict[int, dict[str, Any]] = {}
    for freq_index, freq_rows in grouped.items():
        z_values = np.array(
            [complex(float(row["robust_mean_i"]), float(row["robust_mean_q"])) for row in freq_rows],
            dtype=np.complex128,
        )
        if z_values.size == 0:
            continue
        z_bar = complex(np.mean(z_values))
        averaged[freq_index] = {
            "freq_index": int(freq_index),
            "freq_hz": float(freq_rows[0]["freq_hz"]),
            "repeat_count": int(len(freq_rows)),
            "z": z_bar,
            "abs": float(abs(z_bar)),
            "phase": float(np.angle(z_bar)),
        }
    return averaged


def summarize_quality_flags(flags: list[str]) -> str:
    if not flags:
        return "正常"
    reasons: list[str] = []
    if any(flag in {"short_burst", "suspicious_short"} for flag in flags):
        reasons.append("尖针/短burst")
    if any(flag in {"low_coherence", "wide_phase"} for flag in flags):
        reasons.append("angle不聚合")
    if "trimmed_head" in flags:
        reasons.append("边界未截净")
    if "skipped_to_preserve_slots" in flags:
        reasons.append("为保留槽位跳过")
    if not reasons:
        reasons = flags
    seen: list[str] = []
    for reason in reasons:
        if reason not in seen:
            seen.append(reason)
    return ",".join(seen)


def build_side_freq_diagnostics(
    rows: list[dict[str, Any]],
    *,
    expected_freq_count: int,
    expected_repeats: int,
) -> list[dict[str, Any]]:
    grouped: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        freq_index = int(row.get("freq_index", -1))
        if freq_index < 0:
            continue
        grouped.setdefault(freq_index, []).append(row)

    diagnostics: list[dict[str, Any]] = []
    # 历史逻辑默认把 >=2 次视为 usable；当实验本身 repeats=1 时，应允许 1 次即 usable。
    required_valid_repeats = 1 if int(expected_repeats) <= 1 else 2
    for freq_index in range(expected_freq_count):
        freq_rows = sorted(grouped.get(freq_index, []), key=lambda item: int(item.get("repeat_index", -1)))
        assigned_valid = [row for row in freq_rows if bool(row.get("assigned_to_freq", False)) and bool(row.get("sequence_ok", True))]
        invalid_rows = [row for row in freq_rows if not bool(row.get("sequence_ok", True))]
        if assigned_valid:
            state = "usable" if len(assigned_valid) >= required_valid_repeats else "partial"
        elif invalid_rows:
            state = "invalid_slot"
        elif freq_rows:
            state = "unassigned"
        else:
            state = "missing"

        reason_parts: list[str] = []
        if state == "partial":
            reason_parts.append("repeat不足")
        if state == "invalid_slot":
            reason_parts.append("整槽异常")
        if state == "missing":
            reason_parts.append("该频段无样本")
        for row in freq_rows:
            reason = summarize_quality_flags(list(row.get("quality_flags", [])))
            if reason != "正常" and reason not in reason_parts:
                reason_parts.append(reason)
        diagnostics.append(
            {
                "freq_index": int(freq_index),
                "freq_hz": float(freq_rows[0]["freq_hz"]) if freq_rows and freq_rows[0].get("freq_hz") is not None else None,
                "state": state,
                "reason": "、".join(reason_parts) if reason_parts else "正常",
                "rows": freq_rows,
                "valid_repeat_count": int(len(assigned_valid)),
            }
        )
    return diagnostics


def build_pair_freq_diagnostics(
    initiator_diag: list[dict[str, Any]],
    reflector_diag: list[dict[str, Any]],
    pair_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    pair_by_freq = {int(row["freq_index"]): row for row in pair_rows}
    diagnostics: list[dict[str, Any]] = []
    for init_row, refl_row in zip(initiator_diag, reflector_diag):
        freq_index = int(init_row["freq_index"])
        reasons: list[str] = []
        if init_row["state"] != "usable":
            reasons.append(f"initiator:{init_row['reason']}")
        if refl_row["state"] != "usable":
            reasons.append(f"reflector:{refl_row['reason']}")
        pair_row = pair_by_freq.get(freq_index)
        state = "pair_usable" if pair_row is not None else "pair_dropped"
        if pair_row is None and not reasons:
            reasons.append("两侧未同时形成可用频段")
        diagnostics.append(
            {
                "freq_index": freq_index,
                "pair_state": state,
                "pair_reason": "；".join(reasons) if reasons else "正常",
                "pair_row": pair_row,
            }
        )
    return diagnostics


def summarize_freq_rows(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "none"
    valid_count = sum(
        1 for row in rows if bool(row.get("assigned_to_freq", False)) and bool(row.get("sequence_ok", True))
    )
    invalid_count = sum(1 for row in rows if not bool(row.get("sequence_ok", True)))
    mean_abs = np.mean([float(row.get("robust_mean_abs", 0.0)) for row in rows]) if rows else 0.0
    mean_coh = np.mean([float(row.get("segment_coherence", 0.0)) for row in rows]) if rows else 0.0
    mean_std = np.mean([float(row.get("segment_phase_std", 0.0)) for row in rows]) if rows else 0.0
    slot_kinds = ",".join(sorted({str(row.get("slot_kind", "unknown")) for row in rows}))
    return (
        f"bursts={len(rows)}, valid_repeats={valid_count}, invalid_bursts={invalid_count}, "
        f"mean_abs={mean_abs:.3f}, mean_coh={mean_coh:.3f}, mean_std={mean_std:.3f}, slot={slot_kinds}"
    )


def _set_robust_ylim(
    ax: Any,
    values: np.ndarray,
    *,
    min_span: float,
    percentile: tuple[float, float] = (1.0, 99.0),
    margin_ratio: float = 0.20,
) -> None:
    finite = np.asarray(values, dtype=float)
    finite = finite[np.isfinite(finite)]
    if finite.size == 0:
        return

    center = float(np.median(finite))
    low, high = np.percentile(finite, percentile)
    span = max(float(high - low), float(min_span))
    pad = 0.5 * span * float(margin_ratio)
    half_span = 0.5 * span + pad
    ax.set_ylim(center - half_span, center + half_span)


def plot_capture(rows: list[dict[str, Any]], save_path: Path, title: str) -> None:
    if not rows:
        raise SystemExit("no rows to plot")

    xs = np.arange(len(rows), dtype=int)
    mean_abs = np.array([float(row["segment_mean_abs"]) for row in rows], dtype=float)
    coherence = np.array([float(row["segment_coherence"]) for row in rows], dtype=float)
    phase_std = np.array([float(row["segment_phase_std"]) for row in rows], dtype=float)

    fig, axes = plt.subplots(3, 1, figsize=(12, 9), sharex=True)

    axes[0].plot(xs, mean_abs, "o-", linewidth=1.2, markersize=4)
    axes[0].set_ylabel("Mean |x|")
    axes[0].grid(True, alpha=0.3)
    axes[0].set_title(title)

    axes[1].plot(xs, coherence, "o-", color="tab:green", linewidth=1.2, markersize=4)
    axes[1].set_ylabel("Coherence")
    axes[1].grid(True, alpha=0.3)

    axes[2].plot(xs, phase_std, "o-", color="tab:red", linewidth=1.2, markersize=4)
    axes[2].set_ylabel("Phase Std (rad)")
    axes[2].set_xlabel("Window Index")
    axes[2].grid(True, alpha=0.3)

    fig.tight_layout()
    save_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(save_path, dpi=150)
    plt.close(fig)


def plot_burst_samples(
    capture: np.ndarray,
    row: dict[str, Any],
    save_path: Path,
    direction: str,
) -> None:
    start = int(row["segment_start"])
    stop = int(row["segment_stop"])
    segment = capture[start:stop]
    if segment.size == 0:
        return

    xs = np.arange(segment.size, dtype=int)
    amp = np.abs(segment)
    angle_unwrapped = np.unwrap(np.angle(segment))

    fig, axes = plt.subplots(3, 1, figsize=(10, 9))

    axes[0].plot(xs, amp, linewidth=1.0)
    axes[0].set_ylabel("|x|")
    _set_robust_ylim(axes[0], amp, min_span=0.05)
    axes[0].grid(True, alpha=0.3)
    axes[0].set_title(
        (
            f"{direction} burst={row['burst_index']} "
            f"freq_index={row['freq_index']} repeat={row['repeat_index']} "
            f"len={row['segment_len']} "
            f"ok={bool(row.get('sequence_ok', True))} "
            f"assigned={bool(row.get('assigned_to_freq', False))} "
            f"flags={','.join(row.get('quality_flags', [])) or 'none'}"
        )
    )

    axes[1].plot(xs, angle_unwrapped, linewidth=1.0, color="tab:green")
    axes[1].set_xlabel("Sample Index")
    axes[1].set_ylabel("Unwrapped Angle (rad)")
    _set_robust_ylim(axes[1], angle_unwrapped, min_span=0.25)
    axes[1].grid(True, alpha=0.3)

    axes[2].plot(segment.real, segment.imag, ".", markersize=1.2, alpha=0.7, color="tab:purple")
    axes[2].set_xlabel("I")
    axes[2].set_ylabel("Q")
    axes[2].set_aspect("equal", adjustable="box")
    i_center = float(np.median(segment.real))
    q_center = float(np.median(segment.imag))
    i_low, i_high = np.percentile(segment.real, [1.0, 99.0])
    q_low, q_high = np.percentile(segment.imag, [1.0, 99.0])
    iq_span = max(float(i_high - i_low), float(q_high - q_low), 0.08)
    iq_half_span = 0.5 * iq_span * 1.2
    axes[2].set_xlim(i_center - iq_half_span, i_center + iq_half_span)
    axes[2].set_ylim(q_center - iq_half_span, q_center + iq_half_span)
    axes[2].grid(True, alpha=0.3)

    fig.tight_layout()
    save_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(save_path, dpi=140)
    plt.close(fig)


def _plot_burst_samples_task(task: tuple[np.ndarray, dict[str, Any], str, str]) -> None:
    segment, row, save_path_text, direction = task
    if segment.size == 0:
        return

    row_for_plot = dict(row)
    row_for_plot["segment_start"] = 0
    row_for_plot["segment_stop"] = int(segment.size)
    plot_burst_samples(segment, row_for_plot, Path(save_path_text), direction)


def plot_pair_phase_by_freq(
    reflector_rows: list[dict[str, Any]],
    initiator_rows: list[dict[str, Any]],
    save_path: Path,
) -> list[dict[str, Any]]:
    reflector_avg = average_rows_by_freq(reflector_rows)
    initiator_avg = average_rows_by_freq(initiator_rows)
    common_freq_indices = sorted(set(reflector_avg) & set(initiator_avg))
    if not common_freq_indices:
        return []

    freq_mhz: list[float] = []
    pair_phase: list[float] = []
    pair_abs: list[float] = []
    rows: list[dict[str, Any]] = []

    for freq_index in common_freq_indices:
        z_pair = initiator_avg[freq_index]["z"] * reflector_avg[freq_index]["z"]
        freq_hz = float(initiator_avg[freq_index]["freq_hz"])
        freq_mhz.append(freq_hz / 1e6)
        pair_phase.append(float(np.angle(z_pair)))
        pair_abs.append(float(abs(z_pair)))
        rows.append(
            {
                "freq_index": int(freq_index),
                "freq_hz": freq_hz,
                "pair_abs": float(abs(z_pair)),
                "pair_phase_rad": float(np.angle(z_pair)),
                "initiator_abs": float(initiator_avg[freq_index]["abs"]),
                "initiator_phase_rad": float(initiator_avg[freq_index]["phase"]),
                "reflector_abs": float(reflector_avg[freq_index]["abs"]),
                "reflector_phase_rad": float(reflector_avg[freq_index]["phase"]),
            }
        )

    fig, axes = plt.subplots(2, 1, figsize=(11, 7), sharex=True)

    axes[0].plot(freq_mhz, pair_phase, "o-", linewidth=1.2, markersize=4, color="tab:red")
    axes[0].set_ylabel("Pair Phase (rad)")
    axes[0].set_title("initiator * reflector phase by frequency")
    axes[0].grid(True, alpha=0.3)

    axes[1].plot(freq_mhz, pair_abs, "o-", linewidth=1.2, markersize=4, color="tab:blue")
    axes[1].set_xlabel("Frequency (MHz)")
    axes[1].set_ylabel("|initiator * reflector|")
    axes[1].grid(True, alpha=0.3)

    fig.tight_layout()
    save_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(save_path, dpi=150)
    plt.close(fig)
    return rows


def save_pair_phase_csv(rows: list[dict[str, Any]], save_path: Path) -> None:
    save_path.parent.mkdir(parents=True, exist_ok=True)
    with save_path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=PAIR_FREQ_CSV_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "freq_index": int(row["freq_index"]),
                    "freq_hz": float(row["freq_hz"]),
                    "pair_abs": float(row["pair_abs"]),
                    "pair_angle_rad": float(row["pair_phase_rad"]),
                    "pair_phase_rad": float(row["pair_phase_rad"]),
                    "initiator_abs": float(row["initiator_abs"]),
                    "initiator_angle_rad": float(row["initiator_phase_rad"]),
                    "reflector_abs": float(row["reflector_abs"]),
                    "reflector_angle_rad": float(row["reflector_phase_rad"]),
                }
            )


def save_pair_angle_csv(rows: list[dict[str, Any]], save_path: Path) -> None:
    save_path.parent.mkdir(parents=True, exist_ok=True)
    with save_path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=PAIR_ANGLE_CSV_FIELDS)
        writer.writeheader()
        for row in rows:
            phase = float(row["pair_phase_rad"])
            writer.writerow(
                {
                    "freq_index": int(row["freq_index"]),
                    "freq_hz": float(row["freq_hz"]),
                    "pair_angle_rad": phase,
                    "pair_phase_rad": phase,
                }
            )


def load_pair_phase_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"pair csv not found: {path}")

    rows: list[dict[str, Any]] = []
    with path.open("r", newline="", encoding="utf-8") as fp:
        reader = csv.DictReader(fp)
        for csv_row in reader:
            if not csv_row:
                continue
            angle_text = (
                csv_row.get("pair_angle_rad")
                or csv_row.get("pair_phase_rad")
                or csv_row.get("phase_wrapped")
                or csv_row.get("angle_rad")
            )
            if angle_text is None:
                raise ValueError(f"missing pair angle column in {path}")
            rows.append(
                {
                    "freq_index": int(csv_row["freq_index"]),
                    "freq_hz": float(csv_row["freq_hz"]),
                    "pair_abs": float(csv_row.get("pair_abs") or 0.0),
                    "pair_phase_rad": float(angle_text),
                    "initiator_abs": float(csv_row.get("initiator_abs") or 0.0),
                    "initiator_phase_rad": float(csv_row.get("initiator_angle_rad") or 0.0),
                    "reflector_abs": float(csv_row.get("reflector_abs") or 0.0),
                    "reflector_phase_rad": float(csv_row.get("reflector_angle_rad") or 0.0),
                }
            )
    rows.sort(key=lambda item: int(item["freq_index"]))
    return rows


def build_summary(direction: str, path: Path, rows: list[dict[str, Any]], raw_samples: int, expected_bursts: int) -> dict[str, Any]:
    if rows:
        mean_abs = [float(row["segment_mean_abs"]) for row in rows]
        coherence = [float(row["segment_coherence"]) for row in rows]
        phase_std = [float(row["segment_phase_std"]) for row in rows]
        trimmed = [1.0 for row in rows if int(row["core_offset_start"]) > 0]
        invalid = [1.0 for row in rows if not bool(row.get("sequence_ok", True))]
        assigned = [1.0 for row in rows if bool(row.get("assigned_to_freq", False))]
    else:
        mean_abs = []
        coherence = []
        phase_std = []
        trimmed = []
        invalid = []
        assigned = []

    return {
        "direction": direction,
        "path": str(path),
        "raw_samples": int(raw_samples),
        "expected_burst_count": int(expected_bursts),
        "burst_count": int(len(rows)),
        "mean_segment_abs": float(np.mean(mean_abs)) if mean_abs else 0.0,
        "mean_segment_coherence": float(np.mean(coherence)) if coherence else 0.0,
        "mean_segment_phase_std": float(np.mean(phase_std)) if phase_std else 0.0,
        "trimmed_burst_count": int(len(trimmed)),
        "invalid_burst_count": int(len(invalid)),
        "assigned_burst_count": int(len(assigned)),
    }


def analyze_one_capture(
    direction: str,
    path: Path,
    args: argparse.Namespace,
) -> dict[str, Any]:
    capture = load_gr_complex_bin(path)
    expected_bursts = expected_burst_count(
        args.start_offset_hz,
        args.stop_offset_hz,
        args.step_hz,
        args.repeats,
    )
    rows = detect_capture_bursts(
        capture,
        repeats=args.repeats,
        start_offset_hz=args.start_offset_hz,
        step_hz=args.step_hz,
        center_freq_hz=args.center_freq_hz,
        smooth_len=args.smooth_len,
        threshold_ratio=args.threshold_ratio,
        gap_tolerance=args.gap_tolerance,
        min_segment_len=args.min_segment_len,
        expected_bursts=expected_bursts,
        edge_trim_samples=args.edge_trim_samples,
        jobs=args.jobs,
    )

    result = {
        "summary": build_summary(direction, path, rows, raw_samples=int(capture.size), expected_bursts=expected_bursts),
        "rows": rows,
    }

    if args.save_plot_dir is not None:
        save_path = args.save_plot_dir.resolve() / f"{direction}_capture_summary.png"
        plot_capture(rows, save_path, f"{direction} continuous capture summary")
        result["plot_path"] = str(save_path)
        if not args.no_burst_plots:
            burst_plot_dir = args.save_plot_dir.resolve() / direction / "bursts"
            plot_tasks: list[tuple[np.ndarray, dict[str, Any], str, str]] = []
            for row in rows:
                burst_name = (
                    f"burst_{int(row['burst_index']):03d}"
                    f"_f{int(row['freq_index']):02d}"
                    f"_r{int(row['repeat_index'])}.png"
                )
                start = int(row["segment_start"])
                stop = int(row["segment_stop"])
                plot_tasks.append((capture[start:stop], row, str(burst_plot_dir / burst_name), direction))
            worker_count = max(1, int(args.jobs))
            if worker_count > 1 and len(plot_tasks) > 1:
                worker_count = min(worker_count, len(plot_tasks))
                chunksize = max(1, len(plot_tasks) // (worker_count * 4))
                with ProcessPoolExecutor(max_workers=worker_count) as executor:
                    list(executor.map(_plot_burst_samples_task, plot_tasks, chunksize=chunksize))
            else:
                for task in plot_tasks:
                    _plot_burst_samples_task(task)
            result["burst_plot_dir"] = str(burst_plot_dir)

    return result


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze continuous file-sink captures")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH, help="JSON 配置文件路径；默认读取 continuous_capture_config.json")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT, help="实验目录名或路径，例如 1to1 / 1to1_2sides")
    parser.add_argument("--reflector-file", type=Path, default=None)
    parser.add_argument("--initiator-file", type=Path, default=None)
    parser.add_argument("--center-freq-hz", type=float, default=2.44e9)
    parser.add_argument("--start-offset-hz", type=float, default=-40e6)
    parser.add_argument("--stop-offset-hz", type=float, default=40e6)
    parser.add_argument("--step-hz", type=float, default=1e6)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--sample-rate", type=float, default=1e6)
    parser.add_argument("--smooth-len", type=int, default=64)
    parser.add_argument("--threshold-ratio", type=float, default=0.005)
    parser.add_argument("--gap-tolerance", type=int, default=48)
    parser.add_argument("--min-segment-len", type=int, default=64)
    parser.add_argument("--edge-trim-samples", type=int, default=16, help="在稳定 core 基础上额外丢弃每个 burst 头尾各 N 个样本")
    parser.add_argument("--jobs", type=int, default=0, help="并行分析 worker 数；0 表示自动使用 CPU 核数")
    parser.add_argument("--no-burst-plots", action="store_true", help="不保存每个 burst 的单独 PNG，只保存总览图和 CSV/JSON")
    parser.add_argument("--save-json", type=Path, default=None)
    parser.add_argument("--save-plot-dir", type=Path, default=None)
    parser.add_argument("--save-pair-csv", type=Path, default=None)
    parser.add_argument("--save-pair-angle-csv", type=Path, default=None)
    parser.add_argument("--capture-group", default="all", help="要分析的采集组 label；默认 all。配置文件里当前默认有 calibration 和 measurement")
    parser.add_argument("--no-distance-estimates", action="store_true", help="只做 burst 分析，不生成两种距离估计结果")
    parser.add_argument("--distance-min-m", type=float, default=0.0, help="兼容旧配置；当前 phase-match 改为围绕斜率距离局部搜索")
    parser.add_argument("--distance-max-m", type=float, default=20.0, help="兼容旧配置；当前 phase-match 改为围绕斜率距离局部搜索")
    parser.add_argument("--distance-step-m", type=float, default=0.01, help="phase-match 距离搜索步进")
    parser.add_argument("--match-window-m", type=float, default=10.0, help="phase-match 围绕线性斜率距离的搜索半窗口")
    parser.add_argument("--propagation-speed-mps", type=float, default=2.3e8, help="传播速度，默认 2.3e8 m/s（铜质有线测量）")
    parser.add_argument("--unwrap-upward-tolerance-rad", type=float, default=0.8, help="线性拟合 unwrap 时允许相邻频点小幅上升的容差")
    return parser


def add_distance_estimates(
    result: dict[str, Any],
    args: argparse.Namespace,
    pair_phase_rows: list[dict[str, Any]],
    initiator_rows: list[dict[str, Any]],
    reflector_rows: list[dict[str, Any]],
    pair_phase_rows_for_plot: list[dict[str, Any]] | None = None,
) -> None:
    """Run both distance estimators from already detected pair rows."""
    from estimate_distance_continuous import (
        estimate_distance_from_pair_rows,
        save_estimate_plot,
        save_side_phase_plot,
        unwrap_with_negative_slope_prior,
    )
    from estimate_distance_continuous_phase_match import (
        estimate_distance_phase_match_from_pair_rows,
        save_phase_match_plot,
        wrap_to_pi,
    )

    estimate_args = argparse.Namespace(**vars(args))
    estimate_args.pair_csv = None
    plot_dir = args.save_plot_dir.resolve()
    plot_pair_rows = pair_phase_rows if pair_phase_rows_for_plot is None else pair_phase_rows_for_plot

    estimates: dict[str, Any] = {}

    try:
        linear_result = estimate_distance_from_pair_rows(pair_phase_rows, estimate_args, source="analyze")
        if plot_pair_rows:
            rows_for_plot = sorted(plot_pair_rows, key=lambda item: int(item["freq_index"]))
            fit_freq_indices = {
                int(row["freq_index"])
                for row in linear_result.get("rows", [])
                if isinstance(row, dict) and "freq_index" in row
            }
            freqs_hz_np = np.array([float(row["freq_hz"]) for row in rows_for_plot], dtype=float)
            wrapped_phase_np = np.array([float(row["pair_phase_rad"]) for row in rows_for_plot], dtype=float)
            tolerance = float(linear_result.get("unwrap_upward_tolerance_rad", getattr(args, "unwrap_upward_tolerance_rad", 0.8)))
            unwrapped_phase_np = unwrap_with_negative_slope_prior(
                wrapped_phase_np,
                upward_tolerance_rad=tolerance,
            )
            fitted_phase_np = float(linear_result["slope_rad_per_hz"]) * freqs_hz_np + float(linear_result["intercept_rad"])
            residual_phase_np = unwrapped_phase_np - fitted_phase_np
            linear_result["plot_rows"] = [
                {
                    "freq_index": int(row["freq_index"]),
                    "freq_hz": float(row["freq_hz"]),
                    "phase_wrapped": float(wrapped_phase),
                    "phase_unwrapped": float(unwrapped_phase),
                    "phase_residual": float(phase_residual),
                    "used_for_fit": int(row["freq_index"]) in fit_freq_indices,
                }
                for row, wrapped_phase, unwrapped_phase, phase_residual in zip(
                    rows_for_plot,
                    wrapped_phase_np,
                    unwrapped_phase_np,
                    residual_phase_np,
                )
            ]
        linear_plot_path = plot_dir / "distance_linear_fit.png"
        save_estimate_plot(linear_result, linear_plot_path)
        linear_json_path = plot_dir / "distance_linear_fit.json"
        linear_json_path.write_text(json.dumps(linear_result, indent=2, ensure_ascii=False), encoding="utf-8")

        initiator_phase_plot = plot_dir / "initiator_phase_by_freq.png"
        reflector_phase_plot = plot_dir / "reflector_phase_by_freq.png"
        save_side_phase_plot("initiator", average_rows_by_freq(initiator_rows), initiator_phase_plot)
        save_side_phase_plot("reflector", average_rows_by_freq(reflector_rows), reflector_phase_plot)

        linear_result["plot_path"] = str(linear_plot_path)
        linear_result["json_path"] = str(linear_json_path)
        linear_result["initiator_phase_plot_path"] = str(initiator_phase_plot)
        linear_result["reflector_phase_plot_path"] = str(reflector_phase_plot)
        estimates["linear_fit"] = linear_result
        print(f"linear_distance_m: {linear_result['distance_m']}")
        print(f"saved_linear_distance_plot: {linear_plot_path}")
        print(f"saved_linear_distance_json: {linear_json_path}")
    except SystemExit as exc:
        estimates["linear_fit"] = {"error": str(exc)}
        print(f"linear_distance_error: {exc}")

    try:
        phase_match_result = estimate_distance_phase_match_from_pair_rows(pair_phase_rows, estimate_args, source="analyze")
        if plot_pair_rows:
            rows_for_plot = sorted(plot_pair_rows, key=lambda item: int(item["freq_index"]))
            fit_freq_indices = {
                int(row["freq_index"])
                for row in phase_match_result.get("rows", [])
                if isinstance(row, dict) and "freq_index" in row
            }
            propagation_speed_mps = float(
                phase_match_result.get("propagation_speed_mps", getattr(args, "propagation_speed_mps", 2.3e8))
            )
            best_distance_m = float(phase_match_result["distance_m"])
            phase0_rad = float(phase_match_result.get("phase0_rad", 0.0))
            phase_match_result["plot_rows"] = []
            for row in rows_for_plot:
                freq_hz = float(row["freq_hz"])
                measured_phase = float(row["pair_phase_rad"])
                model_phase = float(-4.0 * np.pi * freq_hz * best_distance_m / propagation_speed_mps + phase0_rad)
                fitted_phase = float(wrap_to_pi(model_phase))
                phase_error = float(wrap_to_pi(measured_phase - fitted_phase))
                phase_match_result["plot_rows"].append(
                    {
                        "freq_index": int(row["freq_index"]),
                        "freq_hz": float(freq_hz),
                        "phase_wrapped_measured": measured_phase,
                        "phase_wrapped_fit": fitted_phase,
                        "phase_wrapped_error": phase_error,
                        "used_for_fit": int(row["freq_index"]) in fit_freq_indices,
                    }
                )
        phase_match_plot_path = plot_dir / "distance_phase_match.png"
        save_phase_match_plot(phase_match_result, phase_match_plot_path)
        phase_match_json_path = plot_dir / "distance_phase_match.json"
        phase_match_json_path.write_text(json.dumps(phase_match_result, indent=2, ensure_ascii=False), encoding="utf-8")

        phase_match_result["plot_path"] = str(phase_match_plot_path)
        phase_match_result["json_path"] = str(phase_match_json_path)
        estimates["phase_match"] = phase_match_result
        print(f"phase_match_distance_m: {phase_match_result['distance_m']}")
        print(f"saved_phase_match_plot: {phase_match_plot_path}")
        print(f"saved_phase_match_json: {phase_match_json_path}")
    except SystemExit as exc:
        estimates["phase_match"] = {"error": str(exc)}
        print(f"phase_match_error: {exc}")

    result["distance_estimates"] = estimates


def print_distance_summary(result: dict[str, Any], *, disabled: bool) -> None:
    """在主输出尾部固定打印两种测距结果，避免被中间日志淹没。"""
    if disabled:
        print("distance_summary: disabled_by_no_distance_estimates")
        return

    estimates = result.get("distance_estimates")
    if not isinstance(estimates, dict):
        print("distance_summary: unavailable")
        return

    linear = estimates.get("linear_fit") if isinstance(estimates.get("linear_fit"), dict) else None
    phase_match = estimates.get("phase_match") if isinstance(estimates.get("phase_match"), dict) else None

    if linear is None:
        print("distance_linear_fit_m: unavailable")
    elif "distance_m" in linear:
        print(f"distance_linear_fit_m: {float(linear['distance_m'])}")
    else:
        print(f"distance_linear_fit_m: error ({linear.get('error', 'unknown')})")

    if phase_match is None:
        print("distance_phase_match_m: unavailable")
    elif "distance_m" in phase_match:
        print(f"distance_phase_match_m: {float(phase_match['distance_m'])}")
    else:
        print(f"distance_phase_match_m: error ({phase_match.get('error', 'unknown')})")


def _format_optional_distance(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{float(value):.6f}"
    except (TypeError, ValueError):
        return str(value)


def _format_estimate_distance(estimate: dict[str, Any] | None) -> str:
    if estimate is None:
        return "unavailable"
    if "distance_m" in estimate:
        return _format_optional_distance(estimate.get("distance_m"))
    return f"error ({estimate.get('error', 'unknown')})"


def _distance_summary_line(group: str, estimates: Any) -> str:
    linear_text = "unavailable"
    phase_match_text = "unavailable"
    if isinstance(estimates, dict):
        linear = estimates.get("linear_fit")
        phase_match = estimates.get("phase_match")
        linear_text = _format_estimate_distance(linear if isinstance(linear, dict) else None)
        phase_match_text = _format_estimate_distance(phase_match if isinstance(phase_match, dict) else None)
    return (
        "distance_summary_group: {group}, "
        "distance_linear_fit_m: {linear}, distance_phase_match_m: {phase_match}".format(
            group=group,
            linear=linear_text,
            phase_match=phase_match_text,
        )
    )


def _group_path_has_token(result: dict[str, Any], token: str) -> bool:
    initiator_name = Path(str(result.get("initiator_file", ""))).name.lower()
    reflector_name = Path(str(result.get("reflector_file", ""))).name.lower()
    return token in initiator_name or token in reflector_name


def _pick_group_result_for_pre_cancel(
    results: list[dict[str, Any]],
    *,
    role: str,
) -> dict[str, Any] | None:
    role_lower = role.lower()
    token = "_calibration" if role_lower == "calibration" else "_measurement"

    for item in results:
        label = str(item.get("capture_group", "")).lower()
        if label == role_lower:
            return item
    for item in results:
        if _group_path_has_token(item, token):
            return item
    return None


def _collect_valid_freq_repeat_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    valid_rows: list[dict[str, Any]] = []
    for row in rows:
        if not bool(row.get("sequence_ok", True)):
            continue
        if not bool(row.get("assigned_to_freq", False)):
            continue
        freq_index = int(row.get("freq_index", -1))
        repeat_index = int(row.get("repeat_index", -1))
        if freq_index < 0 or repeat_index < 0:
            continue
        if row.get("freq_hz") is None:
            continue
        valid_rows.append(row)
    valid_rows.sort(
        key=lambda item: (
            int(item.get("freq_index", -1)),
            int(item.get("repeat_index", -1)),
            int(item.get("burst_index", -1)),
        )
    )
    return valid_rows


def build_phase_canceled_rows(
    measurement_rows: list[dict[str, Any]],
    calibration_rows: list[dict[str, Any]],
    *,
    side_name: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    measurement_valid = _collect_valid_freq_repeat_rows(measurement_rows)
    calibration_valid = _collect_valid_freq_repeat_rows(calibration_rows)

    calibration_by_key: dict[tuple[int, int], dict[str, Any]] = {}
    for row in calibration_valid:
        key = (int(row["freq_index"]), int(row["repeat_index"]))
        calibration_by_key.setdefault(key, row)

    canceled_rows: list[dict[str, Any]] = []
    seen_keys: set[tuple[int, int]] = set()
    duplicate_measurement_rows = 0
    missing_reference_rows = 0

    for row in measurement_valid:
        key = (int(row["freq_index"]), int(row["repeat_index"]))
        if key in seen_keys:
            duplicate_measurement_rows += 1
            continue
        seen_keys.add(key)

        calibration_row = calibration_by_key.get(key)
        if calibration_row is None:
            missing_reference_rows += 1
            continue

        z_measure = complex(float(row["robust_mean_i"]), float(row["robust_mean_q"]))
        z_calibration = complex(float(calibration_row["robust_mean_i"]), float(calibration_row["robust_mean_q"]))
        z_canceled = z_measure * np.conj(z_calibration)
        canceled_rows.append(
            {
                "sequence_ok": True,
                "assigned_to_freq": True,
                "freq_index": int(key[0]),
                "repeat_index": int(key[1]),
                "freq_hz": float(row["freq_hz"]),
                "slot_kind": "phase_canceled",
                "quality_flags": [],
                "robust_mean_i": float(np.real(z_canceled)),
                "robust_mean_q": float(np.imag(z_canceled)),
                "robust_mean_abs": float(abs(z_canceled)),
                "robust_mean_phase": float(np.angle(z_canceled)),
                "measurement_burst_index": int(row.get("burst_index", -1)),
                "calibration_burst_index": int(calibration_row.get("burst_index", -1)),
            }
        )

    stats = {
        "side": side_name,
        "measurement_valid_rows": int(len(measurement_valid)),
        "calibration_valid_rows": int(len(calibration_valid)),
        "matched_rows": int(len(canceled_rows)),
        "missing_reference_rows": int(missing_reference_rows),
        "duplicate_measurement_rows": int(duplicate_measurement_rows),
    }
    return canceled_rows, stats


def _wrap_to_pi(values: np.ndarray) -> np.ndarray:
    return (np.asarray(values, dtype=float) + np.pi) % (2.0 * np.pi) - np.pi


def select_pre_cancel_front_segment(
    pair_phase_rows: list[dict[str, Any]],
    *,
    min_segment_points: int = 12,
    min_slope_diff_rad_per_bin: float = 0.25,
    min_relative_sse_gain: float = 0.18,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    total_points = int(len(pair_phase_rows))
    base_info: dict[str, Any] = {
        "use_front_only": False,
        "total_points": total_points,
        "selected_points": total_points,
        "split_row_index": None,
        "split_freq_mhz": None,
        "front_slope_rad_per_bin": None,
        "back_slope_rad_per_bin": None,
        "slope_diff_rad_per_bin": None,
        "relative_sse_gain": 0.0,
        "reason": "insufficient_points",
    }
    if total_points < max(2, 2 * int(min_segment_points)):
        return pair_phase_rows, base_info

    phases = np.array([float(row["pair_phase_rad"]) for row in pair_phase_rows], dtype=float)
    freq_indices = np.array([int(row["freq_index"]) for row in pair_phase_rows], dtype=int)
    phase_step = _wrap_to_pi(np.diff(phases))
    index_step = np.maximum(1.0, np.diff(freq_indices).astype(float))
    step_slope = phase_step / index_step
    if step_slope.size < 2 * int(min_segment_points):
        return pair_phase_rows, base_info

    global_mean = float(np.mean(step_slope))
    global_sse = float(np.sum((step_slope - global_mean) ** 2))
    best_split = -1
    best_sse = float("inf")
    best_front_mean = 0.0
    best_back_mean = 0.0
    min_steps = int(min_segment_points)
    for split in range(min_steps, int(step_slope.size) - min_steps + 1):
        front = step_slope[:split]
        back = step_slope[split:]
        front_mean = float(np.mean(front))
        back_mean = float(np.mean(back))
        sse = float(np.sum((front - front_mean) ** 2) + np.sum((back - back_mean) ** 2))
        if sse < best_sse:
            best_sse = sse
            best_split = int(split)
            best_front_mean = front_mean
            best_back_mean = back_mean

    if best_split < 0:
        return pair_phase_rows, base_info

    split_row_index = int(best_split)
    split_freq_mhz = float(pair_phase_rows[split_row_index]["freq_hz"]) / 1e6
    slope_diff = float(abs(best_front_mean - best_back_mean))
    if global_sse <= 1e-12:
        relative_gain = 0.0
    else:
        relative_gain = float((global_sse - best_sse) / global_sse)
    use_front_only = bool(
        slope_diff >= float(min_slope_diff_rad_per_bin)
        and relative_gain >= float(min_relative_sse_gain)
    )
    selected_rows = pair_phase_rows[: split_row_index + 1] if use_front_only else pair_phase_rows
    reason = "split_detected_front_only" if use_front_only else "single_slope_or_weak_split"
    info: dict[str, Any] = {
        "use_front_only": use_front_only,
        "total_points": total_points,
        "selected_points": int(len(selected_rows)),
        "split_row_index": split_row_index,
        "split_freq_mhz": split_freq_mhz,
        "front_slope_rad_per_bin": float(best_front_mean),
        "back_slope_rad_per_bin": float(best_back_mean),
        "slope_diff_rad_per_bin": slope_diff,
        "relative_sse_gain": relative_gain,
        "reason": reason,
    }
    return selected_rows, info


def run_pre_cancel_distance_analysis(
    *,
    args: argparse.Namespace,
    all_results: list[dict[str, Any]],
    base_plot_dir: Path,
) -> dict[str, Any] | None:
    calibration_result = _pick_group_result_for_pre_cancel(all_results, role="calibration")
    measurement_result = _pick_group_result_for_pre_cancel(all_results, role="measurement")
    if calibration_result is None or measurement_result is None:
        print("pre_cancel_distance: skipped (需要同时存在 calibration 与 measurement 采集组)")
        return None

    calibration_initiator_rows = calibration_result.get("initiator", {}).get("rows", [])
    calibration_reflector_rows = calibration_result.get("reflector", {}).get("rows", [])
    measurement_initiator_rows = measurement_result.get("initiator", {}).get("rows", [])
    measurement_reflector_rows = measurement_result.get("reflector", {}).get("rows", [])
    if (
        not isinstance(calibration_initiator_rows, list)
        or not isinstance(calibration_reflector_rows, list)
        or not isinstance(measurement_initiator_rows, list)
        or not isinstance(measurement_reflector_rows, list)
    ):
        print("pre_cancel_distance: skipped (组内 rows 数据格式异常)")
        return None

    initiator_canceled_rows, initiator_canceled_stats = build_phase_canceled_rows(
        measurement_initiator_rows,
        calibration_initiator_rows,
        side_name="initiator",
    )
    reflector_canceled_rows, reflector_canceled_stats = build_phase_canceled_rows(
        measurement_reflector_rows,
        calibration_reflector_rows,
        side_name="reflector",
    )

    pre_cancel_plot_dir = (base_plot_dir / "measurement_minus_calibration_pre_cancel").resolve()
    pair_phase_plot_path = pre_cancel_plot_dir / "pair_phase_by_freq_pre_cancel.png"
    pair_phase_rows = plot_pair_phase_by_freq(
        reflector_canceled_rows,
        initiator_canceled_rows,
        pair_phase_plot_path,
    )
    pair_phase_csv_path = pre_cancel_plot_dir / "pair_phase_by_freq_pre_cancel.csv"
    pair_angle_csv_path = pre_cancel_plot_dir / "pair_angle_by_freq_pre_cancel.csv"
    save_pair_phase_csv(pair_phase_rows, pair_phase_csv_path)
    save_pair_angle_csv(pair_phase_rows, pair_angle_csv_path)
    pair_phase_rows_for_distance, pre_cancel_segment_info = select_pre_cancel_front_segment(pair_phase_rows)
    distance_pair_phase_csv_path = pre_cancel_plot_dir / "pair_phase_by_freq_pre_cancel_distance_input.csv"
    distance_pair_angle_csv_path = pre_cancel_plot_dir / "pair_angle_by_freq_pre_cancel_distance_input.csv"
    save_pair_phase_csv(pair_phase_rows_for_distance, distance_pair_phase_csv_path)
    save_pair_angle_csv(pair_phase_rows_for_distance, distance_pair_angle_csv_path)

    expected_bursts = expected_burst_count(
        args.start_offset_hz,
        args.stop_offset_hz,
        args.step_hz,
        args.repeats,
    )
    expected_freq_count = int(round(float(expected_bursts) / float(args.repeats))) if args.repeats > 0 else 0
    initiator_diag = build_side_freq_diagnostics(
        initiator_canceled_rows,
        expected_freq_count=expected_freq_count,
        expected_repeats=args.repeats,
    )
    reflector_diag = build_side_freq_diagnostics(
        reflector_canceled_rows,
        expected_freq_count=expected_freq_count,
        expected_repeats=args.repeats,
    )
    pair_diag = build_pair_freq_diagnostics(initiator_diag, reflector_diag, pair_phase_rows)

    pre_cancel_result: dict[str, Any] = {
        "root": str(measurement_result.get("root", resolve_root(args.root))),
        "capture_group": "measurement_minus_calibration_pre_cancel",
        "capture_distance_m": measurement_result.get("capture_distance_m"),
        "config_path": measurement_result.get("config_path"),
        "reflector_file": str(measurement_result.get("reflector_file", "")),
        "initiator_file": str(measurement_result.get("initiator_file", "")),
        "measurement_capture_group": str(measurement_result.get("capture_group", "measurement")),
        "calibration_capture_group": str(calibration_result.get("capture_group", "calibration")),
        "analysis_parameters": dict(measurement_result.get("analysis_parameters", {})),
        "expected_burst_count": int(expected_bursts),
        "pre_cancel_method": "measurement_phase - calibration_phase on each side, then pair & fit",
        "initiator": {
            "rows": initiator_canceled_rows,
            "summary": initiator_canceled_stats,
        },
        "reflector": {
            "rows": reflector_canceled_rows,
            "summary": reflector_canceled_stats,
        },
        "pair_phase_by_freq": pair_phase_rows,
        "pair_phase_plot_path": str(pair_phase_plot_path),
        "pair_phase_csv_path": str(pair_phase_csv_path),
        "pair_angle_csv_path": str(pair_angle_csv_path),
        "distance_input_pair_phase_csv_path": str(distance_pair_phase_csv_path),
        "distance_input_pair_angle_csv_path": str(distance_pair_angle_csv_path),
        "distance_input_pair_point_count": int(len(pair_phase_rows_for_distance)),
        "pre_cancel_segment_selection": pre_cancel_segment_info,
        "initiator_freq_diagnostics": initiator_diag,
        "reflector_freq_diagnostics": reflector_diag,
        "pair_freq_diagnostics": pair_diag,
    }

    print("pre_cancel_distance_mode: measurement_minus_calibration_before_fit")
    print(f"pre_cancel_measurement_group: {pre_cancel_result['measurement_capture_group']}")
    print(f"pre_cancel_calibration_group: {pre_cancel_result['calibration_capture_group']}")
    print(f"saved_pre_cancel_pair_phase_plot: {pair_phase_plot_path}")
    print(f"saved_pre_cancel_pair_phase_csv: {pair_phase_csv_path}")
    print(f"saved_pre_cancel_pair_angle_csv: {pair_angle_csv_path}")
    print(f"saved_pre_cancel_distance_input_pair_phase_csv: {distance_pair_phase_csv_path}")
    if bool(pre_cancel_segment_info.get("use_front_only", False)):
        print("pre_cancel_segment_selector: front_only")
        print(
            "pre_cancel_segment_points: {selected}/{total}, split_freq_mhz: {split_freq:.3f}, "
            "front_slope_rad_per_bin: {front_slope:.4f}, back_slope_rad_per_bin: {back_slope:.4f}".format(
                selected=int(pre_cancel_segment_info.get("selected_points", 0)),
                total=int(pre_cancel_segment_info.get("total_points", 0)),
                split_freq=float(pre_cancel_segment_info.get("split_freq_mhz") or 0.0),
                front_slope=float(pre_cancel_segment_info.get("front_slope_rad_per_bin") or 0.0),
                back_slope=float(pre_cancel_segment_info.get("back_slope_rad_per_bin") or 0.0),
            )
        )
    else:
        print("pre_cancel_segment_selector: full_range")

    if not args.no_distance_estimates:
        pre_cancel_args = argparse.Namespace(**vars(args))
        pre_cancel_args.save_plot_dir = pre_cancel_plot_dir
        add_distance_estimates(
            pre_cancel_result,
            pre_cancel_args,
            pair_phase_rows_for_distance,
            initiator_canceled_rows,
            reflector_canceled_rows,
            pair_phase_rows_for_plot=pair_phase_rows,
        )

    print_distance_summary(pre_cancel_result, disabled=bool(args.no_distance_estimates))
    return pre_cancel_result


def print_final_distance_summary(
    results: list[dict[str, Any]],
    *,
    disabled: bool,
    pre_cancel_result: dict[str, Any] | None = None,
) -> None:
    """在终端最后统一打印所有 capture_group 的测距结果摘要。"""
    print("final_distance_summary_begin")
    if disabled:
        print("final_distance_summary: disabled_by_no_distance_estimates")
        print("final_distance_summary_end")
        return
    if not results:
        print("final_distance_summary: unavailable")
        print("final_distance_summary_end")
        return

    for item in results:
        group = str(item.get("capture_group", "unknown"))
        print(_distance_summary_line(group, item.get("distance_estimates")))
    if pre_cancel_result is not None:
        print(
            _distance_summary_line(
                str(pre_cancel_result.get("capture_group", "measurement_minus_calibration_pre_cancel")),
                pre_cancel_result.get("distance_estimates"),
            )
        )
    print("final_distance_summary_end")


def run_capture_analysis(
    *,
    args: argparse.Namespace,
    root: Path,
    capture_group: str,
    capture_distance_m: Any,
    reflector_file: Path,
    initiator_file: Path,
    save_plot_dir: Path,
    save_pair_csv_path: Path | None,
    save_pair_angle_csv_path: Path | None,
    save_json: Path | None,
) -> dict[str, Any]:
    expected_bursts = expected_burst_count(
        args.start_offset_hz,
        args.stop_offset_hz,
        args.step_hz,
        args.repeats,
    )

    run_args = argparse.Namespace(**vars(args))
    run_args.reflector_file = reflector_file
    run_args.initiator_file = initiator_file
    run_args.save_plot_dir = save_plot_dir
    run_args.save_pair_csv = save_pair_csv_path
    run_args.save_pair_angle_csv = save_pair_angle_csv_path
    run_args.save_json = save_json

    result = {
        "root": str(root),
        "capture_group": str(capture_group),
        "capture_distance_m": capture_distance_m,
        "config_path": args.loaded_config,
        "reflector_file": str(reflector_file),
        "initiator_file": str(initiator_file),
        "analysis_parameters": {
            "center_freq_hz": float(args.center_freq_hz),
            "start_offset_hz": float(args.start_offset_hz),
            "stop_offset_hz": float(args.stop_offset_hz),
            "step_hz": float(args.step_hz),
            "repeats": int(args.repeats),
            "sample_rate": float(args.sample_rate),
            "smooth_len": int(args.smooth_len),
            "threshold_ratio": float(args.threshold_ratio),
            "gap_tolerance": int(args.gap_tolerance),
            "min_segment_len": int(args.min_segment_len),
            "edge_trim_samples": int(args.edge_trim_samples),
            "jobs": int(args.jobs),
            "no_burst_plots": bool(args.no_burst_plots),
        },
        "expected_burst_count": int(expected_bursts),
        "reflector": analyze_one_capture("reflector", reflector_file, run_args),
        "initiator": analyze_one_capture("initiator", initiator_file, run_args),
    }

    pair_phase_plot_path = run_args.save_plot_dir.resolve() / "pair_phase_by_freq.png"
    pair_phase_rows = plot_pair_phase_by_freq(
        result["reflector"]["rows"],
        result["initiator"]["rows"],
        pair_phase_plot_path,
    )
    pair_phase_csv_path = (
        run_args.save_pair_csv.resolve()
        if run_args.save_pair_csv is not None
        else run_args.save_plot_dir.resolve() / "pair_phase_by_freq.csv"
    )
    pair_angle_csv_path = (
        run_args.save_pair_angle_csv.resolve()
        if run_args.save_pair_angle_csv is not None
        else run_args.save_plot_dir.resolve() / "pair_angle_by_freq.csv"
    )
    save_pair_phase_csv(pair_phase_rows, pair_phase_csv_path)
    save_pair_angle_csv(pair_phase_rows, pair_angle_csv_path)
    expected_freq_count = int(round(float(expected_bursts) / float(args.repeats))) if args.repeats > 0 else 0
    initiator_diag = build_side_freq_diagnostics(
        result["initiator"]["rows"],
        expected_freq_count=expected_freq_count,
        expected_repeats=args.repeats,
    )
    reflector_diag = build_side_freq_diagnostics(
        result["reflector"]["rows"],
        expected_freq_count=expected_freq_count,
        expected_repeats=args.repeats,
    )
    pair_diag = build_pair_freq_diagnostics(initiator_diag, reflector_diag, pair_phase_rows)
    result["pair_phase_by_freq"] = pair_phase_rows
    result["pair_phase_plot_path"] = str(pair_phase_plot_path)
    result["pair_phase_csv_path"] = str(pair_phase_csv_path)
    result["pair_angle_csv_path"] = str(pair_angle_csv_path)
    result["initiator_freq_diagnostics"] = initiator_diag
    result["reflector_freq_diagnostics"] = reflector_diag
    result["pair_freq_diagnostics"] = pair_diag

    if not args.no_distance_estimates:
        add_distance_estimates(
            result,
            run_args,
            pair_phase_rows,
            result["initiator"]["rows"],
            result["reflector"]["rows"],
            pair_phase_rows_for_plot=pair_phase_rows,
        )

    print(f"capture_group: {capture_group}")
    print(f"capture_distance_m: {capture_distance_m}")
    print(f"reflector_file: {reflector_file}")
    print(f"initiator_file: {initiator_file}")
    for row in pair_diag:
        pair_row = row["pair_row"]
        if pair_row is None:
            print(
                f"pair_freq_index: {row['freq_index']}, state: {row['pair_state']}, reason: {row['pair_reason']}"
            )
            continue
        print(
            "pair_freq_index: {freq_index}, freq_mhz: {freq_mhz:.3f}, state: {state}, reason: {reason}, pair_phase_rad: {pair_phase_rad:.6f}, pair_abs: {pair_abs:.6f}".format(
                freq_index=int(row["freq_index"]),
                freq_mhz=float(pair_row["freq_hz"]) / 1e6,
                state=row["pair_state"],
                reason=row["pair_reason"],
                pair_phase_rad=float(pair_row["pair_phase_rad"]),
                pair_abs=float(pair_row["pair_abs"]),
            )
        )
    print(f"saved_plot_dir: {run_args.save_plot_dir.resolve()}")
    print(f"loaded_config: {args.loaded_config}")
    print(f"saved_pair_phase_plot: {pair_phase_plot_path}")
    print(f"saved_pair_phase_csv: {pair_phase_csv_path}")
    print(f"saved_pair_angle_csv: {pair_angle_csv_path}")
    print_distance_summary(result, disabled=bool(args.no_distance_estimates))

    if run_args.save_json is not None:
        out_path = run_args.save_json.resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"saved_json: {out_path}")

    return result


def main() -> None:
    parser = build_argument_parser()
    config_args, _ = parser.parse_known_args()
    config = load_config(config_args.config)
    apply_config_defaults(parser, config)
    args = parser.parse_args()
    args.config = config_args.config
    args.loaded_config = str((args.config if args.config.is_absolute() else (PROJECT_ROOT / args.config)).resolve()) if args.config is not None else None
    if int(args.jobs) <= 0:
        args.jobs = max(1, os.cpu_count() or 1)
    root = resolve_root(args.root)

    base_plot_dir = args.save_plot_dir.resolve() if args.save_plot_dir is not None else default_plot_dir(root)
    explicit_files = args.reflector_file is not None or args.initiator_file is not None

    run_specs: list[dict[str, Any]] = []
    if explicit_files:
        default_reflector_file, default_initiator_file = default_capture_paths(root)
        reflector_file = default_reflector_file if args.reflector_file is None else args.reflector_file.resolve()
        initiator_file = default_initiator_file if args.initiator_file is None else args.initiator_file.resolve()
        label = str(args.capture_group) if str(args.capture_group) != "all" else "custom"
        run_specs.append(
            {
                "label": label,
                "distance_m": None,
                "reflector_file": reflector_file,
                "initiator_file": initiator_file,
                "plot_dir": base_plot_dir,
                "save_pair_csv": args.save_pair_csv,
                "save_pair_angle_csv": args.save_pair_angle_csv,
                "save_json": args.save_json,
            }
        )
    else:
        groups = capture_group_specs(config)
        selected_label = str(args.capture_group)
        if selected_label != "all":
            groups = [group for group in groups if str(group["label"]) == selected_label]
            if not groups:
                raise SystemExit(f"找不到 capture_group: {selected_label}")
        single_config_group = selected_label != "all"

        for group in groups:
            label = str(group["label"])
            run_specs.append(
                {
                    "label": label,
                    "distance_m": group.get("distance_m"),
                    "reflector_file": resolve_group_file(root, group["reflector_file"]),
                    "initiator_file": resolve_group_file(root, group["initiator_file"]),
                    "plot_dir": base_plot_dir / label,
                    "save_pair_csv": args.save_pair_csv if single_config_group else None,
                    "save_pair_angle_csv": args.save_pair_angle_csv if single_config_group else None,
                    "save_json": args.save_json if single_config_group else None,
                }
            )

    all_results: list[dict[str, Any]] = []
    for spec in run_specs:
        all_results.append(
            run_capture_analysis(
                args=args,
                root=root,
                capture_group=spec["label"],
                capture_distance_m=spec["distance_m"],
                reflector_file=spec["reflector_file"],
                initiator_file=spec["initiator_file"],
                save_plot_dir=spec["plot_dir"],
                save_pair_csv_path=spec["save_pair_csv"],
                save_pair_angle_csv_path=spec["save_pair_angle_csv"],
                save_json=spec["save_json"],
            )
        )

    pre_cancel_result: dict[str, Any] | None = None
    if not explicit_files:
        pre_cancel_result = run_pre_cancel_distance_analysis(
            args=args,
            all_results=all_results,
            base_plot_dir=base_plot_dir,
        )

    if not explicit_files and args.save_json is not None and len(run_specs) > 1:
        out_path = args.save_json.resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        combined_result: dict[str, Any] = {"root": str(root), "results": all_results}
        if pre_cancel_result is not None:
            combined_result["pre_cancel_result"] = pre_cancel_result
        out_path.write_text(json.dumps(combined_result, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"saved_json: {out_path}")

    print_final_distance_summary(
        all_results,
        disabled=bool(args.no_distance_estimates),
        pre_cancel_result=pre_cancel_result,
    )


if __name__ == "__main__":
    main()
