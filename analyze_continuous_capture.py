#!/usr/bin/env python3
"""Analyze continuous file-sink captures and recover burst quality."""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
import csv
import json
import os
import sys
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
DEFAULT_ROOT = PROJECT_ROOT / "1to1_rfhop"
DEFAULT_PLOT_ROOT = PROJECT_ROOT / "output_analyze_continuous"
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "continuous_capture_config.json"
DEFAULT_CAPTURE_GROUPS = [
    {
        "label": "2m",
        "distance_m": 2.0,
        "reflector_file": "data_reflector_rx_from_initiator_2m",
        "initiator_file": "data_initiator_rx_from_reflector_2m",
    },
    {
        "label": "4m",
        "distance_m": 4.0,
        "reflector_file": "data_reflector_rx_from_initiator_4m",
        "initiator_file": "data_initiator_rx_from_reflector_4m",
    },
]


def progress_enabled(args: argparse.Namespace | None = None) -> bool:
    return not bool(getattr(args, "no_progress", False))


def progress_log(message: str, args: argparse.Namespace | None = None) -> None:
    if progress_enabled(args):
        print(f"[progress] {message}", file=sys.stderr, flush=True)


def progress_interval(total: int) -> int:
    if total <= 20:
        return 1
    return max(1, total // 20)
PAIR_FREQ_CSV_FIELDS = [
    "freq_index",
    "freq_hz",
    "pair_abs",
    "pair_angle_rad",
    "pair_phase_rad",
    "pair_repeat_count",
    "pair_repeat_phase_spread_rad",
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
            resolved_root / "data_reflector_rx_from_initiator_2m",
            resolved_root / "data_reflector_rx_from_initiator2",
            resolved_root / "continuous_capture" / "data_reflector_rx_from_initiator.bin",
            resolved_root / "data_reflector_rx_from_initiator.bin",
        ]
    )
    initiator = _pick_existing_path(
        [
            resolved_root / "data_initiator_rx_from_reflector_2m",
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


def _root_config_key(root: Any) -> str:
    root_path = Path(str(root))
    return root_path.name if root_path.name else str(root)


def _cli_root_was_provided() -> bool:
    return any(arg == "--root" or arg.startswith("--root=") for arg in sys.argv[1:])


def select_config_for_root(
    config: dict[str, Any],
    requested_root: Any,
    *,
    root_was_provided: bool = True,
) -> tuple[dict[str, Any], str | None]:
    """Return defaults for the requested root.

    New format:
      {
        "defaults": {...},
        "roots": {
          "1to1": {...},
          "1to1_2sides": {...}
        }
      }

    The old single-root format is returned unchanged.
    """
    profiles = config.get("roots") or config.get("profiles")
    if not isinstance(profiles, dict):
        return config, None

    root_key = _root_config_key(requested_root)
    if not root_was_provided:
        root_key = str(config.get("default_root", root_key))

    profile = profiles.get(root_key)
    if profile is None:
        available = ", ".join(sorted(str(key) for key in profiles))
        raise SystemExit(f"配置文件里找不到 root={root_key!r}；可用 root: {available}")
    if not isinstance(profile, dict):
        raise SystemExit(f"root={root_key!r} 的配置必须是 JSON object")

    selected: dict[str, Any] = {}
    defaults = config.get("defaults", {})
    if isinstance(defaults, dict):
        selected.update(defaults)
    selected.update(profile)
    selected.setdefault("root", root_key)
    return selected, root_key


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
    cfo_compensate: bool = False,
    sample_rate: float = 1.0,
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
    cfo_hz = 0.0
    cfo_slope_rad_per_sample = 0.0
    segment_for_stats = segment
    if cfo_compensate and segment.size >= 2:
        segment_for_stats, cfo_hz, cfo_slope_rad_per_sample = compensate_cfo_slope_only(segment, sample_rate)
    cluster = phase_cluster_stats(segment_for_stats)
    classification = classify_signal(segment_for_stats)
    z_mean, robust_samples, outlier_samples = robust_complex_mean(segment_for_stats)
    return {
        "selection": selection,
        "cfo_compensated": bool(cfo_compensate),
        "cfo_hz": float(cfo_hz),
        "cfo_slope_rad_per_sample": float(cfo_slope_rad_per_sample),
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
        "segment_mean_abs": float(np.mean(np.abs(segment_for_stats))) if segment_for_stats.size else 0.0,
        "segment_max_abs": float(np.max(np.abs(segment_for_stats))) if segment_for_stats.size else 0.0,
        "robust_samples": int(robust_samples),
        "outlier_samples": int(outlier_samples),
        "robust_mean_i": float(np.real(z_mean)),
        "robust_mean_q": float(np.imag(z_mean)),
        "robust_mean_abs": float(abs(z_mean)),
        "robust_mean_phase": float(np.angle(z_mean)) if abs(z_mean) > 0 else 0.0,
    }


def compensate_cfo_slope_only(segment: np.ndarray, sample_rate: float) -> tuple[np.ndarray, float, float]:
    """Estimate per-burst CFO and remove only the phase slope.

    The constant phase term is intentionally preserved because downstream distance
    estimation needs the burst's mean phase.
    """
    if segment.size < 2:
        return segment, 0.0, 0.0
    n = np.arange(segment.size, dtype=np.float64)
    phase = np.unwrap(np.angle(segment))
    slope, _intercept = np.polyfit(n, phase, 1)
    corrected = segment * np.exp(-1j * float(slope) * n)
    cfo_hz = float(slope) * float(sample_rate) / (2.0 * np.pi)
    return corrected, float(cfo_hz), float(slope)


def _summarize_candidate_task(task: tuple[int, int, int, np.ndarray, int, int, bool, float]) -> dict[str, Any]:
    candidate_index, start, stop, raw_segment, min_segment_len, edge_trim_samples, cfo_compensate, sample_rate = task
    summary = summarize_segment(
        raw_segment,
        min_segment_len=min_segment_len,
        edge_trim_samples=edge_trim_samples,
        cfo_compensate=cfo_compensate,
        sample_rate=sample_rate,
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
    direction: str = "capture",
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
    progress: bool = True,
    cfo_compensate: bool = False,
    sample_rate: float = 1.0,
    assignment_mode: str = "sequential",
) -> list[dict[str, Any]]:
    if progress:
        print(f"[progress] {direction}: computing amplitude envelope ({capture.size} samples)", file=sys.stderr, flush=True)
    amp = np.abs(capture)
    amp_smooth = moving_average(amp, smooth_len)
    noise_floor = float(np.percentile(amp_smooth, 20)) if amp_smooth.size else 0.0
    signal_level = float(np.percentile(amp_smooth, 99.5)) if amp_smooth.size else 0.0
    threshold = noise_floor + float(threshold_ratio) * max(0.0, signal_level - noise_floor)
    mask = _merge_short_gaps(amp_smooth >= threshold, gap_tolerance)

    tasks: list[tuple[int, int, int, np.ndarray, int, int, bool, float]] = []
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
                bool(cfo_compensate),
                float(sample_rate),
            )
        )

    if progress:
        print(
            f"[progress] {direction}: found {len(tasks)} candidate bursts, expected {expected_bursts}",
            file=sys.stderr,
            flush=True,
        )
    worker_count = max(1, int(jobs))
    if worker_count > 1 and len(tasks) > 1:
        worker_count = min(worker_count, len(tasks))
        candidates = []
        with ProcessPoolExecutor(max_workers=worker_count) as executor:
            futures = [executor.submit(_summarize_candidate_task, task) for task in tasks]
            interval = progress_interval(len(futures))
            for completed, future in enumerate(as_completed(futures), start=1):
                candidates.append(future.result())
                if progress and (completed == len(futures) or completed % interval == 0):
                    print(
                        f"[progress] {direction}: summarized {completed}/{len(futures)} candidate bursts",
                        file=sys.stderr,
                        flush=True,
                    )
    else:
        candidates = []
        interval = progress_interval(len(tasks))
        for completed, task in enumerate(tasks, start=1):
            candidates.append(_summarize_candidate_task(task))
            if progress and (completed == len(tasks) or completed % interval == 0):
                print(
                    f"[progress] {direction}: summarized {completed}/{len(tasks)} candidate bursts",
                    file=sys.stderr,
                    flush=True,
                )

    if progress:
        print(f"[progress] {direction}: filtering and assigning burst sequence", file=sys.stderr, flush=True)
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
        assignment_mode=assignment_mode,
    )
    for burst_index, row in enumerate(rows):
        row["burst_index"] = int(burst_index)
        row["amp_threshold"] = float(threshold)
        row["amp_noise_floor"] = float(noise_floor)
        row["amp_signal_level"] = float(signal_level)
    if progress:
        print(f"[progress] {direction}: assigned {len(rows)} bursts", file=sys.stderr, flush=True)
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


def _partition_repeat_groups_sequential(rows: list[dict[str, Any]], repeats: int) -> list[list[dict[str, Any]]]:
    group_size = max(1, int(repeats))
    return [rows[idx : idx + group_size] for idx in range(0, len(rows), group_size)]


def assign_freq_groups(
    candidates: list[dict[str, Any]],
    *,
    expected_freq_count: int,
    repeats: int,
    center_freq_hz: float,
    start_offset_hz: float,
    step_hz: float,
    assignment_mode: str = "sequential",
) -> list[dict[str, Any]]:
    rows = sorted((dict(row) for row in candidates), key=lambda item: int(item["segment_start"]))
    for row in rows:
        row["assigned_to_freq"] = False
        row["freq_index"] = -1
        row["repeat_index"] = -1
        row["freq_hz"] = None
        row["slot_kind"] = "unassigned"
        row["assignment_mode"] = assignment_mode

    if expected_freq_count <= 0 or not rows:
        return rows

    if repeats > 1:
        if assignment_mode == "cluster":
            groups = _partition_repeat_groups(rows, repeats)
        else:
            groups = _partition_repeat_groups_sequential(rows, repeats)
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
    grouped = group_valid_rows_by_freq(rows)

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


def group_valid_rows_by_freq(rows: list[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    grouped: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        if not bool(row.get("sequence_ok", True)):
            continue
        if not bool(row.get("assigned_to_freq", False)):
            continue
        grouped.setdefault(int(row["freq_index"]), []).append(row)
    for freq_rows in grouped.values():
        freq_rows.sort(key=lambda item: int(item.get("repeat_index", 0)))
    return grouped


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
    plot_segment = segment
    if bool(row.get("cfo_compensated", False)):
        slope = float(row.get("cfo_slope_rad_per_sample", 0.0))
        n = np.arange(segment.size, dtype=np.float64)
        plot_segment = segment * np.exp(-1j * slope * n)

    xs = np.arange(plot_segment.size, dtype=int)
    amp = np.abs(plot_segment)
    angle_unwrapped = np.unwrap(np.angle(plot_segment))

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
            f"flags={','.join(row.get('quality_flags', [])) or 'none'} "
            f"cfo={'on' if bool(row.get('cfo_compensated', False)) else 'off'}"
            f"({float(row.get('cfo_hz', 0.0)):.3f} Hz)"
        )
    )

    axes[1].plot(xs, angle_unwrapped, linewidth=1.0, color="tab:green")
    axes[1].set_xlabel("Sample Index")
    axes[1].set_ylabel("Unwrapped Angle (rad)")
    _set_robust_ylim(axes[1], angle_unwrapped, min_span=0.25)
    axes[1].grid(True, alpha=0.3)

    axes[2].plot(plot_segment.real, plot_segment.imag, ".", markersize=1.2, alpha=0.7, color="tab:purple")
    axes[2].set_xlabel("I")
    axes[2].set_ylabel("Q")
    axes[2].set_aspect("equal", adjustable="box")
    i_center = float(np.median(plot_segment.real))
    q_center = float(np.median(plot_segment.imag))
    i_low, i_high = np.percentile(plot_segment.real, [1.0, 99.0])
    q_low, q_high = np.percentile(plot_segment.imag, [1.0, 99.0])
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
    reflector_by_freq = group_valid_rows_by_freq(reflector_rows)
    initiator_by_freq = group_valid_rows_by_freq(initiator_rows)
    common_freq_indices = sorted(set(reflector_by_freq) & set(initiator_by_freq))
    if not common_freq_indices:
        return []

    freq_mhz: list[float] = []
    pair_phase: list[float] = []
    pair_abs: list[float] = []
    rows: list[dict[str, Any]] = []

    for freq_index in common_freq_indices:
        initiator_by_repeat = {int(row.get("repeat_index", 0)): row for row in initiator_by_freq[freq_index]}
        reflector_by_repeat = {int(row.get("repeat_index", 0)): row for row in reflector_by_freq[freq_index]}
        common_repeats = sorted(set(initiator_by_repeat) & set(reflector_by_repeat))
        if not common_repeats:
            continue
        pair_values = []
        for repeat_index in common_repeats:
            initiator_row = initiator_by_repeat[repeat_index]
            reflector_row = reflector_by_repeat[repeat_index]
            z_initiator = complex(float(initiator_row["robust_mean_i"]), float(initiator_row["robust_mean_q"]))
            z_reflector = complex(float(reflector_row["robust_mean_i"]), float(reflector_row["robust_mean_q"]))
            pair_values.append(z_initiator * z_reflector)
        pair_array = np.array(pair_values, dtype=np.complex128)
        z_pair = complex(np.mean(pair_array))
        pair_repeat_phases = [float(np.angle(value)) for value in pair_values]
        pair_repeat_phase_spread = circular_phase_spread_rad(pair_repeat_phases) if len(pair_repeat_phases) > 1 else 0.0
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
                "pair_repeat_count": int(len(common_repeats)),
                "pair_repeat_phase_spread_rad": float(pair_repeat_phase_spread),
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
                    "pair_repeat_count": int(row.get("pair_repeat_count", 0)),
                    "pair_repeat_phase_spread_rad": float(row.get("pair_repeat_phase_spread_rad", 0.0)),
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
        cfo_values = [float(row.get("cfo_hz", 0.0)) for row in rows if bool(row.get("cfo_compensated", False))]
        trimmed = [1.0 for row in rows if int(row["core_offset_start"]) > 0]
        invalid = [1.0 for row in rows if not bool(row.get("sequence_ok", True))]
        assigned = [1.0 for row in rows if bool(row.get("assigned_to_freq", False))]
    else:
        mean_abs = []
        coherence = []
        phase_std = []
        cfo_values = []
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
        "mean_cfo_hz": float(np.mean(cfo_values)) if cfo_values else 0.0,
        "std_cfo_hz": float(np.std(cfo_values)) if cfo_values else 0.0,
        "trimmed_burst_count": int(len(trimmed)),
        "invalid_burst_count": int(len(invalid)),
        "assigned_burst_count": int(len(assigned)),
    }


def analyze_one_capture(
    direction: str,
    path: Path,
    args: argparse.Namespace,
) -> dict[str, Any]:
    progress_log(f"{direction}: loading {path}", args)
    capture = load_gr_complex_bin(path)
    progress_log(
        f"{direction}: loaded {capture.size} complex64 samples ({path.stat().st_size / (1024 ** 3):.2f} GiB)",
        args,
    )
    expected_bursts = expected_burst_count(
        args.start_offset_hz,
        args.stop_offset_hz,
        args.step_hz,
        args.repeats,
    )
    rows = detect_capture_bursts(
        capture,
        direction=direction,
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
        progress=progress_enabled(args),
        cfo_compensate=args.cfo_compensate,
        sample_rate=args.sample_rate,
        assignment_mode=args.assignment_mode,
    )

    progress_log(f"{direction}: building summary", args)
    result = {
        "summary": build_summary(direction, path, rows, raw_samples=int(capture.size), expected_bursts=expected_bursts),
        "rows": rows,
    }

    if args.save_plot_dir is not None:
        save_path = args.save_plot_dir.resolve() / f"{direction}_capture_summary.png"
        progress_log(f"{direction}: saving capture summary plot", args)
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
            progress_log(f"{direction}: saving {len(plot_tasks)} burst plots", args)
            if worker_count > 1 and len(plot_tasks) > 1:
                worker_count = min(worker_count, len(plot_tasks))
                with ProcessPoolExecutor(max_workers=worker_count) as executor:
                    futures = [executor.submit(_plot_burst_samples_task, task) for task in plot_tasks]
                    interval = progress_interval(len(futures))
                    for completed, future in enumerate(as_completed(futures), start=1):
                        future.result()
                        if progress_enabled(args) and (completed == len(futures) or completed % interval == 0):
                            progress_log(f"{direction}: saved {completed}/{len(futures)} burst plots", args)
            else:
                interval = progress_interval(len(plot_tasks))
                for completed, task in enumerate(plot_tasks, start=1):
                    _plot_burst_samples_task(task)
                    if progress_enabled(args) and (completed == len(plot_tasks) or completed % interval == 0):
                        progress_log(f"{direction}: saved {completed}/{len(plot_tasks)} burst plots", args)
            result["burst_plot_dir"] = str(burst_plot_dir)

    progress_log(f"{direction}: done", args)
    return result


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze continuous file-sink captures")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH, help="JSON 配置文件路径；默认读取 continuous_capture_config.json")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
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
    parser.add_argument(
        "--assignment-mode",
        choices=("sequential", "cluster"),
        default="sequential",
        help="burst 到频点/repeat 的归属方式；sequential 按时间顺序每 repeats 个一组，cluster 使用旧的相位/幅度聚类",
    )
    parser.add_argument(
        "--cfo-compensate",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="对每个 burst 单独估计一阶 CFO 并只去掉相位斜率，保留平均相位",
    )
    parser.add_argument("--jobs", type=int, default=0, help="并行分析 worker 数；0 表示自动使用 CPU 核数")
    parser.add_argument("--no-burst-plots", action="store_true", help="不保存每个 burst 的单独 PNG，只保存总览图和 CSV/JSON")
    parser.add_argument("--no-progress", action="store_true", help="关闭进度日志")
    parser.add_argument("--save-json", type=Path, default=None)
    parser.add_argument("--save-plot-dir", type=Path, default=None)
    parser.add_argument("--save-pair-csv", type=Path, default=None)
    parser.add_argument("--save-pair-angle-csv", type=Path, default=None)
    parser.add_argument("--capture-group", default="all", help="要分析的采集组 label；默认 all。组名从配置文件 capture_groups 读取")
    parser.add_argument("--no-distance-estimates", action="store_true", help="只做 burst 分析，不生成两种距离估计结果")
    parser.add_argument("--distance-min-m", type=float, default=0.0, help="兼容旧配置；当前 phase-match 改为围绕斜率距离局部搜索")
    parser.add_argument("--distance-max-m", type=float, default=20.0, help="兼容旧配置；当前 phase-match 改为围绕斜率距离局部搜索")
    parser.add_argument("--distance-step-m", type=float, default=0.01, help="phase-match 距离搜索步进")
    parser.add_argument("--match-window-m", type=float, default=10.0, help="phase-match 围绕线性斜率距离的搜索半窗口")
    parser.add_argument("--propagation-speed-mps", type=float, default=2.3e8, help="传播速度，默认 2.3e8 m/s（铜质有线测量）")
    parser.add_argument("--unwrap-upward-tolerance-rad", type=float, default=0.2, help="线性拟合 unwrap 时允许相邻频点小幅上升的容差")
    parser.add_argument("--calibration-reference-m", type=float, default=None, help="校对组真实距离；存在 calibration/measurement 两组时，最终输出 measurement - calibration + reference")
    return parser


def add_distance_estimates(
    result: dict[str, Any],
    args: argparse.Namespace,
    pair_phase_rows: list[dict[str, Any]],
    initiator_rows: list[dict[str, Any]],
    reflector_rows: list[dict[str, Any]],
) -> None:
    """Run both distance estimators from already detected pair rows."""
    from estimate_distance_continuous import (
        estimate_distance_from_pair_rows,
        save_estimate_plot,
        save_side_phase_plot,
    )
    from estimate_distance_continuous_phase_match import (
        estimate_distance_phase_match_from_pair_rows,
        save_phase_match_plot,
    )

    estimate_args = argparse.Namespace(**vars(args))
    estimate_args.pair_csv = None
    plot_dir = args.save_plot_dir.resolve()

    estimates: dict[str, Any] = {}

    try:
        progress_log("distance: running linear fit estimator", args)
        linear_result = estimate_distance_from_pair_rows(pair_phase_rows, estimate_args, source="analyze")
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
        progress_log("distance: running phase-match estimator", args)
        phase_match_result = estimate_distance_phase_match_from_pair_rows(pair_phase_rows, estimate_args, source="analyze")
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


def _distance_value_for_summary(estimates: dict[str, Any], key: str) -> str:
    value = estimates.get(key)
    if not isinstance(value, dict):
        return "unavailable"
    if "distance_m" in value:
        return f"{float(value['distance_m']):.6f} m"
    return f"error ({value.get('error', 'unknown')})"


def _distance_float_for_summary(result: dict[str, Any], key: str) -> float | None:
    estimates = result.get("distance_estimates")
    if not isinstance(estimates, dict):
        return None
    value = estimates.get(key)
    if not isinstance(value, dict) or "distance_m" not in value:
        return None
    return float(value["distance_m"])


def print_calibrated_measurement_summary(
    results: list[dict[str, Any]],
    *,
    disabled: bool,
    calibration_reference_m: float,
    calibration_label: str = "calibration",
    measurement_label: str = "measurement",
) -> None:
    print("calibrated_measurement_summary:")
    if disabled:
        print("  disabled_by_no_distance_estimates")
        return

    by_label = {str(result.get("capture_group")): result for result in results}
    calibration = by_label.get(calibration_label)
    measurement = by_label.get(measurement_label)
    if calibration is None or measurement is None:
        print(f"  unavailable: need groups {calibration_label!r} and {measurement_label!r}")
        return

    print(f"  calibration_reference_m: {float(calibration_reference_m):.6f}")
    for key in ("linear_fit", "phase_match"):
        calibration_distance = _distance_float_for_summary(calibration, key)
        measurement_distance = _distance_float_for_summary(measurement, key)
        if calibration_distance is None or measurement_distance is None:
            print(f"  {key}: unavailable")
            continue
        calibrated = measurement_distance - calibration_distance + float(calibration_reference_m)
        print(
            f"  {key}: measurement_raw={measurement_distance:.6f} m, "
            f"calibration_raw={calibration_distance:.6f} m, "
            f"calibrated_measurement={calibrated:.6f} m"
        )


def print_all_distance_summary(
    results: list[dict[str, Any]],
    *,
    disabled: bool,
    calibration_reference_m: float | None = None,
) -> None:
    """Print a compact final table after all capture groups finish."""
    print("all_distance_summary:")
    if disabled:
        print("  disabled_by_no_distance_estimates")
        return
    if not results:
        print("  unavailable")
        return

    for result in results:
        capture_group = result.get("capture_group", "unknown")
        capture_distance_m = result.get("capture_distance_m")
        estimates = result.get("distance_estimates")
        if not isinstance(estimates, dict):
            print(f"  {capture_group}: unavailable")
            continue

        expected = "unknown" if capture_distance_m is None else f"{float(capture_distance_m):.6f} m"
        linear = _distance_value_for_summary(estimates, "linear_fit")
        phase_match = _distance_value_for_summary(estimates, "phase_match")
        print(
            f"  {capture_group}: expected={expected}, "
            f"linear_fit={linear}, phase_match={phase_match}"
        )

    if calibration_reference_m is not None:
        print_calibrated_measurement_summary(
            results,
            disabled=disabled,
            calibration_reference_m=float(calibration_reference_m),
        )


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
    progress_log(f"group {capture_group}: starting analysis", args)
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
            "assignment_mode": str(args.assignment_mode),
            "cfo_compensate": bool(args.cfo_compensate),
            "jobs": int(args.jobs),
            "no_burst_plots": bool(args.no_burst_plots),
        },
        "expected_burst_count": int(expected_bursts),
    }
    progress_log(f"group {capture_group}: expected {expected_bursts} bursts per side", args)
    result["reflector"] = analyze_one_capture("reflector", reflector_file, run_args)
    result["initiator"] = analyze_one_capture("initiator", initiator_file, run_args)

    pair_phase_plot_path = run_args.save_plot_dir.resolve() / "pair_phase_by_freq.png"
    progress_log(f"group {capture_group}: pairing phases by frequency", args)
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
    progress_log(f"group {capture_group}: building frequency diagnostics", args)
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
        progress_log(f"group {capture_group}: writing JSON {out_path}", args)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"saved_json: {out_path}")

    progress_log(f"group {capture_group}: done", args)
    return result


def main() -> None:
    parser = build_argument_parser()
    config_args, _ = parser.parse_known_args()
    raw_config = load_config(config_args.config)
    config, config_profile = select_config_for_root(
        raw_config,
        config_args.root,
        root_was_provided=_cli_root_was_provided(),
    )
    apply_config_defaults(parser, config)
    args = parser.parse_args()
    args.config = config_args.config
    args.loaded_config = str((args.config if args.config.is_absolute() else (PROJECT_ROOT / args.config)).resolve()) if args.config is not None else None
    args.loaded_config_profile = config_profile
    if int(args.jobs) <= 0:
        args.jobs = max(1, os.cpu_count() or 1)
    root = resolve_root(args.root)
    if config_profile is not None:
        progress_log(f"config_profile: {config_profile}", args)
    progress_log(f"root: {root}", args)
    progress_log(f"jobs: {args.jobs}", args)

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
    for spec_index, spec in enumerate(run_specs, start=1):
        progress_log(f"run {spec_index}/{len(run_specs)}: capture_group={spec['label']}", args)
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

    if not explicit_files and args.save_json is not None and len(run_specs) > 1:
        out_path = args.save_json.resolve()
        progress_log(f"writing combined JSON {out_path}", args)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps({"root": str(root), "results": all_results}, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"saved_json: {out_path}")

    print_all_distance_summary(
        all_results,
        disabled=bool(args.no_distance_estimates),
        calibration_reference_m=args.calibration_reference_m,
    )


if __name__ == "__main__":
    main()
