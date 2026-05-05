#!/usr/bin/env python3
"""Split continuous 1to1 captures into per-frequency burst binary files."""

from __future__ import annotations

import argparse
import csv
import json
import re
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from check_bin import circular_phase_spread_rad, classify_signal, phase_cluster_stats


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_INPUT_ROOT = PROJECT_ROOT / "DATA" / "DATA"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "DATA" / "DATA_burst"
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "continuous_capture_config.json"

INITIATOR_PREFIX = "data_initiator_rx_from_reflector_"
REFLECTOR_PREFIX = "data_reflector_rx_from_initiator_"
GROUP_RE = re.compile(r"^(calibration|measurement\d*)$")


@dataclass
class ExportTask:
    distance_dir: str
    group_label: str
    initiator_file: str
    reflector_file: str
    output_root: str
    center_freq_hz: float
    start_offset_hz: float
    stop_offset_hz: float
    step_hz: float
    repeats: int
    smooth_len: int
    threshold_ratio: float
    gap_tolerance: int
    min_segment_len: int
    edge_trim_samples: int
    jobs: int
    save_invalid_bursts: bool


def load_gr_complex_bin(path: Path) -> np.ndarray:
    if not path.exists():
        raise FileNotFoundError(f"capture file not found: {path}")
    return np.fromfile(path, dtype=np.complex64)


def load_config(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    resolved = path if path.is_absolute() else (PROJECT_ROOT / path).resolve()
    if not resolved.exists():
        return {}
    with resolved.open("r", encoding="utf-8") as fp:
        data = json.load(fp)
    if not isinstance(data, dict):
        raise SystemExit(f"config must be a JSON object: {resolved}")
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


def expected_burst_count(start_offset_hz: float, stop_offset_hz: float, step_hz: float, repeats: int) -> int:
    freq_count = int(round((float(stop_offset_hz) - float(start_offset_hz)) / float(step_hz))) + 1
    return freq_count * int(repeats)


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


def find_stable_core_segment(x: np.ndarray, *, min_segment_len: int) -> tuple[int, int, str]:
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


def summarize_segment(x: np.ndarray, *, min_segment_len: int, edge_trim_samples: int = 0) -> dict[str, Any]:
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
    summary.update(
        {
            "candidate_index": int(candidate_index),
            "raw_segment_start": int(start),
            "raw_segment_stop": int(stop),
            "raw_segment_len": int(stop - start),
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
    return 4.0 * phase_spread + 1.5 * abs_spread_ratio + 0.5 * len_spread_ratio + 0.2 * gap_spread_ratio + size_penalty


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
        row["assigned_to_freq"] = bool(row.get("sequence_ok", True))
        row["freq_index"] = int(freq_index)
        row["repeat_index"] = 0
        row["freq_hz"] = float(center_freq_hz + start_offset_hz + freq_index * step_hz)
        row["slot_kind"] = "valid_slot" if bool(row.get("sequence_ok", True)) else "invalid_slot"
        freq_index += 1
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
        invalid = [row for row in slotted if not bool(row["sequence_ok"])]
        valid = [row for row in slotted if bool(row["sequence_ok"])]
        invalid_keep = sorted(invalid, key=lambda item: float(item["score"]), reverse=True)
        valid_keep = sorted(valid, key=lambda item: float(item["score"]), reverse=True)
        chosen: list[dict[str, Any]] = []
        for pool in (valid_keep, invalid_keep):
            for row in pool:
                if len(chosen) >= expected_bursts:
                    break
                chosen.append(row)
        chosen_ids = {id(item) for item in chosen}
        slotted = [row for row in slotted if id(row) in chosen_ids]

    slotted.sort(key=lambda item: int(item["segment_start"]))
    if expected_bursts > 0 and len(slotted) > expected_bursts:
        slotted = slotted[:expected_bursts]
    return slotted


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
        if stop - start < min_segment_len:
            continue
        tasks.append((int(candidate_index), int(start), int(stop), capture[start:stop], int(min_segment_len), int(edge_trim_samples)))

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


def parse_group_name(file_name: str, prefix: str) -> str | None:
    if not file_name.startswith(prefix):
        return None
    suffix = file_name[len(prefix) :]
    if GROUP_RE.fullmatch(suffix) is None:
        return None
    return suffix


def discover_capture_groups(distance_dir: Path) -> list[dict[str, Any]]:
    initiator_files: dict[str, Path] = {}
    reflector_files: dict[str, Path] = {}

    for path in sorted(distance_dir.iterdir()):
        if not path.is_file():
            continue
        group = parse_group_name(path.name, INITIATOR_PREFIX)
        if group is not None:
            initiator_files[group] = path
            continue
        group = parse_group_name(path.name, REFLECTOR_PREFIX)
        if group is not None:
            reflector_files[group] = path

    groups: list[dict[str, Any]] = []
    for group in sorted(set(initiator_files) & set(reflector_files), key=group_sort_key):
        groups.append(
            {
                "label": group,
                "initiator_file": initiator_files[group],
                "reflector_file": reflector_files[group],
            }
        )
    return groups


def group_sort_key(label: str) -> tuple[int, int]:
    if label == "calibration":
        return (0, 0)
    if label == "measurement":
        return (1, 1)
    match = re.fullmatch(r"measurement(\d+)", label)
    if match is not None:
        return (1, int(match.group(1)))
    return (2, 0)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Split continuous captures into per-frequency burst files")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--distance", action="append", default=None, help="Only process selected distance folder name(s)")
    parser.add_argument("--group", action="append", default=None, help="Only process selected group label(s)")
    parser.add_argument("--center-freq-hz", type=float, default=2.44e9)
    parser.add_argument("--start-offset-hz", type=float, default=-4e6)
    parser.add_argument("--stop-offset-hz", type=float, default=4e6)
    parser.add_argument("--step-hz", type=float, default=1e5)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--smooth-len", type=int, default=64)
    parser.add_argument("--threshold-ratio", type=float, default=0.005)
    parser.add_argument("--gap-tolerance", type=int, default=48)
    parser.add_argument("--min-segment-len", type=int, default=64)
    parser.add_argument("--edge-trim-samples", type=int, default=16)
    parser.add_argument("--jobs", type=int, default=0)
    parser.add_argument("--group-workers", type=int, default=1, help="Process different measurement groups in parallel")
    parser.add_argument("--skip-existing", action="store_true", help="Skip groups that already have split_summary.json")
    parser.add_argument("--save-invalid-bursts", action="store_true", help="Also save invalid/unassigned bursts")
    return parser


def should_keep_row(row: dict[str, Any], save_invalid_bursts: bool) -> bool:
    if save_invalid_bursts:
        return True
    return bool(row.get("sequence_ok", True)) and bool(row.get("assigned_to_freq", False))


def burst_file_name(row: dict[str, Any]) -> str:
    return (
        f"burst_{int(row.get('burst_index', -1)):03d}"
        f"_f{int(row.get('freq_index', -1)):02d}"
        f"_r{int(row.get('repeat_index', -1))}.bin"
    )


def side_name_to_file_stem(side_name: str) -> str:
    return f"{side_name}_burst_rows"


def save_rows_csv(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            normalized = {
                key: (";".join(value) if isinstance(value, list) else value)
                for key, value in row.items()
            }
            writer.writerow(normalized)


def save_rows_json(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")


def export_side_bursts(
    *,
    capture_path: Path,
    output_dir: Path,
    side_name: str,
    args: argparse.Namespace,
) -> dict[str, Any]:
    capture = load_gr_complex_bin(capture_path)
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
        jobs=max(1, int(args.jobs)) if int(args.jobs) > 0 else 1,
    )

    side_dir = output_dir / side_name / "bursts"
    kept_rows = 0
    for row in rows:
        if not should_keep_row(row, bool(args.save_invalid_bursts)):
            continue
        start = int(row["segment_start"])
        stop = int(row["segment_stop"])
        burst = capture[start:stop]
        if burst.size <= 0:
            continue
        side_dir.mkdir(parents=True, exist_ok=True)
        burst.astype("complex64", copy=False).tofile(side_dir / burst_file_name(row))
        kept_rows += 1

    rows_csv_path = output_dir / f"{side_name_to_file_stem(side_name)}.csv"
    rows_json_path = output_dir / f"{side_name_to_file_stem(side_name)}.json"
    save_rows_csv(rows, rows_csv_path)
    save_rows_json(rows, rows_json_path)

    return {
        "capture_file": str(capture_path),
        "raw_samples": int(capture.size),
        "expected_bursts": int(expected_bursts),
        "detected_rows": int(len(rows)),
        "exported_bursts": int(kept_rows),
        "rows_csv": str(rows_csv_path),
        "rows_json": str(rows_json_path),
    }


def _task_args_namespace(task: ExportTask) -> argparse.Namespace:
    return argparse.Namespace(
        center_freq_hz=float(task.center_freq_hz),
        start_offset_hz=float(task.start_offset_hz),
        stop_offset_hz=float(task.stop_offset_hz),
        step_hz=float(task.step_hz),
        repeats=int(task.repeats),
        smooth_len=int(task.smooth_len),
        threshold_ratio=float(task.threshold_ratio),
        gap_tolerance=int(task.gap_tolerance),
        min_segment_len=int(task.min_segment_len),
        edge_trim_samples=int(task.edge_trim_samples),
        jobs=int(task.jobs),
        save_invalid_bursts=bool(task.save_invalid_bursts),
    )


def run_export_task(task: ExportTask) -> dict[str, Any]:
    distance_dir = Path(task.distance_dir)
    group_output_dir = Path(task.output_root) / distance_dir.name / str(task.group_label)
    group_output_dir.mkdir(parents=True, exist_ok=True)
    task_args = _task_args_namespace(task)
    group_result = {
        "initiator": export_side_bursts(
            capture_path=Path(task.initiator_file),
            output_dir=group_output_dir,
            side_name="initiator",
            args=task_args,
        ),
        "reflector": export_side_bursts(
            capture_path=Path(task.reflector_file),
            output_dir=group_output_dir,
            side_name="reflector",
            args=task_args,
        ),
    }
    (group_output_dir / "split_summary.json").write_text(
        json.dumps(group_result, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return {
        "distance": distance_dir.name,
        "group": str(task.group_label),
        "result": group_result,
    }


def build_export_tasks(distance_dirs: list[Path], output_root: Path, args: argparse.Namespace) -> tuple[dict[str, Any], list[ExportTask]]:
    requested_groups = set(args.group) if args.group else None
    all_results: dict[str, Any] = {}
    tasks: list[ExportTask] = []

    for distance_dir in distance_dirs:
        distance_result: dict[str, Any] = {
            "distance_dir": str(distance_dir),
            "groups": {},
        }
        all_results[distance_dir.name] = distance_result

        for group in discover_capture_groups(distance_dir):
            group_label = str(group["label"])
            if requested_groups is not None and group_label not in requested_groups:
                continue
            group_output_dir = output_root / distance_dir.name / group_label
            summary_path = group_output_dir / "split_summary.json"
            if bool(args.skip_existing) and summary_path.exists():
                try:
                    distance_result["groups"][group_label] = json.loads(summary_path.read_text(encoding="utf-8"))
                except json.JSONDecodeError:
                    pass
                continue
            tasks.append(
                ExportTask(
                    distance_dir=str(distance_dir),
                    group_label=group_label,
                    initiator_file=str(group["initiator_file"]),
                    reflector_file=str(group["reflector_file"]),
                    output_root=str(output_root),
                    center_freq_hz=float(args.center_freq_hz),
                    start_offset_hz=float(args.start_offset_hz),
                    stop_offset_hz=float(args.stop_offset_hz),
                    step_hz=float(args.step_hz),
                    repeats=int(args.repeats),
                    smooth_len=int(args.smooth_len),
                    threshold_ratio=float(args.threshold_ratio),
                    gap_tolerance=int(args.gap_tolerance),
                    min_segment_len=int(args.min_segment_len),
                    edge_trim_samples=int(args.edge_trim_samples),
                    jobs=max(1, int(args.jobs)) if int(args.jobs) > 0 else 1,
                    save_invalid_bursts=bool(args.save_invalid_bursts),
                )
            )
    return all_results, tasks


def main() -> None:
    parser = build_argument_parser()
    pre_args, _ = parser.parse_known_args()
    config = load_config(pre_args.config)
    apply_config_defaults(parser, config)
    args = parser.parse_args()

    input_root = args.input_root.resolve()
    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    requested_distances = set(args.distance) if args.distance else None
    distance_dirs = [
        path
        for path in sorted(input_root.iterdir())
        if path.is_dir() and (requested_distances is None or path.name in requested_distances)
    ]

    all_results: dict[str, Any] = {
        "input_root": str(input_root),
        "output_root": str(output_root),
        "parameters": {
            "center_freq_hz": float(args.center_freq_hz),
            "start_offset_hz": float(args.start_offset_hz),
            "stop_offset_hz": float(args.stop_offset_hz),
            "step_hz": float(args.step_hz),
            "repeats": int(args.repeats),
            "smooth_len": int(args.smooth_len),
            "threshold_ratio": float(args.threshold_ratio),
            "gap_tolerance": int(args.gap_tolerance),
            "min_segment_len": int(args.min_segment_len),
            "edge_trim_samples": int(args.edge_trim_samples),
            "jobs": max(1, int(args.jobs)) if int(args.jobs) > 0 else 1,
            "group_workers": int(args.group_workers),
            "skip_existing": bool(args.skip_existing),
            "save_invalid_bursts": bool(args.save_invalid_bursts),
        },
        "distances": {},
    }

    distances_result, tasks = build_export_tasks(distance_dirs, output_root, args)
    all_results["distances"] = distances_result

    for distance_name in sorted(distances_result):
        print(f"queued_distance: {distance_name}")

    worker_count = max(1, int(args.group_workers))
    if worker_count > 1 and len(tasks) > 1:
        worker_count = min(worker_count, len(tasks))
        with ProcessPoolExecutor(max_workers=worker_count) as executor:
            for task_result in executor.map(run_export_task, tasks):
                distance_name = str(task_result["distance"])
                group_label = str(task_result["group"])
                group_result = dict(task_result["result"])
                all_results["distances"][distance_name]["groups"][group_label] = group_result
                init_exported = int(group_result["initiator"]["exported_bursts"])
                refl_exported = int(group_result["reflector"]["exported_bursts"])
                print(
                    f"processed_group: distance={distance_name}, group={group_label}, "
                    f"initiator_exported={init_exported}, reflector_exported={refl_exported}"
                )
    else:
        for task in tasks:
            task_result = run_export_task(task)
            distance_name = str(task_result["distance"])
            group_label = str(task_result["group"])
            group_result = dict(task_result["result"])
            all_results["distances"][distance_name]["groups"][group_label] = group_result
            init_exported = int(group_result["initiator"]["exported_bursts"])
            refl_exported = int(group_result["reflector"]["exported_bursts"])
            print(
                f"processed_group: distance={distance_name}, group={group_label}, "
                f"initiator_exported={init_exported}, reflector_exported={refl_exported}"
            )

    summary_path = output_root / "split_summary_all.json"
    summary_path.write_text(json.dumps(all_results, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"saved_summary: {summary_path}")


if __name__ == "__main__":
    main()
