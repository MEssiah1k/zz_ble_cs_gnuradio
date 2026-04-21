#!/usr/bin/env python3
"""Analyze continuous file-sink captures and recover burst quality."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from check_bin import circular_phase_spread_rad, classify_signal, phase_cluster_stats


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_ROOT = PROJECT_ROOT / "1to1_rfhop"
DEFAULT_PLOT_ROOT = PROJECT_ROOT / "output_analyze_continuous"


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
            resolved_root / "data_reflector_rx_from_initiator2",
            resolved_root / "continuous_capture" / "data_reflector_rx_from_initiator.bin",
            resolved_root / "data_reflector_rx_from_initiator.bin",
        ]
    )
    initiator = _pick_existing_path(
        [
            resolved_root / "data_initiator_rx_from_reflector2",
            resolved_root / "continuous_capture" / "data_initiator_rx_from_reflector.bin",
            resolved_root / "data_initiator_rx_from_reflector.bin",
        ]
    )
    return reflector, initiator


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
) -> dict[str, Any]:
    raw_cluster = phase_cluster_stats(x)
    raw_classification = classify_signal(x)
    core_start, core_stop, selection = find_stable_core_segment(x, min_segment_len=min_segment_len)
    segment = x[core_start:core_stop]
    cluster = phase_cluster_stats(segment)
    classification = classify_signal(segment)
    z_mean, robust_samples, outlier_samples = robust_complex_mean(segment)
    return {
        "selection": selection,
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
) -> list[dict[str, Any]]:
    amp = np.abs(capture)
    amp_smooth = moving_average(amp, smooth_len)
    noise_floor = float(np.percentile(amp_smooth, 20)) if amp_smooth.size else 0.0
    signal_level = float(np.percentile(amp_smooth, 99.5)) if amp_smooth.size else 0.0
    threshold = noise_floor + float(threshold_ratio) * max(0.0, signal_level - noise_floor)
    mask = _merge_short_gaps(amp_smooth >= threshold, gap_tolerance)

    candidates: list[dict[str, Any]] = []
    for candidate_index, (start, stop) in enumerate(_true_runs(mask)):
        raw_len = stop - start
        if raw_len < min_segment_len:
            continue
        raw_segment = capture[start:stop]
        summary = summarize_segment(raw_segment, min_segment_len=min_segment_len)
        core_start = int(summary["core_offset_start"])
        core_stop = int(summary["core_offset_stop"])
        abs_start = int(start + core_start)
        abs_stop = int(start + core_stop)
        summary.update(
            {
                "candidate_index": int(candidate_index),
                "raw_segment_start": int(start),
                "raw_segment_stop": int(stop),
                "raw_segment_len": int(raw_len),
                "segment_start": int(abs_start),
                "segment_stop": int(abs_stop),
                "segment_len": int(max(0, abs_stop - abs_start)),
                "score": float(summary["robust_mean_abs"] * (0.25 + summary["segment_coherence"]) * np.sqrt(max(1, abs_stop - abs_start))),
            }
        )
        candidates.append(summary)

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

    valid_starts = np.array(
        [int(row["raw_segment_start"]) for row in rows if bool(row.get("sequence_ok", True))],
        dtype=float,
    )
    if valid_starts.size >= 2:
        nominal_repeat_gap = float(np.median(np.diff(valid_starts)))
    else:
        nominal_repeat_gap = 20000.0
    short_fragment_gap = 0.75 * nominal_repeat_gap

    freq_index = 0
    idx = 0
    while idx < len(rows):
        row = rows[idx]
        if freq_index >= expected_freq_count:
            row["slot_kind"] = "overflow"
            row["quality_flags"] = list(row.get("quality_flags", [])) + ["overflow_after_expected_slots"]
            idx += 1
            continue

        freq_hz = float(center_freq_hz + start_offset_hz + freq_index * step_hz)

        if not bool(row.get("sequence_ok", True)):
            invalid_run: list[int] = [idx]
            idx += 1
            while idx < len(rows) and not bool(rows[idx].get("sequence_ok", True)):
                prev_row = rows[invalid_run[-1]]
                next_row = rows[idx]
                start_gap = int(next_row["raw_segment_start"]) - int(prev_row["raw_segment_start"])
                allow_extra_fragment = start_gap <= short_fragment_gap
                if len(invalid_run) >= repeats and not allow_extra_fragment:
                    break
                invalid_run.append(idx)
                idx += 1
            for repeat_index, row_index in enumerate(invalid_run):
                rows[row_index]["assigned_to_freq"] = False
                rows[row_index]["freq_index"] = int(freq_index)
                rows[row_index]["repeat_index"] = int(repeat_index)
                rows[row_index]["freq_hz"] = freq_hz
                rows[row_index]["slot_kind"] = "invalid_slot"
            freq_index += 1
            continue

        valid_run: list[int] = [idx]
        idx += 1
        while idx < len(rows) and bool(rows[idx].get("sequence_ok", True)) and len(valid_run) < repeats:
            valid_run.append(idx)
            idx += 1

        for repeat_index, row_index in enumerate(valid_run):
            rows[row_index]["assigned_to_freq"] = True
            rows[row_index]["freq_index"] = int(freq_index)
            rows[row_index]["repeat_index"] = int(repeat_index)
            rows[row_index]["freq_hz"] = freq_hz
            rows[row_index]["slot_kind"] = "valid_slot"
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
) -> list[dict[str, Any]]:
    grouped: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        freq_index = int(row.get("freq_index", -1))
        if freq_index < 0:
            continue
        grouped.setdefault(freq_index, []).append(row)

    diagnostics: list[dict[str, Any]] = []
    for freq_index in range(expected_freq_count):
        freq_rows = sorted(grouped.get(freq_index, []), key=lambda item: int(item.get("repeat_index", -1)))
        assigned_valid = [row for row in freq_rows if bool(row.get("assigned_to_freq", False)) and bool(row.get("sequence_ok", True))]
        invalid_rows = [row for row in freq_rows if not bool(row.get("sequence_ok", True))]
        if assigned_valid:
            state = "usable" if len(assigned_valid) >= 2 else "partial"
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
    axes[1].grid(True, alpha=0.3)

    axes[2].plot(segment.real, segment.imag, ".", markersize=1.2, alpha=0.7, color="tab:purple")
    axes[2].set_xlabel("I")
    axes[2].set_ylabel("Q")
    axes[2].set_aspect("equal", adjustable="box")
    axes[2].grid(True, alpha=0.3)

    fig.tight_layout()
    save_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(save_path, dpi=140)
    plt.close(fig)


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
    )

    result = {
        "summary": build_summary(direction, path, rows, raw_samples=int(capture.size), expected_bursts=expected_bursts),
        "rows": rows,
    }

    if args.save_plot_dir is not None:
        save_path = args.save_plot_dir.resolve() / f"{direction}_capture_summary.png"
        plot_capture(rows, save_path, f"{direction} continuous capture summary")
        result["plot_path"] = str(save_path)
        burst_plot_dir = args.save_plot_dir.resolve() / direction / "bursts"
        for row in rows:
            burst_name = (
                f"burst_{int(row['burst_index']):03d}"
                f"_f{int(row['freq_index']):02d}"
                f"_r{int(row['repeat_index'])}.png"
            )
            plot_burst_samples(capture, row, burst_plot_dir / burst_name, direction)
        result["burst_plot_dir"] = str(burst_plot_dir)

    return result


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze continuous file-sink captures")
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
    parser.add_argument("--threshold-ratio", type=float, default=0.35)
    parser.add_argument("--gap-tolerance", type=int, default=48)
    parser.add_argument("--min-segment-len", type=int, default=64)
    parser.add_argument("--save-json", type=Path, default=None)
    parser.add_argument("--save-plot-dir", type=Path, default=None)
    return parser


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()
    root = resolve_root(args.root)
    default_reflector_file, default_initiator_file = default_capture_paths(root)
    reflector_file = default_reflector_file if args.reflector_file is None else args.reflector_file.resolve()
    initiator_file = default_initiator_file if args.initiator_file is None else args.initiator_file.resolve()

    expected_bursts = expected_burst_count(
        args.start_offset_hz,
        args.stop_offset_hz,
        args.step_hz,
        args.repeats,
    )

    if args.save_plot_dir is None:
        args.save_plot_dir = default_plot_dir(root)

    result = {
        "root": str(root),
        "expected_burst_count": int(expected_bursts),
        "reflector": analyze_one_capture("reflector", reflector_file, args),
        "initiator": analyze_one_capture("initiator", initiator_file, args),
    }

    pair_phase_plot_path = args.save_plot_dir.resolve() / "pair_phase_by_freq.png"
    pair_phase_rows = plot_pair_phase_by_freq(
        result["reflector"]["rows"],
        result["initiator"]["rows"],
        pair_phase_plot_path,
    )
    expected_freq_count = int(round(float(expected_bursts) / float(args.repeats))) if args.repeats > 0 else 0
    initiator_diag = build_side_freq_diagnostics(result["initiator"]["rows"], expected_freq_count=expected_freq_count)
    reflector_diag = build_side_freq_diagnostics(result["reflector"]["rows"], expected_freq_count=expected_freq_count)
    pair_diag = build_pair_freq_diagnostics(initiator_diag, reflector_diag, pair_phase_rows)
    result["pair_phase_by_freq"] = pair_phase_rows
    result["pair_phase_plot_path"] = str(pair_phase_plot_path)
    result["initiator_freq_diagnostics"] = initiator_diag
    result["reflector_freq_diagnostics"] = reflector_diag
    result["pair_freq_diagnostics"] = pair_diag

    for name in ("reflector", "initiator"):
        summary = result[name]["summary"]
        print(f"{name}_raw_samples: {summary['raw_samples']}")
        print(f"{name}_burst_count: {summary['burst_count']}")
        print(f"{name}_expected_burst_count: {summary['expected_burst_count']}")
        print(f"{name}_mean_segment_abs: {summary['mean_segment_abs']}")
        print(f"{name}_mean_segment_coherence: {summary['mean_segment_coherence']}")
        print(f"{name}_mean_segment_phase_std: {summary['mean_segment_phase_std']}")
        print(f"{name}_trimmed_burst_count: {summary['trimmed_burst_count']}")
        print(f"{name}_invalid_burst_count: {summary['invalid_burst_count']}")
        print(f"{name}_assigned_burst_count: {summary['assigned_burst_count']}")
    for side_name, diagnostics in (("initiator", initiator_diag), ("reflector", reflector_diag)):
        for row in diagnostics:
            freq_hz = row["freq_hz"]
            freq_mhz = float(freq_hz) / 1e6 if freq_hz is not None else float(args.center_freq_hz + args.start_offset_hz + row["freq_index"] * args.step_hz) / 1e6
            print(
                f"{side_name}_freq_index: {row['freq_index']}, freq_mhz: {freq_mhz:.3f}, state: {row['state']}, "
                f"valid_repeats: {row['valid_repeat_count']}, reason: {row['reason']}, summary: {summarize_freq_rows(row['rows'])}"
            )
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
    print(f"saved_plot_dir: {args.save_plot_dir.resolve()}")
    print(f"saved_pair_phase_plot: {pair_phase_plot_path}")

    if args.save_json is not None:
        out_path = args.save_json.resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"saved_json: {out_path}")


if __name__ == "__main__":
    main()
