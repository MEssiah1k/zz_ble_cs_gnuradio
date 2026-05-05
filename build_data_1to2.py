#!/usr/bin/env python3
"""Build simulated 1-to-2 datasets from DATA_random by summing initiator signals."""

from __future__ import annotations

import argparse
import csv
import itertools
import json
import re
from pathlib import Path
from typing import Any

import numpy as np

from analyze_continuous_capture import (
    average_rows_by_freq,
    align_phase_segments_across_missing_freqs,
    disabled_pre_cancel_segment_info,
    plot_pair_phase_by_freq,
    reject_pair_phase_outliers,
    save_pair_angle_csv,
    save_pair_phase_csv,
)
from estimate_distance_continuous_phase_match import (
    estimate_distance_phase_match_from_pair_rows,
    save_phase_match_plot,
    wrap_to_pi,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_INPUT_ROOT = PROJECT_ROOT / "DATA" / "DATA_random"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "DATA" / "DATA_1to2"
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "continuous_capture_config.json"
DIST_RE = re.compile(r"^(\d+)")


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


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build simulated 1-to-2 datasets from DATA_random")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--distance", action="append", default=None)
    parser.add_argument("--group", action="append", default=None)
    parser.add_argument("--distance-min-m", type=float, default=0.0)
    parser.add_argument("--distance-max-m", type=float, default=30.0)
    parser.add_argument("--distance-step-m", type=float, default=0.01)
    parser.add_argument("--propagation-speed-mps", type=float, default=2.3e8)
    parser.add_argument("--skip-existing", action="store_true")
    parser.add_argument("--no-plots", action="store_true")
    return parser


def load_rows(path: Path) -> list[dict[str, Any]]:
    return json.loads(path.read_text(encoding="utf-8"))


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


def group_sort_key(label: str) -> tuple[int, int]:
    if label == "measurement":
        return (0, 1)
    if label.startswith("measurement"):
        suffix = label[len("measurement") :]
        if suffix.isdigit():
            return (0, int(suffix))
    return (1, 0)


def nominal_distance_label(name: str) -> str:
    match = DIST_RE.match(name)
    if match is None:
        return name
    return match.group(1)


def should_pair(distance_a: str, distance_b: str) -> bool:
    return nominal_distance_label(distance_a) != nominal_distance_label(distance_b)


def measurement_groups(distance_dir: Path) -> list[str]:
    return sorted(
        [path.name for path in distance_dir.iterdir() if path.is_dir() and path.name.startswith("measurement")],
        key=group_sort_key,
    )


def load_target_pair_template(group_dir: Path) -> dict[str, Any]:
    summary_path = group_dir / "summary.json"
    kept_pair_path = group_dir / "pair_phase_by_freq_random_distance_input.json"
    if not summary_path.exists() or not kept_pair_path.exists():
        return {
            "enabled": False,
            "reason": "missing_summary_or_distance_input",
        }

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    kept_pair_rows = json.loads(kept_pair_path.read_text(encoding="utf-8"))
    kept_freq_indices = sorted(
        int(row["freq_index"])
        for row in kept_pair_rows
        if isinstance(row, dict) and row.get("freq_index") is not None
    )
    kept_freq_set = set(kept_freq_indices)
    gap_alignment = summary.get("pre_cancel_gap_alignment", {})
    segment_offsets = gap_alignment.get("segment_offsets_rad", [])
    removed_freq_indices = summary.get("pre_cancel_outlier_filter", {}).get("removed_freq_indices", [])

    return {
        "enabled": True,
        "reason": "apply_target_1to1_pair_template_after_1to2_pair_formation",
        "kept_freq_indices": kept_freq_indices,
        "kept_freq_count": int(len(kept_freq_indices)),
        "kept_freq_set": kept_freq_set,
        "adjusted_segment_count": int(gap_alignment.get("adjusted_segment_count", 0)),
        "segment_offsets_rad": segment_offsets,
        "removed_freq_indices": [int(freq) for freq in removed_freq_indices],
    }


def apply_target_pair_template(
    pair_phase_rows: list[dict[str, Any]],
    template: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not bool(template.get("enabled", False)):
        info = {
            "enabled": False,
            "reason": str(template.get("reason", "template_disabled")),
            "input_point_count": int(len(pair_phase_rows)),
            "output_point_count": int(len(pair_phase_rows)),
        }
        return pair_phase_rows, info

    kept_freq_set = set(template.get("kept_freq_set", set()))
    if not kept_freq_set:
        kept_freq_set = {int(freq) for freq in template.get("kept_freq_indices", [])}

    offset_by_freq: dict[int, float] = {}
    adjusted_segment_count = 0
    for segment in template.get("segment_offsets_rad", []):
        if not isinstance(segment, dict):
            continue
        start = segment.get("start_freq_index")
        stop = segment.get("stop_freq_index")
        if start is None or stop is None:
            continue
        adjusted = bool(segment.get("adjusted", False))
        offset_delta = float(segment.get("offset_delta_rad", 0.0)) if adjusted else 0.0
        if adjusted:
            adjusted_segment_count += 1
        for freq_index in range(int(start), int(stop) + 1):
            offset_by_freq[int(freq_index)] = offset_delta

    processed_rows: list[dict[str, Any]] = []
    removed_freq_indices: list[int] = []
    for row in pair_phase_rows:
        freq_index = int(row["freq_index"])
        if kept_freq_set and freq_index not in kept_freq_set:
            removed_freq_indices.append(freq_index)
            continue
        updated = dict(row)
        offset_delta = float(offset_by_freq.get(freq_index, 0.0))
        updated["template_pair_offset_delta_rad"] = offset_delta
        if abs(offset_delta) > 1e-12:
            updated["pair_phase_rad"] = float(wrap_to_pi(float(updated["pair_phase_rad"]) - offset_delta))
        processed_rows.append(updated)

    info = {
        "enabled": True,
        "reason": str(template.get("reason", "template_applied")),
        "input_point_count": int(len(pair_phase_rows)),
        "output_point_count": int(len(processed_rows)),
        "kept_freq_count": int(len(kept_freq_set)),
        "adjusted_segment_count": int(adjusted_segment_count),
        "removed_freq_indices": sorted(set(int(freq) for freq in removed_freq_indices)),
    }
    return processed_rows, info


def load_random_baselines(input_root: Path) -> dict[tuple[str, str], float]:
    baselines: dict[tuple[str, str], float] = {}

    summary_all_path = input_root / "summary_all.json"
    if summary_all_path.exists():
        summary_random = json.loads(summary_all_path.read_text(encoding="utf-8"))
        baselines.update(
            {
                (distance_name, group_name): float(group_result["random_distance_spectrum_match"]["distance_m"])
                for distance_name, distance_result in summary_random.get("distances", {}).items()
                for group_name, group_result in distance_result.get("groups", {}).items()
                if group_result.get("random_distance_spectrum_match") is not None
            }
        )

    for distance_dir in sorted([path for path in input_root.iterdir() if path.is_dir()], key=lambda path: path.name):
        for group_name in measurement_groups(distance_dir):
            key = (distance_dir.name, group_name)
            if key in baselines:
                continue
            summary_path = distance_dir / group_name / "summary.json"
            if not summary_path.exists():
                continue
            group_result = json.loads(summary_path.read_text(encoding="utf-8"))
            match = group_result.get("random_distance_spectrum_match")
            if isinstance(match, dict) and match.get("distance_m") is not None:
                baselines[key] = float(match["distance_m"])
    return baselines


def build_phase_match_plot_rows(result: dict[str, Any], rows_for_plot: list[dict[str, Any]]) -> list[dict[str, Any]]:
    fit_freq_indices = {
        int(row["freq_index"])
        for row in result.get("rows", [])
        if isinstance(row, dict) and "freq_index" in row
    }
    propagation_speed_mps = float(result["propagation_speed_mps"])
    best_distance_m = float(result["distance_m"])
    phase0_rad = float(result.get("phase0_rad", 0.0))
    plot_rows: list[dict[str, Any]] = []
    for row in sorted(rows_for_plot, key=lambda item: int(item["freq_index"])):
        freq_hz = float(row["freq_hz"])
        measured_phase = float(row["pair_phase_rad"])
        model_phase = float(-4.0 * np.pi * freq_hz * best_distance_m / propagation_speed_mps + phase0_rad)
        fitted_phase = float(wrap_to_pi(model_phase))
        phase_error = float(wrap_to_pi(measured_phase - fitted_phase))
        plot_rows.append(
            {
                "freq_index": int(row["freq_index"]),
                "freq_hz": float(freq_hz),
                "phase_wrapped_measured": measured_phase,
                "phase_wrapped_fit": fitted_phase,
                "phase_wrapped_error": phase_error,
                "used_for_fit": int(row["freq_index"]) in fit_freq_indices,
            }
        )
    return plot_rows


def estimate_distance_coherent_complex_match(
    pair_phase_rows: list[dict[str, Any]],
    *,
    distance_min_m: float,
    distance_max_m: float,
    distance_step_m: float,
    propagation_speed_mps: float,
    normalize_amplitude: bool = False,
) -> dict[str, Any]:
    rows = sorted(pair_phase_rows, key=lambda item: int(item["freq_index"]))
    if len(rows) < 2:
        raise SystemExit("effective pair points fewer than 2")

    freqs_hz = np.array([float(row["freq_hz"]) for row in rows], dtype=float)
    z_pair = np.array(
        [
            complex(float(row["pair_abs"]) * np.cos(float(row["pair_phase_rad"])),
                    float(row["pair_abs"]) * np.sin(float(row["pair_phase_rad"])))
            for row in rows
        ],
        dtype=np.complex128,
    )
    if normalize_amplitude:
        amp = np.abs(z_pair)
        safe = np.where(amp > 1e-12, amp, 1.0)
        z_used = z_pair / safe
    else:
        z_used = z_pair

    distance_grid = np.arange(
        float(distance_min_m),
        float(distance_max_m) + 0.5 * float(distance_step_m),
        float(distance_step_m),
        dtype=float,
    )
    if distance_grid.size == 0:
        raise SystemExit("empty distance grid")

    scores = np.empty(distance_grid.size, dtype=float)
    phase0_values = np.empty(distance_grid.size, dtype=float)
    coherent_values = np.empty(distance_grid.size, dtype=np.complex128)
    norm = max(1e-12, float(np.mean(np.abs(z_used))))

    for idx, distance_m in enumerate(distance_grid):
        rot = np.exp(1j * (4.0 * np.pi * freqs_hz * float(distance_m) / float(propagation_speed_mps)))
        coherent = np.mean(z_used * rot)
        coherent_values[idx] = coherent
        phase0_values[idx] = float(np.angle(coherent))
        scores[idx] = float(np.abs(coherent) / norm)

    best_index = int(np.argmax(scores))
    best_distance_m = float(distance_grid[best_index])
    best_phase0 = float(phase0_values[best_index])
    best_score = float(scores[best_index])

    suppress_radius = max(0.25, 2.0 * float(distance_step_m))
    second_scores = scores.copy()
    second_scores[np.abs(distance_grid - best_distance_m) <= suppress_radius] = -np.inf
    second_best_index = int(np.argmax(second_scores))
    second_best_distance_m = float(distance_grid[second_best_index])
    second_best_score = float(scores[second_best_index])
    score_margin = float(best_score - second_best_score)
    score_ratio = float(best_score / (second_best_score + 1e-12))

    best_rot = np.exp(1j * (4.0 * np.pi * freqs_hz * best_distance_m / float(propagation_speed_mps)))
    aligned = z_used * best_rot * np.exp(-1j * best_phase0)
    aligned_mean = complex(np.mean(aligned))
    aligned_phase_error = np.angle(aligned)

    return {
        "distance_m": float(best_distance_m),
        "phase0_rad": float(best_phase0),
        "coherent_score": float(best_score),
        "second_best_distance_m": float(second_best_distance_m),
        "second_best_score": float(second_best_score),
        "score_margin": float(score_margin),
        "score_ratio": float(score_ratio),
        "normalize_amplitude": bool(normalize_amplitude),
        "distance_grid_m": [float(x) for x in distance_grid],
        "score_grid": [float(x) for x in scores],
        "rows": [
            {
                "freq_index": int(row["freq_index"]),
                "freq_hz": float(row["freq_hz"]),
                "pair_abs": float(row["pair_abs"]),
                "pair_phase_rad": float(row["pair_phase_rad"]),
                "aligned_phase_error_rad": float(err),
                "aligned_abs": float(abs(val)),
            }
            for row, err, val in zip(rows, aligned_phase_error, aligned)
        ],
        "aligned_mean_abs": float(abs(aligned_mean)),
    }


def estimate_distance_phase_cluster_match(
    pair_phase_rows: list[dict[str, Any]],
    *,
    distance_min_m: float,
    distance_max_m: float,
    distance_step_m: float,
    propagation_speed_mps: float,
    inlier_threshold_rad: float = 0.45,
    amplitude_weight_power: float = 2.0,
) -> dict[str, Any]:
    rows = sorted(pair_phase_rows, key=lambda item: int(item["freq_index"]))
    if len(rows) < 2:
        raise SystemExit("effective raw pair points fewer than 2")

    freqs_hz = np.array([float(row["freq_hz"]) for row in rows], dtype=float)
    z_pair = np.array(
        [
            complex(
                float(row["pair_abs"]) * np.cos(float(row["pair_phase_rad"])),
                float(row["pair_abs"]) * np.sin(float(row["pair_phase_rad"])),
            )
            for row in rows
        ],
        dtype=np.complex128,
    )
    weights = np.power(np.maximum(np.abs(z_pair), 1e-12), float(amplitude_weight_power))
    weights_sum = float(np.sum(weights))
    if weights_sum <= 0.0:
        weights = np.ones_like(weights)
        weights_sum = float(np.sum(weights))

    distance_grid = np.arange(
        float(distance_min_m),
        float(distance_max_m) + 0.5 * float(distance_step_m),
        float(distance_step_m),
        dtype=float,
    )
    if distance_grid.size == 0:
        raise SystemExit("empty distance grid")

    rotation = np.exp(1j * (4.0 * np.pi / float(propagation_speed_mps)) * np.outer(freqs_hz, distance_grid))
    z_rot = z_pair[:, None] * rotation
    z_unit = z_rot / np.maximum(np.abs(z_rot), 1e-12)
    mean_vector = np.sum(z_unit * weights[:, None], axis=0) / weights_sum
    center_phase = np.angle(mean_vector)
    phase_error = np.abs(np.angle(z_unit * np.exp(-1j * center_phase[None, :])))
    inlier_mask = phase_error <= float(inlier_threshold_rad)
    inlier_weight = np.sum(inlier_mask * weights[:, None], axis=0)
    inlier_coherence = np.abs(
        np.sum(z_unit * inlier_mask * weights[:, None], axis=0) / np.maximum(inlier_weight, 1e-12)
    )
    score = (inlier_weight / weights_sum) * inlier_coherence

    best_index = int(np.argmax(score))
    best_distance_m = float(distance_grid[best_index])
    best_score = float(score[best_index])
    best_center_phase = float(center_phase[best_index])
    best_inlier_mask = np.asarray(inlier_mask[:, best_index], dtype=bool)

    suppress_radius = max(0.25, 2.0 * float(distance_step_m))
    second_scores = np.asarray(score, dtype=float).copy()
    second_scores[np.abs(distance_grid - best_distance_m) <= suppress_radius] = -np.inf
    second_best_index = int(np.argmax(second_scores))
    second_best_distance_m = float(distance_grid[second_best_index])
    second_best_score = float(score[second_best_index])

    return {
        "distance_m": float(best_distance_m),
        "phase0_rad": float(best_center_phase),
        "cluster_score": float(best_score),
        "inlier_threshold_rad": float(inlier_threshold_rad),
        "amplitude_weight_power": float(amplitude_weight_power),
        "inlier_weight_fraction": float(inlier_weight[best_index] / weights_sum),
        "inlier_coherence": float(inlier_coherence[best_index]),
        "inlier_count": int(np.count_nonzero(best_inlier_mask)),
        "second_best_distance_m": float(second_best_distance_m),
        "second_best_score": float(second_best_score),
        "score_margin": float(best_score - second_best_score),
        "score_ratio": float(best_score / (second_best_score + 1e-12)),
        "distance_grid_m": [float(x) for x in distance_grid],
        "score_grid": [float(x) for x in score],
        "rows": [
            {
                "freq_index": int(row["freq_index"]),
                "freq_hz": float(row["freq_hz"]),
                "pair_abs": float(row["pair_abs"]),
                "pair_phase_rad": float(row["pair_phase_rad"]),
                "is_inlier": bool(is_inlier),
                "aligned_phase_error_rad": float(err),
                "weight": float(weight),
            }
            for row, is_inlier, err, weight in zip(
                rows,
                best_inlier_mask,
                phase_error[:, best_index],
                weights,
            )
        ],
    }


def estimate_distance_v4_target_scan(
    pair_phase_rows: list[dict[str, Any]],
    *,
    distance_min_m: float,
    distance_max_m: float,
    distance_step_m: float,
    propagation_speed_mps: float,
    score_mode: str = "composite",
) -> dict[str, Any]:
    rows = sorted(pair_phase_rows, key=lambda item: int(item["freq_index"]))
    if len(rows) < 2:
        raise SystemExit("effective raw pair points fewer than 2")

    freqs_hz = np.array([float(row["freq_hz"]) for row in rows], dtype=float)
    response = np.array(
        [
            complex(
                float(row["pair_abs"]) * np.cos(float(row["pair_phase_rad"])),
                float(row["pair_abs"]) * np.sin(float(row["pair_phase_rad"])),
            )
            for row in rows
        ],
        dtype=np.complex128,
    )
    distance_grid = np.arange(
        float(distance_min_m),
        float(distance_max_m) + 0.5 * float(distance_step_m),
        float(distance_step_m),
        dtype=float,
    )
    if distance_grid.size == 0:
        raise SystemExit("empty distance grid")

    legacy_scores: list[float] = []
    projection_scores: list[float] = []
    adjacent_scores: list[float] = []
    composite_scores: list[float] = []
    aligned_phase_errors: list[np.ndarray] = []

    for distance_m in distance_grid:
        compensated = response * np.exp(
            1j * (4.0 * np.pi * freqs_hz * float(distance_m) / float(propagation_speed_mps))
        )
        coherent_sum = np.abs(np.sum(compensated))
        energy = np.sum(np.abs(compensated)) + 1e-12
        legacy = float(coherent_sum / energy)

        projection = float((np.abs(np.mean(compensated)) ** 2) / (np.mean(np.abs(compensated) ** 2) + 1e-12))
        if compensated.size >= 2:
            deltas = compensated[1:] * np.conj(compensated[:-1])
            normalized = deltas / (np.abs(deltas) + 1e-12)
            adjacent = float(np.abs(np.mean(normalized)))
        else:
            adjacent = 0.0
        composite = 0.8 * projection + 0.2 * adjacent

        center_phase = float(np.angle(np.mean(compensated)))
        phase_err = np.angle(compensated * np.exp(-1j * center_phase))
        aligned_phase_errors.append(np.asarray(phase_err, dtype=float))
        legacy_scores.append(legacy)
        projection_scores.append(projection)
        adjacent_scores.append(adjacent)
        composite_scores.append(float(composite))

    score_map = {
        "legacy": np.asarray(legacy_scores, dtype=float),
        "projection": np.asarray(projection_scores, dtype=float),
        "adjacent": np.asarray(adjacent_scores, dtype=float),
        "composite": np.asarray(composite_scores, dtype=float),
    }
    if score_mode not in score_map:
        raise SystemExit(f"unsupported score_mode: {score_mode}")
    scores = score_map[score_mode]
    best_index = int(np.argmax(scores))
    best_distance_m = float(distance_grid[best_index])
    best_score = float(scores[best_index])

    suppress_radius = max(0.25, 2.0 * float(distance_step_m))
    second_scores = np.asarray(scores, dtype=float).copy()
    second_scores[np.abs(distance_grid - best_distance_m) <= suppress_radius] = -np.inf
    second_best_index = int(np.argmax(second_scores))
    second_best_distance_m = float(distance_grid[second_best_index])
    second_best_score = float(scores[second_best_index])
    score_margin = float(best_score - second_best_score)
    score_ratio = float(best_score / (second_best_score + 1e-12))
    confidence = float(score_margin / (best_score + 1e-12))
    best_phase_errors = aligned_phase_errors[best_index]

    return {
        "distance_m": float(best_distance_m),
        "score_mode": str(score_mode),
        "best_score": float(best_score),
        "second_best_score": float(second_best_score),
        "second_best_distance_m": float(second_best_distance_m),
        "score_margin": float(score_margin),
        "score_ratio": float(score_ratio),
        "confidence": float(confidence),
        "legacy_scores": [float(x) for x in legacy_scores],
        "projection_scores": [float(x) for x in projection_scores],
        "adjacent_scores": [float(x) for x in adjacent_scores],
        "composite_scores": [float(x) for x in composite_scores],
        "score_grid": [float(x) for x in scores],
        "distance_grid_m": [float(x) for x in distance_grid],
        "rows": [
            {
                "freq_index": int(row["freq_index"]),
                "freq_hz": float(row["freq_hz"]),
                "pair_abs": float(row["pair_abs"]),
                "pair_phase_rad": float(row["pair_phase_rad"]),
                "aligned_phase_error_rad": float(err),
            }
            for row, err in zip(rows, best_phase_errors)
        ],
    }


def build_pair_phase_rows_without_plot(
    reflector_rows: list[dict[str, Any]],
    initiator_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    reflector_avg = average_rows_by_freq(reflector_rows)
    initiator_avg = average_rows_by_freq(initiator_rows)
    common_freq_indices = sorted(set(reflector_avg) & set(initiator_avg))
    rows: list[dict[str, Any]] = []
    for freq_index in common_freq_indices:
        z_pair = initiator_avg[freq_index]["z"] * reflector_avg[freq_index]["z"]
        rows.append(
            {
                "freq_index": int(freq_index),
                "freq_hz": float(initiator_avg[freq_index]["freq_hz"]),
                "pair_abs": float(abs(z_pair)),
                "pair_phase_rad": float(np.angle(z_pair)),
                "initiator_abs": float(initiator_avg[freq_index]["abs"]),
                "initiator_phase_rad": float(initiator_avg[freq_index]["phase"]),
                "reflector_abs": float(reflector_avg[freq_index]["abs"]),
                "reflector_phase_rad": float(reflector_avg[freq_index]["phase"]),
            }
        )
    return rows


def sum_initiator_rows(rows_a: list[dict[str, Any]], rows_b: list[dict[str, Any]]) -> list[dict[str, Any]]:
    map_a = {
        (int(row["freq_index"]), int(row.get("repeat_index", 0))): row
        for row in rows_a
    }
    map_b = {
        (int(row["freq_index"]), int(row.get("repeat_index", 0))): row
        for row in rows_b
    }
    common_keys = sorted(set(map_a) & set(map_b))
    mixed_rows: list[dict[str, Any]] = []
    for freq_index, repeat_index in common_keys:
        row_a = map_a[(freq_index, repeat_index)]
        row_b = map_b[(freq_index, repeat_index)]
        z_a = complex(float(row_a["robust_mean_i"]), float(row_a["robust_mean_q"]))
        z_b = complex(float(row_b["robust_mean_i"]), float(row_b["robust_mean_q"]))
        z_sum = z_a + z_b
        mixed_rows.append(
            {
                "sequence_ok": True,
                "assigned_to_freq": True,
                "freq_index": int(freq_index),
                "repeat_index": int(repeat_index),
                "freq_hz": float(row_a["freq_hz"]),
                "slot_kind": "initiator_mixed_sum",
                "quality_flags": [],
                "robust_mean_i": float(np.real(z_sum)),
                "robust_mean_q": float(np.imag(z_sum)),
                "robust_mean_abs": float(abs(z_sum)),
                "robust_mean_phase": float(np.angle(z_sum)) if abs(z_sum) > 0 else 0.0,
                "source_distance_a": str(row_a.get("source_distance", "")),
                "source_distance_b": str(row_b.get("source_distance", "")),
            }
        )
    return mixed_rows


def annotate_source_distance(rows: list[dict[str, Any]], distance_name: str) -> list[dict[str, Any]]:
    tagged: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["source_distance"] = distance_name
        tagged.append(item)
    return tagged


def process_target(
    *,
    mixed_initiator_rows: list[dict[str, Any]],
    reflector_rows: list[dict[str, Any]],
    baseline_distance_m: float,
    target_template_group_dir: Path,
    output_dir: Path,
    args: argparse.Namespace,
) -> dict[str, Any]:
    pair_plot_path = output_dir / "pair_phase_by_freq.png"
    try:
        if bool(args.no_plots):
            pair_phase_rows = build_pair_phase_rows_without_plot(reflector_rows, mixed_initiator_rows)
        else:
            pair_phase_rows = plot_pair_phase_by_freq(reflector_rows, mixed_initiator_rows, pair_plot_path)
    except MemoryError:
        print(f"warning: skipped_pair_plot_due_to_memory: {pair_plot_path}")
        pair_phase_rows = build_pair_phase_rows_without_plot(reflector_rows, mixed_initiator_rows)
    save_pair_phase_csv(pair_phase_rows, output_dir / "pair_phase_by_freq.csv")
    save_pair_angle_csv(pair_phase_rows, output_dir / "pair_angle_by_freq.csv")
    save_rows_json(pair_phase_rows, output_dir / "pair_phase_by_freq.json")

    pair_template = load_target_pair_template(target_template_group_dir)
    template_pair_rows, template_apply_info = apply_target_pair_template(pair_phase_rows, pair_template)
    save_pair_phase_csv(template_pair_rows, output_dir / "pair_phase_by_freq_target_template.csv")
    save_pair_angle_csv(template_pair_rows, output_dir / "pair_angle_by_freq_target_template.csv")
    save_rows_json(template_pair_rows, output_dir / "pair_phase_by_freq_target_template.json")

    coherent_raw_pair = estimate_distance_coherent_complex_match(
        template_pair_rows,
        distance_min_m=args.distance_min_m,
        distance_max_m=args.distance_max_m,
        distance_step_m=args.distance_step_m,
        propagation_speed_mps=args.propagation_speed_mps,
        normalize_amplitude=False,
    )
    coherent_unit_pair = estimate_distance_coherent_complex_match(
        template_pair_rows,
        distance_min_m=args.distance_min_m,
        distance_max_m=args.distance_max_m,
        distance_step_m=args.distance_step_m,
        propagation_speed_mps=args.propagation_speed_mps,
        normalize_amplitude=True,
    )
    v4_legacy_pair = estimate_distance_v4_target_scan(
        template_pair_rows,
        distance_min_m=args.distance_min_m,
        distance_max_m=args.distance_max_m,
        distance_step_m=args.distance_step_m,
        propagation_speed_mps=args.propagation_speed_mps,
        score_mode="legacy",
    )
    v4_projection_pair = estimate_distance_v4_target_scan(
        template_pair_rows,
        distance_min_m=args.distance_min_m,
        distance_max_m=args.distance_max_m,
        distance_step_m=args.distance_step_m,
        propagation_speed_mps=args.propagation_speed_mps,
        score_mode="projection",
    )
    v4_adjacent_pair = estimate_distance_v4_target_scan(
        template_pair_rows,
        distance_min_m=args.distance_min_m,
        distance_max_m=args.distance_max_m,
        distance_step_m=args.distance_step_m,
        propagation_speed_mps=args.propagation_speed_mps,
        score_mode="adjacent",
    )
    v4_composite_pair = estimate_distance_v4_target_scan(
        template_pair_rows,
        distance_min_m=args.distance_min_m,
        distance_max_m=args.distance_max_m,
        distance_step_m=args.distance_step_m,
        propagation_speed_mps=args.propagation_speed_mps,
        score_mode="composite",
    )
    phase_cluster = estimate_distance_phase_cluster_match(
        template_pair_rows,
        distance_min_m=args.distance_min_m,
        distance_max_m=args.distance_max_m,
        distance_step_m=args.distance_step_m,
        propagation_speed_mps=args.propagation_speed_mps,
    )
    (output_dir / "distance_coherent_match_raw_pair.json").write_text(
        json.dumps(coherent_raw_pair, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (output_dir / "distance_coherent_match_unit_pair.json").write_text(
        json.dumps(coherent_unit_pair, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (output_dir / "distance_v4_legacy_pair.json").write_text(
        json.dumps(v4_legacy_pair, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (output_dir / "distance_v4_projection_pair.json").write_text(
        json.dumps(v4_projection_pair, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (output_dir / "distance_v4_adjacent_pair.json").write_text(
        json.dumps(v4_adjacent_pair, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (output_dir / "distance_v4_composite_pair.json").write_text(
        json.dumps(v4_composite_pair, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (output_dir / "distance_phase_cluster_match.json").write_text(
        json.dumps(phase_cluster, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    pre_cancel_segment_info = disabled_pre_cancel_segment_info(template_pair_rows)
    pair_phase_rows_pre_filtered, initial_outlier_filter_info = reject_pair_phase_outliers(
        template_pair_rows,
        distance_min_m=args.distance_min_m,
        distance_max_m=args.distance_max_m,
        distance_step_m=args.distance_step_m,
        propagation_speed_mps=args.propagation_speed_mps,
    )
    pair_phase_rows_aligned, gap_alignment_info = align_phase_segments_across_missing_freqs(
        pair_phase_rows_pre_filtered,
        distance_min_m=args.distance_min_m,
        distance_max_m=args.distance_max_m,
        distance_step_m=args.distance_step_m,
        propagation_speed_mps=args.propagation_speed_mps,
    )
    pair_phase_rows_for_distance, outlier_filter_info = reject_pair_phase_outliers(
        pair_phase_rows_aligned,
        distance_min_m=args.distance_min_m,
        distance_max_m=args.distance_max_m,
        distance_step_m=args.distance_step_m,
        propagation_speed_mps=args.propagation_speed_mps,
    )
    save_pair_phase_csv(pair_phase_rows_for_distance, output_dir / "pair_phase_distance_input.csv")
    save_pair_angle_csv(pair_phase_rows_for_distance, output_dir / "pair_angle_distance_input.csv")
    save_rows_json(pair_phase_rows_for_distance, output_dir / "pair_phase_distance_input.json")

    estimate_args = argparse.Namespace(
        root=output_dir,
        pair_csv=None,
        distance_min_m=float(args.distance_min_m),
        distance_max_m=float(args.distance_max_m),
        distance_step_m=float(args.distance_step_m),
        propagation_speed_mps=float(args.propagation_speed_mps),
    )
    match_result = estimate_distance_phase_match_from_pair_rows(
        pair_phase_rows_for_distance,
        estimate_args,
        source="data_1to2",
    )
    match_result["plot_rows"] = build_phase_match_plot_rows(match_result, pair_phase_rows_for_distance)
    if not bool(args.no_plots):
        try:
            save_phase_match_plot(match_result, output_dir / "distance_spectrum_match.png")
        except MemoryError:
            print(f"warning: skipped_phase_match_plot_due_to_memory: {output_dir / 'distance_spectrum_match.png'}")
    (output_dir / "distance_spectrum_match.json").write_text(
        json.dumps(match_result, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    coherent_raw = estimate_distance_coherent_complex_match(
        pair_phase_rows_for_distance,
        distance_min_m=args.distance_min_m,
        distance_max_m=args.distance_max_m,
        distance_step_m=args.distance_step_m,
        propagation_speed_mps=args.propagation_speed_mps,
        normalize_amplitude=False,
    )
    coherent_unit = estimate_distance_coherent_complex_match(
        pair_phase_rows_for_distance,
        distance_min_m=args.distance_min_m,
        distance_max_m=args.distance_max_m,
        distance_step_m=args.distance_step_m,
        propagation_speed_mps=args.propagation_speed_mps,
        normalize_amplitude=True,
    )
    (output_dir / "distance_coherent_match_raw.json").write_text(
        json.dumps(coherent_raw, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (output_dir / "distance_coherent_match_unit.json").write_text(
        json.dumps(coherent_unit, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    return {
        "pair_phase_point_count": int(len(pair_phase_rows)),
        "target_template_application": template_apply_info,
        "distance_input_pair_point_count": int(len(pair_phase_rows_for_distance)),
        "pre_cancel_segment_selection": pre_cancel_segment_info,
        "pre_cancel_initial_outlier_filter": initial_outlier_filter_info,
        "pre_cancel_gap_alignment": gap_alignment_info,
        "pre_cancel_outlier_filter": outlier_filter_info,
        "baseline_distance_m": float(baseline_distance_m),
        "distance_spectrum_match": {
            "distance_m": float(match_result["distance_m"]),
            "delta_from_baseline_m": float(float(match_result["distance_m"]) - float(baseline_distance_m)),
            "wrapped_phase_cost": float(match_result["wrapped_phase_cost"]),
            "wrapped_phase_rms_error": float(match_result["wrapped_phase_rms_error"]),
            "wrapped_phase_max_abs_error": float(match_result["wrapped_phase_max_abs_error"]),
            "match_point_count": int(match_result["match_point_count"]),
            "valid_freq_count": int(match_result["valid_freq_count"]),
            "confidence": float(match_result["confidence"]),
            "second_best_distance_m": float(match_result["second_best_distance_m"]),
        },
        "distance_coherent_match_raw": {
            "distance_m": float(coherent_raw["distance_m"]),
            "delta_from_baseline_m": float(float(coherent_raw["distance_m"]) - float(baseline_distance_m)),
            "coherent_score": float(coherent_raw["coherent_score"]),
            "score_margin": float(coherent_raw["score_margin"]),
            "score_ratio": float(coherent_raw["score_ratio"]),
            "second_best_distance_m": float(coherent_raw["second_best_distance_m"]),
        },
        "distance_coherent_match_unit": {
            "distance_m": float(coherent_unit["distance_m"]),
            "delta_from_baseline_m": float(float(coherent_unit["distance_m"]) - float(baseline_distance_m)),
            "coherent_score": float(coherent_unit["coherent_score"]),
            "score_margin": float(coherent_unit["score_margin"]),
            "score_ratio": float(coherent_unit["score_ratio"]),
            "second_best_distance_m": float(coherent_unit["second_best_distance_m"]),
        },
        "distance_coherent_match_raw_pair": {
            "distance_m": float(coherent_raw_pair["distance_m"]),
            "delta_from_baseline_m": float(float(coherent_raw_pair["distance_m"]) - float(baseline_distance_m)),
            "coherent_score": float(coherent_raw_pair["coherent_score"]),
            "score_margin": float(coherent_raw_pair["score_margin"]),
            "score_ratio": float(coherent_raw_pair["score_ratio"]),
            "second_best_distance_m": float(coherent_raw_pair["second_best_distance_m"]),
        },
        "distance_coherent_match_unit_pair": {
            "distance_m": float(coherent_unit_pair["distance_m"]),
            "delta_from_baseline_m": float(float(coherent_unit_pair["distance_m"]) - float(baseline_distance_m)),
            "coherent_score": float(coherent_unit_pair["coherent_score"]),
            "score_margin": float(coherent_unit_pair["score_margin"]),
            "score_ratio": float(coherent_unit_pair["score_ratio"]),
            "second_best_distance_m": float(coherent_unit_pair["second_best_distance_m"]),
        },
        "distance_v4_legacy_pair": {
            "distance_m": float(v4_legacy_pair["distance_m"]),
            "delta_from_baseline_m": float(float(v4_legacy_pair["distance_m"]) - float(baseline_distance_m)),
            "best_score": float(v4_legacy_pair["best_score"]),
            "score_margin": float(v4_legacy_pair["score_margin"]),
            "score_ratio": float(v4_legacy_pair["score_ratio"]),
            "confidence": float(v4_legacy_pair["confidence"]),
            "second_best_distance_m": float(v4_legacy_pair["second_best_distance_m"]),
        },
        "distance_v4_projection_pair": {
            "distance_m": float(v4_projection_pair["distance_m"]),
            "delta_from_baseline_m": float(float(v4_projection_pair["distance_m"]) - float(baseline_distance_m)),
            "best_score": float(v4_projection_pair["best_score"]),
            "score_margin": float(v4_projection_pair["score_margin"]),
            "score_ratio": float(v4_projection_pair["score_ratio"]),
            "confidence": float(v4_projection_pair["confidence"]),
            "second_best_distance_m": float(v4_projection_pair["second_best_distance_m"]),
        },
        "distance_v4_adjacent_pair": {
            "distance_m": float(v4_adjacent_pair["distance_m"]),
            "delta_from_baseline_m": float(float(v4_adjacent_pair["distance_m"]) - float(baseline_distance_m)),
            "best_score": float(v4_adjacent_pair["best_score"]),
            "score_margin": float(v4_adjacent_pair["score_margin"]),
            "score_ratio": float(v4_adjacent_pair["score_ratio"]),
            "confidence": float(v4_adjacent_pair["confidence"]),
            "second_best_distance_m": float(v4_adjacent_pair["second_best_distance_m"]),
        },
        "distance_v4_composite_pair": {
            "distance_m": float(v4_composite_pair["distance_m"]),
            "delta_from_baseline_m": float(float(v4_composite_pair["distance_m"]) - float(baseline_distance_m)),
            "best_score": float(v4_composite_pair["best_score"]),
            "score_margin": float(v4_composite_pair["score_margin"]),
            "score_ratio": float(v4_composite_pair["score_ratio"]),
            "confidence": float(v4_composite_pair["confidence"]),
            "second_best_distance_m": float(v4_composite_pair["second_best_distance_m"]),
        },
        "distance_phase_cluster_match": {
            "distance_m": float(phase_cluster["distance_m"]),
            "delta_from_baseline_m": float(float(phase_cluster["distance_m"]) - float(baseline_distance_m)),
            "cluster_score": float(phase_cluster["cluster_score"]),
            "inlier_weight_fraction": float(phase_cluster["inlier_weight_fraction"]),
            "inlier_coherence": float(phase_cluster["inlier_coherence"]),
            "inlier_count": int(phase_cluster["inlier_count"]),
            "score_margin": float(phase_cluster["score_margin"]),
            "score_ratio": float(phase_cluster["score_ratio"]),
            "second_best_distance_m": float(phase_cluster["second_best_distance_m"]),
        },
        "recommended_distance_estimate": {
            "method": "v4_composite_pair",
            "distance_m": float(v4_composite_pair["distance_m"]),
            "delta_from_baseline_m": float(float(v4_composite_pair["distance_m"]) - float(baseline_distance_m)),
            "score": float(v4_composite_pair["best_score"]),
            "score_margin": float(v4_composite_pair["score_margin"]),
            "score_ratio": float(v4_composite_pair["score_ratio"]),
            "confidence": float(v4_composite_pair["confidence"]),
            "second_best_distance_m": float(v4_composite_pair["second_best_distance_m"]),
        },
    }


def main() -> None:
    parser = build_argument_parser()
    pre_args, _ = parser.parse_known_args()
    config = load_config(pre_args.config)
    apply_config_defaults(parser, config)
    args = parser.parse_args()

    input_root = args.input_root.resolve()
    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    baseline_by_distance_group = load_random_baselines(input_root)
    distance_dirs = sorted([p for p in input_root.iterdir() if p.is_dir()])
    if args.distance:
        requested = set(args.distance)
        distance_dirs = [p for p in distance_dirs if p.name in requested]

    requested_groups = set(args.group) if args.group else None

    all_results: dict[str, Any] = {
        "input_root": str(input_root),
        "output_root": str(output_root),
        "parameters": {
            "distance_min_m": float(args.distance_min_m),
            "distance_max_m": float(args.distance_max_m),
            "distance_step_m": float(args.distance_step_m),
            "propagation_speed_mps": float(args.propagation_speed_mps),
            "skip_existing": bool(args.skip_existing),
        },
        "pairings": {},
    }

    for distance_a_dir, distance_b_dir in itertools.combinations(distance_dirs, 2):
        distance_a = distance_a_dir.name
        distance_b = distance_b_dir.name
        if not should_pair(distance_a, distance_b):
            continue
        groups_a = measurement_groups(distance_a_dir)
        groups_b = measurement_groups(distance_b_dir)
        if requested_groups is not None:
            groups_a = [group for group in groups_a if group in requested_groups]
            groups_b = [group for group in groups_b if group in requested_groups]
        group_pairs = sorted(
            [(group_a, group_b) for group_a in groups_a for group_b in groups_b],
            key=lambda item: (group_sort_key(item[0]), group_sort_key(item[1])),
        )
        if not group_pairs:
            continue

        pair_key = f"{distance_a}__{distance_b}"
        pair_result: dict[str, Any] = {"groups": {}}
        print(f"processing_pair: {pair_key}")

        for group_a_label, group_b_label in group_pairs:
            group_label = f"{group_a_label}__{group_b_label}"
            group_output_dir = output_root / pair_key / group_label
            group_summary_path = group_output_dir / "summary.json"
            if bool(args.skip_existing) and group_summary_path.exists():
                pair_result["groups"][group_label] = json.loads(group_summary_path.read_text(encoding="utf-8"))
                continue

            init_a = annotate_source_distance(
                load_rows(distance_a_dir / group_a_label / "initiator_random_phase_rows.json"),
                distance_a,
            )
            init_b = annotate_source_distance(
                load_rows(distance_b_dir / group_b_label / "initiator_random_phase_rows.json"),
                distance_b,
            )
            refl_a = annotate_source_distance(
                load_rows(distance_a_dir / group_a_label / "reflector_random_phase_rows.json"),
                distance_a,
            )
            refl_b = annotate_source_distance(
                load_rows(distance_b_dir / group_b_label / "reflector_random_phase_rows.json"),
                distance_b,
            )

            mixed_rows = sum_initiator_rows(init_a, init_b)
            group_output_dir.mkdir(parents=True, exist_ok=True)
            save_rows_csv(mixed_rows, group_output_dir / "initiator_mixed_rows.csv")
            save_rows_json(mixed_rows, group_output_dir / "initiator_mixed_rows.json")

            target_a_dir = group_output_dir / f"target_{distance_a}"
            target_b_dir = group_output_dir / f"target_{distance_b}"
            target_a_dir.mkdir(parents=True, exist_ok=True)
            target_b_dir.mkdir(parents=True, exist_ok=True)
            save_rows_csv(refl_a, target_a_dir / "reflector_rows.csv")
            save_rows_json(refl_a, target_a_dir / "reflector_rows.json")
            save_rows_csv(refl_b, target_b_dir / "reflector_rows.csv")
            save_rows_json(refl_b, target_b_dir / "reflector_rows.json")

            target_a_result = process_target(
                mixed_initiator_rows=mixed_rows,
                reflector_rows=refl_a,
                baseline_distance_m=baseline_by_distance_group[(distance_a, group_a_label)],
                target_template_group_dir=distance_a_dir / group_a_label,
                output_dir=target_a_dir,
                args=args,
            )
            target_b_result = process_target(
                mixed_initiator_rows=mixed_rows,
                reflector_rows=refl_b,
                baseline_distance_m=baseline_by_distance_group[(distance_b, group_b_label)],
                target_template_group_dir=distance_b_dir / group_b_label,
                output_dir=target_b_dir,
                args=args,
            )

            group_result = {
                "distance_a": distance_a,
                "distance_b": distance_b,
                "group": group_label,
                "group_a": group_a_label,
                "group_b": group_b_label,
                "mixed_initiator_point_count": int(len(mixed_rows)),
                "target_results": {
                    distance_a: target_a_result,
                    distance_b: target_b_result,
                },
            }
            group_summary_path.write_text(json.dumps(group_result, indent=2, ensure_ascii=False), encoding="utf-8")
            pair_result["groups"][group_label] = group_result

            print(
                f"processed_group: pair={pair_key}, group={group_label}, "
                f"target_{distance_a}={target_a_result['recommended_distance_estimate']['distance_m']:.3f} "
                f"(delta={target_a_result['recommended_distance_estimate']['delta_from_baseline_m']:+.3f}, "
                f"method={target_a_result['recommended_distance_estimate']['method']}), "
                f"target_{distance_b}={target_b_result['recommended_distance_estimate']['distance_m']:.3f} "
                f"(delta={target_b_result['recommended_distance_estimate']['delta_from_baseline_m']:+.3f}, "
                f"method={target_b_result['recommended_distance_estimate']['method']})"
            )

        if pair_result["groups"]:
            all_results["pairings"][pair_key] = pair_result

    summary_path = output_root / "summary_all.json"
    summary_path.write_text(json.dumps(all_results, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"saved_summary: {summary_path}")


if __name__ == "__main__":
    main()
