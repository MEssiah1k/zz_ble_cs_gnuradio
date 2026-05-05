#!/usr/bin/env python3
"""Inject canceling random phase differences into DATA_minus results."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np

from analyze_continuous_capture import (
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
DEFAULT_INPUT_ROOT = PROJECT_ROOT / "DATA" / "DATA_minus"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "DATA" / "DATA_random"
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "continuous_capture_config.json"
TWO_PI = 2.0 * np.pi


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
    parser = argparse.ArgumentParser(description="Inject canceling random phase differences into DATA_minus")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--distance", action="append", default=None)
    parser.add_argument("--group", action="append", default=None)
    parser.add_argument("--distance-min-m", type=float, default=0.0)
    parser.add_argument("--distance-max-m", type=float, default=30.0)
    parser.add_argument("--distance-step-m", type=float, default=0.01)
    parser.add_argument("--propagation-speed-mps", type=float, default=2.3e8)
    parser.add_argument("--seed", type=int, default=20260504)
    parser.add_argument("--phase-range", choices=("full", "half"), default="full", help="full=(-pi,pi], half=(-pi/2,pi/2]")
    parser.add_argument("--skip-existing", action="store_true")
    return parser


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


def load_rows(path: Path) -> list[dict[str, Any]]:
    return json.loads(path.read_text(encoding="utf-8"))


def group_sort_key(label: str) -> tuple[int, int]:
    if label == "measurement":
        return (0, 1)
    if label.startswith("measurement"):
        suffix = label[len("measurement") :]
        if suffix.isdigit():
            return (0, int(suffix))
    return (1, 0)


def measurement_groups(distance_dir: Path) -> list[str]:
    return sorted(
        [path.name for path in distance_dir.iterdir() if path.is_dir() and path.name.startswith("measurement")],
        key=group_sort_key,
    )


def stable_group_seed(base_seed: int, distance: str, group: str) -> int:
    payload = f"{base_seed}:{distance}:{group}".encode("utf-8")
    digest = hashlib.sha256(payload).digest()
    return int.from_bytes(digest[:8], "little") & 0x7FFFFFFF


def phase_delta_map(keys: list[tuple[int, int]], seed: int, phase_range: str) -> dict[tuple[int, int], float]:
    unique_keys = sorted(set(keys))
    rng = np.random.default_rng(seed)
    if phase_range == "half":
        low, high = -0.5 * np.pi, 0.5 * np.pi
    else:
        low, high = -np.pi, np.pi
    values = rng.uniform(low, high, size=len(unique_keys))
    return {key: float(value) for key, value in zip(unique_keys, values)}


def rotate_complex_row(row: dict[str, Any], delta_rad: float, *, sign: float) -> dict[str, Any]:
    z = complex(float(row["robust_mean_i"]), float(row["robust_mean_q"]))
    z_rot = z * np.exp(1j * float(sign) * float(delta_rad))
    updated = dict(row)
    updated["random_phase_delta_rad"] = float(delta_rad)
    updated["random_phase_sign"] = float(sign)
    updated["robust_mean_i"] = float(np.real(z_rot))
    updated["robust_mean_q"] = float(np.imag(z_rot))
    updated["robust_mean_abs"] = float(abs(z_rot))
    updated["robust_mean_phase"] = float(np.angle(z_rot)) if abs(z_rot) > 0 else 0.0
    return updated


def apply_random_phase(
    rows: list[dict[str, Any]],
    deltas: dict[tuple[int, int], float],
    *,
    sign: float,
) -> list[dict[str, Any]]:
    updated_rows: list[dict[str, Any]] = []
    for row in rows:
        key = (int(row["freq_index"]), int(row.get("repeat_index", 0)))
        delta = float(deltas[key])
        updated_rows.append(rotate_complex_row(row, delta, sign=sign))
    return updated_rows


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


def process_group(distance_dir: Path, group_label: str, output_root: Path, args: argparse.Namespace) -> dict[str, Any]:
    source_dir = distance_dir / group_label
    output_dir = output_root / distance_dir.name / group_label
    summary_path = output_dir / "summary.json"
    if bool(args.skip_existing) and summary_path.exists():
        return json.loads(summary_path.read_text(encoding="utf-8"))

    base_summary = json.loads((source_dir / "summary.json").read_text(encoding="utf-8"))
    initiator_rows = load_rows(source_dir / "initiator_phase_canceled_rows.json")
    reflector_rows = load_rows(source_dir / "reflector_phase_canceled_rows.json")

    group_seed = stable_group_seed(int(args.seed), distance_dir.name, group_label)
    key_union = [
        (int(row["freq_index"]), int(row.get("repeat_index", 0)))
        for row in initiator_rows + reflector_rows
        if row.get("freq_index") is not None
    ]
    delta_by_key = phase_delta_map(key_union, group_seed, args.phase_range)
    initiator_random_rows = apply_random_phase(initiator_rows, delta_by_key, sign=+1.0)
    reflector_random_rows = apply_random_phase(reflector_rows, delta_by_key, sign=-1.0)

    output_dir.mkdir(parents=True, exist_ok=True)
    save_rows_csv(initiator_random_rows, output_dir / "initiator_random_phase_rows.csv")
    save_rows_json(initiator_random_rows, output_dir / "initiator_random_phase_rows.json")
    save_rows_csv(reflector_random_rows, output_dir / "reflector_random_phase_rows.csv")
    save_rows_json(reflector_random_rows, output_dir / "reflector_random_phase_rows.json")

    delta_rows = [
        {
            "freq_index": int(freq_index),
            "repeat_index": int(repeat_index),
            "random_phase_delta_rad": float(delta),
        }
        for (freq_index, repeat_index), delta in sorted(delta_by_key.items())
    ]
    save_rows_csv(delta_rows, output_dir / "random_phase_map.csv")
    save_rows_json(delta_rows, output_dir / "random_phase_map.json")

    pair_plot_path = output_dir / "pair_phase_by_freq_random.png"
    pair_phase_rows = plot_pair_phase_by_freq(
        reflector_random_rows,
        initiator_random_rows,
        pair_plot_path,
    )
    save_pair_phase_csv(pair_phase_rows, output_dir / "pair_phase_by_freq_random.csv")
    save_pair_angle_csv(pair_phase_rows, output_dir / "pair_angle_by_freq_random.csv")
    save_rows_json(pair_phase_rows, output_dir / "pair_phase_by_freq_random.json")

    pre_cancel_segment_info = disabled_pre_cancel_segment_info(pair_phase_rows)
    pair_phase_rows_pre_filtered, initial_outlier_filter_info = reject_pair_phase_outliers(
        pair_phase_rows,
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
    save_pair_phase_csv(pair_phase_rows_for_distance, output_dir / "pair_phase_by_freq_random_distance_input.csv")
    save_pair_angle_csv(pair_phase_rows_for_distance, output_dir / "pair_angle_by_freq_random_distance_input.csv")
    save_rows_json(pair_phase_rows_for_distance, output_dir / "pair_phase_by_freq_random_distance_input.json")

    estimate_args = argparse.Namespace(
        root=distance_dir,
        pair_csv=None,
        distance_min_m=float(args.distance_min_m),
        distance_max_m=float(args.distance_max_m),
        distance_step_m=float(args.distance_step_m),
        propagation_speed_mps=float(args.propagation_speed_mps),
    )
    match_result = estimate_distance_phase_match_from_pair_rows(
        pair_phase_rows_for_distance,
        estimate_args,
        source="data_random",
    )
    match_result["plot_rows"] = build_phase_match_plot_rows(match_result, pair_phase_rows_for_distance)
    save_phase_match_plot(match_result, output_dir / "distance_spectrum_match_random.png")
    (output_dir / "distance_spectrum_match_random.json").write_text(
        json.dumps(match_result, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    base_distance = float(base_summary["distance_spectrum_match"]["distance_m"])
    random_distance = float(match_result["distance_m"])
    result = {
        "distance": distance_dir.name,
        "group": group_label,
        "random_seed": int(group_seed),
        "phase_range": str(args.phase_range),
        "phase_delta_count": int(len(delta_rows)),
        "pair_phase_point_count": int(len(pair_phase_rows)),
        "distance_input_pair_point_count": int(len(pair_phase_rows_for_distance)),
        "pre_cancel_segment_selection": pre_cancel_segment_info,
        "pre_cancel_initial_outlier_filter": initial_outlier_filter_info,
        "pre_cancel_gap_alignment": gap_alignment_info,
        "pre_cancel_outlier_filter": outlier_filter_info,
        "baseline_distance_spectrum_match_m": base_distance,
        "random_distance_spectrum_match": {
            "distance_m": random_distance,
            "delta_from_baseline_m": float(random_distance - base_distance),
            "wrapped_phase_cost": float(match_result["wrapped_phase_cost"]),
            "wrapped_phase_rms_error": float(match_result["wrapped_phase_rms_error"]),
            "wrapped_phase_max_abs_error": float(match_result["wrapped_phase_max_abs_error"]),
            "match_point_count": int(match_result["match_point_count"]),
            "valid_freq_count": int(match_result["valid_freq_count"]),
            "confidence": float(match_result["confidence"]),
            "second_best_distance_m": float(match_result["second_best_distance_m"]),
        },
    }
    summary_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    return result


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
    requested_groups = set(args.group) if args.group else None

    all_results: dict[str, Any] = {
        "input_root": str(input_root),
        "output_root": str(output_root),
        "parameters": {
            "distance_min_m": float(args.distance_min_m),
            "distance_max_m": float(args.distance_max_m),
            "distance_step_m": float(args.distance_step_m),
            "propagation_speed_mps": float(args.propagation_speed_mps),
            "seed": int(args.seed),
            "phase_range": str(args.phase_range),
            "skip_existing": bool(args.skip_existing),
        },
        "distances": {},
    }

    for distance_dir in sorted([p for p in input_root.iterdir() if p.is_dir()]):
        if requested_distances is not None and distance_dir.name not in requested_distances:
            continue
        groups = measurement_groups(distance_dir)
        if requested_groups is not None:
            groups = [group for group in groups if group in requested_groups]
        distance_result: dict[str, Any] = {"groups": {}}
        print(f"processing_distance: {distance_dir.name}")
        for group_label in groups:
            result = process_group(distance_dir, group_label, output_root, args)
            distance_result["groups"][group_label] = result
            random_result = result["random_distance_spectrum_match"]
            print(
                f"processed_group: distance={distance_dir.name}, group={group_label}, "
                f"random_match_distance_m={float(random_result['distance_m']):.3f}, "
                f"delta_from_baseline_m={float(random_result['delta_from_baseline_m']):+.6f}"
            )
        all_results["distances"][distance_dir.name] = distance_result

    summary_path = output_root / "summary_all.json"
    summary_path.write_text(json.dumps(all_results, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"saved_summary: {summary_path}")


if __name__ == "__main__":
    main()
