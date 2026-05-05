#!/usr/bin/env python3
"""Build measurement-minus-calibration phase-canceled results from burst data."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

from analyze_continuous_capture import (
    align_phase_segments_across_missing_freqs,
    build_phase_canceled_rows,
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
DEFAULT_INPUT_ROOT = PROJECT_ROOT / "DATA" / "DATA_phase"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "DATA" / "DATA_minus"
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "continuous_capture_config.json"


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
    parser = argparse.ArgumentParser(description="Build pre-cancel DATA_minus results from DATA_phase")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--distance", action="append", default=None)
    parser.add_argument("--group", action="append", default=None, help="Only process selected measurement group(s)")
    parser.add_argument("--distance-min-m", type=float, default=0.0)
    parser.add_argument("--distance-max-m", type=float, default=30.0)
    parser.add_argument("--distance-step-m", type=float, default=0.01)
    parser.add_argument("--propagation-speed-mps", type=float, default=2.3e8)
    parser.add_argument("--skip-existing", action="store_true")
    return parser


def load_rows(path: Path) -> list[dict[str, Any]]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_side_rows(group_dir: Path, side_name: str) -> list[dict[str, Any]]:
    phase_path = group_dir / f"{side_name}_phase_rows.json"
    if phase_path.exists():
        return load_rows(phase_path)
    burst_path = group_dir / f"{side_name}_burst_rows.json"
    if burst_path.exists():
        return load_rows(burst_path)
    raise SystemExit(f"missing input rows for {side_name}: {group_dir}")


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


def measurement_groups(distance_dir: Path) -> list[str]:
    groups = [path.name for path in distance_dir.iterdir() if path.is_dir()]
    return sorted([name for name in groups if name.startswith("measurement")], key=group_sort_key)


def group_sort_key(label: str) -> tuple[int, int]:
    if label == "measurement":
        return (0, 1)
    if label.startswith("measurement"):
        suffix = label[len("measurement") :]
        if suffix.isdigit():
            return (0, int(suffix))
    if label == "calibration":
        return (-1, 0)
    return (1, 0)


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
        model_phase = float(-4.0 * 3.141592653589793 * freq_hz * best_distance_m / propagation_speed_mps + phase0_rad)
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
    calibration_dir = distance_dir / "calibration"
    measurement_dir = distance_dir / group_label
    output_dir = output_root / distance_dir.name / group_label
    summary_path = output_dir / "summary.json"
    if bool(args.skip_existing) and summary_path.exists():
        return json.loads(summary_path.read_text(encoding="utf-8"))

    calibration_initiator = load_side_rows(calibration_dir, "initiator")
    calibration_reflector = load_side_rows(calibration_dir, "reflector")
    measurement_initiator = load_side_rows(measurement_dir, "initiator")
    measurement_reflector = load_side_rows(measurement_dir, "reflector")

    initiator_canceled_rows, initiator_stats = build_phase_canceled_rows(
        measurement_initiator,
        calibration_initiator,
        side_name="initiator",
    )
    reflector_canceled_rows, reflector_stats = build_phase_canceled_rows(
        measurement_reflector,
        calibration_reflector,
        side_name="reflector",
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    save_rows_csv(initiator_canceled_rows, output_dir / "initiator_phase_canceled_rows.csv")
    save_rows_json(initiator_canceled_rows, output_dir / "initiator_phase_canceled_rows.json")
    save_rows_csv(reflector_canceled_rows, output_dir / "reflector_phase_canceled_rows.csv")
    save_rows_json(reflector_canceled_rows, output_dir / "reflector_phase_canceled_rows.json")

    pair_plot_path = output_dir / "pair_phase_by_freq_pre_cancel.png"
    pair_phase_rows = plot_pair_phase_by_freq(
        reflector_canceled_rows,
        initiator_canceled_rows,
        pair_plot_path,
    )
    save_pair_phase_csv(pair_phase_rows, output_dir / "pair_phase_by_freq_pre_cancel.csv")
    save_pair_angle_csv(pair_phase_rows, output_dir / "pair_angle_by_freq_pre_cancel.csv")
    save_rows_json(pair_phase_rows, output_dir / "pair_phase_by_freq_pre_cancel.json")

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
    save_pair_phase_csv(pair_phase_rows_for_distance, output_dir / "pair_phase_by_freq_pre_cancel_distance_input.csv")
    save_pair_angle_csv(pair_phase_rows_for_distance, output_dir / "pair_angle_by_freq_pre_cancel_distance_input.csv")
    save_rows_json(pair_phase_rows_for_distance, output_dir / "pair_phase_by_freq_pre_cancel_distance_input.json")

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
        source="data_minus",
    )
    match_result["plot_rows"] = build_phase_match_plot_rows(match_result, pair_phase_rows_for_distance)
    save_phase_match_plot(match_result, output_dir / "distance_spectrum_match.png")
    (output_dir / "distance_spectrum_match.json").write_text(
        json.dumps(match_result, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    result = {
        "distance": distance_dir.name,
        "group": group_label,
        "initiator_phase_canceled_summary": initiator_stats,
        "reflector_phase_canceled_summary": reflector_stats,
        "pair_phase_point_count": int(len(pair_phase_rows)),
        "distance_input_pair_point_count": int(len(pair_phase_rows_for_distance)),
        "pre_cancel_segment_selection": pre_cancel_segment_info,
        "pre_cancel_initial_outlier_filter": initial_outlier_filter_info,
        "pre_cancel_gap_alignment": gap_alignment_info,
        "pre_cancel_outlier_filter": outlier_filter_info,
        "distance_spectrum_match": {
            "distance_m": float(match_result["distance_m"]),
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
            "skip_existing": bool(args.skip_existing),
        },
        "distances": {},
    }

    for distance_dir in sorted([p for p in input_root.iterdir() if p.is_dir()]):
        if requested_distances is not None and distance_dir.name not in requested_distances:
            continue
        calibration_dir = distance_dir / "calibration"
        if not calibration_dir.exists():
            continue
        groups = measurement_groups(distance_dir)
        if requested_groups is not None:
            groups = [group for group in groups if group in requested_groups]
        distance_result: dict[str, Any] = {"groups": {}}
        print(f"processing_distance: {distance_dir.name}")
        for group_label in groups:
            result = process_group(distance_dir, group_label, output_root, args)
            distance_result["groups"][group_label] = result
            print(
                f"processed_group: distance={distance_dir.name}, group={group_label}, "
                f"match_distance_m={result['distance_spectrum_match']['distance_m']:.3f}, "
                f"match_points={result['distance_spectrum_match']['match_point_count']}"
            )
        all_results["distances"][distance_dir.name] = distance_result

    summary_path = output_root / "summary_all.json"
    summary_path.write_text(json.dumps(all_results, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"saved_summary: {summary_path}")


if __name__ == "__main__":
    main()
