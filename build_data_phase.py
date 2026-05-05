#!/usr/bin/env python3
"""Collapse burst-level rows into final per-frequency phase rows for offline processing."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

from analyze_continuous_capture import average_rows_by_freq


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_INPUT_ROOT = PROJECT_ROOT / "DATA" / "DATA_burst"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "DATA" / "DATA_phase"
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
    parser = argparse.ArgumentParser(description="Build final per-frequency phase rows from DATA_burst")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--distance", action="append", default=None)
    parser.add_argument("--group", action="append", default=None)
    parser.add_argument("--skip-existing", action="store_true")
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
    if label == "calibration":
        return (-1, 0)
    if label == "measurement":
        return (0, 1)
    if label.startswith("measurement"):
        suffix = label[len("measurement") :]
        if suffix.isdigit():
            return (0, int(suffix))
    return (1, 0)


def selected_groups(distance_dir: Path) -> list[str]:
    names = [path.name for path in distance_dir.iterdir() if path.is_dir()]
    return sorted(
        [name for name in names if name == "calibration" or name.startswith("measurement")],
        key=group_sort_key,
    )


def collapse_side_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    averaged = average_rows_by_freq(rows)
    collapsed: list[dict[str, Any]] = []
    for freq_index in sorted(averaged):
        item = averaged[freq_index]
        z = complex(item["z"])
        collapsed.append(
            {
                "sequence_ok": True,
                "assigned_to_freq": True,
                "freq_index": int(item["freq_index"]),
                "repeat_index": 0,
                "freq_hz": float(item["freq_hz"]),
                "slot_kind": "freq_phase_average",
                "quality_flags": [],
                "robust_mean_i": float(z.real),
                "robust_mean_q": float(z.imag),
                "robust_mean_abs": float(item["abs"]),
                "robust_mean_phase": float(item["phase"]),
                "source_repeat_count": int(item["repeat_count"]),
            }
        )
    return collapsed


def summarize_side(rows: list[dict[str, Any]], collapsed_rows: list[dict[str, Any]]) -> dict[str, Any]:
    valid_freq_indices = sorted(int(row["freq_index"]) for row in collapsed_rows)
    missing_inside_span: list[int] = []
    if valid_freq_indices:
        freq_span = range(valid_freq_indices[0], valid_freq_indices[-1] + 1)
        present = set(valid_freq_indices)
        missing_inside_span = [freq_index for freq_index in freq_span if freq_index not in present]
    return {
        "source_row_count": int(len(rows)),
        "collapsed_freq_count": int(len(collapsed_rows)),
        "valid_freq_indices": valid_freq_indices,
        "missing_freq_indices_inside_span": missing_inside_span,
    }


def process_group(distance_dir: Path, group_label: str, output_root: Path, args: argparse.Namespace) -> dict[str, Any]:
    group_dir = distance_dir / group_label
    output_dir = output_root / distance_dir.name / group_label
    summary_path = output_dir / "summary.json"
    if bool(args.skip_existing) and summary_path.exists():
        return json.loads(summary_path.read_text(encoding="utf-8"))

    initiator_rows = load_rows(group_dir / "initiator_burst_rows.json")
    reflector_rows = load_rows(group_dir / "reflector_burst_rows.json")
    initiator_phase_rows = collapse_side_rows(initiator_rows)
    reflector_phase_rows = collapse_side_rows(reflector_rows)

    output_dir.mkdir(parents=True, exist_ok=True)
    save_rows_csv(initiator_phase_rows, output_dir / "initiator_phase_rows.csv")
    save_rows_json(initiator_phase_rows, output_dir / "initiator_phase_rows.json")
    save_rows_csv(reflector_phase_rows, output_dir / "reflector_phase_rows.csv")
    save_rows_json(reflector_phase_rows, output_dir / "reflector_phase_rows.json")

    result = {
        "distance": distance_dir.name,
        "group": group_label,
        "initiator_summary": summarize_side(initiator_rows, initiator_phase_rows),
        "reflector_summary": summarize_side(reflector_rows, reflector_phase_rows),
    }
    summary_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    return result


def distance_dirs(input_root: Path, selected: list[str] | None) -> list[Path]:
    dirs = [path for path in input_root.iterdir() if path.is_dir()]
    if selected:
        chosen = set(selected)
        dirs = [path for path in dirs if path.name in chosen]
    return sorted(dirs, key=lambda path: path.name)


def main() -> None:
    parser = build_argument_parser()
    config = load_config(DEFAULT_CONFIG_PATH)
    apply_config_defaults(parser, config)
    args = parser.parse_args()

    input_root = args.input_root if args.input_root.is_absolute() else (PROJECT_ROOT / args.input_root).resolve()
    output_root = args.output_root if args.output_root.is_absolute() else (PROJECT_ROOT / args.output_root).resolve()
    if not input_root.exists():
        raise SystemExit(f"input root not found: {input_root}")

    results: dict[str, Any] = {
        "input_root": str(input_root),
        "output_root": str(output_root),
        "distances": {},
    }

    selected_distance_names = list(args.distance) if args.distance else None
    selected_group_names = set(args.group) if args.group else None

    for distance_dir in distance_dirs(input_root, selected_distance_names):
        groups = selected_groups(distance_dir)
        if selected_group_names is not None:
            groups = [group for group in groups if group in selected_group_names]
        if not groups:
            continue

        distance_result: dict[str, Any] = {"groups": {}}
        print(f"processing_distance: {distance_dir.name}")
        for group_label in groups:
            print(f"  processing_group: {group_label}")
            distance_result["groups"][group_label] = process_group(distance_dir, group_label, output_root, args)
        results["distances"][distance_dir.name] = distance_result

    output_root.mkdir(parents=True, exist_ok=True)
    summary_path = output_root / "summary_all.json"
    summary_path.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"saved_summary: {summary_path}")


if __name__ == "__main__":
    main()
