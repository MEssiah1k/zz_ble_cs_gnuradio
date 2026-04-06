#!/usr/bin/env python3
"""批量检查 GNU Radio 实验目录下四个 data_store 生成的 gr_complex 二进制文件。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np


BYTES_PER_GR_COMPLEX = 8
PROJECT_ROOT = Path("/home/mess1ah/zz_ble_cs_gnuradio")
DEFAULT_ROOT = PROJECT_ROOT / "self"
PAIR_MAPPINGS = {
    "reflector": (
        "data_reflector_rx_from_initiator",
        "data_reflector_ref_local",
    ),
    "initiator": (
        "data_initiator_rx_from_reflector",
        "data_initiator_ref_local",
    ),
}


def discover_data_dirs(root: Path) -> list[Path]:
    """自动发现实验目录下的 data_* 文件夹。"""
    return sorted([p for p in root.iterdir() if p.is_dir() and p.name.startswith("data_")])


def load_gr_complex_bin(path: Path) -> np.ndarray:
    return np.fromfile(path, dtype=np.complex64)


def resolve_root(root: Path) -> Path:
    if root.is_absolute():
        return root
    return (PROJECT_ROOT / root).resolve()


def list_bin_files(dir_path: Path) -> list[Path]:
    return sorted(dir_path.glob("data_*.bin"), key=lambda p: int(p.stem.split("_")[-1]))


def validate_bin_layout(path: Path) -> dict[str, Any]:
    result: dict[str, Any] = {
        "path": str(path),
        "exists": path.exists(),
        "file_size_bytes": 0,
        "is_multiple_of_8": False,
        "samples": 0,
        "ok": False,
        "error": "",
    }
    if not path.exists():
        result["error"] = "file_not_found"
        return result

    size = path.stat().st_size
    result["file_size_bytes"] = size
    result["is_multiple_of_8"] = (size % BYTES_PER_GR_COMPLEX == 0)
    if not result["is_multiple_of_8"]:
        result["error"] = "file_size_not_multiple_of_8"
        return result

    try:
        x = load_gr_complex_bin(path)
    except Exception as exc:
        result["error"] = f"read_failed: {exc}"
        return result

    result["samples"] = int(x.size)
    result["ok"] = True
    return result


def classify_signal(x: np.ndarray) -> str:
    if x.size == 0:
        return "empty"
    amp = np.abs(x)
    mean_abs = float(np.mean(amp))
    if mean_abs < 1e-9:
        return "all_zero_or_invalid"

    normalized = x / (amp + 1e-12)
    coherent = float(np.abs(np.mean(normalized)))
    if coherent > 0.98:
        return "stable_cluster"

    phase = np.unwrap(np.angle(x))
    if x.size >= 8:
        idx = np.arange(x.size, dtype=float)
        slope, intercept = np.polyfit(idx, phase, 1)
        residual = phase - (slope * idx + intercept)
        if float(np.std(residual)) < 0.5:
            return "rotating_tone"

    return "noisy_or_misaligned"


def summarize_complex_signal(x: np.ndarray) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "samples": int(x.size),
        "nonzero_samples": 0,
        "has_nan": False,
        "has_inf": False,
        "mean_abs": 0.0,
        "max_abs": 0.0,
        "mean_power": 0.0,
        "mean_phase": 0.0,
        "phase_std": 0.0,
        "classification": "empty",
    }
    if x.size == 0:
        return summary

    amp = np.abs(x)
    phase = np.angle(x)
    summary.update(
        {
            "nonzero_samples": int(np.count_nonzero(amp > 1e-12)),
            "has_nan": bool(np.isnan(x.real).any() or np.isnan(x.imag).any()),
            "has_inf": bool(np.isinf(x.real).any() or np.isinf(x.imag).any()),
            "mean_abs": float(np.mean(amp)),
            "max_abs": float(np.max(amp)),
            "mean_power": float(np.mean(amp ** 2)),
            "mean_phase": float(np.angle(np.mean(x))) if np.any(amp > 1e-12) else 0.0,
            "phase_std": float(np.std(phase)),
            "classification": classify_signal(x),
        }
    )
    return summary


def scan_directory(dir_path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    if not dir_path.exists():
        return records

    for file_path in list_bin_files(dir_path):
        layout = validate_bin_layout(file_path)
        flat = {
            "path": str(file_path),
            "directory": dir_path.name,
            "file": file_path.name,
            "ok": layout["ok"],
            "file_size_bytes": layout["file_size_bytes"],
            "samples": layout["samples"],
        }
        if layout["ok"]:
            x = load_gr_complex_bin(file_path)
            summary = summarize_complex_signal(x)
            flat.update(
                {
                    "mean_abs": summary["mean_abs"],
                    "max_abs": summary["max_abs"],
                    "mean_power": summary["mean_power"],
                    "mean_phase": summary["mean_phase"],
                    "phase_std": summary["phase_std"],
                    "classification": summary["classification"],
                }
            )
        else:
            flat["error"] = layout["error"]
        records.append(flat)
    return records


def check_pair(rx_dir: Path, ref_dir: Path) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    rx_files = {p.name: p for p in list_bin_files(rx_dir)}
    ref_files = {p.name: p for p in list_bin_files(ref_dir)}
    all_names = sorted(set(rx_files) | set(ref_files), key=lambda name: int(Path(name).stem.split("_")[-1]))

    for name in all_names:
        rx_path = rx_files.get(name)
        ref_path = ref_files.get(name)
        item: dict[str, Any] = {
            "file": name,
            "rx_exists": rx_path is not None,
            "ref_exists": ref_path is not None,
        }
        if rx_path is None or ref_path is None:
            item["pair_ok"] = False
            item["reason"] = "missing_counterpart"
            results.append(item)
            continue

        rx_layout = validate_bin_layout(rx_path)
        ref_layout = validate_bin_layout(ref_path)
        item["rx_samples"] = rx_layout["samples"]
        item["ref_samples"] = ref_layout["samples"]
        item["pair_ok"] = bool(rx_layout["ok"] and ref_layout["ok"] and rx_layout["samples"] == ref_layout["samples"])
        item["reason"] = "ok" if item["pair_ok"] else "length_or_layout_mismatch"
        results.append(item)
    return results


def print_directory_report(records: list[dict[str, Any]]) -> None:
    if not records:
        print("directory is empty or does not exist")
        return
    for row in records:
        brief = {
            "file": row["file"],
            "ok": row["ok"],
            "samples": row["samples"],
            "classification": row.get("classification", ""),
            "mean_abs": row.get("mean_abs", 0.0),
        }
        print(json.dumps(brief, ensure_ascii=False))


def print_pair_report(records: list[dict[str, Any]]) -> None:
    for row in records:
        print(json.dumps(row, ensure_ascii=False))


def save_json(data: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="批量检查 GNU Radio 实验目录下四个 data_store 文件夹")
    parser.add_argument(
        "--root",
        type=Path,
        default=DEFAULT_ROOT,
        help="实验根目录，支持相对路径，例如 self、1to1、1to2",
    )
    parser.add_argument("--save-json", action="store_true", help="是否保存 json 摘要")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "output_check_bin",
        help="json 摘要输出目录",
    )
    return parser


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()
    args.root = resolve_root(args.root)
    args.output_dir = args.output_dir.resolve()

    if not args.root.exists():
        raise SystemExit(f"实验目录不存在: {args.root}")

    all_reports: dict[str, Any] = {
        "root": str(args.root),
        "directories": {},
        "pairs": {},
    }

    data_dirs = discover_data_dirs(args.root)
    for dir_path in data_dirs:
        all_reports["directories"][dir_path.name] = scan_directory(dir_path)

    for _, (rx_name, ref_name) in PAIR_MAPPINGS.items():
        rx_dir = args.root / rx_name
        ref_dir = args.root / ref_name
        if rx_dir.exists() and ref_dir.exists():
            all_reports["pairs"][rx_name] = check_pair(rx_dir, ref_dir)

    print(f"== root: {args.root} ==")
    for name, records in all_reports["directories"].items():
        print(f"== directory: {name} ==")
        print_directory_report(records)
    for name, records in all_reports["pairs"].items():
        print(f"== pair: {name} ==")
        print_pair_report(records)

    if args.save_json:
        save_json(all_reports, args.output_dir / args.root.name / "full_summary.json")


if __name__ == "__main__":
    main()
