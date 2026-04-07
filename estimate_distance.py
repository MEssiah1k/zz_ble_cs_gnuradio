#!/usr/bin/env python3
"""用双向同频点相位乘积估计距离。"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

import numpy as np


PROJECT_ROOT = Path("/home/mess1ah/zz_ble_cs_gnuradio")
DEFAULT_ROOT = PROJECT_ROOT / "self"
DIR_INITIATOR_RX = "data_initiator_rx_from_reflector"
DIR_REFLECTOR_RX = "data_reflector_rx_from_initiator"
NEW_STYLE_RE = re.compile(r"^data_f(?P<freq>\d+)_r(?P<repeat>\d+)$")
SPEED_OF_LIGHT = 299792458.0


def resolve_root(root: Path) -> Path:
    if root.is_absolute():
        return root
    return (PROJECT_ROOT / root).resolve()


def parse_file_tokens(path: Path) -> tuple[int, int] | None:
    match = NEW_STYLE_RE.match(path.stem)
    if not match:
        return None
    return int(match.group("freq")), int(match.group("repeat"))


def load_gr_complex_bin(path: Path) -> np.ndarray:
    return np.fromfile(path, dtype=np.complex64)


def robust_iq_mean(x: np.ndarray, outlier_mad_scale: float) -> tuple[complex, int, int]:
    """剔除明显离群 IQ 点后返回平均复数、保留数、离群数。"""
    if x.size == 0:
        return 0j, 0, 0

    finite_mask = np.isfinite(x.real) & np.isfinite(x.imag)
    finite = x[finite_mask]
    if finite.size == 0:
        return 0j, 0, int(x.size)

    center = np.median(finite.real) + 1j * np.median(finite.imag)
    radius = np.abs(finite - center)
    median_radius = float(np.median(radius))
    mad_radius = float(np.median(np.abs(radius - median_radius)))

    if mad_radius <= 1e-12:
        threshold = median_radius + 1e-9
    else:
        threshold = median_radius + outlier_mad_scale * mad_radius

    valid = finite[radius <= threshold]
    if valid.size == 0:
        valid = finite

    return complex(np.mean(valid)), int(valid.size), int(x.size - valid.size)


def collect_repeats(
    dir_path: Path,
    outlier_mad_scale: float,
    min_abs: float,
) -> dict[int, list[dict[str, Any]]]:
    by_freq: dict[int, list[dict[str, Any]]] = {}
    if not dir_path.exists():
        raise SystemExit(f"目录不存在: {dir_path}")

    for file_path in sorted(dir_path.glob("data_f*_r*.bin")):
        tokens = parse_file_tokens(file_path)
        if tokens is None:
            continue
        freq_index, repeat_index = tokens
        x = load_gr_complex_bin(file_path)
        z, robust_samples, outlier_samples = robust_iq_mean(x, outlier_mad_scale)
        record = {
            "file": file_path.name,
            "freq_index": freq_index,
            "repeat_index": repeat_index,
            "z": z,
            "abs": float(abs(z)),
            "phase": float(np.angle(z)) if abs(z) > 0 else 0.0,
            "samples": int(x.size),
            "robust_samples": robust_samples,
            "outlier_samples": outlier_samples,
            "valid": bool(x.size > 0 and robust_samples > 0 and abs(z) >= min_abs),
        }
        by_freq.setdefault(freq_index, []).append(record)

    return by_freq


def average_by_freq(
    repeats_by_freq: dict[int, list[dict[str, Any]]],
    min_valid_repeats: int,
) -> dict[int, dict[str, Any]]:
    averaged: dict[int, dict[str, Any]] = {}
    for freq_index, records in repeats_by_freq.items():
        valid_records = [row for row in records if row["valid"]]
        if len(valid_records) < min_valid_repeats:
            continue

        z_values = np.array([row["z"] for row in valid_records], dtype=np.complex128)
        z_bar = complex(np.mean(z_values))
        averaged[freq_index] = {
            "freq_index": freq_index,
            "z": z_bar,
            "valid_repeat_count": len(valid_records),
            "total_repeat_count": len(records),
            "outlier_samples": int(sum(row["outlier_samples"] for row in valid_records)),
            "abs": float(abs(z_bar)),
            "phase": float(np.angle(z_bar)),
        }

    return averaged


def estimate_distance(args: argparse.Namespace) -> dict[str, Any]:
    root = resolve_root(args.root)
    initiator_dir = root / DIR_INITIATOR_RX
    reflector_dir = root / DIR_REFLECTOR_RX

    initiator_repeats = collect_repeats(initiator_dir, args.outlier_mad_scale, args.min_abs)
    reflector_repeats = collect_repeats(reflector_dir, args.outlier_mad_scale, args.min_abs)

    initiator_avg = average_by_freq(initiator_repeats, args.min_valid_repeats)
    reflector_avg = average_by_freq(reflector_repeats, args.min_valid_repeats)
    common_freq_indices = sorted(set(initiator_avg) & set(reflector_avg))
    if len(common_freq_indices) < 2:
        raise SystemExit("有效公共频点少于 2 个，无法拟合距离")

    rows: list[dict[str, Any]] = []
    freqs_hz: list[float] = []
    pair_phase_wrapped: list[float] = []

    for freq_index in common_freq_indices:
        f_offset_hz = args.start_offset_hz + freq_index * args.step_hz
        f_hz = args.center_freq_hz + f_offset_hz
        z_pair = initiator_avg[freq_index]["z"] * reflector_avg[freq_index]["z"]

        freqs_hz.append(float(f_hz))
        pair_phase_wrapped.append(float(np.angle(z_pair)))
        rows.append(
            {
                "freq_index": freq_index,
                "freq_hz": float(f_hz),
                "initiator_valid_repeats": initiator_avg[freq_index]["valid_repeat_count"],
                "reflector_valid_repeats": reflector_avg[freq_index]["valid_repeat_count"],
                "pair_i": float(np.real(z_pair)),
                "pair_q": float(np.imag(z_pair)),
                "pair_abs": float(abs(z_pair)),
                "phase_wrapped": float(np.angle(z_pair)),
            }
        )

    freqs = np.array(freqs_hz, dtype=float)
    phases_wrapped = np.array(pair_phase_wrapped, dtype=float)
    phases_unwrapped = np.unwrap(phases_wrapped)
    slope, intercept = np.polyfit(freqs, phases_unwrapped, 1)
    fitted = slope * freqs + intercept
    residual = phases_unwrapped - fitted
    distance_m = -SPEED_OF_LIGHT * float(slope) / (4.0 * np.pi)

    for row, phase_unwrapped, phase_residual in zip(rows, phases_unwrapped, residual):
        row["phase_unwrapped"] = float(phase_unwrapped)
        row["phase_residual"] = float(phase_residual)

    return {
        "root": str(root),
        "center_freq_hz": float(args.center_freq_hz),
        "start_offset_hz": float(args.start_offset_hz),
        "step_hz": float(args.step_hz),
        "valid_freq_count": len(rows),
        "distance_m": distance_m,
        "slope_rad_per_hz": float(slope),
        "intercept_rad": float(intercept),
        "rms_phase_residual": float(np.sqrt(np.mean(residual ** 2))),
        "max_abs_phase_residual": float(np.max(np.abs(residual))),
        "rows": rows,
    }


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="用双向同频点相位乘积估计距离")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT, help="实验根目录，例如 self")
    parser.add_argument("--center-freq-hz", type=float, default=2.44e9, help="真实等效中心频率")
    parser.add_argument("--start-offset-hz", type=float, default=-40e6, help="freq_index=0 对应的频率偏移")
    parser.add_argument("--step-hz", type=float, default=1e6, help="相邻 freq_index 的频率步进")
    parser.add_argument("--min-valid-repeats", type=int, default=2, help="每个方向每个频点至少需要多少次有效重复")
    parser.add_argument("--min-abs", type=float, default=0.5, help="单次重复平均复数幅度低于该值则判为无效")
    parser.add_argument("--outlier-mad-scale", type=float, default=8.0, help="IQ 离群点 MAD 阈值倍率")
    parser.add_argument("--save-json", type=Path, default=None, help="保存完整估计结果到 JSON 文件")
    return parser


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()
    result = estimate_distance(args)

    print(json.dumps(
        {
            "root": result["root"],
            "valid_freq_count": result["valid_freq_count"],
            "distance_m": result["distance_m"],
            "slope_rad_per_hz": result["slope_rad_per_hz"],
            "rms_phase_residual": result["rms_phase_residual"],
            "max_abs_phase_residual": result["max_abs_phase_residual"],
        },
        ensure_ascii=False,
    ))

    if args.save_json is not None:
        out_path = args.save_json.resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"saved: {out_path}")


if __name__ == "__main__":
    main()
