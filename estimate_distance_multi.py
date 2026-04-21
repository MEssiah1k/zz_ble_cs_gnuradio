#!/usr/bin/env python3
"""用 1 对多数据的目标匹配扫描估计各 reflector 距离。"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

import numpy as np


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_ROOT = PROJECT_ROOT / "self_2"
DIR_INITIATOR_RX = "data_initiator_rx_from_reflectors"
REFLECTOR_DIRS = {
    "reflector1": "data_reflector1_rx_from_initiator",
    "reflector2": "data_reflector2_rx_from_initiator",
}
NEW_STYLE_RE = re.compile(r"^data_f(?P<freq>\d+)_r(?P<repeat>\d+)$")
SPEED_OF_LIGHT = 299792458.0
DEFAULT_GRID_STEP_M = 0.01


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
        by_freq.setdefault(freq_index, []).append(
            {
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
        )

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


def propagation_delay(distance_m: float, round_trip: bool = True) -> float:
    factor = 2.0 if round_trip else 1.0
    return factor * float(distance_m) / SPEED_OF_LIGHT


def apply_target_compensation(
    response: np.ndarray,
    freqs_hz: np.ndarray,
    distance_m: float,
    round_trip: bool = True,
) -> np.ndarray:
    tau = propagation_delay(distance_m, round_trip=round_trip)
    return np.asarray(response, dtype=np.complex128) * np.exp(1j * 2.0 * np.pi * freqs_hz * tau)


def projection_score(compensated: np.ndarray) -> float:
    coherent_power = np.abs(np.mean(compensated)) ** 2
    total_power = np.mean(np.abs(compensated) ** 2) + 1e-12
    return float(coherent_power / total_power)


def adjacent_phase_score(compensated: np.ndarray) -> float:
    if compensated.size < 2:
        return 0.0
    deltas = compensated[1:] * np.conj(compensated[:-1])
    normalized = deltas / (np.abs(deltas) + 1e-12)
    return float(np.abs(np.mean(normalized)))


def composite_score(compensated: np.ndarray) -> tuple[float, float, float]:
    projection = projection_score(compensated)
    adjacent = adjacent_phase_score(compensated)
    return 0.8 * projection + 0.2 * adjacent, projection, adjacent


def peak_diagnostics(distance_grid: np.ndarray, scores: np.ndarray, best_idx: int) -> dict[str, float]:
    best_score = float(scores[best_idx])
    if len(scores) <= 1:
        return {
            "best_score": best_score,
            "second_best_score": 0.0,
            "peak_margin": best_score,
            "peak_ratio": best_score / 1e-12,
            "confidence": 1.0,
        }

    step = float(abs(distance_grid[1] - distance_grid[0])) if len(distance_grid) > 1 else DEFAULT_GRID_STEP_M
    exclude_radius = max(0.25, 2.0 * step)
    neighbor_mask = np.abs(distance_grid - distance_grid[best_idx]) <= exclude_radius
    candidate_scores = scores[~neighbor_mask]
    second_best = float(np.max(candidate_scores)) if candidate_scores.size else 0.0
    peak_margin = best_score - second_best
    return {
        "best_score": best_score,
        "second_best_score": second_best,
        "peak_margin": float(peak_margin),
        "peak_ratio": float(best_score / (second_best + 1e-12)),
        "confidence": float(peak_margin / (best_score + 1e-12)),
    }


def scan_distance(
    response: np.ndarray,
    freqs_hz: np.ndarray,
    distance_grid: np.ndarray,
    score_mode: str,
) -> dict[str, Any]:
    composite_scores: list[float] = []
    projection_scores: list[float] = []
    adjacent_scores: list[float] = []

    for distance_m in distance_grid:
        compensated = apply_target_compensation(response, freqs_hz, float(distance_m), round_trip=True)
        composite, projection, adjacent = composite_score(compensated)
        composite_scores.append(composite)
        projection_scores.append(projection)
        adjacent_scores.append(adjacent)

    score_map = {
        "composite": np.asarray(composite_scores, dtype=float),
        "projection": np.asarray(projection_scores, dtype=float),
        "adjacent": np.asarray(adjacent_scores, dtype=float),
    }
    if score_mode not in score_map:
        raise SystemExit(f"不支持的 score_mode: {score_mode}")

    scores = score_map[score_mode]
    best_idx = int(np.argmax(scores))
    result = {
        "distance_m": float(distance_grid[best_idx]),
        "score_mode": score_mode,
        "scores": scores,
        "distance_grid": distance_grid,
        "composite_scores": score_map["composite"],
        "projection_scores": score_map["projection"],
        "adjacent_scores": score_map["adjacent"],
    }
    result.update(peak_diagnostics(distance_grid, scores, best_idx))
    return result


def build_response(
    local_z: complex,
    peer_z: complex,
    match_mode: str,
) -> complex:
    if match_mode == "multiply":
        return local_z * peer_z
    if match_mode == "conj":
        return local_z * np.conj(peer_z)
    raise SystemExit(f"不支持的 match_mode: {match_mode}")


def estimate_one_reflector(
    label: str,
    local_avg: dict[int, dict[str, Any]],
    peer_avg: dict[int, dict[str, Any]],
    args: argparse.Namespace,
) -> dict[str, Any]:
    common_freq_indices = sorted(set(local_avg) & set(peer_avg))
    if len(common_freq_indices) < 2:
        raise SystemExit(f"{label}: 有效公共频点少于 2 个，无法估计距离")

    rows: list[dict[str, Any]] = []
    freqs_hz: list[float] = []
    response_values: list[complex] = []

    for freq_index in common_freq_indices:
        f_offset_hz = args.start_offset_hz + freq_index * args.step_hz
        f_hz = args.center_freq_hz + f_offset_hz
        response = build_response(local_avg[freq_index]["z"], peer_avg[freq_index]["z"], args.match_mode)

        freqs_hz.append(float(f_hz))
        response_values.append(response)
        rows.append(
            {
                "freq_index": freq_index,
                "freq_hz": float(f_hz),
                "local_valid_repeats": local_avg[freq_index]["valid_repeat_count"],
                "peer_valid_repeats": peer_avg[freq_index]["valid_repeat_count"],
                "local_i": float(np.real(local_avg[freq_index]["z"])),
                "local_q": float(np.imag(local_avg[freq_index]["z"])),
                "peer_i": float(np.real(peer_avg[freq_index]["z"])),
                "peer_q": float(np.imag(peer_avg[freq_index]["z"])),
                "response_i": float(np.real(response)),
                "response_q": float(np.imag(response)),
                "response_abs": float(abs(response)),
                "response_phase": float(np.angle(response)),
            }
        )

    freqs = np.asarray(freqs_hz, dtype=float)
    response_arr = np.asarray(response_values, dtype=np.complex128)
    distance_grid = np.arange(args.grid_start_m, args.grid_stop_m + 0.5 * args.grid_step_m, args.grid_step_m, dtype=float)
    scan = scan_distance(response_arr, freqs, distance_grid, args.score_mode)

    return {
        "label": label,
        "valid_freq_count": len(rows),
        "distance_m": scan["distance_m"],
        "score_mode": scan["score_mode"],
        "best_score": scan["best_score"],
        "second_best_score": scan["second_best_score"],
        "peak_margin": scan["peak_margin"],
        "peak_ratio": scan["peak_ratio"],
        "confidence": scan["confidence"],
        "rows": rows,
        "distance_grid": scan["distance_grid"].tolist(),
        "scores": scan["scores"].tolist(),
        "composite_scores": scan["composite_scores"].tolist(),
        "projection_scores": scan["projection_scores"].tolist(),
        "adjacent_scores": scan["adjacent_scores"].tolist(),
    }


def estimate_distance_multi(args: argparse.Namespace) -> dict[str, Any]:
    root = resolve_root(args.root)
    local_dir = root / args.local_dir
    local_avg = average_by_freq(
        collect_repeats(local_dir, args.outlier_mad_scale, args.min_abs),
        args.min_valid_repeats,
    )

    reflectors: dict[str, Any] = {}
    for label, dir_name in REFLECTOR_DIRS.items():
        peer_dir = root / dir_name
        peer_avg = average_by_freq(
            collect_repeats(peer_dir, args.outlier_mad_scale, args.min_abs),
            args.min_valid_repeats,
        )
        reflectors[label] = estimate_one_reflector(label, local_avg, peer_avg, args)

    return {
        "root": str(root),
        "local_dir": args.local_dir,
        "reflector_dirs": REFLECTOR_DIRS,
        "center_freq_hz": float(args.center_freq_hz),
        "start_offset_hz": float(args.start_offset_hz),
        "step_hz": float(args.step_hz),
        "match_mode": args.match_mode,
        "score_mode": args.score_mode,
        "grid_start_m": float(args.grid_start_m),
        "grid_stop_m": float(args.grid_stop_m),
        "grid_step_m": float(args.grid_step_m),
        "reflectors": reflectors,
    }


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="用 1 对多复数匹配扫描估计各 reflector 距离")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT, help="实验根目录，例如 self_2 或 1to2")
    parser.add_argument("--local-dir", default=DIR_INITIATOR_RX, help="initiator 收到多个 reflector 叠加信号的数据目录名")
    parser.add_argument("--center-freq-hz", type=float, default=2.44e9, help="真实等效中心频率")
    parser.add_argument("--start-offset-hz", type=float, default=-40e6, help="freq_index=0 对应的频率偏移")
    parser.add_argument("--step-hz", type=float, default=1e6, help="相邻 freq_index 的频率步进")
    parser.add_argument("--min-valid-repeats", type=int, default=2, help="每个目录每个频点至少需要多少次有效重复")
    parser.add_argument("--min-abs", type=float, default=0.5, help="单次重复平均复数幅度低于该值则判为无效")
    parser.add_argument("--outlier-mad-scale", type=float, default=8.0, help="IQ 离群点 MAD 阈值倍率")
    parser.add_argument("--grid-start-m", type=float, default=0.0, help="距离扫描起点")
    parser.add_argument("--grid-stop-m", type=float, default=30.0, help="距离扫描终点")
    parser.add_argument("--grid-step-m", type=float, default=DEFAULT_GRID_STEP_M, help="距离扫描步进")
    parser.add_argument("--score-mode", choices=("composite", "projection", "adjacent"), default="composite", help="距离扫描打分方式")
    parser.add_argument("--match-mode", choices=("multiply", "conj"), default="multiply", help="local 与 peer 的组合方式；GNU Radio 数据默认 multiply")
    parser.add_argument("--save-json", type=Path, default=None, help="保存完整估计结果到 JSON 文件")
    return parser


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()
    if args.grid_step_m <= 0:
        raise SystemExit("--grid-step-m 必须大于 0")
    if args.grid_stop_m < args.grid_start_m:
        raise SystemExit("--grid-stop-m 必须大于等于 --grid-start-m")
    result = estimate_distance_multi(args)

    for label, row in result["reflectors"].items():
        print(json.dumps(
            {
                "reflector": label,
                "valid_freq_count": row["valid_freq_count"],
                "distance_m": row["distance_m"],
                "best_score": row["best_score"],
                "peak_margin": row["peak_margin"],
                "confidence": row["confidence"],
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
