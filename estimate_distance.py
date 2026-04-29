#!/usr/bin/env python3
"""用双向同频点相位乘积估计距离。"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from check_bin import circular_phase_spread_rad, classify_signal, phase_cluster_stats
from analyze_continuous_capture import (
    DEFAULT_CONFIG_PATH,
    apply_config_defaults,
    load_config,
    select_config_for_root,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_ROOT = PROJECT_ROOT / "self"
DEFAULT_PLOT_DIR = PROJECT_ROOT / "output_estimate_plot"
DIR_INITIATOR_RX = "data_initiator_rx_from_reflector"
DIR_REFLECTOR_RX = "data_reflector_rx_from_initiator"
NEW_STYLE_RE = re.compile(r"^data_f(?P<freq>\d+)_r(?P<repeat>\d+)$")
SPEED_OF_LIGHT = 299792458.0
TWO_PI = 2.0 * np.pi


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


def cli_root_was_provided() -> bool:
    return any(arg == "--root" or arg.startswith("--root=") for arg in sys.argv[1:])


def unwrap_with_negative_slope_prior(
    phases_wrapped: np.ndarray,
    *,
    upward_tolerance_rad: float = 0.2,
) -> np.ndarray:
    phases = np.asarray(phases_wrapped, dtype=float)
    if phases.size <= 1:
        return phases.copy()

    unwrapped = np.empty_like(phases)
    unwrapped[0] = phases[0]
    for idx in range(1, phases.size):
        value = float(phases[idx])
        prev = float(unwrapped[idx - 1])
        while value > prev + float(upward_tolerance_rad):
            value -= TWO_PI
        unwrapped[idx] = value
    return unwrapped


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


def coherent_score(x: np.ndarray) -> float:
    """返回相位相干度，越接近 1 越像 stable_cluster。"""
    if x.size == 0:
        return 0.0

    finite_mask = np.isfinite(x.real) & np.isfinite(x.imag)
    finite = x[finite_mask]
    if finite.size == 0:
        return 0.0

    amp = np.abs(finite)
    if float(np.mean(amp)) < 1e-12:
        return 0.0

    normalized = finite / (amp + 1e-12)
    return float(np.abs(np.mean(normalized)))


def collect_repeats(
    dir_path: Path,
    outlier_mad_scale: float,
    min_abs: float,
    min_coherence: float,
    valid_classifications: set[str],
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
        coherence = coherent_score(x)
        cluster = phase_cluster_stats(x)
        classification = classify_signal(x)
        class_ok = not valid_classifications or classification in valid_classifications
        record = {
            "file": file_path.name,
            "freq_index": freq_index,
            "repeat_index": repeat_index,
            "classification": classification,
            "z": z,
            "abs": float(abs(z)),
            "coherence": coherence,
            "cluster_phase_std": cluster["cluster_phase_std"],
            "cluster_phase_p95_abs": cluster["cluster_phase_p95_abs"],
            "cluster_phase_max_abs": cluster["cluster_phase_max_abs"],
            "phase": float(np.angle(z)) if abs(z) > 0 else 0.0,
            "samples": int(x.size),
            "robust_samples": robust_samples,
            "outlier_samples": outlier_samples,
            "valid": bool(
                x.size > 0
                and robust_samples > 0
                and abs(z) >= min_abs
                and coherence >= min_coherence
                and class_ok
            ),
        }
        by_freq.setdefault(freq_index, []).append(record)

    return by_freq


def average_by_freq(
    repeats_by_freq: dict[int, list[dict[str, Any]]],
    min_valid_repeats: int,
    max_repeat_abs_spread: float,
    max_repeat_phase_spread: float,
) -> dict[int, dict[str, Any]]:
    averaged: dict[int, dict[str, Any]] = {}
    for freq_index, records in repeats_by_freq.items():
        valid_records = [row for row in records if row["valid"]]
        if len(valid_records) < min_valid_repeats:
            continue

        repeat_abs_values = [float(row["abs"]) for row in valid_records]
        repeat_abs_spread = max(repeat_abs_values) - min(repeat_abs_values)
        if repeat_abs_spread > max_repeat_abs_spread:
            continue
        repeat_phases = [float(row["phase"]) for row in valid_records]
        repeat_phase_spread = circular_phase_spread_rad(repeat_phases)
        if repeat_phase_spread > max_repeat_phase_spread:
            continue

        z_values = np.array([row["z"] for row in valid_records], dtype=np.complex128)
        z_bar = complex(np.mean(z_values))
        averaged[freq_index] = {
            "freq_index": freq_index,
            "z": z_bar,
            "valid_repeat_count": len(valid_records),
            "total_repeat_count": len(records),
            "outlier_samples": int(sum(row["outlier_samples"] for row in valid_records)),
            "repeat_abs_min": float(min(repeat_abs_values)),
            "repeat_abs_max": float(max(repeat_abs_values)),
            "repeat_abs_spread": float(repeat_abs_spread),
            "repeat_phase_spread_rad": float(repeat_phase_spread),
            "abs": float(abs(z_bar)),
            "phase": float(np.angle(z_bar)),
        }

    return averaged


def save_estimate_plot(result: dict[str, Any], save_path: Path) -> None:
    rows = result["rows"]
    if len(rows) < 2:
        raise SystemExit("有效频点少于 2 个，无法出图")

    freqs_hz = np.array([float(row["freq_hz"]) for row in rows], dtype=float)
    freq_mhz = freqs_hz / 1e6
    phase_wrapped = np.array([float(row["phase_wrapped"]) for row in rows], dtype=float)
    phase_unwrapped = np.array([float(row["phase_unwrapped"]) for row in rows], dtype=float)
    phase_residual = np.array([float(row["phase_residual"]) for row in rows], dtype=float)
    fitted = result["slope_rad_per_hz"] * freqs_hz + result["intercept_rad"]

    fig, axes = plt.subplots(3, 1, figsize=(10, 10), sharex=True)

    axes[0].plot(freq_mhz, phase_wrapped, "o-", linewidth=1.2, markersize=4)
    axes[0].set_ylabel("Wrapped Phase (rad)")
    axes[0].set_title("Pair Phase vs Frequency")
    axes[0].grid(True, alpha=0.3)

    axes[1].plot(freq_mhz, phase_unwrapped, "o", label="unwrapped", markersize=5)
    axes[1].plot(freq_mhz, fitted, "-", label="linear fit", linewidth=1.5)
    axes[1].set_ylabel("Unwrapped Phase (rad)")
    axes[1].legend()
    axes[1].grid(True, alpha=0.3)

    axes[2].axhline(0.0, color="black", linewidth=1.0, alpha=0.6)
    axes[2].plot(freq_mhz, phase_residual, "o-", color="tab:red", linewidth=1.2, markersize=4)
    axes[2].set_xlabel("Frequency (MHz)")
    axes[2].set_ylabel("Residual (rad)")
    axes[2].grid(True, alpha=0.3)

    fig.suptitle(
        "distance={:.3f} m, rms_residual={:.4f} rad, max_abs_residual={:.4f} rad".format(
            result["distance_m"],
            result["rms_phase_residual"],
            result["max_abs_phase_residual"],
        )
    )
    fig.tight_layout()
    save_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(save_path, dpi=150)
    plt.close(fig)


def default_plot_path(root: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return (DEFAULT_PLOT_DIR / root.name / f"estimate_fit_{timestamp}.png").resolve()


def estimate_distance(args: argparse.Namespace, data_root: Path | None = None) -> dict[str, Any]:
    root = resolve_root(data_root if data_root is not None else args.root)
    initiator_dir = root / DIR_INITIATOR_RX
    reflector_dir = root / DIR_REFLECTOR_RX
    valid_classifications = parse_classifications(args.classifications)

    initiator_repeats = collect_repeats(
        initiator_dir,
        args.outlier_mad_scale,
        args.min_abs,
        args.min_coherence,
        valid_classifications,
    )
    reflector_repeats = collect_repeats(
        reflector_dir,
        args.outlier_mad_scale,
        args.min_abs,
        args.min_coherence,
        valid_classifications,
    )

    initiator_avg = average_by_freq(
        initiator_repeats,
        args.min_valid_repeats,
        args.max_repeat_abs_spread,
        args.max_repeat_phase_spread,
    )
    reflector_avg = average_by_freq(
        reflector_repeats,
        args.min_valid_repeats,
        args.max_repeat_abs_spread,
        args.max_repeat_phase_spread,
    )
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
                "initiator_repeat_abs_spread": initiator_avg[freq_index]["repeat_abs_spread"],
                "reflector_repeat_abs_spread": reflector_avg[freq_index]["repeat_abs_spread"],
                "initiator_repeat_phase_spread_rad": initiator_avg[freq_index]["repeat_phase_spread_rad"],
                "reflector_repeat_phase_spread_rad": reflector_avg[freq_index]["repeat_phase_spread_rad"],
                "pair_i": float(np.real(z_pair)),
                "pair_q": float(np.imag(z_pair)),
                "pair_abs": float(abs(z_pair)),
                "phase_wrapped": float(np.angle(z_pair)),
            }
        )

    freqs = np.array(freqs_hz, dtype=float)
    phases_wrapped = np.array(pair_phase_wrapped, dtype=float)
    unwrap_upward_tolerance_rad = float(getattr(args, "unwrap_upward_tolerance_rad", 0.2))
    phases_unwrapped_np = np.unwrap(phases_wrapped)
    phases_unwrapped = unwrap_with_negative_slope_prior(
        phases_wrapped,
        upward_tolerance_rad=unwrap_upward_tolerance_rad,
    )
    slope, intercept = np.polyfit(freqs, phases_unwrapped, 1)
    fitted = slope * freqs + intercept
    residual = phases_unwrapped - fitted
    propagation_speed_mps = float(getattr(args, "propagation_speed_mps", SPEED_OF_LIGHT))
    distance_m = -propagation_speed_mps * float(slope) / (4.0 * np.pi)

    for row, phase_unwrapped, phase_unwrapped_np, phase_residual in zip(rows, phases_unwrapped, phases_unwrapped_np, residual):
        row["phase_unwrapped"] = float(phase_unwrapped)
        row["phase_unwrapped_np"] = float(phase_unwrapped_np)
        row["phase_residual"] = float(phase_residual)

    return {
        "root": str(root),
        "center_freq_hz": float(args.center_freq_hz),
        "start_offset_hz": float(args.start_offset_hz),
        "step_hz": float(args.step_hz),
        "repeats": int(args.repeats),
        "propagation_speed_mps": float(propagation_speed_mps),
        "unwrap_method": "monotonic_downward_branch",
        "unwrap_upward_tolerance_rad": float(unwrap_upward_tolerance_rad),
        "classifications": sorted(valid_classifications),
        "min_abs": float(args.min_abs),
        "min_valid_repeats": int(args.min_valid_repeats),
        "max_repeat_abs_spread": float(args.max_repeat_abs_spread),
        "max_repeat_phase_spread_rad": float(args.max_repeat_phase_spread),
        "valid_freq_count": len(rows),
        "distance_m": distance_m,
        "slope_rad_per_hz": float(slope),
        "intercept_rad": float(intercept),
        "rms_phase_residual": float(np.sqrt(np.mean(residual ** 2))),
        "max_abs_phase_residual": float(np.max(np.abs(residual))),
        "rows": rows,
    }


def parse_classifications(value: str) -> set[str]:
    if not value:
        return set()
    return {item.strip() for item in value.split(",") if item.strip()}


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="用双向同频点相位乘积估计距离")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH, help="JSON 配置文件路径；默认读取 continuous_capture_config.json")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT, help="实验根目录，例如 self")
    parser.add_argument("--capture-group", default="all", help="data_store 采集组 label；默认 all，会按配置里的 capture_groups 扫描")
    parser.add_argument("--center-freq-hz", type=float, default=2.44e9, help="真实等效中心频率")
    parser.add_argument("--start-offset-hz", type=float, default=-40e6, help="freq_index=0 对应的频率偏移")
    parser.add_argument("--stop-offset-hz", type=float, default=40e6, help="兼容 continuous_capture_config.json；data_store 文件名决定实际频点数量")
    parser.add_argument("--step-hz", type=float, default=1e6, help="相邻 freq_index 的频率步进")
    parser.add_argument("--repeats", type=int, default=2, help="配置中的 repeat 次数；未显式指定 min-valid-repeats 时用它作为有效 repeat 要求")
    parser.add_argument("--sample-rate", type=float, default=1e6, help="兼容 continuous_capture_config.json；data_store 估距本身不直接使用")
    parser.add_argument("--propagation-speed-mps", type=float, default=SPEED_OF_LIGHT, help="传播速度，用于由相位斜率换算距离")
    parser.add_argument("--unwrap-upward-tolerance-rad", type=float, default=0.2, help="负斜率 unwrap 时允许相邻频点小幅上升的容差")
    parser.add_argument("--min-valid-repeats", type=int, default=None, help="每个方向每个频点至少需要多少次有效重复；默认等于 repeats")
    parser.add_argument(
        "--min-abs",
        type=float,
        default=0.0,
        help="单次重复平均复数幅度低于该值则判为无效；默认 0 表示不按绝对幅度过滤，因为幅度取决于 TX/RX 增益和链路损耗",
    )
    parser.add_argument(
        "--max-repeat-abs-spread",
        type=float,
        default=0.2,
        help="同一方向同一频点的有效重复之间，平均复数幅度最大差值超过该值则丢弃该方向",
    )
    parser.add_argument(
        "--max-repeat-phase-spread",
        type=float,
        default=0.7,
        help="同一方向同一频点的有效重复之间，平均复数相位最大角度散布超过该值则丢弃该方向，单位 rad",
    )
    parser.add_argument(
        "--min-coherence",
        type=float,
        default=0.0,
        help="单次重复相位相干度低于该值则判为无效；0.98 近似只用 stable_cluster",
    )
    parser.add_argument(
        "--classifications",
        type=str,
        default="stable_cluster",
        help="只使用 check_bin.py 判定出的分类，默认只用 stable_cluster；可用逗号分隔覆盖",
    )
    parser.add_argument("--outlier-mad-scale", type=float, default=8.0, help="IQ 离群点 MAD 阈值倍率")
    parser.add_argument("--save-json", type=Path, default=None, help="保存完整估计结果到 JSON 文件")
    parser.add_argument(
        "--save-plot",
        type=Path,
        default=None,
        help="保存拟合诊断图到 PNG 文件，包含 wrapped/unwrapped phase、线性拟合和 residual；默认会自动保存到 output_estimate_plot/<root>/estimate_fit_<timestamp>.png",
    )
    parser.add_argument(
        "--no-save-plot",
        action="store_true",
        help="不自动保存拟合诊断图",
    )
    return parser


def build_datastore_run_specs(args: argparse.Namespace, config: dict[str, Any]) -> list[dict[str, Any]]:
    root = resolve_root(args.root)
    capture_group = str(args.capture_group)
    configured_groups = config.get("capture_groups", [])
    if not isinstance(configured_groups, list):
        configured_groups = []

    specs: list[dict[str, Any]] = []
    if capture_group == "all":
        for group in configured_groups:
            if not isinstance(group, dict):
                continue
            label = str(group.get("label", ""))
            if not label:
                continue
            group_root = root / label
            if (group_root / DIR_INITIATOR_RX).exists() or (group_root / DIR_REFLECTOR_RX).exists():
                specs.append(
                    {
                        "label": label,
                        "distance_m": group.get("distance_m"),
                        "data_root": group_root,
                    }
                )
        if specs:
            return specs
        return [{"label": root.name, "distance_m": None, "data_root": root}]

    group_root = root / capture_group
    if group_root.exists():
        distance_m = None
        for group in configured_groups:
            if isinstance(group, dict) and str(group.get("label")) == capture_group:
                distance_m = group.get("distance_m")
                break
        return [{"label": capture_group, "distance_m": distance_m, "data_root": group_root}]

    return [{"label": capture_group, "distance_m": None, "data_root": root}]


def main() -> None:
    parser = build_argument_parser()
    config_args, _ = parser.parse_known_args()
    raw_config = load_config(config_args.config)
    config, config_profile = select_config_for_root(
        raw_config,
        config_args.root,
        root_was_provided=cli_root_was_provided(),
    )
    apply_config_defaults(parser, config)
    args = parser.parse_args()
    args.config = config_args.config
    args.loaded_config_profile = config_profile
    if args.min_valid_repeats is None:
        args.min_valid_repeats = max(1, int(args.repeats))
    run_specs = build_datastore_run_specs(args, config)

    results: list[dict[str, Any]] = []
    for spec in run_specs:
        result = estimate_distance(args, data_root=spec["data_root"])
        result["capture_group"] = spec["label"]
        result["capture_distance_m"] = spec["distance_m"]
        results.append(result)

        print(f"capture_group: {spec['label']}")
        if spec["distance_m"] is not None:
            print(f"capture_distance_m: {spec['distance_m']}")
        summary = {
            "root": result["root"],
            "valid_freq_count": result["valid_freq_count"],
            "distance_m": result["distance_m"],
            "slope_rad_per_hz": result["slope_rad_per_hz"],
            "rms_phase_residual": result["rms_phase_residual"],
            "max_abs_phase_residual": result["max_abs_phase_residual"],
            "propagation_speed_mps": result["propagation_speed_mps"],
            "min_valid_repeats": result["min_valid_repeats"],
        }
        for key, value in summary.items():
            print(f"{key}: {value}")

    if args.save_json is not None:
        out_path = args.save_json.resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        payload: dict[str, Any]
        if len(results) == 1:
            payload = results[0]
        else:
            payload = {"root": str(resolve_root(args.root)), "results": results}
        out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"saved: {out_path}")

    if not args.no_save_plot:
        for result in results:
            if args.save_plot is None:
                plot_path = default_plot_path(Path(result["root"]))
            elif len(results) == 1:
                plot_path = args.save_plot.resolve()
            else:
                stem = args.save_plot.stem
                suffix = args.save_plot.suffix or ".png"
                plot_path = args.save_plot.with_name(f"{stem}_{result['capture_group']}{suffix}").resolve()
            save_estimate_plot(result, plot_path)
            print(f"saved plot: {plot_path}")


if __name__ == "__main__":
    main()
