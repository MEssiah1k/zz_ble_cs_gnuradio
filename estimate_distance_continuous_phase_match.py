#!/usr/bin/env python3
"""Estimate distance from continuous captures via wrapped phase model matching."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib-codex")
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from analyze_continuous_capture import (
    average_rows_by_freq,
    build_pair_freq_diagnostics,
    build_side_freq_diagnostics,
    default_capture_paths,
    detect_capture_bursts,
    expected_burst_count,
    load_gr_complex_bin,
    load_pair_phase_csv,
    resolve_root,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_ROOT = PROJECT_ROOT / "1to1_rfhop"
DEFAULT_PLOT_DIR = PROJECT_ROOT / "output_estimate_plot_continuous_phase_match"
DEFAULT_PROPAGATION_SPEED_MPS = 2.3e8
TWO_PI = 2.0 * np.pi


def wrap_to_pi(x: np.ndarray | float) -> np.ndarray | float:
    return (np.asarray(x) + np.pi) % (2.0 * np.pi) - np.pi


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


def slope_distance_from_wrapped_phase(
    freqs_hz: np.ndarray,
    measured_wrapped: np.ndarray,
    *,
    upward_tolerance_rad: float,
    propagation_speed_mps: float,
) -> tuple[float, float, float, np.ndarray]:
    measured_unwrapped = unwrap_with_negative_slope_prior(
        measured_wrapped,
        upward_tolerance_rad=upward_tolerance_rad,
    )
    slope, intercept = np.polyfit(freqs_hz, measured_unwrapped, 1)
    slope_distance_m = -float(propagation_speed_mps) * float(slope) / (4.0 * np.pi)
    return float(slope_distance_m), float(slope), float(intercept), measured_unwrapped


def default_plot_path(root: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return (DEFAULT_PLOT_DIR / root.name / f"estimate_phase_match_{timestamp}.png").resolve()


def load_capture_rows(path: Path, args: argparse.Namespace) -> list[dict[str, Any]]:
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
    return rows


def solve_phase_offset(measured_wrapped: np.ndarray, model_phase: np.ndarray) -> tuple[float, np.ndarray]:
    residual = measured_wrapped - model_phase
    phase0 = float(np.angle(np.mean(np.exp(1j * residual))))
    wrapped_error = wrap_to_pi(measured_wrapped - (model_phase + phase0))
    return phase0, np.asarray(wrapped_error, dtype=float)


def estimate_distance_phase_match_from_pair_rows(
    pair_rows: list[dict[str, Any]],
    args: argparse.Namespace,
    *,
    source: str,
) -> dict[str, Any]:
    usable_pair_rows = sorted(pair_rows, key=lambda item: int(item["freq_index"]))
    if len(usable_pair_rows) < 2:
        raise SystemExit("有效公共频点少于 2 个，无法做相位匹配测距")

    freqs_hz = np.array([float(row["freq_hz"]) for row in usable_pair_rows], dtype=float)
    measured_wrapped = np.array([float(row["pair_phase_rad"]) for row in usable_pair_rows], dtype=float)

    distance_step_m = float(args.distance_step_m)
    if distance_step_m <= 0.0:
        raise SystemExit("distance_step_m 必须大于 0")
    match_window_m = float(getattr(args, "match_window_m", 10.0))
    if match_window_m < 0.0:
        raise SystemExit("match_window_m 不能为负数")
    unwrap_upward_tolerance_rad = float(getattr(args, "unwrap_upward_tolerance_rad", 0.2))
    propagation_speed_mps = float(getattr(args, "propagation_speed_mps", DEFAULT_PROPAGATION_SPEED_MPS))
    slope_distance_m, slope_rad_per_hz, slope_intercept_rad, measured_unwrapped = slope_distance_from_wrapped_phase(
        freqs_hz,
        measured_wrapped,
        upward_tolerance_rad=unwrap_upward_tolerance_rad,
        propagation_speed_mps=propagation_speed_mps,
    )
    distance_min_m = slope_distance_m - match_window_m
    distance_max_m = slope_distance_m + match_window_m
    num_steps = int(round((distance_max_m - distance_min_m) / distance_step_m)) + 1
    distance_grid = distance_min_m + np.arange(num_steps, dtype=float) * distance_step_m
    if distance_grid.size == 0:
        raise SystemExit("distance grid is empty")

    costs = np.empty(distance_grid.size, dtype=float)
    phase0_values = np.empty(distance_grid.size, dtype=float)
    for idx, distance_m in enumerate(distance_grid):
        model_phase = -4.0 * np.pi * freqs_hz * float(distance_m) / propagation_speed_mps
        phase0, wrapped_error = solve_phase_offset(measured_wrapped, model_phase)
        phase0_values[idx] = phase0
        costs[idx] = float(np.mean(wrapped_error ** 2))

    best_index = int(np.argmin(costs))
    best_distance_m = float(distance_grid[best_index])
    best_distance_m = round(best_distance_m / args.distance_step_m) * args.distance_step_m
    if abs(best_distance_m) < 0.5 * args.distance_step_m:
        best_distance_m = 0.0
    best_phase0 = float(phase0_values[best_index])
    best_model_phase = -4.0 * np.pi * freqs_hz * best_distance_m / propagation_speed_mps
    best_wrapped_fit = wrap_to_pi(best_model_phase + best_phase0)
    best_wrapped_error = np.asarray(wrap_to_pi(measured_wrapped - best_wrapped_fit), dtype=float)

    rows: list[dict[str, Any]] = []
    for pair_row, fit_phase, phase_error in zip(usable_pair_rows, best_wrapped_fit, best_wrapped_error):
        rows.append(
            {
                "freq_index": int(pair_row["freq_index"]),
                "freq_hz": float(pair_row["freq_hz"]),
                "pair_abs": float(pair_row.get("pair_abs", 0.0)),
                "phase_wrapped_measured": float(pair_row["pair_phase_rad"]),
                "phase_wrapped_fit": float(fit_phase),
                "phase_wrapped_error": float(phase_error),
            }
        )

    return {
        "root": str(resolve_root(args.root)),
        "source": source,
        "pair_csv": str(args.pair_csv.resolve()) if args.pair_csv is not None else None,
        "reflector_file": None,
        "initiator_file": None,
        "reflector_invalid_burst_count": 0,
        "initiator_invalid_burst_count": 0,
        "valid_freq_count": int(len(rows)),
        "distance_m": best_distance_m,
        "slope_distance_m": float(slope_distance_m),
        "propagation_speed_mps": float(propagation_speed_mps),
        "slope_rad_per_hz": float(slope_rad_per_hz),
        "slope_intercept_rad": float(slope_intercept_rad),
        "match_window_m": float(match_window_m),
        "match_distance_min_m": float(distance_grid[0]),
        "match_distance_max_m": float(distance_grid[-1]),
        "unwrap_upward_tolerance_rad": float(unwrap_upward_tolerance_rad),
        "phase0_rad": best_phase0,
        "wrapped_phase_cost": float(costs[best_index]),
        "wrapped_phase_rms_error": float(np.sqrt(np.mean(best_wrapped_error ** 2))),
        "wrapped_phase_max_abs_error": float(np.max(np.abs(best_wrapped_error))),
        "distance_grid_m": [float(x) for x in distance_grid],
        "cost_grid": [float(x) for x in costs],
        "measured_unwrapped_by_slope_prior": [float(x) for x in measured_unwrapped],
        "rows": rows,
    }


def estimate_distance_phase_match(args: argparse.Namespace) -> dict[str, Any]:
    if args.pair_csv is not None:
        pair_rows = load_pair_phase_csv(args.pair_csv.resolve())
        return estimate_distance_phase_match_from_pair_rows(pair_rows, args, source="pair_csv")

    root = resolve_root(args.root)
    default_reflector_file, default_initiator_file = default_capture_paths(root)
    reflector_file = default_reflector_file if args.reflector_file is None else args.reflector_file.resolve()
    initiator_file = default_initiator_file if args.initiator_file is None else args.initiator_file.resolve()

    reflector_rows = load_capture_rows(reflector_file, args)
    initiator_rows = load_capture_rows(initiator_file, args)
    reflector_invalid_burst_count = int(sum(not bool(row.get("sequence_ok", True)) for row in reflector_rows))
    initiator_invalid_burst_count = int(sum(not bool(row.get("sequence_ok", True)) for row in initiator_rows))
    expected_freq_count = int(round(expected_burst_count(
        args.start_offset_hz,
        args.stop_offset_hz,
        args.step_hz,
        args.repeats,
    ) / float(args.repeats))) if args.repeats > 0 else 0

    initiator_diag = build_side_freq_diagnostics(
        initiator_rows,
        expected_freq_count=expected_freq_count,
        expected_repeats=args.repeats,
    )
    reflector_diag = build_side_freq_diagnostics(
        reflector_rows,
        expected_freq_count=expected_freq_count,
        expected_repeats=args.repeats,
    )
    initiator_avg = average_rows_by_freq(initiator_rows)
    reflector_avg = average_rows_by_freq(reflector_rows)

    pair_rows: list[dict[str, Any]] = []
    for freq_index in sorted(set(initiator_avg) & set(reflector_avg)):
        z_pair = initiator_avg[freq_index]["z"] * reflector_avg[freq_index]["z"]
        pair_rows.append(
            {
                "freq_index": int(freq_index),
                "freq_hz": float(initiator_avg[freq_index]["freq_hz"]),
                "pair_abs": float(abs(z_pair)),
                "pair_phase_rad": float(np.angle(z_pair)),
                "z_pair": z_pair,
            }
        )
    pair_diag = build_pair_freq_diagnostics(initiator_diag, reflector_diag, pair_rows)
    usable_pair_rows = [row["pair_row"] for row in pair_diag if row["pair_row"] is not None]
    result = estimate_distance_phase_match_from_pair_rows(usable_pair_rows, args, source="capture")
    result.update(
        {
            "root": str(root),
            "reflector_file": str(reflector_file),
            "initiator_file": str(initiator_file),
            "reflector_invalid_burst_count": reflector_invalid_burst_count,
            "initiator_invalid_burst_count": initiator_invalid_burst_count,
        }
    )
    return result


def save_phase_match_plot(result: dict[str, Any], save_path: Path) -> None:
    rows = result["rows"]
    if not rows:
        return

    freq_mhz = np.array([float(row["freq_hz"]) for row in rows], dtype=float) / 1e6
    measured = np.array([float(row["phase_wrapped_measured"]) for row in rows], dtype=float)
    fitted = np.array([float(row["phase_wrapped_fit"]) for row in rows], dtype=float)
    error = np.array([float(row["phase_wrapped_error"]) for row in rows], dtype=float)
    fitted_unwrapped = np.unwrap(fitted)
    measured_unwrapped = fitted_unwrapped + error
    distance_grid = np.array(result["distance_grid_m"], dtype=float)
    cost_grid = np.array(result["cost_grid"], dtype=float)

    fig, axes = plt.subplots(3, 1, figsize=(10, 10))

    axes[0].plot(distance_grid, cost_grid, "-", linewidth=1.5, color="tab:purple")
    axes[0].axvline(float(result["distance_m"]), color="tab:red", linestyle="--", linewidth=1.2)
    axes[0].set_ylabel("Mean Wrapped Error^2")
    axes[0].set_title("Wrapped phase distance matching cost")
    axes[0].grid(True, alpha=0.3)

    axes[1].plot(freq_mhz, measured_unwrapped, "o-", linewidth=1.2, markersize=4, label="measured")
    axes[1].plot(freq_mhz, fitted_unwrapped, "o-", linewidth=1.2, markersize=4, label="model fit")
    axes[1].set_ylabel("Unwrapped Phase (rad)")
    axes[1].legend()
    axes[1].grid(True, alpha=0.3)

    axes[2].axhline(0.0, color="black", linewidth=1.0, alpha=0.6)
    axes[2].plot(freq_mhz, error, "o-", linewidth=1.2, markersize=4, color="tab:red")
    axes[2].set_xlabel("Frequency (MHz)")
    axes[2].set_ylabel("Model-aligned Error (rad)")
    axes[2].grid(True, alpha=0.3)

    fig.suptitle(
        "distance={:.3f} m, wrapped_rms_error={:.4f} rad, max_abs_error={:.4f} rad".format(
            float(result["distance_m"]),
            float(result["wrapped_phase_rms_error"]),
            float(result["wrapped_phase_max_abs_error"]),
        )
    )
    fig.tight_layout()
    save_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(save_path, dpi=150)
    plt.close(fig)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Estimate distance from continuous captures by wrapped phase matching")
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
    parser.add_argument("--distance-min-m", type=float, default=0.0, help="兼容旧命令，当前 phase-match 不再用绝对距离下限")
    parser.add_argument("--distance-max-m", type=float, default=20.0, help="兼容旧命令，当前 phase-match 不再用绝对距离上限")
    parser.add_argument("--distance-step-m", type=float, default=0.01)
    parser.add_argument("--match-window-m", type=float, default=10.0, help="围绕线性斜率距离做 phase-match 的半窗口，默认 slope_distance ±10m")
    parser.add_argument("--propagation-speed-mps", type=float, default=DEFAULT_PROPAGATION_SPEED_MPS, help="传播速度，默认 2.3e8 m/s（铜质有线测量）")
    parser.add_argument("--unwrap-upward-tolerance-rad", type=float, default=0.2)
    parser.add_argument("--save-json", type=Path, default=None)
    parser.add_argument("--save-plot", type=Path, default=None)
    parser.add_argument("--no-save-plot", action="store_true")
    parser.add_argument("--pair-csv", type=Path, default=None, help="直接使用 analyze_continuous_capture.py 导出的 pair_phase_by_freq.csv")
    return parser


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()
    result = estimate_distance_phase_match(args)

    summary = {
        "root": result["root"],
        "reflector_invalid_burst_count": result["reflector_invalid_burst_count"],
        "initiator_invalid_burst_count": result["initiator_invalid_burst_count"],
        "valid_freq_count": result["valid_freq_count"],
        "distance_m": result["distance_m"],
        "slope_distance_m": result["slope_distance_m"],
        "match_window_m": result["match_window_m"],
        "match_distance_min_m": result["match_distance_min_m"],
        "match_distance_max_m": result["match_distance_max_m"],
        "phase0_rad": result["phase0_rad"],
        "wrapped_phase_cost": result["wrapped_phase_cost"],
        "wrapped_phase_rms_error": result["wrapped_phase_rms_error"],
        "wrapped_phase_max_abs_error": result["wrapped_phase_max_abs_error"],
    }
    for key, value in summary.items():
        print(f"{key}: {value}")

    if args.save_json is not None:
        out_path = args.save_json.resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"saved_json: {out_path}")

    if not args.no_save_plot:
        plot_path = default_plot_path(resolve_root(args.root)) if args.save_plot is None else args.save_plot.resolve()
        save_phase_match_plot(result, plot_path)
        print(f"saved_plot: {plot_path}")


if __name__ == "__main__":
    main()
