#!/usr/bin/env python3
"""Estimate distance from continuous file-sink captures."""

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
    summarize_freq_rows,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_ROOT = PROJECT_ROOT / "1to1_rfhop"
DEFAULT_PLOT_DIR = PROJECT_ROOT / "output_estimate_plot_continuous"
SPEED_OF_LIGHT = 299792458.0


def save_estimate_plot(result: dict[str, Any], save_path: Path) -> None:
    rows = result["rows"]
    freqs_hz = np.array([float(row["freq_hz"]) for row in rows], dtype=float)
    freq_mhz = freqs_hz / 1e6
    phase_wrapped = np.array([float(row["phase_wrapped"]) for row in rows], dtype=float)
    phase_unwrapped = np.array([float(row["phase_unwrapped"]) for row in rows], dtype=float)
    phase_residual = np.array([float(row["phase_residual"]) for row in rows], dtype=float)
    fitted = result["slope_rad_per_hz"] * freqs_hz + result["intercept_rad"]

    fig, axes = plt.subplots(3, 1, figsize=(10, 10), sharex=True)
    axes[0].plot(freq_mhz, phase_wrapped, "o-", linewidth=1.2, markersize=4)
    axes[0].set_ylabel("Wrapped Phase (rad)")
    axes[0].grid(True, alpha=0.3)
    axes[0].set_title("Pair Phase vs Frequency")

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


def save_side_phase_plot(
    side_name: str,
    side_avg: dict[int, dict[str, Any]],
    save_path: Path,
) -> None:
    if not side_avg:
        return

    freq_indices = sorted(side_avg)
    freqs_hz = np.array([float(side_avg[freq_index]["freq_hz"]) for freq_index in freq_indices], dtype=float)
    freq_mhz = freqs_hz / 1e6
    phase_wrapped = np.array([float(side_avg[freq_index]["phase"]) for freq_index in freq_indices], dtype=float)
    phase_unwrapped = np.unwrap(phase_wrapped)

    fig, axes = plt.subplots(2, 1, figsize=(10, 7), sharex=True)

    axes[0].plot(freq_mhz, phase_wrapped, "o-", linewidth=1.2, markersize=4, color="tab:blue")
    axes[0].set_ylabel("Wrapped Phase (rad)")
    axes[0].set_title(f"{side_name} phase vs frequency")
    axes[0].grid(True, alpha=0.3)

    axes[1].plot(freq_mhz, phase_unwrapped, "o-", linewidth=1.2, markersize=4, color="tab:green")
    axes[1].set_xlabel("Frequency (MHz)")
    axes[1].set_ylabel("Unwrapped Phase (rad)")
    axes[1].grid(True, alpha=0.3)

    fig.tight_layout()
    save_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(save_path, dpi=150)
    plt.close(fig)


def default_plot_path(root: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return (DEFAULT_PLOT_DIR / root.name / f"estimate_fit_{timestamp}.png").resolve()


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


def estimate_distance_from_pair_rows(
    pair_rows: list[dict[str, Any]],
    args: argparse.Namespace,
    *,
    source: str,
) -> dict[str, Any]:
    if len(pair_rows) < 2:
        raise SystemExit("有效公共频点少于 2 个，无法拟合距离")

    rows: list[dict[str, Any]] = []
    freqs_hz: list[float] = []
    pair_phase_wrapped: list[float] = []

    for pair_row in sorted(pair_rows, key=lambda item: int(item["freq_index"])):
        freq_index = int(pair_row["freq_index"])
        f_hz = float(pair_row["freq_hz"])
        phase = float(pair_row["pair_phase_rad"])
        freqs_hz.append(f_hz)
        pair_phase_wrapped.append(phase)
        rows.append(
            {
                "freq_index": int(freq_index),
                "freq_hz": float(f_hz),
                "initiator_valid_repeats": int(pair_row.get("initiator_repeat_count", 0)),
                "reflector_valid_repeats": int(pair_row.get("reflector_repeat_count", 0)),
                "pair_i": float(pair_row.get("pair_i", 0.0)),
                "pair_q": float(pair_row.get("pair_q", 0.0)),
                "pair_abs": float(pair_row.get("pair_abs", 0.0)),
                "phase_wrapped": phase,
                "initiator_summary": str(pair_row.get("initiator_summary", "from_pair_csv")),
                "reflector_summary": str(pair_row.get("reflector_summary", "from_pair_csv")),
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
        "root": str(resolve_root(args.root)),
        "source": source,
        "pair_csv": str(args.pair_csv.resolve()) if args.pair_csv is not None else None,
        "reflector_file": None,
        "initiator_file": None,
        "center_freq_hz": float(args.center_freq_hz),
        "start_offset_hz": float(args.start_offset_hz),
        "step_hz": float(args.step_hz),
        "sample_rate": float(args.sample_rate),
        "initiator_freq_diagnostics": [],
        "reflector_freq_diagnostics": [],
        "pair_freq_diagnostics": [],
        "reflector_invalid_burst_count": 0,
        "initiator_invalid_burst_count": 0,
        "valid_freq_count": int(len(rows)),
        "distance_m": float(distance_m),
        "slope_rad_per_hz": float(slope),
        "intercept_rad": float(intercept),
        "rms_phase_residual": float(np.sqrt(np.mean(residual ** 2))),
        "max_abs_phase_residual": float(np.max(np.abs(residual))),
        "rows": rows,
        "initiator_avg_by_freq": [],
        "reflector_avg_by_freq": [],
    }


def estimate_distance(args: argparse.Namespace) -> dict[str, Any]:
    if args.pair_csv is not None:
        pair_rows = load_pair_phase_csv(args.pair_csv.resolve())
        return estimate_distance_from_pair_rows(pair_rows, args, source="pair_csv")

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

    initiator_diag = build_side_freq_diagnostics(initiator_rows, expected_freq_count=expected_freq_count)
    reflector_diag = build_side_freq_diagnostics(reflector_rows, expected_freq_count=expected_freq_count)
    reflector_avg = average_rows_by_freq(reflector_rows)
    initiator_avg = average_rows_by_freq(initiator_rows)

    pair_phase_rows: list[dict[str, Any]] = []
    common_freq_indices = sorted(set(reflector_avg) & set(initiator_avg))
    for freq_index in common_freq_indices:
        z_pair = initiator_avg[freq_index]["z"] * reflector_avg[freq_index]["z"]
        pair_phase_rows.append(
            {
                "freq_index": int(freq_index),
                "freq_hz": float(initiator_avg[freq_index]["freq_hz"]),
                "pair_abs": float(abs(z_pair)),
                "pair_phase_rad": float(np.angle(z_pair)),
                "initiator_abs": float(initiator_avg[freq_index]["abs"]),
                "initiator_phase_rad": float(initiator_avg[freq_index]["phase"]),
                "reflector_abs": float(reflector_avg[freq_index]["abs"]),
                "reflector_phase_rad": float(reflector_avg[freq_index]["phase"]),
                "initiator_repeat_count": int(initiator_avg[freq_index]["repeat_count"]),
                "reflector_repeat_count": int(reflector_avg[freq_index]["repeat_count"]),
            }
        )

    pair_diag = build_pair_freq_diagnostics(initiator_diag, reflector_diag, pair_phase_rows)
    usable_pair_rows = [row["pair_row"] for row in pair_diag if row["pair_row"] is not None]
    if len(usable_pair_rows) < 2:
        raise SystemExit("有效公共频点少于 2 个，无法拟合距离")

    csv_like_rows: list[dict[str, Any]] = []
    for pair_row in usable_pair_rows:
        freq_index = int(pair_row["freq_index"])
        z_pair = initiator_avg[freq_index]["z"] * reflector_avg[freq_index]["z"]
        csv_like_rows.append(
            {
                "freq_index": int(freq_index),
                "freq_hz": float(pair_row["freq_hz"]),
                "initiator_repeat_count": initiator_avg[freq_index]["repeat_count"],
                "reflector_repeat_count": reflector_avg[freq_index]["repeat_count"],
                "pair_i": float(np.real(z_pair)),
                "pair_q": float(np.imag(z_pair)),
                "pair_abs": float(abs(z_pair)),
                "pair_phase_rad": float(pair_row["pair_phase_rad"]),
                "initiator_summary": summarize_freq_rows(initiator_diag[freq_index]["rows"]),
                "reflector_summary": summarize_freq_rows(reflector_diag[freq_index]["rows"]),
            }
        )
    result = estimate_distance_from_pair_rows(csv_like_rows, args, source="capture")
    result.update(
        {
            "reflector_file": str(reflector_file),
            "initiator_file": str(initiator_file),
            "center_freq_hz": float(args.center_freq_hz),
            "start_offset_hz": float(args.start_offset_hz),
            "step_hz": float(args.step_hz),
            "sample_rate": float(args.sample_rate),
            "initiator_freq_diagnostics": initiator_diag,
            "reflector_freq_diagnostics": reflector_diag,
            "pair_freq_diagnostics": pair_diag,
            "reflector_invalid_burst_count": reflector_invalid_burst_count,
            "initiator_invalid_burst_count": initiator_invalid_burst_count,
            "initiator_avg_by_freq": [
                {
                    "freq_index": int(freq_index),
                    "freq_hz": float(initiator_avg[freq_index]["freq_hz"]),
                    "repeat_count": int(initiator_avg[freq_index]["repeat_count"]),
                    "abs": float(initiator_avg[freq_index]["abs"]),
                    "phase_wrapped": float(initiator_avg[freq_index]["phase"]),
                }
                for freq_index in sorted(initiator_avg)
            ],
            "reflector_avg_by_freq": [
                {
                    "freq_index": int(freq_index),
                    "freq_hz": float(reflector_avg[freq_index]["freq_hz"]),
                    "repeat_count": int(reflector_avg[freq_index]["repeat_count"]),
                    "abs": float(reflector_avg[freq_index]["abs"]),
                    "phase_wrapped": float(reflector_avg[freq_index]["phase"]),
                }
                for freq_index in sorted(reflector_avg)
            ],
        }
    )
    return result


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Estimate distance from continuous captures")
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
    parser.add_argument("--save-plot", type=Path, default=None)
    parser.add_argument("--no-save-plot", action="store_true")
    parser.add_argument("--pair-csv", type=Path, default=None, help="直接使用 analyze_continuous_capture.py 导出的 pair_phase_by_freq.csv")
    return parser


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()
    result = estimate_distance(args)

    summary = {
        "root": result["root"],
        "reflector_invalid_burst_count": result["reflector_invalid_burst_count"],
        "initiator_invalid_burst_count": result["initiator_invalid_burst_count"],
        "valid_freq_count": result["valid_freq_count"],
        "distance_m": result["distance_m"],
        "slope_rad_per_hz": result["slope_rad_per_hz"],
        "rms_phase_residual": result["rms_phase_residual"],
        "max_abs_phase_residual": result["max_abs_phase_residual"],
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
        save_estimate_plot(result, plot_path)
        print(f"saved_plot: {plot_path}")
        if result["initiator_avg_by_freq"] and result["reflector_avg_by_freq"]:
            initiator_phase_plot = plot_path.with_name(plot_path.stem + "_initiator_phase.png")
            reflector_phase_plot = plot_path.with_name(plot_path.stem + "_reflector_phase.png")
            save_side_phase_plot(
                "initiator",
                {
                    int(row["freq_index"]): {
                        "freq_hz": float(row["freq_hz"]),
                        "phase": float(row["phase_wrapped"]),
                    }
                    for row in result["initiator_avg_by_freq"]
                },
                initiator_phase_plot,
            )
            save_side_phase_plot(
                "reflector",
                {
                    int(row["freq_index"]): {
                        "freq_hz": float(row["freq_hz"]),
                        "phase": float(row["phase_wrapped"]),
                    }
                    for row in result["reflector_avg_by_freq"]
                },
                reflector_phase_plot,
            )
            print(f"saved_initiator_phase_plot: {initiator_phase_plot}")
            print(f"saved_reflector_phase_plot: {reflector_phase_plot}")


if __name__ == "__main__":
    main()
