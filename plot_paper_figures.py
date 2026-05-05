#!/usr/bin/env python3
"""Generate paper-oriented summary figures for Chapter 4."""

from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


PROJECT_ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = PROJECT_ROOT / "论文" / "figures"

V4_DIR = PROJECT_ROOT / "zz_ble_cs_py" / "zz_ble_cs_py" / "results" / "v4" / "tables"
REAL_DIR = PROJECT_ROOT / "DATA" / "DATA_1to2_template" / "error_plots" / "recommended_distance_estimate"


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def ensure_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def as_float(rows: list[dict[str, str]], key: str) -> np.ndarray:
    return np.asarray([float(row[key]) for row in rows], dtype=float)


def plot_v4_key_results() -> Path:
    num_rows = read_csv_rows(V4_DIR / "summary_num_devices.csv")
    power_rows = read_csv_rows(V4_DIR / "summary_power_gap.csv")
    gap_rows = read_csv_rows(V4_DIR / "summary_two_device_gap.csv")

    fig, axes = plt.subplots(1, 3, figsize=(16, 4.8), constrained_layout=True)

    num_devices = as_float(num_rows, "num_devices")
    rmse = as_float(num_rows, "rmse")
    success = as_float(num_rows, "success_rate_le_0.5m") * 100.0
    axes[0].plot(num_devices, rmse, marker="o", color="#1f77b4", label="RMSE")
    ax0b = axes[0].twinx()
    ax0b.plot(num_devices, success, marker="s", color="#d62728", label="<=0.5 m")
    axes[0].set_title("V4: Device Count Scan")
    axes[0].set_xlabel("Number of Devices")
    axes[0].set_ylabel("RMSE (m)", color="#1f77b4")
    ax0b.set_ylabel("Success Rate within 0.5 m (%)", color="#d62728")
    axes[0].grid(True, alpha=0.25)

    power_gap = as_float(power_rows, "power_gap_db")
    mae = as_float(power_rows, "mae")
    success_power = as_float(power_rows, "success_rate_le_0.5m") * 100.0
    axes[1].plot(power_gap, mae, marker="o", color="#2ca02c", label="MAE")
    ax1b = axes[1].twinx()
    ax1b.plot(power_gap, success_power, marker="s", color="#ff7f0e", label="<=0.5 m")
    axes[1].set_title("V4: Near-Far Effect Scan")
    axes[1].set_xlabel("Target Power Gap (dB)")
    axes[1].set_ylabel("MAE (m)", color="#2ca02c")
    ax1b.set_ylabel("Success Rate within 0.5 m (%)", color="#ff7f0e")
    axes[1].grid(True, alpha=0.25)

    spacing = as_float(gap_rows, "device_spacing_m")
    gap_rmse = as_float(gap_rows, "rmse")
    axes[2].plot(spacing, gap_rmse, color="#9467bd", linewidth=1.6)
    axes[2].fill_between(spacing, 0.0, gap_rmse, color="#9467bd", alpha=0.15)
    axes[2].set_title("V4: Fixed Two-Target Gap Scan")
    axes[2].set_xlabel("Target Spacing (m)")
    axes[2].set_ylabel("RMSE (m)")
    axes[2].grid(True, alpha=0.25)

    output_path = OUTPUT_DIR / "v4_key_results.png"
    fig.savefig(output_path, dpi=180)
    plt.close(fig)
    return output_path


def summarize_target_rows(error_rows: list[dict[str, str]]) -> list[dict[str, float | str]]:
    grouped: dict[str, list[float]] = defaultdict(list)
    for row in error_rows:
        grouped[row["target"]].append(float(row["abs_error_m"]))

    def target_sort_key(label: str) -> tuple[float, str]:
        numeric = label.replace("m", "").replace("-", ".")
        try:
            return (float(numeric), label)
        except ValueError:
            return (999.0, label)

    summary: list[dict[str, float | str]] = []
    for target in sorted(grouped.keys(), key=target_sort_key):
        errors = np.asarray(grouped[target], dtype=float)
        summary.append(
            {
                "target": target,
                "mae": float(np.mean(errors)),
                "median": float(np.median(errors)),
                "within_0p5": float(np.mean(errors <= 0.5)),
                "gt_2m": float(np.mean(errors > 2.0)),
            }
        )
    return summary


def plot_real_target_analysis() -> Path:
    error_rows = read_csv_rows(REAL_DIR / "error_rows.csv")
    summary = summarize_target_rows(error_rows)

    labels = [str(row["target"]) for row in summary]
    mae = np.asarray([float(row["mae"]) for row in summary], dtype=float)
    median = np.asarray([float(row["median"]) for row in summary], dtype=float)
    within = np.asarray([float(row["within_0p5"]) * 100.0 for row in summary], dtype=float)
    catastrophic = np.asarray([float(row["gt_2m"]) * 100.0 for row in summary], dtype=float)
    x = np.arange(len(labels), dtype=float)

    fig, axes = plt.subplots(2, 1, figsize=(11, 8), constrained_layout=True)

    width = 0.36
    axes[0].bar(x - width / 2.0, mae, width=width, color="#1f77b4", label="MAE")
    axes[0].bar(x + width / 2.0, median, width=width, color="#2ca02c", label="Median AE")
    axes[0].set_xticks(x, labels)
    axes[0].set_ylabel("Error (m)")
    axes[0].set_title("Recommended 1to2 Method by Target Distance")
    axes[0].legend()
    axes[0].grid(True, axis="y", alpha=0.25)

    axes[1].plot(x, within, marker="o", color="#d62728", label="<=0.5 m")
    axes[1].plot(x, catastrophic, marker="s", color="#9467bd", label=">2 m")
    axes[1].set_xticks(x, labels)
    axes[1].set_ylabel("Sample Ratio (%)")
    axes[1].set_xlabel("Target Distance")
    axes[1].legend()
    axes[1].grid(True, axis="y", alpha=0.25)

    output_path = OUTPUT_DIR / "recommended_target_analysis.png"
    fig.savefig(output_path, dpi=180)
    plt.close(fig)
    return output_path


def plot_real_pairing_analysis() -> Path:
    rows = read_csv_rows(REAL_DIR / "pairing_target_summary.csv")
    pairing_errors: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        pairing_errors[row["pairing"]].append(float(row["mae_m"]))

    summary = []
    for pairing, values in pairing_errors.items():
        values_arr = np.asarray(values, dtype=float)
        summary.append(
            {
                "pairing": pairing,
                "mean_target_mae": float(np.mean(values_arr)),
                "worst_target_mae": float(np.max(values_arr)),
            }
        )
    summary.sort(key=lambda item: (float(item["mean_target_mae"]), str(item["pairing"])))

    labels = [str(item["pairing"]) for item in summary]
    mean_mae = np.asarray([float(item["mean_target_mae"]) for item in summary], dtype=float)
    worst_mae = np.asarray([float(item["worst_target_mae"]) for item in summary], dtype=float)
    y = np.arange(len(labels), dtype=float)

    fig, ax = plt.subplots(figsize=(11, 9), constrained_layout=True)
    bar_h = 0.38
    ax.barh(y + bar_h / 2.0, mean_mae, height=bar_h, color="#1f77b4", label="Mean Target MAE")
    ax.barh(y - bar_h / 2.0, worst_mae, height=bar_h, color="#ff7f0e", label="Worst Target MAE")
    ax.set_yticks(y, labels)
    ax.set_xlabel("Error (m)")
    ax.set_title("Recommended 1to2 Method by Distance Pairing")
    ax.legend()
    ax.grid(True, axis="x", alpha=0.25)

    output_path = OUTPUT_DIR / "recommended_pairing_analysis.png"
    fig.savefig(output_path, dpi=180)
    plt.close(fig)
    return output_path


def main() -> None:
    ensure_output_dir()
    outputs = [
        plot_v4_key_results(),
        plot_real_target_analysis(),
        plot_real_pairing_analysis(),
    ]
    for path in outputs:
        print(path)


if __name__ == "__main__":
    main()
