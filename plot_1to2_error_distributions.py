#!/usr/bin/env python3
"""Plot 1-to-2 error distributions from DATA_1to2 summary."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_SUMMARY = PROJECT_ROOT / "DATA" / "DATA_1to2" / "summary_all.json"
DEFAULT_OUTPUT = PROJECT_ROOT / "DATA" / "DATA_1to2" / "error_plots"
DEFAULT_METHODS = [
    "distance_spectrum_match",
    "distance_coherent_match_raw",
    "distance_coherent_match_unit",
    "distance_coherent_match_raw_pair",
    "distance_coherent_match_unit_pair",
    "distance_v4_legacy_pair",
    "distance_v4_projection_pair",
    "distance_v4_adjacent_pair",
    "distance_v4_composite_pair",
    "distance_phase_cluster_match",
    "recommended_distance_estimate",
]


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Plot 1-to-2 error distributions")
    parser.add_argument("--summary", type=Path, default=DEFAULT_SUMMARY)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--method", action="append", default=None, help="Method key in summary target_results")
    return parser


def load_summary(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def collect_rows(summary: dict[str, Any], methods: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for pairing_name, pairing in summary["pairings"].items():
        for group_name, group in pairing["groups"].items():
            for target_name, target_result in group["target_results"].items():
                baseline = float(target_result["baseline_distance_m"])
                for method in methods:
                    if method not in target_result:
                        continue
                    entry = target_result[method]
                    distance_m = float(entry["distance_m"])
                    signed_error = float(entry["delta_from_baseline_m"])
                    abs_error = abs(signed_error)
                    rows.append(
                        {
                            "pairing": pairing_name,
                            "group": group_name,
                            "target": target_name,
                            "method": method,
                            "baseline_distance_m": baseline,
                            "estimated_distance_m": distance_m,
                            "signed_error_m": signed_error,
                            "abs_error_m": abs_error,
                        }
                    )
    return rows


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def save_rows_csv(rows: list[dict[str, Any]], path: Path) -> None:
    ensure_dir(path.parent)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8-sig") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def unique_in_order(rows: list[dict[str, Any]], key: str) -> list[str]:
    seen: list[str] = []
    for row in rows:
        value = str(row[key])
        if value not in seen:
            seen.append(value)
    return seen


def filter_rows(rows: list[dict[str, Any]], **criteria: Any) -> list[dict[str, Any]]:
    result = rows
    for key, value in criteria.items():
        result = [row for row in result if row.get(key) == value]
    return result


def summarize_method_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    methods = unique_in_order(rows, "method")
    summary_rows: list[dict[str, Any]] = []
    for method in methods:
        sub = filter_rows(rows, method=method)
        abs_errors = np.array([float(row["abs_error_m"]) for row in sub], dtype=float)
        summary_rows.append(
            {
                "method": method,
                "mae_m": float(np.mean(abs_errors)),
                "median_abs_error_m": float(np.median(abs_errors)),
                "p90_abs_error_m": float(np.percentile(abs_errors, 90)),
                "within_0p5m": int(np.sum(abs_errors <= 0.5)),
                "within_1m": int(np.sum(abs_errors <= 1.0)),
                "within_2m": int(np.sum(abs_errors <= 2.0)),
                "count": int(abs_errors.size),
            }
        )
    return summary_rows


def plot_method_overview(rows: list[dict[str, Any]], output_dir: Path) -> None:
    methods = unique_in_order(rows, "method")
    method_summary = summarize_method_rows(rows)
    mae_by_method = [next(item for item in method_summary if item["method"] == method)["mae_m"] for method in methods]

    fig, axes = plt.subplots(2, 1, figsize=(14, 10), constrained_layout=True)
    axes[0].bar(methods, mae_by_method, color="tab:blue")
    axes[0].set_ylabel("MAE (m)")
    axes[0].set_title("1to2 Method Mean Absolute Error")
    axes[0].tick_params(axis="x", rotation=35)
    axes[0].grid(True, axis="y", alpha=0.25)

    error_data = [
        np.array([float(row["abs_error_m"]) for row in filter_rows(rows, method=method)], dtype=float)
        for method in methods
    ]
    axes[1].boxplot(error_data, labels=methods, showfliers=True)
    axes[1].set_ylabel("Absolute Error (m)")
    axes[1].set_title("1to2 Absolute Error Distribution by Method")
    axes[1].tick_params(axis="x", rotation=35)
    axes[1].grid(True, axis="y", alpha=0.25)

    fig.savefig(output_dir / "method_overview.png", dpi=160)
    plt.close(fig)


def plot_method_histograms(rows: list[dict[str, Any]], output_dir: Path) -> None:
    methods = unique_in_order(rows, "method")
    for method in methods:
        sub = filter_rows(rows, method=method)
        signed = np.array([float(row["signed_error_m"]) for row in sub], dtype=float)
        abs_errors = np.array([float(row["abs_error_m"]) for row in sub], dtype=float)
        fig, axes = plt.subplots(1, 2, figsize=(13, 4.8), constrained_layout=True)

        axes[0].hist(signed, bins=30, color="tab:orange", edgecolor="black", alpha=0.8)
        axes[0].axvline(0.0, color="black", linewidth=1.0, linestyle="--")
        axes[0].set_title(f"{method} Signed Error")
        axes[0].set_xlabel("Signed Error (m)")
        axes[0].set_ylabel("Count")
        axes[0].grid(True, axis="y", alpha=0.25)

        axes[1].hist(abs_errors, bins=30, color="tab:green", edgecolor="black", alpha=0.8)
        axes[1].set_title(f"{method} Absolute Error")
        axes[1].set_xlabel("Absolute Error (m)")
        axes[1].set_ylabel("Count")
        axes[1].grid(True, axis="y", alpha=0.25)

        fig.savefig(output_dir / f"{method}_histograms.png", dpi=160)
        plt.close(fig)


def plot_pairing_distributions(rows: list[dict[str, Any]], output_dir: Path) -> None:
    pairings = sorted({str(row["pairing"]) for row in rows})
    for pairing in pairings:
        sub = filter_rows(rows, pairing=pairing)
        targets = sorted({str(row["target"]) for row in sub})
        target_positions = {target: idx + 1 for idx, target in enumerate(targets)}

        fig, axes = plt.subplots(2, 1, figsize=(11, 8), constrained_layout=True)
        signed_data = [
            np.array([float(row["signed_error_m"]) for row in sub if row["target"] == target], dtype=float)
            for target in targets
        ]
        abs_data = [
            np.array([float(row["abs_error_m"]) for row in sub if row["target"] == target], dtype=float)
            for target in targets
        ]

        axes[0].boxplot(signed_data, labels=targets, showfliers=True)
        for target in targets:
            target_sub = [row for row in sub if row["target"] == target]
            xpos = np.full(len(target_sub), target_positions[target], dtype=float)
            jitter = np.linspace(-0.08, 0.08, max(1, len(target_sub)))
            yvals = np.array([float(row["signed_error_m"]) for row in target_sub], dtype=float)
            axes[0].plot(xpos + jitter[: len(target_sub)], yvals, "o", alpha=0.65, markersize=4)
        axes[0].axhline(0.0, color="black", linewidth=1.0, linestyle="--")
        axes[0].set_title(f"{pairing} Signed Error by Target")
        axes[0].set_ylabel("Signed Error (m)")
        axes[0].grid(True, axis="y", alpha=0.25)

        axes[1].boxplot(abs_data, labels=targets, showfliers=True)
        for target in targets:
            target_sub = [row for row in sub if row["target"] == target]
            xpos = np.full(len(target_sub), target_positions[target], dtype=float)
            jitter = np.linspace(-0.08, 0.08, max(1, len(target_sub)))
            yvals = np.array([float(row["abs_error_m"]) for row in target_sub], dtype=float)
            axes[1].plot(xpos + jitter[: len(target_sub)], yvals, "o", alpha=0.65, markersize=4)
        axes[1].set_title(f"{pairing} Absolute Error by Target")
        axes[1].set_ylabel("Absolute Error (m)")
        axes[1].grid(True, axis="y", alpha=0.25)

        fig.savefig(output_dir / f"{pairing}.png", dpi=160)
        plt.close(fig)


def summarize_pairing_target_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keys = sorted({(str(row["pairing"]), str(row["target"])) for row in rows})
    summary_rows: list[dict[str, Any]] = []
    for pairing, target in keys:
        sub = [row for row in rows if row["pairing"] == pairing and row["target"] == target]
        abs_errors = np.array([float(row["abs_error_m"]) for row in sub], dtype=float)
        summary_rows.append(
            {
                "pairing": pairing,
                "target": target,
                "mae_m": float(np.mean(abs_errors)),
                "median_abs_error_m": float(np.median(abs_errors)),
                "max_abs_error_m": float(np.max(abs_errors)),
                "count": int(abs_errors.size),
            }
        )
    return summary_rows


def plot_target_summary(rows: list[dict[str, Any]], output_dir: Path) -> None:
    targets = sorted({str(row["target"]) for row in rows})
    summary_rows: list[dict[str, Any]] = []
    for target in targets:
        sub = [row for row in rows if row["target"] == target]
        abs_errors = np.array([float(row["abs_error_m"]) for row in sub], dtype=float)
        summary_rows.append(
            {
                "target": target,
                "mae_m": float(np.mean(abs_errors)),
                "median_abs_error_m": float(np.median(abs_errors)),
                "p90_abs_error_m": float(np.percentile(abs_errors, 90)),
                "count": int(abs_errors.size),
            }
        )
    fig, ax = plt.subplots(figsize=(10, 4.8), constrained_layout=True)
    ax.bar([row["target"] for row in summary_rows], [row["mae_m"] for row in summary_rows], color="tab:red", alpha=0.85, label="MAE")
    ax.plot([row["target"] for row in summary_rows], [row["p90_abs_error_m"] for row in summary_rows], "o-", color="tab:blue", label="P90")
    ax.set_title("Absolute Error Summary by Target Distance Label")
    ax.set_ylabel("Error (m)")
    ax.grid(True, axis="y", alpha=0.25)
    ax.legend()
    fig.savefig(output_dir / "target_summary.png", dpi=160)
    plt.close(fig)


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()

    methods = args.method or list(DEFAULT_METHODS)
    summary = load_summary(args.summary.resolve())
    output_dir = args.output_dir.resolve()
    ensure_dir(output_dir)

    all_rows = collect_rows(summary, methods)
    save_rows_csv(all_rows, output_dir / "all_error_rows.csv")
    save_rows_csv(summarize_method_rows(all_rows), output_dir / "method_summary.csv")

    plot_method_overview(all_rows, output_dir)
    plot_method_histograms(all_rows, output_dir)

    for method in methods:
        method_dir = output_dir / method
        ensure_dir(method_dir)
        method_rows = filter_rows(all_rows, method=method)
        save_rows_csv(method_rows, method_dir / "error_rows.csv")
        save_rows_csv(summarize_pairing_target_rows(method_rows), method_dir / "pairing_target_summary.csv")
        plot_pairing_distributions(method_rows, method_dir)
        plot_target_summary(method_rows, method_dir)

    print(f"saved_plots: {output_dir}")


if __name__ == "__main__":
    main()
