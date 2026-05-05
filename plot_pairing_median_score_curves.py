from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_ERROR_ROWS = (
    PROJECT_ROOT
    / "DATA"
    / "DATA_1to2"
    / "error_plots"
    / "recommended_distance_estimate"
    / "error_rows.csv"
)
DEFAULT_ROOT = PROJECT_ROOT / "DATA" / "DATA_1to2"


@dataclass
class SampleRow:
    pairing: str
    group: str
    target: str
    baseline_distance_m: float
    estimated_distance_m: float
    signed_error_m: float
    abs_error_m: float


def load_error_rows(path: Path, pairing: str) -> list[SampleRow]:
    rows: list[SampleRow] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row["pairing"] != pairing:
                continue
            rows.append(
                SampleRow(
                    pairing=row["pairing"],
                    group=row["group"],
                    target=row["target"],
                    baseline_distance_m=float(row["baseline_distance_m"]),
                    estimated_distance_m=float(row["estimated_distance_m"]),
                    signed_error_m=float(row["signed_error_m"]),
                    abs_error_m=float(row["abs_error_m"]),
                )
            )
    return rows


def choose_median_sample(rows: list[SampleRow]) -> tuple[SampleRow, float]:
    signed_errors = np.asarray([row.signed_error_m for row in rows], dtype=float)
    median_error = float(np.median(signed_errors))
    chosen = min(
        rows,
        key=lambda row: (
            abs(row.signed_error_m - median_error),
            abs(row.abs_error_m - abs(median_error)),
            row.group,
        ),
    )
    return chosen, median_error


def load_score_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def find_top_peaks(distance_grid: np.ndarray, score_grid: np.ndarray, top_k: int = 5) -> list[tuple[float, float]]:
    if score_grid.size == 0:
        return []

    candidate_indices: list[int] = []
    for idx in range(score_grid.size):
        left = score_grid[idx - 1] if idx > 0 else -np.inf
        right = score_grid[idx + 1] if idx + 1 < score_grid.size else -np.inf
        if score_grid[idx] >= left and score_grid[idx] >= right:
            candidate_indices.append(idx)

    if not candidate_indices:
        candidate_indices = list(range(score_grid.size))

    candidate_indices.sort(key=lambda idx: float(score_grid[idx]), reverse=True)
    unique_peaks: list[tuple[float, float]] = []
    for idx in candidate_indices:
        distance = float(distance_grid[idx])
        score = float(score_grid[idx])
        if any(abs(distance - prev_distance) <= 0.2 for prev_distance, _ in unique_peaks):
            continue
        unique_peaks.append((distance, score))
        if len(unique_peaks) >= top_k:
            break
    return unique_peaks


def plot_target_curve(
    ax: plt.Axes,
    sample: SampleRow,
    median_error: float,
    score_payload: dict[str, Any],
) -> list[tuple[float, float]]:
    distance_grid = np.asarray(score_payload["distance_grid_m"], dtype=float)
    score_grid = np.asarray(score_payload["score_grid"], dtype=float)
    peaks = find_top_peaks(distance_grid, score_grid, top_k=5)

    ax.plot(distance_grid, score_grid, color="#1f77b4", lw=1.6)
    ax.axvline(sample.baseline_distance_m, color="#2ca02c", ls="--", lw=1.2, label="1to1 baseline")
    ax.axvline(sample.estimated_distance_m, color="#d62728", ls="-", lw=1.2, label="estimated")

    for idx, (peak_distance, peak_score) in enumerate(peaks, start=1):
        color = "#d62728" if abs(peak_distance - sample.estimated_distance_m) <= 1e-9 else "#ff7f0e"
        ax.scatter([peak_distance], [peak_score], color=color, s=28, zorder=3)
        ax.text(
            peak_distance,
            peak_score + 0.003,
            f"#{idx} {peak_distance:.2f}m",
            ha="center",
            va="bottom",
            fontsize=8,
            color=color,
        )

    ax.set_title(
        f"{sample.target} | {sample.group}\n"
        f"median signed err {median_error:+.2f} m, sample err {sample.signed_error_m:+.2f} m",
        fontsize=11,
    )
    ax.set_xlabel("Candidate Distance (m)")
    ax.set_ylabel("Cluster Score")
    ax.grid(True, alpha=0.25)
    ax.legend(loc="upper right", fontsize=8)
    return peaks


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot median-near score curves for a pairing.")
    parser.add_argument("--pairing", required=True, help="Pairing label such as 2m__3m")
    parser.add_argument(
        "--error-rows",
        type=Path,
        default=DEFAULT_ERROR_ROWS,
        help="CSV file containing recommended_distance_estimate error rows.",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=DEFAULT_ROOT,
        help="DATA_1to2 root containing pairing folders.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output image path. Defaults under DATA_1to2/error_plots/median_score_curves/",
    )
    args = parser.parse_args()

    pairing_rows = load_error_rows(args.error_rows, args.pairing)
    if not pairing_rows:
        raise SystemExit(f"no rows found for pairing {args.pairing}")

    targets = sorted({row.target for row in pairing_rows})
    chosen_samples: list[tuple[SampleRow, float, dict[str, Any]]] = []
    for target in targets:
        target_rows = [row for row in pairing_rows if row.target == target]
        sample, median_error = choose_median_sample(target_rows)
        score_path = args.root / args.pairing / sample.group / f"target_{target}" / "distance_phase_cluster_match.json"
        score_payload = load_score_json(score_path)
        chosen_samples.append((sample, median_error, score_payload))

    output_path = args.output
    if output_path is None:
        output_path = (
            args.root
            / "error_plots"
            / "median_score_curves"
            / f"{args.pairing}_median_score_curves.png"
        )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fig, axes = plt.subplots(1, len(chosen_samples), figsize=(7.5 * len(chosen_samples), 5.5), constrained_layout=True)
    if len(chosen_samples) == 1:
        axes = [axes]

    summary_lines: list[str] = []
    for ax, (sample, median_error, score_payload) in zip(axes, chosen_samples):
        peaks = plot_target_curve(ax, sample, median_error, score_payload)
        peak_text = ", ".join(f"{distance:.2f}m({score:.3f})" for distance, score in peaks[:5])
        summary_lines.append(
            f"{sample.target}: group={sample.group}, baseline={sample.baseline_distance_m:.2f}m, "
            f"estimate={sample.estimated_distance_m:.2f}m, sample_err={sample.signed_error_m:+.2f}m, "
            f"median_err={median_error:+.2f}m, top_peaks={peak_text}"
        )

    fig.suptitle(f"{args.pairing} Median-Near Phase-Cluster Score Curves", fontsize=14)
    fig.savefig(output_path, dpi=180)
    plt.close(fig)

    print(f"saved_plot={output_path}")
    for line in summary_lines:
        print(line)


if __name__ == "__main__":
    main()
