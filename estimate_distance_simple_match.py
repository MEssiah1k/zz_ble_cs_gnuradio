#!/usr/bin/env python3
"""
Simple distance estimation via wrapped phase error minimization.

Direct scan without unwrapping or slope fitting.
Optional: take differential phase if two CSVs provided.
"""

from __future__ import annotations

import argparse
import csv
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

PROPAGATION_SPEED_MPS = 2.3e8  # Effective BLE channel sounder speed (0.767c)


def wrap_to_pi(x: np.ndarray | float) -> np.ndarray | float:
    """Wrap angle to (-π, π]."""
    return (np.asarray(x) + np.pi) % (2.0 * np.pi) - np.pi


def load_pair_phase_csv(csv_path: Path) -> tuple[np.ndarray, np.ndarray]:
    """Load frequencies and pair phases. Return (freqs_hz, pair_phase_rad)."""
    freqs = []
    phases = []
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            freqs.append(float(row["freq_hz"]))
            phase = float(row.get("pair_phase_rad", row.get("pair_angle_rad", 0.0)))
            phases.append(phase)
    return np.array(freqs, dtype=float), np.array(phases, dtype=float)


def solve_phase_offset_and_error(
    measured_wrapped: np.ndarray,
    model_phase: np.ndarray,
) -> tuple[float, float, float]:
    """Find optimal phase0 to minimize wrapped error.
    
    Return: (phase0, rms_error, coherency)
    
    coherency = |mean(exp(j*residual))| measures how well phases align
    """
    residual = measured_wrapped - model_phase
    coherent_sum = np.mean(np.exp(1j * residual))
    phase0 = float(np.angle(coherent_sum))
    coherency = float(np.abs(coherent_sum))  # ← KEY: this measures alignment quality
    
    wrapped_error = wrap_to_pi(measured_wrapped - (model_phase + phase0))
    rms_error = float(np.sqrt(np.mean(wrapped_error ** 2)))
    return phase0, rms_error, coherency


def estimate_distance_simple(
    measurement_csv: Path,
    calibration_csv: Path | None = None,
    grid_start_m: float = 0.0,
    grid_stop_m: float = 30.0,
    grid_step_m: float = 0.01,
) -> dict[str, Any]:
    """
    Scan distance grid and find best wrapped phase match.
    
    Uses wrapped phase COST (mean of squared error) as criterion:
      cost = mean(wrapped_error²)
    Lower cost = better distance match
    
    This matches the existing phase_match evaluation method.
    """
    
    freq_m, phase_m = load_pair_phase_csv(measurement_csv)
    
    if calibration_csv is not None:
        freq_c, phase_c = load_pair_phase_csv(calibration_csv)
        n = min(len(freq_m), len(freq_c))
        freq_m = freq_m[:n]
        phase_m = phase_m[:n]
        phase_c = phase_c[:n]
        phase_m = phase_m - phase_c
    
    if len(freq_m) < 2:
        raise ValueError("Need at least 2 frequency points")
    
    measured_wrapped = wrap_to_pi(phase_m)
    
    # Distance grid
    distance_grid = np.arange(
        grid_start_m,
        grid_stop_m + 0.5 * grid_step_m,
        grid_step_m,
        dtype=float,
    )
    
    costs = []
    rms_errors = []
    phase0_values = []
    
    for distance_m in distance_grid:
        model_phase = -4.0 * np.pi * freq_m * distance_m / PROPAGATION_SPEED_MPS
        phase0, rms_error, coherency = solve_phase_offset_and_error(measured_wrapped, model_phase)
        phase0_values.append(phase0)
        cost = rms_error ** 2  # Same as mean(wrapped_error²)
        costs.append(cost)
        rms_errors.append(rms_error)
    
    # Find best distance by MINIMUM COST (mean of squared error)
    best_idx = int(np.argmin(costs))
    best_distance = float(distance_grid[best_idx])
    best_cost = float(costs[best_idx])
    best_rms_error = float(rms_errors[best_idx])
    best_phase0 = float(phase0_values[best_idx])
    
    # Second best (by cost)
    exclude_radius = max(0.25, 2.0 * grid_step_m)
    neighbor_mask = np.abs(distance_grid - best_distance) <= exclude_radius
    candidate_costs = np.array(costs)
    candidate_costs[neighbor_mask] = np.inf
    second_best_idx = int(np.argmin(candidate_costs))
    second_best_distance = float(distance_grid[second_best_idx])
    second_best_cost = float(costs[second_best_idx])
    
    cost_margin = second_best_cost - best_cost
    cost_ratio = second_best_cost / (best_cost + 1e-12)
    
    return {
        "distance_m": best_distance,
        "cost": best_cost,
        "rms_error": best_rms_error,
        "phase0_rad": best_phase0,
        "second_best_distance_m": second_best_distance,
        "second_best_cost": second_best_cost,
        "cost_margin": cost_margin,
        "cost_ratio": cost_ratio,
        "num_frequencies": len(freq_m),
        "freq_min_hz": float(freq_m[0]),
        "freq_max_hz": float(freq_m[-1]),
        "distance_grid_m": [float(d) for d in distance_grid],
        "cost_grid": [float(c) for c in costs],
        "rms_error_grid": [float(e) for e in rms_errors],
    }


def plot_error_spectrum(
    distance_grid: np.ndarray,
    costs: list[float],
    best_distance: float,
    save_path: Path,
) -> None:
    """Plot cost spectrum."""
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(distance_grid, costs, "o-", linewidth=1.5, markersize=3, color="blue", label="Cost (lower is better)")
    ax.axvline(best_distance, color="r", linestyle="--", linewidth=2, label=f"Best: {best_distance:.2f} m")
    ax.set_ylabel("Wrapped Phase Cost (mean squared error)")
    ax.set_xlabel("Distance (m)")
    ax.set_title(f"Distance Matching via Wrapped Phase Error\nBest: {best_distance:.2f} m")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    save_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(save_path, dpi=150)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--measurement", type=Path, required=True)
    parser.add_argument("--calibration", type=Path, default=None)
    parser.add_argument("--output", type=Path, default=Path("output_distance_simple"))
    parser.add_argument("--grid-start", type=float, default=0.0)
    parser.add_argument("--grid-stop", type=float, default=30.0)
    parser.add_argument("--grid-step", type=float, default=0.01)
    
    args = parser.parse_args()
    
    result = estimate_distance_simple(
        args.measurement,
        args.calibration,
        args.grid_start,
        args.grid_stop,
        args.grid_step,
    )
    
    args.output.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    json_path = args.output / f"distance_{ts}.json"
    json_path.write_text(json.dumps(result, indent=2))
    print(f"Saved: {json_path}")
    
    plot_path = args.output / f"spectrum_{ts}.png"
    plot_error_spectrum(
        np.array(result["distance_grid_m"]),
        result["cost_grid"],
        result["distance_m"],
        plot_path,
    )
    print(f"Saved: {plot_path}")
    
    print(f"\nDistance: {result['distance_m']:.3f} m")
    print(f"Cost: {result['cost']:.6f}")
    print(f"RMS error: {result['rms_error']:.4f} rad")


if __name__ == "__main__":
    main()
