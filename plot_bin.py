#!/usr/bin/env python3
"""批量为 GNU Radio 实验目录下四个 data_store 的 bin 文件出图。"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


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


def resolve_root(root: Path) -> Path:
    if root.is_absolute():
        return root
    return (PROJECT_ROOT / root).resolve()


def list_bin_files(dir_path: Path) -> list[Path]:
    return sorted(dir_path.glob("data_*.bin"), key=lambda p: int(p.stem.split("_")[-1]))


def load_gr_complex_bin(path: Path) -> np.ndarray:
    return np.fromfile(path, dtype=np.complex64)


def select_window(x: np.ndarray, max_points: int) -> tuple[np.ndarray, np.ndarray]:
    """默认选择文件中间窗口，避免总是画到头部零段。"""
    n = min(x.size, max_points)
    start = max((x.size - n) // 2, 0)
    end = start + n
    idx = np.arange(start, end)
    return x[start:end], idx


def plot_time_iq(x: np.ndarray, save_path: Path, max_points: int) -> None:
    window, idx = select_window(x, max_points)
    plt.figure(figsize=(10, 4))
    plt.plot(idx, np.real(window), label="I", linewidth=1.0)
    plt.plot(idx, np.imag(window), label="Q", linewidth=1.0)
    plt.xlabel("Sample Index")
    plt.ylabel("Amplitude")
    plt.title("Time Domain I/Q")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(save_path, dpi=150)
    plt.close()


def plot_amplitude_phase(x: np.ndarray, save_prefix: Path, max_points: int) -> None:
    window, idx = select_window(x, max_points)
    amp = np.abs(window)
    phase = np.angle(window)
    unwrapped = np.unwrap(phase)

    plt.figure(figsize=(10, 4))
    plt.plot(idx, amp, linewidth=1.0)
    plt.xlabel("Sample Index")
    plt.ylabel("|x[n]|")
    plt.title("Amplitude")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(save_prefix.with_name(save_prefix.name + "_amplitude.png"), dpi=150)
    plt.close()

    plt.figure(figsize=(10, 4))
    plt.plot(idx, phase, linewidth=1.0)
    plt.xlabel("Sample Index")
    plt.ylabel("Phase (rad)")
    plt.title("Wrapped Phase")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(save_prefix.with_name(save_prefix.name + "_phase.png"), dpi=150)
    plt.close()

    plt.figure(figsize=(10, 4))
    plt.plot(idx, unwrapped, linewidth=1.0)
    plt.xlabel("Sample Index")
    plt.ylabel("Unwrapped Phase (rad)")
    plt.title("Unwrapped Phase")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(save_prefix.with_name(save_prefix.name + "_phase_unwrapped.png"), dpi=150)
    plt.close()


def plot_constellation(x: np.ndarray, save_path: Path, max_points: int) -> None:
    window, _ = select_window(x, max_points)
    plt.figure(figsize=(5, 5))
    plt.scatter(np.real(window), np.imag(window), s=4, alpha=0.5)
    plt.xlabel("Real")
    plt.ylabel("Imag")
    plt.title("Constellation")
    plt.grid(True, alpha=0.3)
    plt.axis("equal")
    plt.tight_layout()
    plt.savefig(save_path, dpi=150)
    plt.close()


def plot_file(file_path: Path, output_base: Path, max_points: int) -> None:
    x = load_gr_complex_bin(file_path)
    target_dir = output_base / file_path.parent.name / file_path.stem
    target_dir.mkdir(parents=True, exist_ok=True)
    plot_time_iq(x, target_dir / f"{file_path.stem}_iq.png", max_points)
    plot_amplitude_phase(x, target_dir / file_path.stem, max_points)
    plot_constellation(x, target_dir / f"{file_path.stem}_constellation.png", max_points)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="批量为 GNU Radio 实验目录下四个 data_store 文件夹出图")
    parser.add_argument(
        "--root",
        type=Path,
        default=DEFAULT_ROOT,
        help="实验根目录，支持相对路径，例如 self、1to1、1to2",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "output_plot_bin",
        help="图像输出目录",
    )
    parser.add_argument(
        "--max-points",
        type=int,
        default=4000,
        help="每张图最多绘制多少个样本点",
    )
    return parser


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()
    args.root = resolve_root(args.root)
    args.output_dir = args.output_dir.resolve()

    if not args.root.exists():
        raise SystemExit(f"实验目录不存在: {args.root}")

    data_dirs = discover_data_dirs(args.root)

    print(f"== root: {args.root} ==")
    for dir_path in data_dirs:
        print(f"== plot directory: {dir_path} ==")
        for file_path in list_bin_files(dir_path):
            plot_file(file_path, args.output_dir / args.root.name, args.max_points)
            print(file_path.name)


if __name__ == "__main__":
    main()
