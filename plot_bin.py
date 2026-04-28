#!/usr/bin/env python3
"""批量为 GNU Radio 实验目录下四个 data_store 的 bin 文件出图。"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


PROJECT_ROOT = Path(__file__).resolve().parent
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
DIR_LABELS = {
    "data_reflector_rx_from_initiator": "reflector",
    "data_reflector_ref_local": "reflector_ref",
    "data_initiator_rx_from_reflector": "initiator",
    "data_initiator_ref_local": "initiator_ref",
}
NEW_STYLE_RE = re.compile(r"^data_f(?P<freq>\d+)_r(?P<repeat>\d+)$")
OLD_STYLE_RE = re.compile(r"^data_(?P<index>\d+)$")


def discover_data_dirs(root: Path) -> list[Path]:
    """自动发现实验目录下的 data_* 文件夹。"""
    return sorted([p for p in root.iterdir() if p.is_dir() and p.name.startswith("data_")])


def resolve_root(root: Path) -> Path:
    if root.is_absolute():
        return root
    return (PROJECT_ROOT / root).resolve()


def list_bin_files(dir_path: Path) -> list[Path]:
    return sorted(dir_path.glob("data_*.bin"), key=file_sort_key)


def filter_bin_files(files: list[Path], first_repeat_only: bool) -> list[Path]:
    """按需只保留每个频点的第一次测量。"""
    if not first_repeat_only:
        return files

    filtered: list[Path] = []
    for file_path in files:
        tokens = parse_file_tokens(file_path)
        repeat_index = tokens["repeat_index"]
        # 旧命名没有重复编号，保持兼容，仍然画出来。
        if repeat_index is None or int(repeat_index) == 0:
            filtered.append(file_path)
    return filtered


def parse_file_tokens(path: Path) -> dict[str, int | None]:
    stem = path.stem
    new_match = NEW_STYLE_RE.match(stem)
    if new_match:
        return {
            "freq_index": int(new_match.group("freq")),
            "repeat_index": int(new_match.group("repeat")),
            "legacy_index": None,
        }

    old_match = OLD_STYLE_RE.match(stem)
    if old_match:
        return {
            "freq_index": None,
            "repeat_index": None,
            "legacy_index": int(old_match.group("index")),
        }

    return {
        "freq_index": None,
        "repeat_index": None,
        "legacy_index": None,
    }


def file_sort_key(path: Path) -> tuple[int, int, int, str]:
    tokens = parse_file_tokens(path)
    freq_index = tokens["freq_index"]
    repeat_index = tokens["repeat_index"]
    legacy_index = tokens["legacy_index"]

    if freq_index is not None and repeat_index is not None:
        return (0, int(freq_index), int(repeat_index), path.name)
    if legacy_index is not None:
        return (1, int(legacy_index), 0, path.name)
    return (2, 0, 0, path.name)


def load_gr_complex_bin(path: Path) -> np.ndarray:
    return np.fromfile(path, dtype=np.complex64)


def output_label(dir_path: Path) -> str:
    return DIR_LABELS.get(dir_path.name, dir_path.name)


def plot_stem(file_path: Path, sequence_index: int) -> str:
    tokens = parse_file_tokens(file_path)
    if tokens["freq_index"] is not None and tokens["repeat_index"] is not None:
        return (
            f"burst_{sequence_index:03d}"
            f"_f{int(tokens['freq_index']):02d}"
            f"_r{int(tokens['repeat_index'])}"
        )
    if tokens["legacy_index"] is not None:
        return f"burst_{sequence_index:03d}_data_{int(tokens['legacy_index']):03d}"
    return f"burst_{sequence_index:03d}_{file_path.stem}"


def select_window(x: np.ndarray, max_points: int | None) -> tuple[np.ndarray, np.ndarray]:
    """max_points 为空或非正数时绘制全部样本。"""
    if max_points is None or max_points <= 0 or max_points >= x.size:
        idx = np.arange(x.size)
        return x, idx

    n = max_points
    start = max((x.size - n) // 2, 0)
    end = start + n
    idx = np.arange(start, end)
    return x[start:end], idx


def plot_time_iq(x: np.ndarray, save_path: Path, max_points: int | None) -> None:
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


def plot_amplitude_phase(x: np.ndarray, save_prefix: Path, max_points: int | None) -> None:
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


def plot_constellation(x: np.ndarray, save_path: Path, max_points: int | None) -> None:
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


def plot_file(
    file_path: Path,
    output_base: Path,
    sequence_index: int,
    max_points: int | None,
    all_plots: bool,
) -> None:
    x = load_gr_complex_bin(file_path)
    target_dir = output_base / output_label(file_path.parent) / "bursts"
    target_dir.mkdir(parents=True, exist_ok=True)
    stem = plot_stem(file_path, sequence_index)
    plot_constellation(x, target_dir / f"{stem}.png", max_points)
    if all_plots:
        plot_time_iq(x, target_dir / f"{stem}_iq.png", max_points)
        plot_amplitude_phase(x, target_dir / stem, max_points)


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
        default=0,
        help="每张图最多绘制多少个样本点；默认 0 表示绘制全部样本",
    )
    parser.add_argument(
        "--all-plots",
        action="store_true",
        help="同时生成 IQ、幅度、相位、解包相位图；默认只生成星座图",
    )
    parser.add_argument(
        "--first-repeat-only",
        action="store_true",
        help="只画每个频点的第一次测量 r00；默认画出所有重复测量",
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
        files = filter_bin_files(
            list_bin_files(dir_path),
            first_repeat_only=args.first_repeat_only,
        )
        for index, file_path in enumerate(files, start=1):
            plot_file(
                file_path,
                args.output_dir / args.root.name,
                index,
                args.max_points,
                args.all_plots,
            )
            print(file_path.name)


if __name__ == "__main__":
    main()
