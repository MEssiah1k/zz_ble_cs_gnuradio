#!/usr/bin/env python3
"""Zero complex64 samples whose amplitude is below a threshold."""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np


PROJECT_ROOT = Path(__file__).resolve().parent


def resolve_path(path: Path) -> Path:
    if path.is_absolute():
        return path
    return (PROJECT_ROOT / path).resolve()


def filter_one_file(input_path: Path, output_path: Path, threshold: float) -> dict[str, float | int | str]:
    if threshold < 0:
        raise SystemExit("threshold 必须 >= 0")
    if not input_path.exists():
        raise SystemExit(f"找不到输入文件: {input_path}")

    x = np.fromfile(input_path, dtype=np.complex64)
    clear = np.abs(x) < float(threshold)
    y = x.copy()
    y[clear] = np.complex64(0.0 + 0.0j)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    y.astype(np.complex64, copy=False).tofile(output_path)

    total = int(x.size)
    cleared = int(np.count_nonzero(clear))
    kept = int(total - cleared)
    cleared_ratio = 0.0 if total == 0 else cleared / total
    return {
        "input": str(input_path),
        "output": str(output_path),
        "threshold": float(threshold),
        "total_samples": total,
        "kept_nonzero_samples": kept,
        "cleared_samples": cleared,
        "cleared_ratio": cleared_ratio,
    }


def default_output_path(input_path: Path, suffix: str) -> Path:
    return input_path.with_name(input_path.name + suffix)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="将 complex64 样本中 abs 小于阈值的点置零，保持样本数量不变")
    parser.add_argument("inputs", type=Path, nargs="+", help="输入 complex64 二进制文件")
    parser.add_argument("--threshold", type=float, default=0.05, help="幅值阈值；默认 0.05")
    parser.add_argument("--output", type=Path, default=None, help="单输入文件时的输出路径；不指定时直接覆盖输入文件")
    parser.add_argument("--output-dir", type=Path, default=None, help="多输入文件时输出目录；不指定时直接覆盖输入文件")
    parser.add_argument("--suffix", default="_amp_ge_0p05", help="指定 --output-dir 时追加到文件名后的后缀")
    return parser


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()

    input_paths = [resolve_path(path) for path in args.inputs]
    if args.output is not None and len(input_paths) != 1:
        raise SystemExit("--output 只能在单个输入文件时使用")
    if args.output is not None and args.output_dir is not None:
        raise SystemExit("--output 和 --output-dir 不能同时使用")

    for input_path in input_paths:
        if args.output is not None:
            output_path = resolve_path(args.output)
        elif args.output_dir is not None:
            output_path = resolve_path(args.output_dir) / (input_path.name + args.suffix)
        else:
            output_path = input_path

        result = filter_one_file(input_path, output_path, args.threshold)
        print(
            "filtered: {input} -> {output}, threshold={threshold}, "
            "kept_nonzero={kept_nonzero_samples}/{total_samples}, cleared={cleared_samples} ({cleared_ratio:.2%})".format(
                **result
            )
        )


if __name__ == "__main__":
    main()
