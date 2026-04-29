#!/usr/bin/env python3
"""对 1to1/1to2 等实验目录中的四个连续采样文件做幅值门限滤波。"""

from __future__ import annotations

import argparse
from array import array
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_ROOT = PROJECT_ROOT / "1to1"
BYTES_PER_GR_COMPLEX = 8
TARGET_FILES = (
    "data_reflector_rx_from_initiator_calibration",
    "data_initiator_rx_from_reflector_calibration",
    "data_reflector_rx_from_initiator_measurement",
    "data_initiator_rx_from_reflector_measurement",
)


def resolve_root(root: Path) -> Path:
    if root.is_absolute():
        return root
    return (PROJECT_ROOT / root).resolve()


def resolve_capture_file(root: Path, base_name: str) -> Path:
    candidates = (
        root / base_name,
        root / f"{base_name}.bin",
    )
    for path in candidates:
        if path.exists():
            return path
    raise SystemExit(f"找不到样本文件: {base_name} (已尝试无后缀和 .bin)")


def load_gr_complex_interleaved_float32(path: Path) -> array:
    size = path.stat().st_size
    if size % BYTES_PER_GR_COMPLEX != 0:
        raise SystemExit(f"文件大小不是 8 字节整数倍: {path} (size={size})")
    raw = array("f")
    with path.open("rb") as file:
        raw.frombytes(file.read())
    return raw


def apply_amplitude_gate(path: Path, threshold: float) -> dict[str, float | int | str]:
    iq = load_gr_complex_interleaved_float32(path)
    samples = len(iq) // 2
    if samples == 0:
        with path.open("wb") as file:
            file.write(iq.tobytes())
        return {
            "file": str(path),
            "samples": 0,
            "replaced": 0,
            "ratio": 0.0,
        }

    threshold_square = threshold * threshold
    replaced = 0
    for index in range(0, len(iq), 2):
        i_val = float(iq[index])
        q_val = float(iq[index + 1])
        if (i_val * i_val + q_val * q_val) < threshold_square:
            iq[index] = 0.0
            iq[index + 1] = 0.0
            replaced += 1

    with path.open("wb") as file:
        file.write(iq.tobytes())

    return {
        "file": str(path),
        "samples": samples,
        "replaced": replaced,
        "ratio": float(replaced / samples),
    }


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="对四个连续采样文件做 abs 门限置零（原地覆盖）")
    parser.add_argument(
        "--root",
        type=Path,
        default=DEFAULT_ROOT,
        help="实验根目录，支持相对路径，例如 1to1、1to1_2sides、1to2",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.05,
        help="幅值门限，若 abs(IQ) < threshold，则将该点 I/Q 置为 0（默认 0.05）",
    )
    return parser


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()
    root = resolve_root(args.root)

    if not root.exists():
        raise SystemExit(f"实验目录不存在: {root}")
    if float(args.threshold) < 0.0:
        raise SystemExit("--threshold 必须 >= 0")

    targets = [resolve_capture_file(root, name) for name in TARGET_FILES]

    total_samples = 0
    total_replaced = 0
    print(f"root: {root}")
    print(f"threshold: {float(args.threshold)}")
    for path in targets:
        stats = apply_amplitude_gate(path, float(args.threshold))
        total_samples += int(stats["samples"])
        total_replaced += int(stats["replaced"])
        print(
            "filtered_file: {file}, samples: {samples}, replaced: {replaced}, ratio: {ratio:.6f}".format(
                file=stats["file"],
                samples=int(stats["samples"]),
                replaced=int(stats["replaced"]),
                ratio=float(stats["ratio"]),
            )
        )

    ratio = (total_replaced / total_samples) if total_samples > 0 else 0.0
    print(
        "filter_summary: files: {files}, total_samples: {samples}, total_replaced: {replaced}, ratio: {ratio:.6f}".format(
            files=len(targets),
            samples=total_samples,
            replaced=total_replaced,
            ratio=ratio,
        )
    )


if __name__ == "__main__":
    main()
