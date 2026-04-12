#!/usr/bin/env python3
"""用 USRP 作为宽带示波器采集 BLE Channel Sounding 空口 IQ。"""

from __future__ import annotations

import argparse
import json
import math
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path

from gnuradio import blocks, gr, uhd


BYTES_PER_COMPLEX64 = 8
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "usrp_ble_scope" / "captures"


class UsrpIqCapture(gr.top_block):
    def __init__(self, args: argparse.Namespace, iq_paths: list[Path]):
        super().__init__("usrp_ble_scope_capture", catch_exceptions=True)

        dev_args = args.device_args
        if args.recv_frame_size:
            dev_args = ",".join([dev_args, f"recv_frame_size={args.recv_frame_size}"])

        self.source = uhd.usrp_source(
            dev_args,
            uhd.stream_args(
                cpu_format="fc32",
                args="",
                channels=args.channels,
            ),
        )
        if args.subdev:
            self.source.set_subdev_spec(args.subdev, 0)

        self.source.set_samp_rate(args.sample_rate)
        self.source.set_time_unknown_pps(uhd.time_spec(0))

        for stream_index, channel in enumerate(args.channels):
            self.source.set_center_freq(args.center_freq, channel)
            self.source.set_antenna(args.antenna, channel)
            self.source.set_gain(args.gain, channel)
            if args.bandwidth > 0:
                self.source.set_bandwidth(args.bandwidth, channel)

            sink = blocks.file_sink(gr.sizeof_gr_complex, str(iq_paths[stream_index]), False)
            sink.set_unbuffered(False)

            if args.duration > 0:
                sample_count = int(math.ceil(args.duration * args.sample_rate))
                head = blocks.head(gr.sizeof_gr_complex, sample_count)
                self.connect((self.source, stream_index), head, sink)
            else:
                self.connect((self.source, stream_index), sink)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="USRP 宽带连续采集 BLE Channel Sounding 空口 IQ，并保存元数据"
    )
    parser.add_argument("--device-args", default="addr=192.168.10.2", help="UHD 设备参数")
    parser.add_argument("--recv-frame-size", type=int, default=1472, help="UHD recv_frame_size；0 表示不设置")
    parser.add_argument("--sample-rate", type=float, default=100e6, help="采样率，默认沿用 1to1.grc 的 100e6")
    parser.add_argument("--center-freq", type=float, default=2.44e9, help="中心频率，默认 2.44 GHz")
    parser.add_argument("--gain", type=float, default=0.0, help="接收增益，默认沿用 1to1.grc 的 0")
    parser.add_argument("--antenna", default="RX2", help="接收天线端口，默认 RX2")
    parser.add_argument("--bandwidth", type=float, default=0.0, help="模拟带宽；0 表示不显式设置")
    parser.add_argument("--subdev", default="A:0 B:0", help="子设备规格；单通道也可保留该默认值")
    parser.add_argument("--channels", type=int, nargs="+", default=[0], help="采集的 USRP 通道，例如 0 或 0 1")
    parser.add_argument("--duration", type=float, default=0.1, help="采集时长，秒；0 表示持续采集直到 Ctrl+C")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="输出目录")
    parser.add_argument("--prefix", default="ble_cs_scope", help="输出文件名前缀")
    return parser


def build_paths(output_dir: Path, prefix: str, channels: list[int]) -> tuple[Path, list[Path]]:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    capture_dir = output_dir / f"{prefix}_{timestamp}"
    iq_paths = [capture_dir / f"{prefix}_ch{channel}.c64" for channel in channels]
    return capture_dir, iq_paths


def write_metadata(args: argparse.Namespace, capture_dir: Path, iq_paths: list[Path], state: str) -> None:
    metadata = {
        "state": state,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "format": "complex64 little-endian interleaved IQ per channel file",
        "bytes_per_sample": BYTES_PER_COMPLEX64,
        "device_args": args.device_args,
        "recv_frame_size": args.recv_frame_size,
        "sample_rate": args.sample_rate,
        "center_freq": args.center_freq,
        "gain": args.gain,
        "antenna": args.antenna,
        "bandwidth": args.bandwidth,
        "subdev": args.subdev,
        "channels": args.channels,
        "duration": args.duration,
        "files": [],
    }

    for channel, path in zip(args.channels, iq_paths):
        size = path.stat().st_size if path.exists() else 0
        metadata["files"].append(
            {
                "channel": channel,
                "path": str(path),
                "file_size_bytes": size,
                "samples": size // BYTES_PER_COMPLEX64,
            }
        )

    metadata_path = capture_dir / "metadata.json"
    metadata_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def main() -> int:
    args = build_argument_parser().parse_args()
    capture_dir, iq_paths = build_paths(args.output_dir, args.prefix, args.channels)
    capture_dir.mkdir(parents=True, exist_ok=True)

    tb = UsrpIqCapture(args, iq_paths)

    interrupted = False

    def handle_signal(signum, frame):  # noqa: ARG001
        nonlocal interrupted
        interrupted = True
        tb.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    print(f"capture_dir: {capture_dir}")
    for path in iq_paths:
        print(f"iq_file: {path}")
    print(f"sample_rate: {args.sample_rate:g} sps, center_freq: {args.center_freq:g} Hz")

    try:
        tb.start()
        tb.wait()
    except KeyboardInterrupt:
        interrupted = True
        tb.stop()
        tb.wait()
    finally:
        write_metadata(args, capture_dir, iq_paths, "interrupted" if interrupted else "completed")

    print(f"metadata: {capture_dir / 'metadata.json'}")
    return 130 if interrupted else 0


if __name__ == "__main__":
    sys.exit(main())
