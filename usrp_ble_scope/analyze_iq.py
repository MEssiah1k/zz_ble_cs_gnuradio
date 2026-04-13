#!/usr/bin/env python3
"""离线分析 USRP 示波器采到的 complex64 IQ 文件。

当前定位：
- overview：整段幅度图和 spectrogram，快速确认是否采到信号。
- scan：自动检测 burst，输出 bursts.csv。
- burst：按 burst_id 或样本范围切出单个 burst，画局部细节图。
- gfsk：对单个 burst 做频移、降采样和 quadrature demod，观察 GFSK 瞬时频率。
- plus：一次性做总览、严格扫描、逐个 burst 细节和 GFSK 图。
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from scipy import signal


BYTES_PER_COMPLEX64 = 8
COMMANDS = {"overview", "scan", "burst", "gfsk", "plus"}


def load_metadata(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def infer_sample_rate(args: argparse.Namespace, metadata: dict[str, Any]) -> float:
    if args.sample_rate is not None:
        return args.sample_rate
    if "sample_rate" in metadata:
        return float(metadata["sample_rate"])
    raise SystemExit("缺少采样率：请传入 --sample-rate，或提供包含 sample_rate 的 --metadata")


def infer_center_freq(args: argparse.Namespace, metadata: dict[str, Any]) -> float:
    if args.center_freq is not None:
        return args.center_freq
    return float(metadata.get("center_freq", 0.0))


def total_samples(path: Path) -> int:
    return path.stat().st_size // BYTES_PER_COMPLEX64


def read_complex_window(path: Path, start_sample: int, max_samples: int | None) -> np.ndarray:
    n_total = total_samples(path)
    if start_sample < 0:
        raise SystemExit("start_sample 不能小于 0")
    if start_sample >= n_total:
        raise SystemExit(f"start_sample 超出文件长度：{start_sample} >= {n_total}")

    count = n_total - start_sample if max_samples is None else min(max_samples, n_total - start_sample)
    x = np.memmap(path, dtype=np.complex64, mode="r", offset=start_sample * BYTES_PER_COMPLEX64, shape=(count,))
    return np.asarray(x)


def output_dir_for(args: argparse.Namespace) -> Path:
    return args.output_dir or (args.iq_file.parent / "analysis")


def envelope_for_plot(x: np.ndarray, sample_rate: float, max_points: int) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    amp = np.abs(x)
    if amp.size <= max_points:
        t = np.arange(amp.size, dtype=float) / sample_rate
        return t, amp, amp

    bin_size = int(np.ceil(amp.size / max_points))
    used = (amp.size // bin_size) * bin_size
    binned = amp[:used].reshape(-1, bin_size)
    amp_max = np.max(binned, axis=1)
    amp_mean = np.mean(binned, axis=1)
    t = (np.arange(amp_max.size, dtype=float) * bin_size) / sample_rate
    return t, amp_mean, amp_max


def plot_amplitude(x: np.ndarray, sample_rate: float, save_path: Path, max_points: int) -> None:
    t, amp_mean, amp_max = envelope_for_plot(x, sample_rate, max_points)
    plt.figure(figsize=(12, 4))
    plt.plot(t, amp_max, linewidth=0.8, label="max envelope")
    if amp_mean.size == amp_max.size and not np.array_equal(amp_mean, amp_max):
        plt.plot(t, amp_mean, linewidth=0.8, alpha=0.8, label="mean envelope")
        plt.legend()
    plt.xlabel("Time (s)")
    plt.ylabel("|IQ|")
    plt.title("Amplitude vs Time")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(save_path, dpi=150)
    plt.close()


def plot_spectrogram(
    x: np.ndarray,
    sample_rate: float,
    center_freq: float,
    save_path: Path,
    nfft: int,
    noverlap: int,
    cmap: str,
) -> None:
    plt.figure(figsize=(12, 6))
    plt.specgram(
        x,
        NFFT=nfft,
        Fs=sample_rate,
        Fc=center_freq,
        noverlap=noverlap,
        mode="psd",
        scale="dB",
        cmap=cmap,
    )
    plt.xlabel("Time (s)")
    plt.ylabel("Frequency (Hz)")
    plt.title("Spectrogram")
    plt.colorbar(label="Power/Frequency (dB/Hz)")
    plt.tight_layout()
    plt.savefig(save_path, dpi=150)
    plt.close()


def merge_short_gaps(mask: np.ndarray, max_gap: int) -> np.ndarray:
    if max_gap <= 0 or mask.size == 0:
        return mask

    edges = np.diff(mask.astype(np.int8), prepend=0, append=0)
    starts = np.flatnonzero(edges == 1)
    ends = np.flatnonzero(edges == -1)
    merged = mask.copy()
    for prev_end, next_start in zip(ends[:-1], starts[1:]):
        if next_start - prev_end <= max_gap:
            merged[prev_end:next_start] = True
    return merged


def find_regions(mask: np.ndarray, min_len: int) -> list[tuple[int, int]]:
    edges = np.diff(mask.astype(np.int8), prepend=0, append=0)
    starts = np.flatnonzero(edges == 1)
    ends = np.flatnonzero(edges == -1)
    return [(int(s), int(e)) for s, e in zip(starts, ends) if e - s >= min_len]


def estimate_peak_frequency(x: np.ndarray, sample_rate: float, center_freq: float, max_fft: int = 262_144) -> tuple[float, float]:
    if x.size == 0:
        return center_freq, 0.0
    nfft = 1 << int(math.floor(math.log2(min(max_fft, max(16, x.size)))))
    window = np.hanning(nfft).astype(np.float32)
    spectrum = np.fft.fftshift(np.fft.fft(x[:nfft] * window))
    power = np.abs(spectrum) ** 2
    freqs = np.fft.fftshift(np.fft.fftfreq(nfft, d=1.0 / sample_rate))
    peak_offset = float(freqs[int(np.argmax(power))])
    return center_freq + peak_offset, peak_offset


def detect_bursts(
    x: np.ndarray,
    sample_rate: float,
    center_freq: float,
    start_sample: int,
    threshold_sigma: float,
    min_burst_us: float,
    merge_gap_us: float,
    pad_us: float,
) -> tuple[list[dict[str, Any]], float]:
    amp = np.abs(x)
    if amp.size == 0:
        return [], 0.0

    median = float(np.median(amp))
    mad = float(np.median(np.abs(amp - median)))
    if mad <= 1e-12:
        mad = float(np.std(amp))
    threshold = median + threshold_sigma * max(mad, 1e-12)

    min_len = max(1, int(round(min_burst_us * 1e-6 * sample_rate)))
    merge_gap = max(0, int(round(merge_gap_us * 1e-6 * sample_rate)))
    pad = max(0, int(round(pad_us * 1e-6 * sample_rate)))

    active = merge_short_gaps(amp > threshold, merge_gap)
    regions = find_regions(active, min_len)
    bursts: list[dict[str, Any]] = []

    for burst_id, (start, end) in enumerate(regions):
        padded_start = max(0, start - pad)
        padded_end = min(x.size, end + pad)
        bx = x[padded_start:padded_end]
        peak_freq, peak_offset = estimate_peak_frequency(bx, sample_rate, center_freq)
        burst_amp = amp[start:end]
        abs_start = start_sample + start
        abs_end = start_sample + end
        bursts.append(
            {
                "burst_id": burst_id,
                "start_sample": abs_start,
                "end_sample": abs_end,
                "start_time_s": abs_start / sample_rate,
                "end_time_s": abs_end / sample_rate,
                "duration_us": (end - start) / sample_rate * 1e6,
                "padded_start_sample": start_sample + padded_start,
                "padded_end_sample": start_sample + padded_end,
                "peak_abs": float(np.max(burst_amp)),
                "mean_abs": float(np.mean(burst_amp)),
                "mean_power": float(np.mean(np.abs(x[start:end]) ** 2)),
                "estimated_peak_freq_hz": peak_freq,
                "estimated_offset_hz": peak_offset,
            }
        )
    return bursts, threshold


def write_bursts_csv(path: Path, bursts: list[dict[str, Any]]) -> None:
    fieldnames = [
        "burst_id",
        "start_sample",
        "end_sample",
        "start_time_s",
        "end_time_s",
        "duration_us",
        "padded_start_sample",
        "padded_end_sample",
        "peak_abs",
        "mean_abs",
        "mean_power",
        "estimated_peak_freq_hz",
        "estimated_offset_hz",
    ]
    with path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(bursts)


def summarize_signal(x: np.ndarray, sample_rate: float, threshold: float | None, burst_count: int) -> dict[str, Any]:
    amp = np.abs(x)
    if amp.size == 0:
        return {"samples": 0}

    return {
        "samples": int(x.size),
        "duration_s": float(x.size / sample_rate),
        "mean_abs": float(np.mean(amp)),
        "max_abs": float(np.max(amp)),
        "median_abs": float(np.median(amp)),
        "burst_threshold_abs": threshold,
        "rough_burst_count": burst_count,
    }


def plot_burst_detail(
    x: np.ndarray,
    sample_rate: float,
    center_freq: float,
    save_prefix: Path,
    nfft: int,
    noverlap: int,
    max_points: int,
) -> None:
    t, amp_mean, amp_max = envelope_for_plot(x, sample_rate, max_points)

    plt.figure(figsize=(12, 4))
    plt.plot(t * 1e6, amp_max, linewidth=0.8, label="max envelope")
    if amp_mean.size == amp_max.size and not np.array_equal(amp_mean, amp_max):
        plt.plot(t * 1e6, amp_mean, linewidth=0.8, alpha=0.8, label="mean envelope")
        plt.legend()
    plt.xlabel("Time in burst window (us)")
    plt.ylabel("|IQ|")
    plt.title("Burst Amplitude")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(save_prefix.with_name(save_prefix.name + "_amplitude.png"), dpi=150)
    plt.close()

    t_iq, _, _ = envelope_for_plot(x, sample_rate, max_points)
    if x.size <= max_points:
        iq = x
    else:
        step = int(np.ceil(x.size / max_points))
        iq = x[::step]
        t_iq = np.arange(iq.size, dtype=float) * step / sample_rate

    plt.figure(figsize=(12, 4))
    plt.plot(t_iq * 1e6, iq.real, linewidth=0.7, label="I")
    plt.plot(t_iq * 1e6, iq.imag, linewidth=0.7, label="Q")
    plt.xlabel("Time in burst window (us)")
    plt.ylabel("IQ")
    plt.title("Burst Raw IQ")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(save_prefix.with_name(save_prefix.name + "_iq.png"), dpi=150)
    plt.close()

    local_nfft = min(nfft, max(64, 1 << int(math.floor(math.log2(max(64, min(x.size, nfft)))))))
    local_noverlap = min(noverlap, local_nfft - 1)
    plot_spectrogram(
        x,
        sample_rate,
        center_freq,
        save_prefix.with_name(save_prefix.name + "_spectrogram.png"),
        local_nfft,
        local_noverlap,
        "viridis",
    )


def load_burst_by_id(args: argparse.Namespace, sample_rate: float, center_freq: float) -> dict[str, Any]:
    if args.start_sample is not None and args.end_sample is not None:
        return {
            "burst_id": "manual",
            "padded_start_sample": args.start_sample,
            "padded_end_sample": args.end_sample,
            "start_sample": args.start_sample,
            "end_sample": args.end_sample,
        }

    x_scan = read_complex_window(args.iq_file, args.scan_start_sample, args.scan_max_samples)
    bursts, _ = detect_bursts(
        x_scan,
        sample_rate,
        center_freq,
        args.scan_start_sample,
        args.threshold_sigma,
        args.min_burst_us,
        args.merge_gap_us,
        args.pad_us,
    )
    if not bursts:
        raise SystemExit("未检测到 burst，请降低 --threshold-sigma 或增大 --scan-max-samples")
    if args.burst_id >= len(bursts):
        raise SystemExit(f"burst_id 超出范围：{args.burst_id} >= {len(bursts)}")
    return bursts[args.burst_id]


def quadrature_demod(x: np.ndarray, sample_rate: float) -> np.ndarray:
    if x.size < 2:
        return np.array([], dtype=np.float32)
    phase_delta = np.angle(x[1:] * np.conj(x[:-1]))
    return (phase_delta * sample_rate / (2.0 * np.pi)).astype(np.float32)


def mix_down(x: np.ndarray, sample_rate: float, freq_offset: float) -> np.ndarray:
    n = np.arange(x.size, dtype=np.float64)
    rotator = np.exp(-1j * 2.0 * np.pi * freq_offset * n / sample_rate)
    return (x * rotator).astype(np.complex64)


def decimate_for_gfsk(x: np.ndarray, sample_rate: float, target_rate: float) -> tuple[np.ndarray, float]:
    if target_rate <= 0 or target_rate >= sample_rate:
        return x, sample_rate
    down = max(1, int(round(sample_rate / target_rate)))
    y = signal.resample_poly(x, up=1, down=down, window=("kaiser", 8.6)).astype(np.complex64)
    return y, sample_rate / down


def plot_gfsk(
    baseband: np.ndarray,
    demod_hz: np.ndarray,
    sample_rate: float,
    demod_rate: float,
    save_prefix: Path,
    max_points: int,
) -> None:
    if baseband.size > max_points:
        step = int(np.ceil(baseband.size / max_points))
        bb = baseband[::step]
        t_bb = np.arange(bb.size, dtype=float) * step / sample_rate
    else:
        bb = baseband
        t_bb = np.arange(bb.size, dtype=float) / sample_rate

    plt.figure(figsize=(12, 4))
    plt.plot(t_bb * 1e6, np.abs(bb), linewidth=0.8)
    plt.xlabel("Time in burst window (us)")
    plt.ylabel("|baseband IQ|")
    plt.title("Downconverted Burst Amplitude")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(save_prefix.with_name(save_prefix.name + "_baseband_amplitude.png"), dpi=150)
    plt.close()

    if demod_hz.size > max_points:
        step = int(np.ceil(demod_hz.size / max_points))
        demod_plot = demod_hz[::step]
        t_demod = np.arange(demod_plot.size, dtype=float) * step / demod_rate
    else:
        demod_plot = demod_hz
        t_demod = np.arange(demod_plot.size, dtype=float) / demod_rate

    plt.figure(figsize=(12, 4))
    plt.plot(t_demod * 1e6, demod_plot / 1e3, linewidth=0.8)
    plt.axhline(0, color="black", linewidth=0.6)
    plt.xlabel("Time in burst window (us)")
    plt.ylabel("Instantaneous frequency offset (kHz)")
    plt.title("GFSK View: Quadrature Demod")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(save_prefix.with_name(save_prefix.name + "_gfsk_demod.png"), dpi=150)
    plt.close()


def save_burst_detail_outputs(
    args: argparse.Namespace,
    info: dict[str, Any],
    sample_rate: float,
    center_freq: float,
    output_dir: Path,
) -> dict[str, Path]:
    start = int(info["padded_start_sample"])
    end = int(info["padded_end_sample"])
    x = read_complex_window(args.iq_file, start, end - start)
    peak_freq, peak_offset = estimate_peak_frequency(x, sample_rate, center_freq)
    burst_id = info["burst_id"]
    prefix = output_dir / f"{args.iq_file.stem}_burst_{burst_id}"

    plot_burst_detail(x, sample_rate, center_freq, prefix, args.nfft, args.noverlap, args.max_time_points)
    detail = dict(info)
    detail.update(
        {
            "sample_rate": sample_rate,
            "center_freq": center_freq,
            "analysis_start_sample": start,
            "analysis_end_sample": end,
            "estimated_peak_freq_hz": peak_freq,
            "estimated_offset_hz": peak_offset,
        }
    )
    detail_path = prefix.with_name(prefix.name + "_detail.json")
    detail_path.write_text(json.dumps(detail, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return {
        "amplitude": prefix.with_name(prefix.name + "_amplitude.png"),
        "iq": prefix.with_name(prefix.name + "_iq.png"),
        "spectrogram": prefix.with_name(prefix.name + "_spectrogram.png"),
        "detail": detail_path,
    }


def save_gfsk_outputs(
    args: argparse.Namespace,
    info: dict[str, Any],
    sample_rate: float,
    center_freq: float,
    output_dir: Path,
) -> dict[str, Path]:
    start = int(info["padded_start_sample"])
    end = int(info["padded_end_sample"])
    x = read_complex_window(args.iq_file, start, end - start)

    _, estimated_offset = estimate_peak_frequency(x, sample_rate, center_freq)
    target_offset = estimated_offset if args.target_offset is None else args.target_offset
    baseband = mix_down(x, sample_rate, target_offset)
    baseband_ds, demod_rate = decimate_for_gfsk(baseband, sample_rate, args.target_rate)
    demod_hz = quadrature_demod(baseband_ds, demod_rate)

    burst_id = info["burst_id"]
    prefix = output_dir / f"{args.iq_file.stem}_burst_{burst_id}"
    gfsk_max_time_points = getattr(args, "gfsk_max_time_points", args.max_time_points)
    plot_gfsk(baseband_ds, demod_hz, demod_rate, demod_rate, prefix, gfsk_max_time_points)

    result = dict(info)
    result.update(
        {
            "sample_rate": sample_rate,
            "center_freq": center_freq,
            "analysis_start_sample": start,
            "analysis_end_sample": end,
            "estimated_offset_hz": estimated_offset,
            "used_target_offset_hz": target_offset,
            "demod_sample_rate": demod_rate,
            "demod_mean_hz": float(np.mean(demod_hz)) if demod_hz.size else 0.0,
            "demod_std_hz": float(np.std(demod_hz)) if demod_hz.size else 0.0,
        }
    )
    result_path = prefix.with_name(prefix.name + "_gfsk.json")
    result_path.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    paths = {
        "baseband_amplitude": prefix.with_name(prefix.name + "_baseband_amplitude.png"),
        "gfsk_demod": prefix.with_name(prefix.name + "_gfsk_demod.png"),
        "gfsk": result_path,
    }
    if args.save_baseband:
        bb_path = prefix.with_name(prefix.name + "_baseband.c64")
        baseband_ds.astype(np.complex64).tofile(bb_path)
        paths["baseband"] = bb_path
    return paths


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("iq_file", type=Path, help="complex64 IQ 文件，例如 ble_cs_scope_ch0.c64")
    parser.add_argument("--metadata", type=Path, help="metadata.json；不传则需要 --sample-rate")
    parser.add_argument("--sample-rate", type=float, help="采样率")
    parser.add_argument("--center-freq", type=float, help="中心频率；默认从 metadata 读取，否则为 0")
    parser.add_argument("--output-dir", type=Path, help="输出目录；默认放到 IQ 文件同目录 analysis 下")


def add_scan_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--start-sample", type=int, default=0, help="扫描窗口起始样本")
    parser.add_argument("--max-samples", type=int, default=10_000_000, help="最多读取多少个样本")
    parser.add_argument("--threshold-sigma", type=float, default=15.0, help="burst 门限：median + N * MAD")
    parser.add_argument("--min-burst-us", type=float, default=50.0, help="最短 burst 时长，小于该值会被过滤")
    parser.add_argument("--merge-gap-us", type=float, default=2.0, help="合并短于该间隔的 burst 缝隙")
    parser.add_argument("--pad-us", type=float, default=5.0, help="记录 burst 周围额外样本，方便后续分析")
    parser.add_argument("--max-time-points", type=int, default=20_000, help="overview 时域图最多绘制多少个点")
    parser.add_argument("--nfft", type=int, default=4096, help="spectrogram FFT 点数")
    parser.add_argument("--noverlap", type=int, default=3072, help="spectrogram 重叠点数")
    parser.add_argument("--cmap", default="viridis", help="spectrogram 色图")


def add_burst_selector_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--burst-id", type=int, default=0, help="要分析的 burst 编号")
    parser.add_argument("--start-sample", type=int, help="手动指定 burst 起始样本；需同时指定 --end-sample")
    parser.add_argument("--end-sample", type=int, help="手动指定 burst 结束样本；需同时指定 --start-sample")
    parser.add_argument("--scan-start-sample", type=int, default=0, help="自动找 burst 时的扫描起点")
    parser.add_argument("--scan-max-samples", type=int, default=10_000_000, help="自动找 burst 时的最大扫描样本数")
    parser.add_argument("--threshold-sigma", type=float, default=15.0, help="自动找 burst 的门限")
    parser.add_argument("--min-burst-us", type=float, default=50.0, help="自动找 burst 的最短时长")
    parser.add_argument("--merge-gap-us", type=float, default=2.0, help="自动找 burst 的合并间隔")
    parser.add_argument("--pad-us", type=float, default=5.0, help="自动找 burst 后向两侧扩展多少 us")


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="对 USRP 采集的 BLE CS IQ 做离线分析")
    subparsers = parser.add_subparsers(dest="command", required=True)

    overview = subparsers.add_parser("overview", help="整段幅度图和 spectrogram")
    add_common_args(overview)
    add_scan_args(overview)

    scan = subparsers.add_parser("scan", help="检测 burst 并输出 bursts.csv")
    add_common_args(scan)
    add_scan_args(scan)

    burst = subparsers.add_parser("burst", help="切出并绘制某个 burst 的细节")
    add_common_args(burst)
    add_burst_selector_args(burst)
    burst.add_argument("--max-time-points", type=int, default=20_000, help="局部图最多绘制多少个点")
    burst.add_argument("--nfft", type=int, default=2048, help="burst spectrogram FFT 点数")
    burst.add_argument("--noverlap", type=int, default=1536, help="burst spectrogram 重叠点数")

    gfsk = subparsers.add_parser("gfsk", help="对单个 burst 做 GFSK 瞬时频率观察")
    add_common_args(gfsk)
    add_burst_selector_args(gfsk)
    gfsk.add_argument("--target-offset", type=float, help="目标频道相对 center_freq 的频偏 Hz；默认用 burst FFT 估计")
    gfsk.add_argument("--target-rate", type=float, default=4e6, help="降采样后的观察采样率")
    gfsk.add_argument("--max-time-points", type=int, default=50_000, help="GFSK 图最多绘制多少个点")
    gfsk.add_argument("--save-baseband", action="store_true", help="保存下变频/降采样后的 burst complex64")

    plus = subparsers.add_parser("plus", help="总览、严格扫描，并逐个输出 burst 细节和 GFSK")
    add_common_args(plus)
    add_scan_args(plus)
    plus.set_defaults(threshold_sigma=20.0, min_burst_us=100.0)
    plus.add_argument("--target-offset", type=float, help="所有 burst 共用的目标频偏 Hz；默认每个 burst 单独 FFT 估计")
    plus.add_argument("--target-rate", type=float, default=4e6, help="降采样后的观察采样率")
    plus.add_argument("--gfsk-max-time-points", type=int, default=50_000, help="GFSK 图最多绘制多少个点")
    plus.add_argument("--save-baseband", action="store_true", help="保存每个 burst 下变频/降采样后的 complex64")
    return parser


def normalize_argv(argv: list[str]) -> list[str]:
    if len(argv) >= 2 and argv[1] not in COMMANDS and not argv[1].startswith("-"):
        return [argv[0], "overview", *argv[1:]]
    return argv


def command_overview(args: argparse.Namespace) -> int:
    metadata = load_metadata(args.metadata)
    sample_rate = infer_sample_rate(args, metadata)
    center_freq = infer_center_freq(args, metadata)
    output_dir = output_dir_for(args)
    output_dir.mkdir(parents=True, exist_ok=True)

    x = read_complex_window(args.iq_file, args.start_sample, args.max_samples)
    bursts, threshold = detect_bursts(
        x,
        sample_rate,
        center_freq,
        args.start_sample,
        args.threshold_sigma,
        args.min_burst_us,
        args.merge_gap_us,
        args.pad_us,
    )
    stem = args.iq_file.stem

    plot_amplitude(x, sample_rate, output_dir / f"{stem}_amplitude.png", args.max_time_points)
    plot_spectrogram(x, sample_rate, center_freq, output_dir / f"{stem}_spectrogram.png", args.nfft, args.noverlap, args.cmap)

    summary = summarize_signal(x, sample_rate, threshold, len(bursts))
    summary.update(
        {
            "iq_file": str(args.iq_file),
            "sample_rate": sample_rate,
            "center_freq": center_freq,
            "start_sample": args.start_sample,
            "max_samples": args.max_samples,
        }
    )
    summary_path = output_dir / f"{stem}_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(f"amplitude: {output_dir / f'{stem}_amplitude.png'}")
    print(f"spectrogram: {output_dir / f'{stem}_spectrogram.png'}")
    print(f"summary: {summary_path}")
    return 0


def command_scan(args: argparse.Namespace) -> int:
    metadata = load_metadata(args.metadata)
    sample_rate = infer_sample_rate(args, metadata)
    center_freq = infer_center_freq(args, metadata)
    output_dir = output_dir_for(args)
    output_dir.mkdir(parents=True, exist_ok=True)

    x = read_complex_window(args.iq_file, args.start_sample, args.max_samples)
    bursts, threshold = detect_bursts(
        x,
        sample_rate,
        center_freq,
        args.start_sample,
        args.threshold_sigma,
        args.min_burst_us,
        args.merge_gap_us,
        args.pad_us,
    )
    stem = args.iq_file.stem
    csv_path = output_dir / f"{stem}_bursts.csv"
    json_path = output_dir / f"{stem}_bursts.json"
    write_bursts_csv(csv_path, bursts)
    json_path.write_text(json.dumps({"threshold": threshold, "bursts": bursts}, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(f"bursts: {len(bursts)}")
    print(f"threshold: {threshold:.6g}")
    print(f"csv: {csv_path}")
    print(f"json: {json_path}")
    return 0


def command_burst(args: argparse.Namespace) -> int:
    metadata = load_metadata(args.metadata)
    sample_rate = infer_sample_rate(args, metadata)
    center_freq = infer_center_freq(args, metadata)
    output_dir = output_dir_for(args)
    output_dir.mkdir(parents=True, exist_ok=True)

    info = load_burst_by_id(args, sample_rate, center_freq)
    burst_id = info["burst_id"]
    paths = save_burst_detail_outputs(args, info, sample_rate, center_freq, output_dir)

    print(f"burst_id: {burst_id}")
    print(f"window: {info['padded_start_sample']}..{info['padded_end_sample']}")
    print(f"detail: {paths['detail']}")
    return 0


def command_gfsk(args: argparse.Namespace) -> int:
    metadata = load_metadata(args.metadata)
    sample_rate = infer_sample_rate(args, metadata)
    center_freq = infer_center_freq(args, metadata)
    output_dir = output_dir_for(args)
    output_dir.mkdir(parents=True, exist_ok=True)

    info = load_burst_by_id(args, sample_rate, center_freq)
    burst_id = info["burst_id"]
    paths = save_gfsk_outputs(args, info, sample_rate, center_freq, output_dir)

    print(f"burst_id: {burst_id}")
    if "baseband" in paths:
        print(f"baseband: {paths['baseband']}")
    print(f"gfsk_plot: {paths['gfsk_demod']}")
    print(f"result: {paths['gfsk']}")
    return 0


def command_plus(args: argparse.Namespace) -> int:
    metadata = load_metadata(args.metadata)
    sample_rate = infer_sample_rate(args, metadata)
    center_freq = infer_center_freq(args, metadata)
    output_dir = output_dir_for(args)
    output_dir.mkdir(parents=True, exist_ok=True)

    x = read_complex_window(args.iq_file, args.start_sample, args.max_samples)
    bursts, threshold = detect_bursts(
        x,
        sample_rate,
        center_freq,
        args.start_sample,
        args.threshold_sigma,
        args.min_burst_us,
        args.merge_gap_us,
        args.pad_us,
    )
    stem = args.iq_file.stem

    amplitude_path = output_dir / f"{stem}_amplitude.png"
    spectrogram_path = output_dir / f"{stem}_spectrogram.png"
    summary_path = output_dir / f"{stem}_summary.json"
    csv_path = output_dir / f"{stem}_bursts.csv"
    bursts_json_path = output_dir / f"{stem}_bursts.json"
    index_path = output_dir / f"{stem}_plus_index.json"

    plot_amplitude(x, sample_rate, amplitude_path, args.max_time_points)
    plot_spectrogram(x, sample_rate, center_freq, spectrogram_path, args.nfft, args.noverlap, args.cmap)

    summary = summarize_signal(x, sample_rate, threshold, len(bursts))
    summary.update(
        {
            "iq_file": str(args.iq_file),
            "sample_rate": sample_rate,
            "center_freq": center_freq,
            "start_sample": args.start_sample,
            "max_samples": args.max_samples,
            "threshold_sigma": args.threshold_sigma,
            "min_burst_us": args.min_burst_us,
            "merge_gap_us": args.merge_gap_us,
            "pad_us": args.pad_us,
        }
    )
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    write_bursts_csv(csv_path, bursts)
    bursts_json_path.write_text(
        json.dumps({"threshold": threshold, "bursts": bursts}, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    outputs: list[dict[str, Any]] = []
    for info in bursts:
        detail_paths = save_burst_detail_outputs(args, info, sample_rate, center_freq, output_dir)
        gfsk_paths = save_gfsk_outputs(args, info, sample_rate, center_freq, output_dir)
        outputs.append(
            {
                "burst_id": info["burst_id"],
                "start_sample": info["start_sample"],
                "end_sample": info["end_sample"],
                "duration_us": info["duration_us"],
                "estimated_offset_hz": info["estimated_offset_hz"],
                "detail_outputs": {key: str(path) for key, path in detail_paths.items()},
                "gfsk_outputs": {key: str(path) for key, path in gfsk_paths.items()},
            }
        )
        print(
            "burst_id={burst_id} duration_us={duration_us:.2f} offset_hz={offset:.3f} detail={detail} gfsk={gfsk}".format(
                burst_id=info["burst_id"],
                duration_us=info["duration_us"],
                offset=info["estimated_offset_hz"],
                detail=detail_paths["detail"],
                gfsk=gfsk_paths["gfsk"],
            )
        )

    index = {
        "iq_file": str(args.iq_file),
        "output_dir": str(output_dir),
        "threshold": threshold,
        "burst_count": len(bursts),
        "overview_outputs": {
            "amplitude": str(amplitude_path),
            "spectrogram": str(spectrogram_path),
            "summary": str(summary_path),
        },
        "scan_outputs": {
            "csv": str(csv_path),
            "json": str(bursts_json_path),
        },
        "burst_outputs": outputs,
    }
    index_path.write_text(json.dumps(index, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(f"overview_amplitude: {amplitude_path}")
    print(f"overview_spectrogram: {spectrogram_path}")
    print(f"summary: {summary_path}")
    print(f"bursts: {len(bursts)}")
    print(f"threshold: {threshold:.6g}")
    print(f"csv: {csv_path}")
    print(f"json: {bursts_json_path}")
    print(f"index: {index_path}")
    return 0


def main() -> int:
    parser = build_argument_parser()
    args = parser.parse_args(normalize_argv(sys.argv)[1:])

    if args.command == "overview":
        return command_overview(args)
    if args.command == "scan":
        return command_scan(args)
    if args.command == "burst":
        return command_burst(args)
    if args.command == "gfsk":
        return command_gfsk(args)
    if args.command == "plus":
        return command_plus(args)
    raise SystemExit(f"未知命令：{args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
