#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Low sample-rate 1-to-1 BLE CS experiment using USRP RF retuning.

This script intentionally lives outside the existing 1to1 flowgraph. It keeps
the baseband tone fixed and changes the USRP center frequency for every hop.
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from pathlib import Path

from gnuradio import analog
from gnuradio import blocks
from gnuradio import gr
from gnuradio import uhd
from gnuradio import usrp_ble
import pmt


def _post(block, port: str, msg) -> None:
    block.to_basic_block()._post(pmt.intern(port), msg)


def _symbol(name: str):
    return pmt.intern(name)


def _store_start(freq_index: int, repeat_index: int):
    msg = pmt.make_dict()
    msg = pmt.dict_add(msg, pmt.intern("cmd"), pmt.intern("store_start"))
    msg = pmt.dict_add(msg, pmt.intern("freq_index"), pmt.from_long(freq_index))
    msg = pmt.dict_add(msg, pmt.intern("repeat_index"), pmt.from_long(repeat_index))
    return msg


class BleCs1To1RfHop(gr.top_block):
    def __init__(self, args: argparse.Namespace):
        super().__init__("ble_cs_1to1_rfhop", catch_exceptions=True)
        self.args = args

        self.samp_rate = float(args.samp_rate)
        self.tone_freq = float(args.tone_freq)
        self.phase_s = args.phase_ms / 1000.0
        self.settle_s = args.settle_ms / 1000.0

        root = Path(args.root).expanduser().resolve()
        self.store_reflector = root / "data_reflector_rx_from_initiator"
        self.store_initiator = root / "data_initiator_rx_from_reflector"
        self.store_reflector.mkdir(parents=True, exist_ok=True)
        self.store_initiator.mkdir(parents=True, exist_ok=True)

        recv_args = f"addr={args.addr}, recv_frame_size={args.frame_size},num_recv_frames={args.num_frames}"
        send_args = f"addr={args.addr}, send_frame_size={args.frame_size},num_send_frames={args.num_frames}"

        self.usrp_source = uhd.usrp_source(
            ",".join((recv_args,)),
            uhd.stream_args(
                cpu_format="fc32",
                otw_format="sc16",
                args="",
                channels=list(range(0, 2)),
            ),
        )
        self.usrp_source.set_subdev_spec(args.subdev, 0)
        self.usrp_source.set_samp_rate(self.samp_rate)
        self.usrp_source.set_time_unknown_pps(uhd.time_spec(0))
        for ch in (0, 1):
            self.usrp_source.set_antenna(args.rx_antenna, ch)
            self.usrp_source.set_bandwidth(args.bandwidth, ch)
            self.usrp_source.set_gain(args.rx_gain, ch)

        self.usrp_sink = uhd.usrp_sink(
            ",".join((send_args,)),
            uhd.stream_args(
                cpu_format="fc32",
                otw_format="sc16",
                args="",
                channels=list(range(0, 2)),
            ),
            "",
        )
        self.usrp_sink.set_subdev_spec(args.subdev, 0)
        self.usrp_sink.set_samp_rate(self.samp_rate)
        self.usrp_sink.set_time_unknown_pps(uhd.time_spec(0))
        for ch in (0, 1):
            self.usrp_sink.set_antenna(args.tx_antenna, ch)
            self.usrp_sink.set_bandwidth(args.bandwidth, ch)
            self.usrp_sink.set_gain(args.tx_gain, ch)

        self.local_tone = analog.sig_source_c(
            self.samp_rate, analog.GR_COS_WAVE, self.tone_freq, 1.0, 0.0, 0.0
        )
        self.random_phase_tx0 = usrp_ble.random_phase(1, 1.0)
        self.random_phase_tx1 = usrp_ble.random_phase(2, 1.0)

        self.data_send_0 = usrp_ble.data_send(self.samp_rate, 0.001)
        self.data_send_1 = usrp_ble.data_send(self.samp_rate, 0.001)
        self.tx_mul_0 = blocks.multiply_vcc(1)
        self.tx_mul_1 = blocks.multiply_vcc(1)
        self.rx_mix_0 = blocks.multiply_conjugate_cc(1)
        self.rx_mix_1 = blocks.multiply_conjugate_cc(1)
        self.data_store_initiator = usrp_ble.data_store(
            args.data_len, args.store_skip, str(self.store_initiator)
        )
        self.data_store_reflector = usrp_ble.data_store(
            args.data_len, args.store_skip, str(self.store_reflector)
        )

        self.connect(self.local_tone, self.random_phase_tx0)
        self.connect(self.local_tone, self.random_phase_tx1)

        self.connect(self.random_phase_tx0, (self.tx_mul_0, 0))
        self.connect(self.data_send_0, (self.tx_mul_0, 1))
        self.connect(self.random_phase_tx1, (self.tx_mul_1, 0))
        self.connect(self.data_send_1, (self.tx_mul_1, 1))
        self.connect(self.tx_mul_0, (self.usrp_sink, 0))
        self.connect(self.tx_mul_1, (self.usrp_sink, 1))

        self.connect((self.usrp_source, 0), (self.rx_mix_0, 0))
        self.connect(self.random_phase_tx0, (self.rx_mix_0, 1))
        self.connect((self.usrp_source, 1), (self.rx_mix_1, 0))
        self.connect(self.random_phase_tx1, (self.rx_mix_1, 1))
        self.connect(self.rx_mix_0, self.data_store_initiator)
        self.connect(self.rx_mix_1, self.data_store_reflector)

        self.tune(args.base_freq)

    def tune(self, desired_rf: float) -> None:
        # The generated RF tone is center + tone_freq. Tune below the target
        # tone so the emitted/received CW sits at desired_rf.
        center = desired_rf - self.tone_freq
        for ch in (0, 1):
            self.usrp_sink.set_center_freq(center, ch)
            self.usrp_source.set_center_freq(center, ch)

    def stop_all(self) -> None:
        for block in (self.data_send_0, self.data_send_1):
            _post(block, "command", _symbol("data_stop"))
        _post(self.data_store_reflector, "command", _symbol("store_stop"))
        _post(self.data_store_initiator, "command", _symbol("store_stop"))

    def run_phase(self, phase: int, freq_index: int, repeat_index: int) -> None:
        if phase == 1:
            store = self.data_store_reflector
            sender = self.data_send_0
            other_sender = self.data_send_1
        else:
            store = self.data_store_initiator
            sender = self.data_send_1
            other_sender = self.data_send_0

        _post(other_sender, "command", _symbol("data_stop"))
        _post(store, "command", _store_start(freq_index, repeat_index))
        _post(sender, "command", _symbol("data_start"))
        time.sleep(self.phase_s)
        _post(sender, "command", _symbol("data_stop"))
        _post(store, "command", _symbol("store_stop"))

    def run_schedule(self) -> None:
        offsets = list(range(int(self.args.start_offset), int(self.args.stop_offset) + 1, int(self.args.step)))
        print(json.dumps({
            "event": "start",
            "sample_rate": self.samp_rate,
            "tone_freq": self.tone_freq,
            "store_skip": self.args.store_skip,
            "data_len": self.args.data_len,
            "freq_count": len(offsets),
        }))
        try:
            for freq_index, offset in enumerate(offsets):
                desired_rf = self.args.base_freq + offset
                self.tune(desired_rf)
                print(json.dumps({
                    "event": "tune",
                    "freq_index": freq_index,
                    "offset_hz": offset,
                    "desired_rf_hz": desired_rf,
                    "usrp_center_hz": desired_rf - self.tone_freq,
                }))
                time.sleep(self.settle_s)
                for repeat_index in range(self.args.repeats):
                    self.run_phase(1, freq_index, repeat_index)
                    self.run_phase(2, freq_index, repeat_index)
        finally:
            self.stop_all()
            print(json.dumps({"event": "done"}))


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default="/home/lfy/workarea/zz_ble_cs_gnuradio/1to1_rfhop")
    parser.add_argument("--addr", default="192.168.30.2")
    parser.add_argument("--subdev", default="A:0 B:0")
    parser.add_argument("--rx-antenna", default="RX2")
    parser.add_argument("--tx-antenna", default="TX/RX")
    parser.add_argument("--frame-size", type=int, default=8000)
    parser.add_argument("--num-frames", type=int, default=512)
    parser.add_argument("--samp-rate", type=float, default=5e6)
    parser.add_argument("--bandwidth", type=float, default=5e6)
    parser.add_argument("--tone-freq", type=float, default=500e3)
    parser.add_argument("--base-freq", type=float, default=2.44e9)
    parser.add_argument("--start-offset", type=float, default=-40e6)
    parser.add_argument("--stop-offset", type=float, default=40e6)
    parser.add_argument("--step", type=float, default=1e6)
    parser.add_argument("--settle-ms", type=float, default=5.0)
    parser.add_argument("--phase-ms", type=float, default=10.0)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--store-skip", type=int, default=2500)
    parser.add_argument("--data-len", type=int, default=200)
    parser.add_argument("--rx-gain", type=float, default=18.0)
    parser.add_argument("--tx-gain", type=float, default=3.0)
    parser.add_argument("--startup-delay", type=float, default=1.0)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def dry_run(args: argparse.Namespace) -> None:
    offsets = list(range(int(args.start_offset), int(args.stop_offset) + 1, int(args.step)))
    print(json.dumps({
        "event": "dry_run",
        "sample_rate": args.samp_rate,
        "tone_freq": args.tone_freq,
        "store_skip": args.store_skip,
        "freq_count": len(offsets),
    }))
    for freq_index, offset in enumerate(offsets):
        desired_rf = args.base_freq + offset
        print(json.dumps({
            "freq_index": freq_index,
            "offset_hz": offset,
            "desired_rf_hz": desired_rf,
            "usrp_center_hz": desired_rf - args.tone_freq,
        }))


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if args.dry_run:
        dry_run(args)
        return 0

    tb = BleCs1To1RfHop(args)

    def handle_signal(_sig, _frame):
        tb.stop_all()
        tb.stop()
        tb.wait()
        raise SystemExit(130)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    tb.start()
    try:
        time.sleep(args.startup_delay)
        tb.run_schedule()
    finally:
        tb.stop()
        tb.wait()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
