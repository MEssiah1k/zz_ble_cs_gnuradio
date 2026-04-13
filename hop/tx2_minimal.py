#!/usr/bin/env python3

import argparse
import time

from gnuradio import analog
from gnuradio import gr
from gnuradio import uhd


class tx2_minimal(gr.top_block):
    def __init__(self, args):
        gr.top_block.__init__(self, "tx2_minimal")

        dev_args = ",".join((
            f"addr={args.addr}",
            f"send_frame_size={args.frame_size}",
            f"num_send_frames={args.num_send_frames}",
        ))

        self.sink = uhd.usrp_sink(
            dev_args,
            uhd.stream_args(
                cpu_format="fc32",
                otw_format=args.otw,
                channels=list(range(0, args.channels)),
            ),
            "",
        )
        if args.channels == 2:
            self.sink.set_subdev_spec("A:0 B:0", 0)
        else:
            self.sink.set_subdev_spec("A:0", 0)

        self.sink.set_samp_rate(args.samp_rate)

        for chan in range(args.channels):
            self.sink.set_center_freq(args.freq, chan)
            self.sink.set_antenna("TX/RX", chan)
            self.sink.set_bandwidth(args.samp_rate, chan)
            self.sink.set_gain(args.gain, chan)

        self.sources = []
        for chan in range(args.channels):
            src = analog.sig_source_c(
                args.samp_rate,
                analog.GR_COS_WAVE,
                0,
                args.amplitude,
                0,
                0,
            )
            self.sources.append(src)
            self.connect((src, 0), (self.sink, chan))


def main():
    parser = argparse.ArgumentParser(
        description="Minimal continuous 1TX/2TX UHD stress test for the X310."
    )
    parser.add_argument("--addr", default="192.168.30.2")
    parser.add_argument("--samp-rate", type=float, default=20e6)
    parser.add_argument("--freq", type=float, default=2.44e9)
    parser.add_argument("--gain", type=float, default=0.0)
    parser.add_argument("--channels", type=int, choices=(1, 2), default=2)
    parser.add_argument("--duration", type=float, default=10.0)
    parser.add_argument("--amplitude", type=float, default=0.05)
    parser.add_argument("--otw", default="sc16", choices=("sc16", "sc12", "sc8"))
    parser.add_argument("--frame-size", type=int, default=8000)
    parser.add_argument("--num-send-frames", type=int, default=512)
    args = parser.parse_args()

    tb = tx2_minimal(args)
    tb.start()
    try:
        time.sleep(args.duration)
    finally:
        tb.stop()
        tb.wait()


if __name__ == "__main__":
    main()
