# 1to1 RF-hop low sample-rate version

This is a parallel experiment path. It does not modify the existing `1to1`
flowgraph or any OOT block.

The difference from the original `1to1` scheme is:

- original: USRP center frequency is fixed at 2.44 GHz, and the baseband tone
  sweeps from -40 MHz to +40 MHz.
- this version: the baseband tone is fixed, and the USRP TX/RX center frequency
  is retuned for every hop.

Default parameters:

- sample rate: 5 MS/s
- baseband tone: 500 kHz
- RF sweep: 2.400 GHz to 2.480 GHz in 1 MHz steps
- TX/RX gate: disabled
- retune settle: 5 ms
- phase duration: 10 ms
- repeats: 3
- store skip: 2500 samples, equivalent to 0.5 ms at 5 MS/s
- store data length: 200 complex samples

Because the tone is not DC, the script tunes the USRP center to:

```text
usrp_center = desired_rf - tone_freq
```

so the transmitted tone appears at `desired_rf`.

Run:

```bash
python3 1to1_rfhop/ble_cs_1to1_rfhop.py
```

Or open the complete GRC flowgraph:

```bash
gnuradio-companion 1to1_rfhop/1to1_rfhop.grc
```

The GRC file is a complete flowgraph. It uses the dedicated
`interact_center_rfhop` block, not the original `interact_center`. The RF-hop
controller sends `freq_ctrl`, waits `settle_time_ms`, then starts the two
TX/RX phases. `freq_ctrl` is routed into a `Message Pair to Var` block that
updates `hop_offset`; `centetr_fre` is then recalculated as
`2.44e9 + hop_offset - tone_freq`.

Continuous capture gate:

- the flowgraph keeps the original `file sink`
- a new OOT control block `capture_gate` is inserted before each `file sink`
- `interact_center_rfhop` publishes a new `capture_ctrl` message port
- `start_button` starts the RF-hop schedule and sends `capture_start`
- `stop_button` or final hop completion sends `capture_stop`
- the gate only lets samples through while capture is enabled
- each direction is still recorded into one continuous file, instead of split files

Continuous capture output files:

```text
1to1_rfhop/data_reflector_rx_from_initiator2
1to1_rfhop/data_initiator_rx_from_reflector2
```

New post-processing scripts for the continuous format:

```bash
python3 analyze_continuous_capture.py --root 1to1_rfhop
python3 estimate_distance_continuous.py --root 1to1_rfhop
```

Dry-run the schedule without touching hardware:

```bash
python3 1to1_rfhop/ble_cs_1to1_rfhop.py --dry-run
```

Output directories:

```text
1to1_rfhop/data_reflector_rx_from_initiator
1to1_rfhop/data_initiator_rx_from_reflector
```

Important: RF retuning can introduce per-frequency phase offsets. Distance
estimation from cross-frequency phase slope will need calibration data collected
with a known reference path.
