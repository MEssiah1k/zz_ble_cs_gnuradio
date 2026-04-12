# USRP BLE Channel Sounding 空口示波器采集

这个目录用于把 USRP 当宽带示波器使用，连续记录 BLE Channel Sounding 空口 IQ，然后离线画时域幅度和 spectrogram。

默认参数参考 `../1to1/1to1.grc`：

- 设备地址：`addr=192.168.10.2`
- `recv_frame_size=1472`
- 采样率：`100e6`
- 中心频率：`2.44e9`
- 接收天线：`RX2`
- 接收增益：`0`
- 子设备：`A:0 B:0`
- 默认只采通道 `0`；需要双通道时传 `--channels 0 1`

## GRC 采集

如果你要在 GNU Radio Companion 里打开调参数，直接打开：

```bash
gnuradio-companion usrp_ble_scope/usrp_ble_scope.grc
```

这个 flowgraph 是：

```text
USRP Source -> Head -> File Sink
                    -> Complex to Mag -> QT GUI Time Sink
                    -> QT GUI Frequency Sink
                    -> QT GUI Waterfall Sink
```

默认采 `capture_seconds=0.1` 秒，输出文件：

```text
usrp_ble_scope/ble_cs_scope_ch0.c64
```

需要更长时间就在 GRC 里改 `capture_seconds`。如果要连续采集，可以删掉或 bypass `Head`，但要注意磁盘会很快写满。

另有一个单 BLE 频点长采集版本。这里的“单频点”不是 USRP 单 RX 通道，而是只观测一个 BLE 频点/信道，用较低采样率换更长观测时间：

```bash
gnuradio-companion usrp_ble_scope/usrp_ble_scope_long.grc
```

默认参数：

```text
center_freq = 2.44e9
samp_rate = 4e6
capture_seconds = 10.0
```

也就是观察范围约为：

```text
2.44 GHz ± 2 MHz
```

输出文件：

```text
usrp_ble_scope/ble_cs_single_freq_long.c64
```

10 秒文件约为：

```text
4 MS/s * 10 s * 8 bytes = 320 MB
```

长采集版本里 `Head` 只限制 File Sink 写文件，Time/Frequency/Waterfall 三个 GUI 分支直接接 USRP Source，会持续刷新，方便边看边等某个频点上的 BLE CS burst。要换 BLE 频点，直接在 GRC 里改 `center_freq`。

## Python 采集

先做短采集，确认不会丢包、磁盘速度够用：

```bash
python3 usrp_ble_scope/capture_iq.py --duration 0.1
```

输出会放到：

```text
usrp_ble_scope/captures/ble_cs_scope_YYYYmmdd_HHMMSS/
```

每个通道一个 IQ 文件：

```text
ble_cs_scope_ch0.c64
metadata.json
```

`.c64` 文件格式是 GNU Radio `gr_complex` / NumPy `complex64`，也就是每个采样点 8 字节，I/Q 交错保存。

100 MS/s 单通道数据量约为：

```text
0.1 秒: 80 MB
1.0 秒: 800 MB
双通道: 再乘以 2
```

如果要采双通道：

```bash
python3 usrp_ble_scope/capture_iq.py --duration 0.1 --channels 0 1
```

如果要改增益或采样时长：

```bash
python3 usrp_ble_scope/capture_iq.py --duration 0.5 --gain 10
```

## 离线分析

### 1. 快速总览

对采集结果画整段时域幅度和 spectrogram：

```bash
python3 usrp_ble_scope/analyze_iq.py \
  usrp_ble_scope/captures/ble_cs_scope_YYYYmmdd_HHMMSS/ble_cs_scope_ch0.c64 \
  --metadata usrp_ble_scope/captures/ble_cs_scope_YYYYmmdd_HHMMSS/metadata.json
```

默认只读取前 `5,000,000` 个样本，避免 100 MS/s 大文件直接占满内存。需要观察更长时间时可以调大：

```bash
python3 usrp_ble_scope/analyze_iq.py \
  usrp_ble_scope/captures/ble_cs_scope_YYYYmmdd_HHMMSS/ble_cs_scope_ch0.c64 \
  --metadata usrp_ble_scope/captures/ble_cs_scope_YYYYmmdd_HHMMSS/metadata.json \
  --max-samples 20000000
```

输出：

- `analysis/*_amplitude.png`：幅度-时间图，用来观察 burst
- `analysis/*_spectrogram.png`：时频图，用来观察跳频轨迹
- `analysis/*_summary.json`：基础统计和粗略 burst 数量

### 2. 自动扫描 burst

```bash
python3 usrp_ble_scope/analyze_iq.py scan \
  usrp_ble_scope/ble_cs_single_freq_long.c64 \
  --sample-rate 4e6 \
  --center-freq 2.44e9 \
  --max-samples 20000000
```

输出：

- `analysis/*_bursts.csv`
- `analysis/*_bursts.json`

里面会记录每个 burst 的起止样本、时间、持续时间、峰值幅度、粗略频点估计。

### 3. 查看某一个 burst

例如查看 `burst_id=3`：

```bash
python3 usrp_ble_scope/analyze_iq.py burst \
  usrp_ble_scope/ble_cs_single_freq_long.c64 \
  --sample-rate 4e6 \
  --center-freq 2.44e9 \
  --burst-id 3 \
  --scan-max-samples 20000000
```

输出：

- `analysis/*_burst_3_amplitude.png`
- `analysis/*_burst_3_iq.png`
- `analysis/*_burst_3_spectrogram.png`
- `analysis/*_burst_3_detail.json`

也可以手动指定样本范围：

```bash
python3 usrp_ble_scope/analyze_iq.py burst \
  usrp_ble_scope/ble_cs_single_freq_long.c64 \
  --sample-rate 4e6 \
  --center-freq 2.44e9 \
  --start-sample 123456 \
  --end-sample 130000
```

### 4. 查看某一个 burst 的 GFSK 形态

```bash
python3 usrp_ble_scope/analyze_iq.py gfsk \
  usrp_ble_scope/ble_cs_single_freq_long.c64 \
  --sample-rate 4e6 \
  --center-freq 2.44e9 \
  --burst-id 3 \
  --scan-max-samples 20000000 \
  --target-rate 4e6
```

输出：

- `analysis/*_burst_3_baseband_amplitude.png`
- `analysis/*_burst_3_gfsk_demod.png`
- `analysis/*_burst_3_gfsk.json`

`gfsk` 命令会先估计该 burst 的频点，把它搬移到基带，再降采样并做 quadrature demod。生成的 `*_gfsk_demod.png` 是瞬时频率图，用来观察 GFSK 的频偏跳变形态。

如果自动估计的频偏不准，可以手动指定目标频偏。比如目标频道相对 `center_freq` 是 `-2 MHz`：

```bash
python3 usrp_ble_scope/analyze_iq.py gfsk \
  usrp_ble_scope/ble_cs_single_freq_long.c64 \
  --sample-rate 4e6 \
  --center-freq 2.44e9 \
  --burst-id 3 \
  --target-offset -2e6
```

注意：当前 `gfsk` 是“观察 GFSK 调制形态”，还不是完整 BLE 包解析器。完整解析还需要符号同步、bit slicing、preamble/access address、白化和 CRC 等步骤。

## 使用建议

先用 `--duration 0.05` 或 `--duration 0.1` 做短采集，确认文件能生成，再扩大采集时长。

如果目标只是看 BLE Channel Sounding 的跳频 burst，建议保持 `center_freq=2.44e9`、`sample_rate=100e6`，这样覆盖 2.390 到 2.490 GHz 左右的观测窗口，基本覆盖 BLE 2.4 GHz 频段。实际可用带宽仍取决于 USRP 型号、母板/子板能力、主机网口和磁盘吞吐。
