#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#
# SPDX-License-Identifier: GPL-3.0
#
# GNU Radio Python Flow Graph
# Title: Not titled yet
# Author: lfy
# GNU Radio version: 3.10.1.1

from packaging.version import Version as StrictVersion

if __name__ == '__main__':
    import ctypes
    import sys
    if sys.platform.startswith('linux'):
        try:
            x11 = ctypes.cdll.LoadLibrary('libX11.so')
            x11.XInitThreads()
        except:
            print("Warning: failed to XInitThreads()")

from PyQt5 import Qt
from gnuradio import analog
from gnuradio import blocks
from gnuradio import gr
from gnuradio.filter import firdes
from gnuradio.fft import window
import sys
import signal
from argparse import ArgumentParser
from gnuradio.eng_arg import eng_float, intx
from gnuradio import eng_notation
from gnuradio import uhd
import time
from gnuradio import usrp_ble
from gnuradio.qtgui import Range, RangeWidget
from PyQt5 import QtCore



from gnuradio import qtgui

class ble_cs_1to1_2sides(gr.top_block, Qt.QWidget):

    def __init__(self):
        gr.top_block.__init__(self, "Not titled yet", catch_exceptions=True)
        Qt.QWidget.__init__(self)
        self.setWindowTitle("Not titled yet")
        qtgui.util.check_set_qss()
        try:
            self.setWindowIcon(Qt.QIcon.fromTheme('gnuradio-grc'))
        except:
            pass
        self.top_scroll_layout = Qt.QVBoxLayout()
        self.setLayout(self.top_scroll_layout)
        self.top_scroll = Qt.QScrollArea()
        self.top_scroll.setFrameStyle(Qt.QFrame.NoFrame)
        self.top_scroll_layout.addWidget(self.top_scroll)
        self.top_scroll.setWidgetResizable(True)
        self.top_widget = Qt.QWidget()
        self.top_scroll.setWidget(self.top_widget)
        self.top_layout = Qt.QVBoxLayout(self.top_widget)
        self.top_grid_layout = Qt.QGridLayout()
        self.top_layout.addLayout(self.top_grid_layout)

        self.settings = Qt.QSettings("GNU Radio", "ble_cs_1to1_2sides")

        try:
            if StrictVersion(Qt.qVersion()) < StrictVersion("5.0.0"):
                self.restoreGeometry(self.settings.value("geometry").toByteArray())
            else:
                self.restoreGeometry(self.settings.value("geometry"))
        except:
            pass

        ##################################################
        # Variables
        ##################################################
        self.wait_time_ms = wait_time_ms = 50
        self.stop_freq_index = stop_freq_index = 40
        self.stop_button = stop_button = 0
        self.step_hz = step_hz = 1e5
        self.start_freq_index = start_freq_index = -40
        self.start_button = start_button = 0
        self.send_gain = send_gain = 1
        self.samp_rate = samp_rate = 10e6
        self.repeat_total = repeat_total = 1
        self.recv_gain = recv_gain = 1
        self.centetr_fre = centetr_fre = 2.44e9

        ##################################################
        # Blocks
        ##################################################
        _stop_button_push_button = Qt.QPushButton('')
        _stop_button_push_button = Qt.QPushButton('stop_button')
        self._stop_button_choices = {'Pressed': 1, 'Released': 0}
        _stop_button_push_button.pressed.connect(lambda: self.set_stop_button(self._stop_button_choices['Pressed']))
        _stop_button_push_button.released.connect(lambda: self.set_stop_button(self._stop_button_choices['Released']))
        self.top_layout.addWidget(_stop_button_push_button)
        _start_button_push_button = Qt.QPushButton('')
        _start_button_push_button = Qt.QPushButton('start_button')
        self._start_button_choices = {'Pressed': 1, 'Released': 0}
        _start_button_push_button.pressed.connect(lambda: self.set_start_button(self._start_button_choices['Pressed']))
        _start_button_push_button.released.connect(lambda: self.set_start_button(self._start_button_choices['Released']))
        self.top_layout.addWidget(_start_button_push_button)
        self._send_gain_range = Range(0, 20, 1, 1, 200)
        self._send_gain_win = RangeWidget(self._send_gain_range, self.set_send_gain, "'send_gain'", "counter_slider", float, QtCore.Qt.Horizontal)
        self.top_layout.addWidget(self._send_gain_win)
        self._recv_gain_range = Range(0, 20, 1, 1, 200)
        self._recv_gain_win = RangeWidget(self._recv_gain_range, self.set_recv_gain, "'recv_gain'", "counter_slider", float, QtCore.Qt.Horizontal)
        self.top_layout.addWidget(self._recv_gain_win)
        self.usrp_ble_random_phase_1 = usrp_ble.random_phase(2, 1.0)
        self.usrp_ble_random_phase_0 = usrp_ble.random_phase(1, 1.0)
        self.usrp_ble_interact_center_0 = usrp_ble.interact_center(int(samp_rate), start_button, stop_button, wait_time_ms, repeat_total, start_freq_index, stop_freq_index, step_hz, 9)
        self.usrp_ble_interact_center_0.set_use_msg_clock(False)
        self.usrp_ble_data_send_0_0 = usrp_ble.data_send(samp_rate, 0.001)
        self.usrp_ble_data_send_0 = usrp_ble.data_send(samp_rate, 0.001)
        self.usrp_ble_capture_gate_8_0 = usrp_ble.capture_gate(1, 8)
        self.usrp_ble_capture_gate_8 = usrp_ble.capture_gate(1, 8)
        self.usrp_ble_capture_gate_7_0 = usrp_ble.capture_gate(1, 7)
        self.usrp_ble_capture_gate_7 = usrp_ble.capture_gate(1, 7)
        self.usrp_ble_capture_gate_6_0 = usrp_ble.capture_gate(1, 6)
        self.usrp_ble_capture_gate_6 = usrp_ble.capture_gate(1, 6)
        self.usrp_ble_capture_gate_5_0 = usrp_ble.capture_gate(1, 5)
        self.usrp_ble_capture_gate_5 = usrp_ble.capture_gate(1, 5)
        self.usrp_ble_capture_gate_4_0 = usrp_ble.capture_gate(1, 4)
        self.usrp_ble_capture_gate_4 = usrp_ble.capture_gate(1, 4)
        self.usrp_ble_capture_gate_3_0 = usrp_ble.capture_gate(1, 3)
        self.usrp_ble_capture_gate_3 = usrp_ble.capture_gate(1, 3)
        self.usrp_ble_capture_gate_2_0 = usrp_ble.capture_gate(1, 2)
        self.usrp_ble_capture_gate_2 = usrp_ble.capture_gate(1, 2)
        self.usrp_ble_capture_gate_1_0 = usrp_ble.capture_gate(1, 1)
        self.usrp_ble_capture_gate_1 = usrp_ble.capture_gate(1, 1)
        self.usrp_ble_capture_gate_0_0 = usrp_ble.capture_gate(1, 0)
        self.usrp_ble_capture_gate_0 = usrp_ble.capture_gate(1, 0)
        self.uhd_usrp_source_0_0 = uhd.usrp_source(
            ",".join(("addr=192.168.40.2", "recv_frame_size=8000,num_recv_frames=512")),
            uhd.stream_args(
                cpu_format="fc32",
                otw_format="sc16",
                args='',
                channels=list(range(0,1)),
            ),
        )
        self.uhd_usrp_source_0_0.set_subdev_spec('A:0', 0)
        self.uhd_usrp_source_0_0.set_samp_rate(samp_rate)
        self.uhd_usrp_source_0_0.set_time_unknown_pps(uhd.time_spec(0))

        self.uhd_usrp_source_0_0.set_center_freq(centetr_fre, 0)
        self.uhd_usrp_source_0_0.set_antenna("RX2", 0)
        self.uhd_usrp_source_0_0.set_bandwidth(samp_rate, 0)
        self.uhd_usrp_source_0_0.set_gain(recv_gain, 0)
        self.uhd_usrp_source_0 = uhd.usrp_source(
            ",".join(("addr=192.168.30.2", "recv_frame_size=8000,num_recv_frames=512")),
            uhd.stream_args(
                cpu_format="fc32",
                otw_format="sc16",
                args='',
                channels=list(range(0,1)),
            ),
        )
        self.uhd_usrp_source_0.set_clock_source('external', 0)
        self.uhd_usrp_source_0.set_time_source('external', 0)
        self.uhd_usrp_source_0.set_subdev_spec('A:0', 0)
        self.uhd_usrp_source_0.set_samp_rate(samp_rate)
        self.uhd_usrp_source_0.set_time_unknown_pps(uhd.time_spec(0))

        self.uhd_usrp_source_0.set_center_freq(centetr_fre, 0)
        self.uhd_usrp_source_0.set_antenna("RX2", 0)
        self.uhd_usrp_source_0.set_bandwidth(samp_rate, 0)
        self.uhd_usrp_source_0.set_gain(recv_gain, 0)
        self.uhd_usrp_sink_0_0_0_0 = uhd.usrp_sink(
            ",".join(("addr=192.168.40.2", "send_frame_size=8000,num_send_frames=512")),
            uhd.stream_args(
                cpu_format="fc32",
                otw_format="sc16",
                args='',
                channels=list(range(0,1)),
            ),
            "",
        )
        self.uhd_usrp_sink_0_0_0_0.set_subdev_spec('A:0', 0)
        self.uhd_usrp_sink_0_0_0_0.set_samp_rate(samp_rate)
        self.uhd_usrp_sink_0_0_0_0.set_time_unknown_pps(uhd.time_spec(0))

        self.uhd_usrp_sink_0_0_0_0.set_center_freq(centetr_fre, 0)
        self.uhd_usrp_sink_0_0_0_0.set_antenna('TX/RX', 0)
        self.uhd_usrp_sink_0_0_0_0.set_bandwidth(samp_rate, 0)
        self.uhd_usrp_sink_0_0_0_0.set_gain(send_gain, 0)
        self.uhd_usrp_sink_0_0_0 = uhd.usrp_sink(
            ",".join(("addr=192.168.30.2", "send_frame_size=8000,num_send_frames=512")),
            uhd.stream_args(
                cpu_format="fc32",
                otw_format="sc16",
                args='',
                channels=list(range(0,1)),
            ),
            "",
        )
        self.uhd_usrp_sink_0_0_0.set_clock_source('external', 0)
        self.uhd_usrp_sink_0_0_0.set_time_source('external', 0)
        self.uhd_usrp_sink_0_0_0.set_subdev_spec('A:0', 0)
        self.uhd_usrp_sink_0_0_0.set_samp_rate(samp_rate)
        self.uhd_usrp_sink_0_0_0.set_time_unknown_pps(uhd.time_spec(0))

        self.uhd_usrp_sink_0_0_0.set_center_freq(centetr_fre, 0)
        self.uhd_usrp_sink_0_0_0.set_antenna('TX/RX', 0)
        self.uhd_usrp_sink_0_0_0.set_bandwidth(samp_rate, 0)
        self.uhd_usrp_sink_0_0_0.set_gain(send_gain, 0)
        self.blocks_multiply_xx_0_0 = blocks.multiply_vcc(1)
        self.blocks_multiply_xx_0 = blocks.multiply_vcc(1)
        self.blocks_multiply_conjugate_cc_0_0 = blocks.multiply_conjugate_cc(1)
        self.blocks_multiply_conjugate_cc_0 = blocks.multiply_conjugate_cc(1)
        self.blocks_message_debug_0 = blocks.message_debug(True)
        self.blocks_file_sink_9 = blocks.file_sink(gr.sizeof_gr_complex*1, '/home/lfy/workarea/zz_ble_cs_gnuradio/1to1_2sides/data_initiator_rx_from_reflector_measurement_4', False)
        self.blocks_file_sink_9.set_unbuffered(False)
        self.blocks_file_sink_8 = blocks.file_sink(gr.sizeof_gr_complex*1, '/home/lfy/workarea/zz_ble_cs_gnuradio/1to1_2sides/data_reflector_rx_from_initiator_measurement_4', False)
        self.blocks_file_sink_8.set_unbuffered(False)
        self.blocks_file_sink_7 = blocks.file_sink(gr.sizeof_gr_complex*1, '/home/lfy/workarea/zz_ble_cs_gnuradio/1to1_2sides/data_initiator_rx_from_reflector_measurement_3', False)
        self.blocks_file_sink_7.set_unbuffered(False)
        self.blocks_file_sink_6 = blocks.file_sink(gr.sizeof_gr_complex*1, '/home/lfy/workarea/zz_ble_cs_gnuradio/1to1_2sides/data_reflector_rx_from_initiator_measurement_3', False)
        self.blocks_file_sink_6.set_unbuffered(False)
        self.blocks_file_sink_5 = blocks.file_sink(gr.sizeof_gr_complex*1, '/home/lfy/workarea/zz_ble_cs_gnuradio/1to1_2sides/data_initiator_rx_from_reflector_measurement_2', False)
        self.blocks_file_sink_5.set_unbuffered(False)
        self.blocks_file_sink_4 = blocks.file_sink(gr.sizeof_gr_complex*1, '/home/lfy/workarea/zz_ble_cs_gnuradio/1to1_2sides/data_reflector_rx_from_initiator_measurement_2', False)
        self.blocks_file_sink_4.set_unbuffered(False)
        self.blocks_file_sink_1_0 = blocks.file_sink(gr.sizeof_gr_complex*1, '/home/lfy/workarea/zz_ble_cs_gnuradio/1to1_2sides/data_initiator_rx_from_reflector_measurement', False)
        self.blocks_file_sink_1_0.set_unbuffered(False)
        self.blocks_file_sink_17 = blocks.file_sink(gr.sizeof_gr_complex*1, '/home/lfy/workarea/zz_ble_cs_gnuradio/1to1_2sides/data_initiator_rx_from_reflector_measurement_8', False)
        self.blocks_file_sink_17.set_unbuffered(False)
        self.blocks_file_sink_16 = blocks.file_sink(gr.sizeof_gr_complex*1, '/home/lfy/workarea/zz_ble_cs_gnuradio/1to1_2sides/data_reflector_rx_from_initiator_measurement_8', False)
        self.blocks_file_sink_16.set_unbuffered(False)
        self.blocks_file_sink_15 = blocks.file_sink(gr.sizeof_gr_complex*1, '/home/lfy/workarea/zz_ble_cs_gnuradio/1to1_2sides/data_initiator_rx_from_reflector_measurement_7', False)
        self.blocks_file_sink_15.set_unbuffered(False)
        self.blocks_file_sink_14 = blocks.file_sink(gr.sizeof_gr_complex*1, '/home/lfy/workarea/zz_ble_cs_gnuradio/1to1_2sides/data_reflector_rx_from_initiator_measurement_7', False)
        self.blocks_file_sink_14.set_unbuffered(False)
        self.blocks_file_sink_13 = blocks.file_sink(gr.sizeof_gr_complex*1, '/home/lfy/workarea/zz_ble_cs_gnuradio/1to1_2sides/data_initiator_rx_from_reflector_measurement_6', False)
        self.blocks_file_sink_13.set_unbuffered(False)
        self.blocks_file_sink_12 = blocks.file_sink(gr.sizeof_gr_complex*1, '/home/lfy/workarea/zz_ble_cs_gnuradio/1to1_2sides/data_reflector_rx_from_initiator_measurement_6', False)
        self.blocks_file_sink_12.set_unbuffered(False)
        self.blocks_file_sink_11 = blocks.file_sink(gr.sizeof_gr_complex*1, '/home/lfy/workarea/zz_ble_cs_gnuradio/1to1_2sides/data_initiator_rx_from_reflector_measurement_5', False)
        self.blocks_file_sink_11.set_unbuffered(False)
        self.blocks_file_sink_10 = blocks.file_sink(gr.sizeof_gr_complex*1, '/home/lfy/workarea/zz_ble_cs_gnuradio/1to1_2sides/data_reflector_rx_from_initiator_measurement_5', False)
        self.blocks_file_sink_10.set_unbuffered(False)
        self.blocks_file_sink_1 = blocks.file_sink(gr.sizeof_gr_complex*1, '/home/lfy/workarea/zz_ble_cs_gnuradio/1to1_2sides/data_reflector_rx_from_initiator_measurement', False)
        self.blocks_file_sink_1.set_unbuffered(False)
        self.blocks_file_sink_0_0 = blocks.file_sink(gr.sizeof_gr_complex*1, '/home/lfy/workarea/zz_ble_cs_gnuradio/1to1_2sides/data_initiator_rx_from_reflector_calibration', False)
        self.blocks_file_sink_0_0.set_unbuffered(False)
        self.blocks_file_sink_0 = blocks.file_sink(gr.sizeof_gr_complex*1, '/home/lfy/workarea/zz_ble_cs_gnuradio/1to1_2sides/data_reflector_rx_from_initiator_calibration', False)
        self.blocks_file_sink_0.set_unbuffered(False)
        self.analog_sig_source_x_0_1 = analog.sig_source_c(samp_rate, analog.GR_COS_WAVE, start_freq_index*step_hz, 1, 0, 0)


        ##################################################
        # Connections
        ##################################################
        self.msg_connect((self.usrp_ble_interact_center_0, 'freq_ctrl'), (self.analog_sig_source_x_0_1, 'cmd'))
        self.msg_connect((self.usrp_ble_interact_center_0, 'freq_ctrl'), (self.blocks_message_debug_0, 'print'))
        self.msg_connect((self.usrp_ble_interact_center_0, 'capture_ctrl'), (self.usrp_ble_capture_gate_0, 'command'))
        self.msg_connect((self.usrp_ble_interact_center_0, 'capture_ctrl'), (self.usrp_ble_capture_gate_0_0, 'command'))
        self.msg_connect((self.usrp_ble_interact_center_0, 'capture_ctrl'), (self.usrp_ble_capture_gate_1, 'command'))
        self.msg_connect((self.usrp_ble_interact_center_0, 'capture_ctrl'), (self.usrp_ble_capture_gate_1_0, 'command'))
        self.msg_connect((self.usrp_ble_interact_center_0, 'capture_ctrl'), (self.usrp_ble_capture_gate_2, 'command'))
        self.msg_connect((self.usrp_ble_interact_center_0, 'capture_ctrl'), (self.usrp_ble_capture_gate_2_0, 'command'))
        self.msg_connect((self.usrp_ble_interact_center_0, 'capture_ctrl'), (self.usrp_ble_capture_gate_3, 'command'))
        self.msg_connect((self.usrp_ble_interact_center_0, 'capture_ctrl'), (self.usrp_ble_capture_gate_3_0, 'command'))
        self.msg_connect((self.usrp_ble_interact_center_0, 'capture_ctrl'), (self.usrp_ble_capture_gate_4, 'command'))
        self.msg_connect((self.usrp_ble_interact_center_0, 'capture_ctrl'), (self.usrp_ble_capture_gate_4_0, 'command'))
        self.msg_connect((self.usrp_ble_interact_center_0, 'capture_ctrl'), (self.usrp_ble_capture_gate_5, 'command'))
        self.msg_connect((self.usrp_ble_interact_center_0, 'capture_ctrl'), (self.usrp_ble_capture_gate_5_0, 'command'))
        self.msg_connect((self.usrp_ble_interact_center_0, 'capture_ctrl'), (self.usrp_ble_capture_gate_6, 'command'))
        self.msg_connect((self.usrp_ble_interact_center_0, 'capture_ctrl'), (self.usrp_ble_capture_gate_6_0, 'command'))
        self.msg_connect((self.usrp_ble_interact_center_0, 'capture_ctrl'), (self.usrp_ble_capture_gate_7, 'command'))
        self.msg_connect((self.usrp_ble_interact_center_0, 'capture_ctrl'), (self.usrp_ble_capture_gate_7_0, 'command'))
        self.msg_connect((self.usrp_ble_interact_center_0, 'capture_ctrl'), (self.usrp_ble_capture_gate_8, 'command'))
        self.msg_connect((self.usrp_ble_interact_center_0, 'capture_ctrl'), (self.usrp_ble_capture_gate_8_0, 'command'))
        self.msg_connect((self.usrp_ble_interact_center_0, 'send1_ctrl'), (self.usrp_ble_data_send_0, 'command'))
        self.msg_connect((self.usrp_ble_interact_center_0, 'send2_ctrl'), (self.usrp_ble_data_send_0_0, 'command'))
        self.msg_connect((self.usrp_ble_interact_center_0, 'freq_ctrl'), (self.usrp_ble_random_phase_0, 'freq'))
        self.msg_connect((self.usrp_ble_interact_center_0, 'freq_ctrl'), (self.usrp_ble_random_phase_1, 'freq'))
        self.connect((self.analog_sig_source_x_0_1, 0), (self.usrp_ble_random_phase_0, 0))
        self.connect((self.analog_sig_source_x_0_1, 0), (self.usrp_ble_random_phase_1, 0))
        self.connect((self.blocks_multiply_conjugate_cc_0, 0), (self.usrp_ble_capture_gate_0, 0))
        self.connect((self.blocks_multiply_conjugate_cc_0, 0), (self.usrp_ble_capture_gate_1, 0))
        self.connect((self.blocks_multiply_conjugate_cc_0, 0), (self.usrp_ble_capture_gate_2, 0))
        self.connect((self.blocks_multiply_conjugate_cc_0, 0), (self.usrp_ble_capture_gate_3, 0))
        self.connect((self.blocks_multiply_conjugate_cc_0, 0), (self.usrp_ble_capture_gate_4, 0))
        self.connect((self.blocks_multiply_conjugate_cc_0, 0), (self.usrp_ble_capture_gate_5, 0))
        self.connect((self.blocks_multiply_conjugate_cc_0, 0), (self.usrp_ble_capture_gate_6, 0))
        self.connect((self.blocks_multiply_conjugate_cc_0, 0), (self.usrp_ble_capture_gate_7, 0))
        self.connect((self.blocks_multiply_conjugate_cc_0, 0), (self.usrp_ble_capture_gate_8, 0))
        self.connect((self.blocks_multiply_conjugate_cc_0_0, 0), (self.usrp_ble_capture_gate_0_0, 0))
        self.connect((self.blocks_multiply_conjugate_cc_0_0, 0), (self.usrp_ble_capture_gate_1_0, 0))
        self.connect((self.blocks_multiply_conjugate_cc_0_0, 0), (self.usrp_ble_capture_gate_2_0, 0))
        self.connect((self.blocks_multiply_conjugate_cc_0_0, 0), (self.usrp_ble_capture_gate_3_0, 0))
        self.connect((self.blocks_multiply_conjugate_cc_0_0, 0), (self.usrp_ble_capture_gate_4_0, 0))
        self.connect((self.blocks_multiply_conjugate_cc_0_0, 0), (self.usrp_ble_capture_gate_5_0, 0))
        self.connect((self.blocks_multiply_conjugate_cc_0_0, 0), (self.usrp_ble_capture_gate_6_0, 0))
        self.connect((self.blocks_multiply_conjugate_cc_0_0, 0), (self.usrp_ble_capture_gate_7_0, 0))
        self.connect((self.blocks_multiply_conjugate_cc_0_0, 0), (self.usrp_ble_capture_gate_8_0, 0))
        self.connect((self.blocks_multiply_xx_0, 0), (self.uhd_usrp_sink_0_0_0, 0))
        self.connect((self.blocks_multiply_xx_0_0, 0), (self.uhd_usrp_sink_0_0_0_0, 0))
        self.connect((self.uhd_usrp_source_0, 0), (self.blocks_multiply_conjugate_cc_0_0, 0))
        self.connect((self.uhd_usrp_source_0_0, 0), (self.blocks_multiply_conjugate_cc_0, 0))
        self.connect((self.usrp_ble_capture_gate_0, 0), (self.blocks_file_sink_0, 0))
        self.connect((self.usrp_ble_capture_gate_0_0, 0), (self.blocks_file_sink_0_0, 0))
        self.connect((self.usrp_ble_capture_gate_1, 0), (self.blocks_file_sink_1, 0))
        self.connect((self.usrp_ble_capture_gate_1_0, 0), (self.blocks_file_sink_1_0, 0))
        self.connect((self.usrp_ble_capture_gate_2, 0), (self.blocks_file_sink_4, 0))
        self.connect((self.usrp_ble_capture_gate_2_0, 0), (self.blocks_file_sink_5, 0))
        self.connect((self.usrp_ble_capture_gate_3, 0), (self.blocks_file_sink_6, 0))
        self.connect((self.usrp_ble_capture_gate_3_0, 0), (self.blocks_file_sink_7, 0))
        self.connect((self.usrp_ble_capture_gate_4, 0), (self.blocks_file_sink_8, 0))
        self.connect((self.usrp_ble_capture_gate_4_0, 0), (self.blocks_file_sink_9, 0))
        self.connect((self.usrp_ble_capture_gate_5, 0), (self.blocks_file_sink_10, 0))
        self.connect((self.usrp_ble_capture_gate_5_0, 0), (self.blocks_file_sink_11, 0))
        self.connect((self.usrp_ble_capture_gate_6, 0), (self.blocks_file_sink_12, 0))
        self.connect((self.usrp_ble_capture_gate_6_0, 0), (self.blocks_file_sink_13, 0))
        self.connect((self.usrp_ble_capture_gate_7, 0), (self.blocks_file_sink_14, 0))
        self.connect((self.usrp_ble_capture_gate_7_0, 0), (self.blocks_file_sink_15, 0))
        self.connect((self.usrp_ble_capture_gate_8, 0), (self.blocks_file_sink_16, 0))
        self.connect((self.usrp_ble_capture_gate_8_0, 0), (self.blocks_file_sink_17, 0))
        self.connect((self.usrp_ble_data_send_0, 0), (self.blocks_multiply_xx_0, 1))
        self.connect((self.usrp_ble_data_send_0, 0), (self.usrp_ble_interact_center_0, 0))
        self.connect((self.usrp_ble_data_send_0_0, 0), (self.blocks_multiply_xx_0_0, 1))
        self.connect((self.usrp_ble_random_phase_0, 0), (self.blocks_multiply_conjugate_cc_0_0, 1))
        self.connect((self.usrp_ble_random_phase_0, 0), (self.blocks_multiply_xx_0, 0))
        self.connect((self.usrp_ble_random_phase_1, 0), (self.blocks_multiply_conjugate_cc_0, 1))
        self.connect((self.usrp_ble_random_phase_1, 0), (self.blocks_multiply_xx_0_0, 0))


    def closeEvent(self, event):
        self.settings = Qt.QSettings("GNU Radio", "ble_cs_1to1_2sides")
        self.settings.setValue("geometry", self.saveGeometry())
        self.stop()
        self.wait()

        event.accept()

    def get_wait_time_ms(self):
        return self.wait_time_ms

    def set_wait_time_ms(self, wait_time_ms):
        self.wait_time_ms = wait_time_ms
        self.usrp_ble_interact_center_0.set_wait_time_ms(self.wait_time_ms)

    def get_stop_freq_index(self):
        return self.stop_freq_index

    def set_stop_freq_index(self, stop_freq_index):
        self.stop_freq_index = stop_freq_index
        self.usrp_ble_interact_center_0.set_stop_freq_index(self.stop_freq_index)

    def get_stop_button(self):
        return self.stop_button

    def set_stop_button(self, stop_button):
        self.stop_button = stop_button
        self.usrp_ble_interact_center_0.set_stop_btn(self.stop_button)

    def get_step_hz(self):
        return self.step_hz

    def set_step_hz(self, step_hz):
        self.step_hz = step_hz
        self.analog_sig_source_x_0_1.set_frequency(self.start_freq_index*self.step_hz)
        self.usrp_ble_interact_center_0.set_step_hz(self.step_hz)

    def get_start_freq_index(self):
        return self.start_freq_index

    def set_start_freq_index(self, start_freq_index):
        self.start_freq_index = start_freq_index
        self.analog_sig_source_x_0_1.set_frequency(self.start_freq_index*self.step_hz)
        self.usrp_ble_interact_center_0.set_start_freq_index(self.start_freq_index)

    def get_start_button(self):
        return self.start_button

    def set_start_button(self, start_button):
        self.start_button = start_button
        self.usrp_ble_interact_center_0.set_start_btn(self.start_button)

    def get_send_gain(self):
        return self.send_gain

    def set_send_gain(self, send_gain):
        self.send_gain = send_gain
        self.uhd_usrp_sink_0_0_0.set_gain(self.send_gain, 0)
        self.uhd_usrp_sink_0_0_0_0.set_gain(self.send_gain, 0)

    def get_samp_rate(self):
        return self.samp_rate

    def set_samp_rate(self, samp_rate):
        self.samp_rate = samp_rate
        self.analog_sig_source_x_0_1.set_sampling_freq(self.samp_rate)
        self.uhd_usrp_sink_0_0_0.set_samp_rate(self.samp_rate)
        self.uhd_usrp_sink_0_0_0.set_bandwidth(self.samp_rate, 0)
        self.uhd_usrp_sink_0_0_0_0.set_samp_rate(self.samp_rate)
        self.uhd_usrp_sink_0_0_0_0.set_bandwidth(self.samp_rate, 0)
        self.uhd_usrp_source_0.set_samp_rate(self.samp_rate)
        self.uhd_usrp_source_0.set_bandwidth(self.samp_rate, 0)
        self.uhd_usrp_source_0_0.set_samp_rate(self.samp_rate)
        self.uhd_usrp_source_0_0.set_bandwidth(self.samp_rate, 0)
        self.usrp_ble_data_send_0.set_sample_rate(self.samp_rate)
        self.usrp_ble_data_send_0_0.set_sample_rate(self.samp_rate)

    def get_repeat_total(self):
        return self.repeat_total

    def set_repeat_total(self, repeat_total):
        self.repeat_total = repeat_total

    def get_recv_gain(self):
        return self.recv_gain

    def set_recv_gain(self, recv_gain):
        self.recv_gain = recv_gain
        self.uhd_usrp_source_0.set_gain(self.recv_gain, 0)
        self.uhd_usrp_source_0_0.set_gain(self.recv_gain, 0)

    def get_centetr_fre(self):
        return self.centetr_fre

    def set_centetr_fre(self, centetr_fre):
        self.centetr_fre = centetr_fre
        self.uhd_usrp_sink_0_0_0.set_center_freq(self.centetr_fre, 0)
        self.uhd_usrp_sink_0_0_0_0.set_center_freq(self.centetr_fre, 0)
        self.uhd_usrp_source_0.set_center_freq(self.centetr_fre, 0)
        self.uhd_usrp_source_0_0.set_center_freq(self.centetr_fre, 0)




def main(top_block_cls=ble_cs_1to1_2sides, options=None):

    if StrictVersion("4.5.0") <= StrictVersion(Qt.qVersion()) < StrictVersion("5.0.0"):
        style = gr.prefs().get_string('qtgui', 'style', 'raster')
        Qt.QApplication.setGraphicsSystem(style)
    qapp = Qt.QApplication(sys.argv)

    tb = top_block_cls()

    tb.start()

    tb.show()

    def sig_handler(sig=None, frame=None):
        tb.stop()
        tb.wait()

        Qt.QApplication.quit()

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    timer = Qt.QTimer()
    timer.start(500)
    timer.timeout.connect(lambda: None)

    qapp.exec_()

if __name__ == '__main__':
    main()
