#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#
# SPDX-License-Identifier: GPL-3.0
#
# GNU Radio Python Flow Graph
# Title: Not titled yet
# Author: lfy
# GNU Radio version: 3.10.7.0

from packaging.version import Version as StrictVersion
from PyQt5 import Qt
from gnuradio import qtgui
from gnuradio import analog
from gnuradio import blocks
from gnuradio import gr
from gnuradio.filter import firdes
from gnuradio.fft import window
import sys
import signal
from PyQt5 import Qt
from argparse import ArgumentParser
from gnuradio.eng_arg import eng_float, intx
from gnuradio import eng_notation
from gnuradio import uhd
import time
from gnuradio import usrp_ble
from gnuradio.qtgui import Range, RangeWidget
from PyQt5 import QtCore



class hop(gr.top_block, Qt.QWidget):

    def __init__(self):
        gr.top_block.__init__(self, "Not titled yet", catch_exceptions=True)
        Qt.QWidget.__init__(self)
        self.setWindowTitle("Not titled yet")
        qtgui.util.check_set_qss()
        try:
            self.setWindowIcon(Qt.QIcon.fromTheme('gnuradio-grc'))
        except BaseException as exc:
            print(f"Qt GUI: Could not set Icon: {str(exc)}", file=sys.stderr)
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

        self.settings = Qt.QSettings("GNU Radio", "hop")

        try:
            if StrictVersion(Qt.qVersion()) < StrictVersion("5.0.0"):
                self.restoreGeometry(self.settings.value("geometry").toByteArray())
            else:
                self.restoreGeometry(self.settings.value("geometry"))
        except BaseException as exc:
            print(f"Qt GUI: Could not restore geometry: {str(exc)}", file=sys.stderr)

        ##################################################
        # Variables
        ##################################################
        self.stop_button = stop_button = 0
        self.start_button = start_button = 0
        self.send_gain = send_gain = 0
        self.samp_rate = samp_rate = 100e6
        self.recv_gain = recv_gain = 0
        self.centetr_fre = centetr_fre = 2.44e9

        ##################################################
        # Blocks
        ##################################################

        self._send_gain_range = Range(0, 20, 1, 0, 200)
        self._send_gain_win = RangeWidget(self._send_gain_range, self.set_send_gain, "'send_gain'", "counter_slider", float, QtCore.Qt.Horizontal)
        self.top_layout.addWidget(self._send_gain_win)
        self.usrp_ble_data_send_0_0 = usrp_ble.data_send(samp_rate, 0.001)
        self.uhd_usrp_sink_0_0_0_0_0 = uhd.usrp_sink(
            ",".join(("addr=192.168.30.2", "send_frame_size=8000,num_send_frames=512")),
            uhd.stream_args(
                cpu_format="fc32",
                otw_format="sc16",
                args='',
                channels=list(range(0,1)),
            ),
            '',
        )
        self.uhd_usrp_sink_0_0_0_0_0.set_subdev_spec('A:0 B:0', 0)
        self.uhd_usrp_sink_0_0_0_0_0.set_samp_rate(samp_rate)
        # No synchronization enforced.

        self.uhd_usrp_sink_0_0_0_0_0.set_center_freq(centetr_fre, 0)
        self.uhd_usrp_sink_0_0_0_0_0.set_antenna('TX/RX', 0)
        self.uhd_usrp_sink_0_0_0_0_0.set_bandwidth(samp_rate, 0)
        self.uhd_usrp_sink_0_0_0_0_0.set_gain(send_gain, 0)
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
        self._recv_gain_range = Range(0, 20, 1, 0, 200)
        self._recv_gain_win = RangeWidget(self._recv_gain_range, self.set_recv_gain, "'recv_gain'", "counter_slider", float, QtCore.Qt.Horizontal)
        self.top_layout.addWidget(self._recv_gain_win)
        self.blocks_multiply_xx_0_1 = blocks.multiply_vcc(1)
        self.blocks_multiply_xx_0_0 = blocks.multiply_vcc(1)
        self.blocks_add_xx_0 = blocks.add_vcc(1)
        self.analog_sig_source_x_0_1 = analog.sig_source_c(samp_rate, analog.GR_COS_WAVE, (-40e6), 1, 0, 0)
        self.analog_sig_source_x_0_0 = analog.sig_source_c(samp_rate, analog.GR_COS_WAVE, (-500e3), 0.5, 0, 0)
        self.analog_sig_source_x_0 = analog.sig_source_c(samp_rate, analog.GR_COS_WAVE, 500e3, 0.5, 0, 0)


        ##################################################
        # Connections
        ##################################################
        self.connect((self.analog_sig_source_x_0, 0), (self.blocks_add_xx_0, 0))
        self.connect((self.analog_sig_source_x_0_0, 0), (self.blocks_add_xx_0, 1))
        self.connect((self.analog_sig_source_x_0_1, 0), (self.blocks_multiply_xx_0_1, 0))
        self.connect((self.blocks_add_xx_0, 0), (self.blocks_multiply_xx_0_1, 1))
        self.connect((self.blocks_multiply_xx_0_0, 0), (self.uhd_usrp_sink_0_0_0_0_0, 0))
        self.connect((self.blocks_multiply_xx_0_1, 0), (self.blocks_multiply_xx_0_0, 0))
        self.connect((self.usrp_ble_data_send_0_0, 0), (self.blocks_multiply_xx_0_0, 1))


    def closeEvent(self, event):
        self.settings = Qt.QSettings("GNU Radio", "hop")
        self.settings.setValue("geometry", self.saveGeometry())
        self.stop()
        self.wait()

        event.accept()

    def get_stop_button(self):
        return self.stop_button

    def set_stop_button(self, stop_button):
        self.stop_button = stop_button

    def get_start_button(self):
        return self.start_button

    def set_start_button(self, start_button):
        self.start_button = start_button

    def get_send_gain(self):
        return self.send_gain

    def set_send_gain(self, send_gain):
        self.send_gain = send_gain
        self.uhd_usrp_sink_0_0_0_0_0.set_gain(self.send_gain, 0)
        self.uhd_usrp_sink_0_0_0_0_0.set_gain(self.send_gain, 1)

    def get_samp_rate(self):
        return self.samp_rate

    def set_samp_rate(self, samp_rate):
        self.samp_rate = samp_rate
        self.analog_sig_source_x_0.set_sampling_freq(self.samp_rate)
        self.analog_sig_source_x_0_0.set_sampling_freq(self.samp_rate)
        self.analog_sig_source_x_0_1.set_sampling_freq(self.samp_rate)
        self.uhd_usrp_sink_0_0_0_0_0.set_samp_rate(self.samp_rate)
        self.uhd_usrp_sink_0_0_0_0_0.set_bandwidth(self.samp_rate, 0)
        self.uhd_usrp_sink_0_0_0_0_0.set_bandwidth(self.samp_rate, 1)
        self.usrp_ble_data_send_0_0.set_sample_rate(self.samp_rate)

    def get_recv_gain(self):
        return self.recv_gain

    def set_recv_gain(self, recv_gain):
        self.recv_gain = recv_gain

    def get_centetr_fre(self):
        return self.centetr_fre

    def set_centetr_fre(self, centetr_fre):
        self.centetr_fre = centetr_fre
        self.uhd_usrp_sink_0_0_0_0_0.set_center_freq(self.centetr_fre, 0)
        self.uhd_usrp_sink_0_0_0_0_0.set_center_freq(self.centetr_fre, 1)




def main(top_block_cls=hop, options=None):

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
