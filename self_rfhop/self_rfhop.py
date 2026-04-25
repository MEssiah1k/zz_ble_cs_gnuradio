#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#
# SPDX-License-Identifier: GPL-3.0
#
# GNU Radio Python Flow Graph
# Title: Not titled yet
# GNU Radio version: 3.10.9.2

from PyQt5 import Qt
from gnuradio import qtgui
from PyQt5 import QtCore
from gnuradio import analog
from gnuradio import blocks
from gnuradio import blocks, gr
from gnuradio import gr
from gnuradio.filter import firdes
from gnuradio.fft import window
import sys
import signal
from PyQt5 import Qt
from argparse import ArgumentParser
from gnuradio.eng_arg import eng_float, intx
from gnuradio import eng_notation
from gnuradio import usrp_ble
import sip



class self_rfhop(gr.top_block, Qt.QWidget):

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

        self.settings = Qt.QSettings("GNU Radio", "self_rfhop")

        try:
            geometry = self.settings.value("geometry")
            if geometry:
                self.restoreGeometry(geometry)
        except BaseException as exc:
            print(f"Qt GUI: Could not restore geometry: {str(exc)}", file=sys.stderr)

        ##################################################
        # Variables
        ##################################################
        self.tone_freq = tone_freq = 100e3
        self.hop_offset = hop_offset = -40e6
        self.stop_button = stop_button = 0
        self.start_button = start_button = 0
        self.send_gain = send_gain = 0
        self.samp_rate_0 = samp_rate_0 = 1e6
        self.samp_rate = samp_rate = 1e6
        self.recv_gain = recv_gain = 18
        self.distance_m = distance_m = 3
        self.display_decim = display_decim = 1
        self.centetr_fre = centetr_fre = 2.44e9 + hop_offset - tone_freq

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
        self.usrp_ble_interact_center_rfhop_0 = usrp_ble.interact_center_rfhop(int(samp_rate), start_button, stop_button, 10, 30, 3, (-40), 40, 1e4)
        self.usrp_ble_data_send_0_0 = usrp_ble.data_send(samp_rate, 0.001)
        self.usrp_ble_data_send_0 = usrp_ble.data_send(samp_rate, 0.001)
        self.usrp_ble_channel_phase_0_0 = usrp_ble.channel_phase((centetr_fre+tone_freq), distance_m, 1.0)
        self.usrp_ble_channel_phase_0 = usrp_ble.channel_phase((centetr_fre+tone_freq), distance_m, 1.0)
        self.usrp_ble_capture_gate_0_0 = usrp_ble.capture_gate(1)
        self.usrp_ble_capture_gate_0 = usrp_ble.capture_gate(1)
        self._send_gain_range = qtgui.Range(0, 20, 1, 0, 200)
        self._send_gain_win = qtgui.RangeWidget(self._send_gain_range, self.set_send_gain, "'send_gain'", "counter_slider", float, QtCore.Qt.Horizontal)
        self.top_layout.addWidget(self._send_gain_win)
        self._recv_gain_range = qtgui.Range(0, 20, 1, 18, 200)
        self._recv_gain_win = qtgui.RangeWidget(self._recv_gain_range, self.set_recv_gain, "'recv_gain'", "counter_slider", float, QtCore.Qt.Horizontal)
        self.top_layout.addWidget(self._recv_gain_win)
        self.qtgui_time_sink_x_0 = qtgui.time_sink_c(
            1024, #size
            samp_rate, #samp_rate
            "", #name
            1, #number of inputs
            None # parent
        )
        self.qtgui_time_sink_x_0.set_update_time(0.10)
        self.qtgui_time_sink_x_0.set_y_axis(-1, 1)

        self.qtgui_time_sink_x_0.set_y_label('Amplitude', "")

        self.qtgui_time_sink_x_0.enable_tags(True)
        self.qtgui_time_sink_x_0.set_trigger_mode(qtgui.TRIG_MODE_FREE, qtgui.TRIG_SLOPE_POS, 0.0, 0, 0, "")
        self.qtgui_time_sink_x_0.enable_autoscale(False)
        self.qtgui_time_sink_x_0.enable_grid(False)
        self.qtgui_time_sink_x_0.enable_axis_labels(True)
        self.qtgui_time_sink_x_0.enable_control_panel(False)
        self.qtgui_time_sink_x_0.enable_stem_plot(False)


        labels = ['Signal 1', 'Signal 2', 'Signal 3', 'Signal 4', 'Signal 5',
            'Signal 6', 'Signal 7', 'Signal 8', 'Signal 9', 'Signal 10']
        widths = [1, 1, 1, 1, 1,
            1, 1, 1, 1, 1]
        colors = ['blue', 'red', 'green', 'black', 'cyan',
            'magenta', 'yellow', 'dark red', 'dark green', 'dark blue']
        alphas = [1.0, 1.0, 1.0, 1.0, 1.0,
            1.0, 1.0, 1.0, 1.0, 1.0]
        styles = [1, 1, 1, 1, 1,
            1, 1, 1, 1, 1]
        markers = [-1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1]


        for i in range(2):
            if len(labels[i]) == 0:
                if (i % 2 == 0):
                    self.qtgui_time_sink_x_0.set_line_label(i, "Re{{Data {0}}}".format(i/2))
                else:
                    self.qtgui_time_sink_x_0.set_line_label(i, "Im{{Data {0}}}".format(i/2))
            else:
                self.qtgui_time_sink_x_0.set_line_label(i, labels[i])
            self.qtgui_time_sink_x_0.set_line_width(i, widths[i])
            self.qtgui_time_sink_x_0.set_line_color(i, colors[i])
            self.qtgui_time_sink_x_0.set_line_style(i, styles[i])
            self.qtgui_time_sink_x_0.set_line_marker(i, markers[i])
            self.qtgui_time_sink_x_0.set_line_alpha(i, alphas[i])

        self._qtgui_time_sink_x_0_win = sip.wrapinstance(self.qtgui_time_sink_x_0.qwidget(), Qt.QWidget)
        self.top_layout.addWidget(self._qtgui_time_sink_x_0_win)
        self.qtgui_freq_sink_x_0_1_0 = qtgui.freq_sink_c(
            8192, #size
            window.WIN_BLACKMAN_hARRIS, #wintype
            0, #fc
            100e6, #bw
            "rx_gate", #name
            1,
            None # parent
        )
        self.qtgui_freq_sink_x_0_1_0.set_update_time(0.10)
        self.qtgui_freq_sink_x_0_1_0.set_y_axis((-140), 10)
        self.qtgui_freq_sink_x_0_1_0.set_y_label('Relative Gain', 'dB')
        self.qtgui_freq_sink_x_0_1_0.set_trigger_mode(qtgui.TRIG_MODE_FREE, 0.0, 0, "")
        self.qtgui_freq_sink_x_0_1_0.enable_autoscale(False)
        self.qtgui_freq_sink_x_0_1_0.enable_grid(False)
        self.qtgui_freq_sink_x_0_1_0.set_fft_average(1.0)
        self.qtgui_freq_sink_x_0_1_0.enable_axis_labels(True)
        self.qtgui_freq_sink_x_0_1_0.enable_control_panel(False)
        self.qtgui_freq_sink_x_0_1_0.set_fft_window_normalized(False)



        labels = ['', '', '', '', '',
            '', '', '', '', '']
        widths = [1, 1, 1, 1, 1,
            1, 1, 1, 1, 1]
        colors = ["blue", "red", "green", "black", "cyan",
            "magenta", "yellow", "dark red", "dark green", "dark blue"]
        alphas = [1.0, 1.0, 1.0, 1.0, 1.0,
            1.0, 1.0, 1.0, 1.0, 1.0]

        for i in range(1):
            if len(labels[i]) == 0:
                self.qtgui_freq_sink_x_0_1_0.set_line_label(i, "Data {0}".format(i))
            else:
                self.qtgui_freq_sink_x_0_1_0.set_line_label(i, labels[i])
            self.qtgui_freq_sink_x_0_1_0.set_line_width(i, widths[i])
            self.qtgui_freq_sink_x_0_1_0.set_line_color(i, colors[i])
            self.qtgui_freq_sink_x_0_1_0.set_line_alpha(i, alphas[i])

        self._qtgui_freq_sink_x_0_1_0_win = sip.wrapinstance(self.qtgui_freq_sink_x_0_1_0.qwidget(), Qt.QWidget)
        self.top_layout.addWidget(self._qtgui_freq_sink_x_0_1_0_win)
        self.blocks_throttle2_0 = blocks.throttle( gr.sizeof_gr_complex*1, samp_rate, True, 0 if "auto" == "auto" else max( int(float(0.1) * samp_rate) if "auto" == "time" else int(0.1), 1) )
        self.blocks_multiply_xx_0_0 = blocks.multiply_vcc(1)
        self.blocks_multiply_xx_0 = blocks.multiply_vcc(1)
        self.blocks_multiply_conjugate_cc_0_0 = blocks.multiply_conjugate_cc(1)
        self.blocks_multiply_conjugate_cc_0 = blocks.multiply_conjugate_cc(1)
        self.blocks_msgpair_to_var_hop_offset = blocks.msg_pair_to_var(self.set_hop_offset)
        self.blocks_message_debug_0 = blocks.message_debug(True, gr.log_levels.info)
        self.blocks_file_sink_0_0 = blocks.file_sink(gr.sizeof_gr_complex*1, '/home/mess1ah/zz_ble_cs_gnuradio/1to1_rfhop/data_initiator_rx_from_reflector2', False)
        self.blocks_file_sink_0_0.set_unbuffered(False)
        self.blocks_file_sink_0 = blocks.file_sink(gr.sizeof_gr_complex*1, '/home/mess1ah/zz_ble_cs_gnuradio/1to1_rfhop/data_reflector_rx_from_initiator2', False)
        self.blocks_file_sink_0.set_unbuffered(False)
        self.analog_sig_source_x_0_1_0 = analog.sig_source_c(100e6, analog.GR_COS_WAVE, hop_offset, 1, 0, 0)
        self.analog_sig_source_x_0_1 = analog.sig_source_c(samp_rate, analog.GR_COS_WAVE, tone_freq, 1, 0, 0)


        ##################################################
        # Connections
        ##################################################
        self.msg_connect((self.usrp_ble_interact_center_rfhop_0, 'freq_ctrl'), (self.blocks_message_debug_0, 'print'))
        self.msg_connect((self.usrp_ble_interact_center_rfhop_0, 'freq_ctrl'), (self.blocks_msgpair_to_var_hop_offset, 'inpair'))
        self.msg_connect((self.usrp_ble_interact_center_rfhop_0, 'capture_ctrl'), (self.usrp_ble_capture_gate_0, 'command'))
        self.msg_connect((self.usrp_ble_interact_center_rfhop_0, 'capture_ctrl'), (self.usrp_ble_capture_gate_0_0, 'command'))
        self.msg_connect((self.usrp_ble_interact_center_rfhop_0, 'send1_ctrl'), (self.usrp_ble_data_send_0, 'command'))
        self.msg_connect((self.usrp_ble_interact_center_rfhop_0, 'send2_ctrl'), (self.usrp_ble_data_send_0_0, 'command'))
        self.connect((self.analog_sig_source_x_0_1, 0), (self.blocks_throttle2_0, 0))
        self.connect((self.analog_sig_source_x_0_1_0, 0), (self.qtgui_freq_sink_x_0_1_0, 0))
        self.connect((self.blocks_multiply_conjugate_cc_0, 0), (self.qtgui_time_sink_x_0, 0))
        self.connect((self.blocks_multiply_conjugate_cc_0, 0), (self.usrp_ble_capture_gate_0, 0))
        self.connect((self.blocks_multiply_conjugate_cc_0_0, 0), (self.usrp_ble_capture_gate_0_0, 0))
        self.connect((self.blocks_multiply_xx_0, 0), (self.usrp_ble_channel_phase_0, 0))
        self.connect((self.blocks_multiply_xx_0_0, 0), (self.usrp_ble_channel_phase_0_0, 0))
        self.connect((self.blocks_throttle2_0, 0), (self.blocks_multiply_conjugate_cc_0, 1))
        self.connect((self.blocks_throttle2_0, 0), (self.blocks_multiply_conjugate_cc_0_0, 1))
        self.connect((self.blocks_throttle2_0, 0), (self.blocks_multiply_xx_0, 0))
        self.connect((self.blocks_throttle2_0, 0), (self.blocks_multiply_xx_0_0, 0))
        self.connect((self.usrp_ble_capture_gate_0, 0), (self.blocks_file_sink_0, 0))
        self.connect((self.usrp_ble_capture_gate_0_0, 0), (self.blocks_file_sink_0_0, 0))
        self.connect((self.usrp_ble_channel_phase_0, 0), (self.blocks_multiply_conjugate_cc_0, 0))
        self.connect((self.usrp_ble_channel_phase_0_0, 0), (self.blocks_multiply_conjugate_cc_0_0, 0))
        self.connect((self.usrp_ble_data_send_0, 0), (self.blocks_multiply_xx_0, 1))
        self.connect((self.usrp_ble_data_send_0, 0), (self.usrp_ble_interact_center_rfhop_0, 0))
        self.connect((self.usrp_ble_data_send_0_0, 0), (self.blocks_multiply_xx_0_0, 1))


    def closeEvent(self, event):
        self.settings = Qt.QSettings("GNU Radio", "self_rfhop")
        self.settings.setValue("geometry", self.saveGeometry())
        self.stop()
        self.wait()

        event.accept()

    def get_tone_freq(self):
        return self.tone_freq

    def set_tone_freq(self, tone_freq):
        self.tone_freq = tone_freq
        self.set_centetr_fre(2.44e9 + self.hop_offset - self.tone_freq)
        self.analog_sig_source_x_0_1.set_frequency(self.tone_freq)
        self.usrp_ble_channel_phase_0.set_center_freq_hz((self.centetr_fre+self.tone_freq))
        self.usrp_ble_channel_phase_0_0.set_center_freq_hz((self.centetr_fre+self.tone_freq))

    def get_hop_offset(self):
        return self.hop_offset

    def set_hop_offset(self, hop_offset):
        self.hop_offset = hop_offset
        self.set_centetr_fre(2.44e9 + self.hop_offset - self.tone_freq)
        self.analog_sig_source_x_0_1_0.set_frequency(self.hop_offset)

    def get_stop_button(self):
        return self.stop_button

    def set_stop_button(self, stop_button):
        self.stop_button = stop_button
        self.usrp_ble_interact_center_rfhop_0.set_stop_btn(self.stop_button)

    def get_start_button(self):
        return self.start_button

    def set_start_button(self, start_button):
        self.start_button = start_button
        self.usrp_ble_interact_center_rfhop_0.set_start_btn(self.start_button)

    def get_send_gain(self):
        return self.send_gain

    def set_send_gain(self, send_gain):
        self.send_gain = send_gain

    def get_samp_rate_0(self):
        return self.samp_rate_0

    def set_samp_rate_0(self, samp_rate_0):
        self.samp_rate_0 = samp_rate_0

    def get_samp_rate(self):
        return self.samp_rate

    def set_samp_rate(self, samp_rate):
        self.samp_rate = samp_rate
        self.analog_sig_source_x_0_1.set_sampling_freq(self.samp_rate)
        self.blocks_throttle2_0.set_sample_rate(self.samp_rate)
        self.qtgui_time_sink_x_0.set_samp_rate(self.samp_rate)
        self.usrp_ble_data_send_0.set_sample_rate(self.samp_rate)
        self.usrp_ble_data_send_0_0.set_sample_rate(self.samp_rate)

    def get_recv_gain(self):
        return self.recv_gain

    def set_recv_gain(self, recv_gain):
        self.recv_gain = recv_gain

    def get_distance_m(self):
        return self.distance_m

    def set_distance_m(self, distance_m):
        self.distance_m = distance_m
        self.usrp_ble_channel_phase_0.set_distance_m(self.distance_m)
        self.usrp_ble_channel_phase_0_0.set_distance_m(self.distance_m)

    def get_display_decim(self):
        return self.display_decim

    def set_display_decim(self, display_decim):
        self.display_decim = display_decim

    def get_centetr_fre(self):
        return self.centetr_fre

    def set_centetr_fre(self, centetr_fre):
        self.centetr_fre = centetr_fre
        self.usrp_ble_channel_phase_0.set_center_freq_hz((self.centetr_fre+self.tone_freq))
        self.usrp_ble_channel_phase_0_0.set_center_freq_hz((self.centetr_fre+self.tone_freq))




def main(top_block_cls=self_rfhop, options=None):

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
