/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#ifndef INCLUDED_USRP_BLE_INTERACT_CENTER_RFHOP_IMPL_H
#define INCLUDED_USRP_BLE_INTERACT_CENTER_RFHOP_IMPL_H

#include <gnuradio/usrp_ble/interact_center_rfhop.h>
#include <pmt/pmt.h>

namespace gr {
namespace usrp_ble {

class interact_center_rfhop_impl : public interact_center_rfhop
{
private:
    enum class state_t {
        idle = 0,
        settle,
        phase1,
        phase2,
    };

    int d_sample_rate;
    bool d_start_btn;
    bool d_stop_btn;
    float d_wait_time_ms;
    float d_settle_time_ms;
    int d_repeat_total;
    int d_repeat_index;
    bool d_is_running;
    bool d_use_msg_clock;
    size_t d_phase_samples;
    size_t d_settle_samples;
    size_t d_wait_counter;
    state_t d_state;
    double d_current_freq;

    void refresh_sample_counts();
    void process_state_machine(int nitems);
    void handle_clock_msg(pmt::pmt_t msg);

    void enter_settle_for_current_freq();
    void send_phase1_start();
    void send_phase2_start();
    void send_all_stop();
    void send_freq_command();
    pmt::pmt_t make_store_start_msg() const;
    int current_freq_index() const;

public:
    interact_center_rfhop_impl(int sample_rate,
                               bool start_btn,
                               bool stop_btn,
                               float wait_time_ms,
                               float settle_time_ms,
                               int repeat_total);
    ~interact_center_rfhop_impl() override;

    void set_start_btn(bool start_btn) override;
    void set_stop_btn(bool stop_btn) override;
    void set_wait_time_ms(float wait_time_ms) override;
    void set_settle_time_ms(float settle_time_ms) override;
    void set_use_msg_clock(bool use_msg_clock) override;

    int work(int noutput_items,
             gr_vector_const_void_star& input_items,
             gr_vector_void_star& output_items) override;
};

} // namespace usrp_ble
} // namespace gr

#endif /* INCLUDED_USRP_BLE_INTERACT_CENTER_RFHOP_IMPL_H */
