/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "interact_center_rfhop_impl.h"
#include <gnuradio/io_signature.h>
#include <algorithm>

namespace gr {
namespace usrp_ble {

using input_type = gr_complex;

interact_center_rfhop::sptr interact_center_rfhop::make(int sample_rate,
                                                        bool start_btn,
                                                        bool stop_btn,
                                                        float wait_time_ms,
                                                        float settle_time_ms,
                                                        int repeat_total)
{
    return gnuradio::make_block_sptr<interact_center_rfhop_impl>(
        sample_rate, start_btn, stop_btn, wait_time_ms, settle_time_ms, repeat_total);
}

interact_center_rfhop_impl::interact_center_rfhop_impl(int sample_rate,
                                                       bool start_btn,
                                                       bool stop_btn,
                                                       float wait_time_ms,
                                                       float settle_time_ms,
                                                       int repeat_total)
    : gr::sync_block("interact_center_rfhop",
                     gr::io_signature::make(1, 1, sizeof(input_type)),
                     gr::io_signature::make(0, 0, 0)),
      d_sample_rate(sample_rate),
      d_start_btn(start_btn),
      d_stop_btn(stop_btn),
      d_wait_time_ms(wait_time_ms),
      d_settle_time_ms(settle_time_ms),
      d_repeat_total(std::max(1, repeat_total)),
      d_repeat_index(0),
      d_is_running(false),
      d_use_msg_clock(false),
      d_phase_samples(0),
      d_settle_samples(0),
      d_wait_counter(0),
      d_state(state_t::idle),
      d_current_freq(-40000000.0)
{
    message_port_register_out(pmt::mp("send1_ctrl"));
    message_port_register_out(pmt::mp("send2_ctrl"));
    message_port_register_out(pmt::mp("store1_ctrl"));
    message_port_register_out(pmt::mp("store2_ctrl"));
    message_port_register_out(pmt::mp("capture_ctrl"));
    message_port_register_out(pmt::mp("freq_ctrl"));
    message_port_register_out(pmt::mp("phase_ctrl"));

    message_port_register_in(pmt::mp("clock"));
    set_msg_handler(pmt::mp("clock"),
                    [this](pmt::pmt_t msg) { this->handle_clock_msg(msg); });

    refresh_sample_counts();
}

interact_center_rfhop_impl::~interact_center_rfhop_impl() {}

void interact_center_rfhop_impl::refresh_sample_counts()
{
    d_phase_samples = static_cast<size_t>(
        std::max(0.0f, d_wait_time_ms) * static_cast<float>(d_sample_rate) / 1000.0f);
    d_settle_samples = static_cast<size_t>(
        std::max(0.0f, d_settle_time_ms) * static_cast<float>(d_sample_rate) / 1000.0f);
}

void interact_center_rfhop_impl::set_start_btn(bool start_btn)
{
    const bool old_val = d_start_btn;
    d_start_btn = start_btn;
    if (!old_val && d_start_btn && !d_is_running) {
        d_is_running = true;
        d_current_freq = -40000000.0;
        d_repeat_index = 0;
        send_all_stop();
        message_port_pub(pmt::mp("capture_ctrl"), pmt::intern("capture_start"));
        enter_settle_for_current_freq();
    }
}

void interact_center_rfhop_impl::set_stop_btn(bool stop_btn)
{
    const bool old_val = d_stop_btn;
    d_stop_btn = stop_btn;
    if (!old_val && d_stop_btn) {
        d_is_running = false;
        d_state = state_t::idle;
        d_wait_counter = 0;
        send_all_stop();
        message_port_pub(pmt::mp("capture_ctrl"), pmt::intern("capture_stop"));
    }
}

void interact_center_rfhop_impl::set_wait_time_ms(float wait_time_ms)
{
    d_wait_time_ms = wait_time_ms;
    refresh_sample_counts();
}

void interact_center_rfhop_impl::set_settle_time_ms(float settle_time_ms)
{
    d_settle_time_ms = settle_time_ms;
    refresh_sample_counts();
}

void interact_center_rfhop_impl::set_use_msg_clock(bool use_msg_clock)
{
    d_use_msg_clock = use_msg_clock;
}

void interact_center_rfhop_impl::handle_clock_msg(pmt::pmt_t msg)
{
    if (!d_use_msg_clock) {
        return;
    }

    pmt::pmt_t value = pmt::is_pair(msg) ? pmt::cdr(msg) : msg;
    if (!pmt::is_integer(value)) {
        return;
    }

    const long nitems = pmt::to_long(value);
    if (nitems > 0) {
        process_state_machine(static_cast<int>(nitems));
    }
}

void interact_center_rfhop_impl::enter_settle_for_current_freq()
{
    send_all_stop();
    send_freq_command();
    d_state = state_t::settle;
    d_wait_counter = 0;
}

void interact_center_rfhop_impl::send_phase1_start()
{
    message_port_pub(pmt::mp("send2_ctrl"), pmt::intern("data_stop"));
    message_port_pub(pmt::mp("store2_ctrl"), pmt::intern("store_stop"));
    message_port_pub(pmt::mp("phase_ctrl"), pmt::cons(pmt::PMT_NIL, pmt::from_long(0)));
    message_port_pub(pmt::mp("store1_ctrl"), make_store_start_msg());
    message_port_pub(pmt::mp("send1_ctrl"), pmt::intern("data_start"));
}

void interact_center_rfhop_impl::send_phase2_start()
{
    message_port_pub(pmt::mp("store1_ctrl"), pmt::intern("store_stop"));
    message_port_pub(pmt::mp("send1_ctrl"), pmt::intern("data_stop"));
    message_port_pub(pmt::mp("phase_ctrl"), pmt::cons(pmt::PMT_NIL, pmt::from_long(1)));
    message_port_pub(pmt::mp("store2_ctrl"), make_store_start_msg());
    message_port_pub(pmt::mp("send2_ctrl"), pmt::intern("data_start"));
}

void interact_center_rfhop_impl::send_all_stop()
{
    message_port_pub(pmt::mp("send1_ctrl"), pmt::intern("data_stop"));
    message_port_pub(pmt::mp("send2_ctrl"), pmt::intern("data_stop"));
    message_port_pub(pmt::mp("store1_ctrl"), pmt::intern("store_stop"));
    message_port_pub(pmt::mp("store2_ctrl"), pmt::intern("store_stop"));
}

void interact_center_rfhop_impl::send_freq_command()
{
    message_port_pub(pmt::mp("freq_ctrl"),
                     pmt::cons(pmt::intern("freq"), pmt::from_double(d_current_freq)));
}

pmt::pmt_t interact_center_rfhop_impl::make_store_start_msg() const
{
    pmt::pmt_t msg = pmt::make_dict();
    msg = pmt::dict_add(msg, pmt::intern("cmd"), pmt::intern("store_start"));
    msg = pmt::dict_add(msg, pmt::intern("freq_index"), pmt::from_long(current_freq_index()));
    msg = pmt::dict_add(msg, pmt::intern("repeat_index"), pmt::from_long(d_repeat_index));
    return msg;
}

int interact_center_rfhop_impl::current_freq_index() const
{
    return static_cast<int>((d_current_freq + 40000000.0) / 1000000.0 + 0.5);
}

void interact_center_rfhop_impl::process_state_machine(int nitems)
{
    if (!d_is_running) {
        return;
    }

    size_t items_processed = 0;
    while (items_processed < static_cast<size_t>(nitems) && d_is_running) {
        const size_t items_remaining = static_cast<size_t>(nitems) - items_processed;
        const size_t target = (d_state == state_t::settle) ? d_settle_samples : d_phase_samples;

        if (d_state == state_t::idle) {
            items_processed += items_remaining;
            continue;
        }

        if (target == 0 || d_wait_counter + items_remaining >= target) {
            const size_t needed = (target > d_wait_counter) ? (target - d_wait_counter) : 0;
            items_processed += std::min(needed, items_remaining);
            d_wait_counter = 0;

            if (d_state == state_t::settle) {
                d_state = state_t::phase1;
                send_phase1_start();
            } else if (d_state == state_t::phase1) {
                d_state = state_t::phase2;
                send_phase2_start();
            } else if (d_state == state_t::phase2) {
                if (d_repeat_index + 1 < d_repeat_total) {
                    d_repeat_index += 1;
                    d_state = state_t::phase1;
                    send_phase1_start();
                } else {
                    d_repeat_index = 0;
                    d_current_freq += 1000000.0;
                    if (d_current_freq > 40000000.0) {
                        d_is_running = false;
                        d_state = state_t::idle;
                        send_all_stop();
                        message_port_pub(pmt::mp("capture_ctrl"), pmt::intern("capture_stop"));
                    } else {
                        enter_settle_for_current_freq();
                    }
                }
            }
        } else {
            d_wait_counter += items_remaining;
            items_processed += items_remaining;
        }
    }
}

int interact_center_rfhop_impl::work(int noutput_items,
                                     gr_vector_const_void_star& input_items,
                                     gr_vector_void_star& output_items)
{
    (void)input_items;
    (void)output_items;

    if (!d_use_msg_clock) {
        process_state_machine(noutput_items);
    }

    return noutput_items;
}

} // namespace usrp_ble
} // namespace gr
