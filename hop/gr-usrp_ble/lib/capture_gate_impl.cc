/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "capture_gate_impl.h"

#include <algorithm>
#include <cstring>

#include <gnuradio/io_signature.h>

namespace gr {
namespace usrp_ble {

using sample_type = gr_complex;

capture_gate::sptr capture_gate::make(int num_channels, int group_index, int output_groups)
{
    return gnuradio::make_block_sptr<capture_gate_impl>(
        std::max(1, num_channels), std::max(0, group_index), std::max(1, output_groups));
}

capture_gate_impl::capture_gate_impl(int num_channels, int group_index, int output_groups)
    : gr::block("capture_gate",
                gr::io_signature::make(num_channels, num_channels, sizeof(sample_type)),
                gr::io_signature::make(num_channels * std::max(1, output_groups),
                                       num_channels * std::max(1, output_groups),
                                       sizeof(sample_type))),
      d_num_channels(std::max(1, num_channels)),
      d_group_index(std::max(0, group_index)),
      d_output_groups(std::max(1, output_groups)),
      d_active_group_index(std::max(0, group_index)),
      d_active(false)
{
    message_port_register_in(pmt::mp("command"));
    set_msg_handler(pmt::mp("command"),
                    [this](pmt::pmt_t msg) { this->handle_msg(msg); });
}

capture_gate_impl::~capture_gate_impl() {}

void capture_gate_impl::set_group_index(int group_index)
{
    std::lock_guard<std::mutex> lock(d_mutex);
    d_group_index = std::max(0, group_index);
    if (!routed_mode()) {
        d_active_group_index = d_group_index;
    }
}

bool capture_gate_impl::routed_mode() const
{
    return d_output_groups > 1;
}

int capture_gate_impl::output_port_index(int channel, int group_index) const
{
    return group_index * d_num_channels + channel;
}

void capture_gate_impl::handle_msg(pmt::pmt_t msg)
{
    std::string cmd;
    bool has_group_index = false;
    long group_index = 0;
    if (pmt::is_symbol(msg)) {
        cmd = pmt::symbol_to_string(msg);
    } else if (pmt::is_dict(msg)) {
        const pmt::pmt_t key = pmt::intern("cmd");
        if (pmt::dict_has_key(msg, key)) {
            const pmt::pmt_t value = pmt::dict_ref(msg, key, pmt::PMT_NIL);
            if (pmt::is_symbol(value)) {
                cmd = pmt::symbol_to_string(value);
            }
        }
        const pmt::pmt_t group_key = pmt::intern("group_index");
        const pmt::pmt_t capture_group_key = pmt::intern("capture_group_index");
        pmt::pmt_t group_value = pmt::PMT_NIL;
        if (pmt::dict_has_key(msg, group_key)) {
            group_value = pmt::dict_ref(msg, group_key, pmt::PMT_NIL);
        } else if (pmt::dict_has_key(msg, capture_group_key)) {
            group_value = pmt::dict_ref(msg, capture_group_key, pmt::PMT_NIL);
        }
        if (pmt::is_integer(group_value)) {
            has_group_index = true;
            group_index = pmt::to_long(group_value);
        }
    }

    std::lock_guard<std::mutex> lock(d_mutex);
    if (cmd == "capture_start") {
        const int requested_group = has_group_index ? std::max(0L, group_index) : d_group_index;
        if (routed_mode()) {
            if (requested_group >= d_output_groups) {
                d_active = false;
                return;
            }
            d_active_group_index = requested_group;
            d_active = true;
        } else {
            if (has_group_index && requested_group != d_group_index) {
                return;
            }
            d_active_group_index = d_group_index;
            d_active = true;
        }
    } else if (cmd == "capture_stop") {
        const int requested_group = has_group_index ? std::max(0L, group_index) : d_active_group_index;
        if (!routed_mode() || !has_group_index || requested_group == d_active_group_index) {
            d_active = false;
        }
    }
}

void capture_gate_impl::forecast(int noutput_items, gr_vector_int& ninput_items_required)
{
    std::fill(ninput_items_required.begin(), ninput_items_required.end(), noutput_items);
}

int capture_gate_impl::general_work(int noutput_items,
                                    gr_vector_int& ninput_items,
                                    gr_vector_const_void_star& input_items,
                                    gr_vector_void_star& output_items)
{
    const int available = *std::min_element(ninput_items.begin(), ninput_items.end());
    if (available <= 0) {
        return 0;
    }

    bool active = false;
    int active_group_index = 0;
    {
        std::lock_guard<std::mutex> lock(d_mutex);
        active = d_active;
        active_group_index = d_active_group_index;
    }

    if (!active) {
        consume_each(available);
        return 0;
    }

    const int nproduce = std::min(noutput_items, available);
    for (int ch = 0; ch < d_num_channels; ++ch) {
        const auto* in = static_cast<const sample_type*>(input_items[ch]);
        const int out_index = output_port_index(ch, routed_mode() ? active_group_index : 0);
        auto* out = static_cast<sample_type*>(output_items[out_index]);
        std::memcpy(out, in, nproduce * sizeof(sample_type));
        produce(out_index, nproduce);
    }

    consume_each(nproduce);
    return WORK_CALLED_PRODUCE;
}

} // namespace usrp_ble
} // namespace gr
