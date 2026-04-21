/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rx_burst_gate_impl.h"
#include <algorithm>
#include <cstring>
#include <gnuradio/io_signature.h>

namespace gr {
namespace usrp_ble {

using sample_type = gr_complex;

rx_burst_gate::sptr rx_burst_gate::make(int num_channels, int burst_len, int skip_len)
{
    return gnuradio::make_block_sptr<rx_burst_gate_impl>(
        std::max(1, num_channels), burst_len, skip_len);
}

rx_burst_gate_impl::rx_burst_gate_impl(int num_channels, int burst_len, int skip_len)
    : gr::block("rx_burst_gate",
                gr::io_signature::make(num_channels, num_channels, sizeof(sample_type)),
                gr::io_signature::make(num_channels, num_channels, sizeof(sample_type))),
      d_num_channels(std::max(1, num_channels)),
      d_burst_len(std::max(1, burst_len)),
      d_skip_len(std::max(0, skip_len)),
      d_remaining(0),
      d_skip_remaining(0),
      d_active(false)
{
    message_port_register_in(pmt::mp("command"));
    set_msg_handler(pmt::mp("command"),
                    [this](pmt::pmt_t msg) { this->handle_msg(msg); });
}

rx_burst_gate_impl::~rx_burst_gate_impl() {}

void rx_burst_gate_impl::set_burst_len(int burst_len)
{
    std::lock_guard<std::mutex> lock(d_mutex);
    d_burst_len = std::max(1, burst_len);
}

void rx_burst_gate_impl::set_skip_len(int skip_len)
{
    std::lock_guard<std::mutex> lock(d_mutex);
    d_skip_len = std::max(0, skip_len);
}

void rx_burst_gate_impl::handle_msg(pmt::pmt_t msg)
{
    bool should_start = false;
    bool should_stop = false;

    if (pmt::is_symbol(msg)) {
        const std::string cmd = pmt::symbol_to_string(msg);
        should_start = (cmd == "store_start" || cmd == "data_start");
        should_stop = (cmd == "store_stop" || cmd == "data_stop");
    } else if (pmt::is_dict(msg)) {
        const pmt::pmt_t key = pmt::intern("cmd");
        if (pmt::dict_has_key(msg, key)) {
            const pmt::pmt_t value = pmt::dict_ref(msg, key, pmt::PMT_NIL);
            if (pmt::is_symbol(value)) {
                const std::string cmd = pmt::symbol_to_string(value);
                should_start = (cmd == "store_start" || cmd == "data_start");
                should_stop = (cmd == "store_stop" || cmd == "data_stop");
            }
        }
    }

    std::lock_guard<std::mutex> lock(d_mutex);
    if (should_start) {
        d_active = true;
        d_skip_remaining = d_skip_len;
        d_remaining = d_burst_len;
    } else if (should_stop) {
        d_active = false;
        d_skip_remaining = 0;
        d_remaining = 0;
    }
}

void rx_burst_gate_impl::forecast(int noutput_items, gr_vector_int& ninput_items_required)
{
    std::fill(ninput_items_required.begin(), ninput_items_required.end(), noutput_items);
}

int rx_burst_gate_impl::general_work(int noutput_items,
                                     gr_vector_int& ninput_items,
                                     gr_vector_const_void_star& input_items,
                                     gr_vector_void_star& output_items)
{
    const int available = *std::min_element(ninput_items.begin(), ninput_items.end());
    if (available <= 0) {
        return 0;
    }

    bool active = false;
    int skip_remaining = 0;
    int remaining = 0;
    {
        std::lock_guard<std::mutex> lock(d_mutex);
        active = d_active;
        skip_remaining = d_skip_remaining;
        remaining = d_remaining;
    }

    if (!active || remaining <= 0) {
        consume_each(available);
        return 0;
    }

    if (skip_remaining > 0) {
        const int nskip = std::min(available, skip_remaining);
        consume_each(nskip);
        {
            std::lock_guard<std::mutex> lock(d_mutex);
            d_skip_remaining -= nskip;
        }
        return 0;
    }

    const int nproduce = std::min({ noutput_items, available, remaining });
    for (int ch = 0; ch < d_num_channels; ++ch) {
        const auto* in = static_cast<const sample_type*>(input_items[ch]);
        auto* out = static_cast<sample_type*>(output_items[ch]);
        std::memcpy(out, in, nproduce * sizeof(sample_type));
    }

    consume_each(nproduce);

    {
        std::lock_guard<std::mutex> lock(d_mutex);
        d_remaining -= nproduce;
        if (d_remaining <= 0) {
            d_active = false;
            d_remaining = 0;
        }
    }

    return nproduce;
}

} // namespace usrp_ble
} // namespace gr
