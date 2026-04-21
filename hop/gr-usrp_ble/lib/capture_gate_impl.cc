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

capture_gate::sptr capture_gate::make(int num_channels)
{
    return gnuradio::make_block_sptr<capture_gate_impl>(std::max(1, num_channels));
}

capture_gate_impl::capture_gate_impl(int num_channels)
    : gr::block("capture_gate",
                gr::io_signature::make(num_channels, num_channels, sizeof(sample_type)),
                gr::io_signature::make(num_channels, num_channels, sizeof(sample_type))),
      d_num_channels(std::max(1, num_channels)),
      d_active(false)
{
    message_port_register_in(pmt::mp("command"));
    set_msg_handler(pmt::mp("command"),
                    [this](pmt::pmt_t msg) { this->handle_msg(msg); });
}

capture_gate_impl::~capture_gate_impl() {}

void capture_gate_impl::handle_msg(pmt::pmt_t msg)
{
    std::string cmd;
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
    }

    std::lock_guard<std::mutex> lock(d_mutex);
    if (cmd == "capture_start") {
        d_active = true;
    } else if (cmd == "capture_stop") {
        d_active = false;
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
    {
        std::lock_guard<std::mutex> lock(d_mutex);
        active = d_active;
    }

    if (!active) {
        consume_each(available);
        return 0;
    }

    const int nproduce = std::min(noutput_items, available);
    for (int ch = 0; ch < d_num_channels; ++ch) {
        const auto* in = static_cast<const sample_type*>(input_items[ch]);
        auto* out = static_cast<sample_type*>(output_items[ch]);
        std::memcpy(out, in, nproduce * sizeof(sample_type));
    }

    consume_each(nproduce);
    return nproduce;
}

} // namespace usrp_ble
} // namespace gr
