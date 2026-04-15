/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "tx_burst_gate_impl.h"
#include <algorithm>
#include <cstring>
#include <gnuradio/io_signature.h>

namespace gr {
namespace usrp_ble {

using sample_type = gr_complex;

tx_burst_gate::sptr tx_burst_gate::make(int num_channels, int burst_len, bool add_uhd_tags)
{
    return gnuradio::make_block_sptr<tx_burst_gate_impl>(
        std::max(1, num_channels), burst_len, add_uhd_tags);
}

tx_burst_gate_impl::tx_burst_gate_impl(int num_channels, int burst_len, bool add_uhd_tags)
    : gr::block("tx_burst_gate",
                gr::io_signature::make(num_channels, num_channels, sizeof(sample_type)),
                gr::io_signature::make(num_channels, num_channels, sizeof(sample_type))),
      d_num_channels(std::max(1, num_channels)),
      d_burst_len(std::max(1, burst_len)),
      d_add_uhd_tags(add_uhd_tags),
      d_active(false),
      d_need_sob(false),
      d_remaining(0)
{
    message_port_register_in(pmt::mp("command"));
    set_msg_handler(pmt::mp("command"),
                    [this](pmt::pmt_t msg) { this->handle_msg(msg); });
}

tx_burst_gate_impl::~tx_burst_gate_impl() {}

void tx_burst_gate_impl::set_burst_len(int burst_len)
{
    std::lock_guard<std::mutex> lock(d_mutex);
    d_burst_len = std::max(1, burst_len);
}

void tx_burst_gate_impl::handle_msg(pmt::pmt_t msg)
{
    if (!pmt::is_symbol(msg)) {
        return;
    }

    const std::string cmd = pmt::symbol_to_string(msg);
    std::lock_guard<std::mutex> lock(d_mutex);
    if (cmd == "data_start") {
        if (d_active && d_remaining > 0) {
            return;
        }
        d_active = true;
        d_need_sob = true;
        d_remaining = d_burst_len;
    }
}

void tx_burst_gate_impl::forecast(int noutput_items, gr_vector_int& ninput_items_required)
{
    std::fill(ninput_items_required.begin(), ninput_items_required.end(), noutput_items);
}

int tx_burst_gate_impl::general_work(int noutput_items,
                                     gr_vector_int& ninput_items,
                                     gr_vector_const_void_star& input_items,
                                     gr_vector_void_star& output_items)
{
    const int available = *std::min_element(ninput_items.begin(), ninput_items.end());
    if (available <= 0) {
        return 0;
    }

    bool active = false;
    bool need_sob = false;
    int remaining = 0;
    {
        std::lock_guard<std::mutex> lock(d_mutex);
        active = d_active;
        need_sob = d_need_sob;
        remaining = d_remaining;
    }

    if (!active || remaining <= 0) {
        // 空闲期不消费、不输出。让上游缓冲区自然填满并停住，
        // 避免在未发送 burst 时持续空转生成/丢弃样本。
        return 0;
    }

    const int nproduce = std::min({ noutput_items, available, remaining });
    for (int ch = 0; ch < d_num_channels; ++ch) {
        const auto* in = static_cast<const sample_type*>(input_items[ch]);
        auto* out = static_cast<sample_type*>(output_items[ch]);
        std::memcpy(out, in, nproduce * sizeof(sample_type));
    }

    const uint64_t abs_out = nitems_written(0);
    if (d_add_uhd_tags && need_sob) {
        add_item_tag(0, abs_out, pmt::intern("tx_sob"), pmt::PMT_T);
    }
    if (d_add_uhd_tags && nproduce == remaining) {
        add_item_tag(0, abs_out + nproduce - 1, pmt::intern("tx_eob"), pmt::PMT_T);
    }

    consume_each(nproduce);

    {
        std::lock_guard<std::mutex> lock(d_mutex);
        d_need_sob = false;
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
