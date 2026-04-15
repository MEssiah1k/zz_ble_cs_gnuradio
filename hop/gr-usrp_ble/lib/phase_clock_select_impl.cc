/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "phase_clock_select_impl.h"
#include <algorithm>
#include <cstring>
#include <gnuradio/io_signature.h>

namespace gr {
namespace usrp_ble {

using sample_type = gr_complex;

phase_clock_select::sptr phase_clock_select::make()
{
    return gnuradio::make_block_sptr<phase_clock_select_impl>();
}

phase_clock_select_impl::phase_clock_select_impl()
    : gr::block("phase_clock_select",
                gr::io_signature::make(2, 2, sizeof(sample_type)),
                gr::io_signature::make(0, 0, 0)),
      d_input_index(0)
{
    message_port_register_in(pmt::mp("iindex"));
    message_port_register_out(pmt::mp("clock"));
    set_msg_handler(pmt::mp("iindex"),
                    [this](pmt::pmt_t msg) { this->handle_msg(msg); });
}

phase_clock_select_impl::~phase_clock_select_impl() {}

int phase_clock_select_impl::input_index() const
{
    return std::clamp(d_input_index, 0, 1);
}

void phase_clock_select_impl::handle_msg(pmt::pmt_t msg)
{
    pmt::pmt_t value = pmt::is_pair(msg) ? pmt::cdr(msg) : msg;
    if (!pmt::is_integer(value)) {
        return;
    }

    std::lock_guard<std::mutex> lock(d_mutex);
    d_input_index = std::clamp(static_cast<int>(pmt::to_long(value)), 0, 1);
}

void phase_clock_select_impl::forecast(int noutput_items,
                                       gr_vector_int& ninput_items_required)
{
    std::fill(ninput_items_required.begin(), ninput_items_required.end(), 0);

    int idx;
    {
        std::lock_guard<std::mutex> lock(d_mutex);
        idx = input_index();
    }
    ninput_items_required[idx] = noutput_items;
}

int phase_clock_select_impl::general_work(int noutput_items,
                                          gr_vector_int& ninput_items,
                                          gr_vector_const_void_star& input_items,
                                          gr_vector_void_star& output_items)
{
    int idx;
    {
        std::lock_guard<std::mutex> lock(d_mutex);
        idx = input_index();
    }

    const int available = ninput_items[idx];
    if (available <= 0) {
        return 0;
    }

    const int nconsume = std::min(noutput_items, available);
    consume(idx, nconsume);
    message_port_pub(pmt::mp("clock"), pmt::from_long(nconsume));
    return nconsume;
}

} // namespace usrp_ble
} // namespace gr
