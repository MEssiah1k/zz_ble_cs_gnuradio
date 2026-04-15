/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#ifndef INCLUDED_USRP_BLE_TX_BURST_GATE_IMPL_H
#define INCLUDED_USRP_BLE_TX_BURST_GATE_IMPL_H

#include <gnuradio/usrp_ble/tx_burst_gate.h>
#include <pmt/pmt.h>
#include <mutex>

namespace gr {
namespace usrp_ble {

class tx_burst_gate_impl : public tx_burst_gate
{
private:
    int d_num_channels;
    int d_burst_len;
    bool d_add_uhd_tags;
    bool d_active;
    bool d_need_sob;
    int d_remaining;
    std::mutex d_mutex;

    void handle_msg(pmt::pmt_t msg);

public:
    tx_burst_gate_impl(int num_channels, int burst_len, bool add_uhd_tags);
    ~tx_burst_gate_impl() override;

    void set_burst_len(int burst_len) override;

    void forecast(int noutput_items, gr_vector_int& ninput_items_required) override;

    int general_work(int noutput_items,
                     gr_vector_int& ninput_items,
                     gr_vector_const_void_star& input_items,
                     gr_vector_void_star& output_items) override;
};

} // namespace usrp_ble
} // namespace gr

#endif /* INCLUDED_USRP_BLE_TX_BURST_GATE_IMPL_H */
