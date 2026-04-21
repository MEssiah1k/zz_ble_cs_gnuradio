/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#ifndef INCLUDED_USRP_BLE_CAPTURE_GATE_IMPL_H
#define INCLUDED_USRP_BLE_CAPTURE_GATE_IMPL_H

#include <mutex>

#include <gnuradio/usrp_ble/capture_gate.h>
#include <pmt/pmt.h>

namespace gr {
namespace usrp_ble {

class capture_gate_impl : public capture_gate
{
private:
    int d_num_channels;
    bool d_active;
    std::mutex d_mutex;

    void handle_msg(pmt::pmt_t msg);

public:
    capture_gate_impl(int num_channels);
    ~capture_gate_impl() override;

    void forecast(int noutput_items, gr_vector_int& ninput_items_required) override;
    int general_work(int noutput_items,
                     gr_vector_int& ninput_items,
                     gr_vector_const_void_star& input_items,
                     gr_vector_void_star& output_items) override;
};

} // namespace usrp_ble
} // namespace gr

#endif /* INCLUDED_USRP_BLE_CAPTURE_GATE_IMPL_H */
