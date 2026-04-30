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
    int d_group_index;
    int d_output_groups;
    int d_active_group_index;
    bool d_active;
    std::mutex d_mutex;

    void handle_msg(pmt::pmt_t msg);
    int output_port_index(int channel, int group_index) const;
    bool routed_mode() const;

public:
    capture_gate_impl(int num_channels, int group_index, int output_groups);
    ~capture_gate_impl() override;
    void set_group_index(int group_index) override;

    void forecast(int noutput_items, gr_vector_int& ninput_items_required) override;
    int general_work(int noutput_items,
                     gr_vector_int& ninput_items,
                     gr_vector_const_void_star& input_items,
                     gr_vector_void_star& output_items) override;
};

} // namespace usrp_ble
} // namespace gr

#endif /* INCLUDED_USRP_BLE_CAPTURE_GATE_IMPL_H */
