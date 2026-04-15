/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#ifndef INCLUDED_USRP_BLE_PHASE_CLOCK_SELECT_IMPL_H
#define INCLUDED_USRP_BLE_PHASE_CLOCK_SELECT_IMPL_H

#include <gnuradio/usrp_ble/phase_clock_select.h>
#include <pmt/pmt.h>
#include <mutex>

namespace gr {
namespace usrp_ble {

class phase_clock_select_impl : public phase_clock_select
{
private:
    int d_input_index;
    std::mutex d_mutex;

    int input_index() const;
    void handle_msg(pmt::pmt_t msg);

public:
    phase_clock_select_impl();
    ~phase_clock_select_impl() override;

    void forecast(int noutput_items, gr_vector_int& ninput_items_required) override;

    int general_work(int noutput_items,
                     gr_vector_int& ninput_items,
                     gr_vector_const_void_star& input_items,
                     gr_vector_void_star& output_items) override;
};

} // namespace usrp_ble
} // namespace gr

#endif /* INCLUDED_USRP_BLE_PHASE_CLOCK_SELECT_IMPL_H */
