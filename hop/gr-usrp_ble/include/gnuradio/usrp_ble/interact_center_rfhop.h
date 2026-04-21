/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#ifndef INCLUDED_USRP_BLE_INTERACT_CENTER_RFHOP_H
#define INCLUDED_USRP_BLE_INTERACT_CENTER_RFHOP_H

#include <gnuradio/usrp_ble/api.h>
#include <gnuradio/sync_block.h>

namespace gr {
namespace usrp_ble {

class USRP_BLE_API interact_center_rfhop : virtual public gr::sync_block
{
public:
    typedef std::shared_ptr<interact_center_rfhop> sptr;

    static sptr make(int sample_rate,
                     bool start_btn,
                     bool stop_btn,
                     float wait_time_ms,
                     float settle_time_ms,
                     int repeat_total);

    virtual void set_start_btn(bool start_btn) = 0;
    virtual void set_stop_btn(bool stop_btn) = 0;
    virtual void set_wait_time_ms(float wait_time_ms) = 0;
    virtual void set_settle_time_ms(float settle_time_ms) = 0;
    virtual void set_use_msg_clock(bool use_msg_clock) = 0;
};

} // namespace usrp_ble
} // namespace gr

#endif /* INCLUDED_USRP_BLE_INTERACT_CENTER_RFHOP_H */
