/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#ifndef INCLUDED_USRP_BLE_RX_BURST_GATE_H
#define INCLUDED_USRP_BLE_RX_BURST_GATE_H

#include <gnuradio/block.h>
#include <gnuradio/usrp_ble/api.h>

namespace gr {
namespace usrp_ble {

class USRP_BLE_API rx_burst_gate : virtual public gr::block
{
public:
    typedef std::shared_ptr<rx_burst_gate> sptr;

    static sptr make(int num_channels, int burst_len, int skip_len = 0);

    virtual void set_burst_len(int burst_len) = 0;
    virtual void set_skip_len(int skip_len) = 0;
};

} // namespace usrp_ble
} // namespace gr

#endif /* INCLUDED_USRP_BLE_RX_BURST_GATE_H */
