/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#ifndef INCLUDED_USRP_BLE_CAPTURE_GATE_H
#define INCLUDED_USRP_BLE_CAPTURE_GATE_H

#include <gnuradio/block.h>
#include <gnuradio/usrp_ble/api.h>

namespace gr {
namespace usrp_ble {

class USRP_BLE_API capture_gate : virtual public gr::block
{
public:
    typedef std::shared_ptr<capture_gate> sptr;

    static sptr make(int num_channels);
};

} // namespace usrp_ble
} // namespace gr

#endif /* INCLUDED_USRP_BLE_CAPTURE_GATE_H */
