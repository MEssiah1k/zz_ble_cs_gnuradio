/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#ifndef INCLUDED_USRP_BLE_PHASE_CLOCK_SELECT_H
#define INCLUDED_USRP_BLE_PHASE_CLOCK_SELECT_H

#include <gnuradio/block.h>
#include <gnuradio/usrp_ble/api.h>

namespace gr {
namespace usrp_ble {

class USRP_BLE_API phase_clock_select : virtual public gr::block
{
public:
    typedef std::shared_ptr<phase_clock_select> sptr;

    static sptr make();
};

} // namespace usrp_ble
} // namespace gr

#endif /* INCLUDED_USRP_BLE_PHASE_CLOCK_SELECT_H */
