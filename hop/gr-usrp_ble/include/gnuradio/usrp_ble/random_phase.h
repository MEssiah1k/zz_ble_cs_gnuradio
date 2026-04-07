/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#ifndef INCLUDED_USRP_BLE_RANDOM_PHASE_H
#define INCLUDED_USRP_BLE_RANDOM_PHASE_H

#include <gnuradio/sync_block.h>
#include <gnuradio/usrp_ble/api.h>

namespace gr {
namespace usrp_ble {

/*!
 * \brief 频率变化时随机更新相位，用于模拟设备本振初相位。
 * \ingroup usrp_ble
 */
class USRP_BLE_API random_phase : virtual public gr::sync_block
{
public:
    typedef std::shared_ptr<random_phase> sptr;

    /*!
     * \brief Return a shared_ptr to a new instance of usrp_ble::random_phase.
     *
     * To avoid accidental use of raw pointers, usrp_ble::random_phase's
     * constructor is in a private implementation
     * class. usrp_ble::random_phase::make is the public interface for
     * creating new instances.
     */
    static sptr make(int seed, float amplitude);

    virtual void set_amplitude(float amplitude) = 0;
};

} // namespace usrp_ble
} // namespace gr

#endif /* INCLUDED_USRP_BLE_RANDOM_PHASE_H */
