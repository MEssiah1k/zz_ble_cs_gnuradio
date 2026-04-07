/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#ifndef INCLUDED_USRP_BLE_CHANNEL_PHASE_H
#define INCLUDED_USRP_BLE_CHANNEL_PHASE_H

#include <gnuradio/sync_block.h>
#include <gnuradio/usrp_ble/api.h>

namespace gr {
namespace usrp_ble {

/*!
 * \brief 根据当前频点消息，为输入复信号注入固定距离对应的传播相位。
 * \ingroup usrp_ble
 */
class USRP_BLE_API channel_phase : virtual public gr::sync_block
{
public:
    typedef std::shared_ptr<channel_phase> sptr;

    /*!
     * \brief Return a shared_ptr to a new instance of usrp_ble::channel_phase.
     *
     * To avoid accidental use of raw pointers, usrp_ble::channel_phase's
     * constructor is in a private implementation
     * class. usrp_ble::channel_phase::make is the public interface for
     * creating new instances.
     */
    static sptr make(double center_freq_hz, double distance_m, float amplitude);

    virtual void set_center_freq_hz(double center_freq_hz) = 0;
    virtual void set_distance_m(double distance_m) = 0;
    virtual void set_amplitude(float amplitude) = 0;
};

} // namespace usrp_ble
} // namespace gr

#endif /* INCLUDED_USRP_BLE_CHANNEL_PHASE_H */
