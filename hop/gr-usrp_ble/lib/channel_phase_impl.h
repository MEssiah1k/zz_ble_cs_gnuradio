/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#ifndef INCLUDED_USRP_BLE_CHANNEL_PHASE_IMPL_H
#define INCLUDED_USRP_BLE_CHANNEL_PHASE_IMPL_H

#include <gnuradio/usrp_ble/channel_phase.h>
#include <pmt/pmt.h>
#include <mutex>

namespace gr {
namespace usrp_ble {

class channel_phase_impl : public channel_phase
{
private:
    static constexpr double SPEED_OF_LIGHT = 299792458.0;

    double d_base_center_freq_hz;
    double d_msg_freq_offset_hz;
    double d_distance_m;
    float d_amplitude;
    gr_complex d_channel_rot;
    std::mutex d_mutex;

    void update_channel_rot_locked();
    void handle_freq_msg(pmt::pmt_t msg);

public:
    channel_phase_impl(double center_freq_hz, double distance_m, float amplitude);
    ~channel_phase_impl();

    void set_center_freq_hz(double center_freq_hz) override;
    void set_distance_m(double distance_m) override;
    void set_amplitude(float amplitude) override;

    int work(int noutput_items,
             gr_vector_const_void_star& input_items,
             gr_vector_void_star& output_items);
};

} // namespace usrp_ble
} // namespace gr

#endif /* INCLUDED_USRP_BLE_CHANNEL_PHASE_IMPL_H */
