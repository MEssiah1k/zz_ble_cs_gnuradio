/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "channel_phase_impl.h"
#include <gnuradio/io_signature.h>
#include <pmt/pmt.h>
#include <cmath>

namespace gr {
namespace usrp_ble {

using input_type = gr_complex;
using output_type = gr_complex;
static constexpr double PI = 3.14159265358979323846;

channel_phase::sptr
channel_phase::make(double center_freq_hz, double distance_m, float amplitude)
{
    return gnuradio::make_block_sptr<channel_phase_impl>(
        center_freq_hz, distance_m, amplitude);
}


channel_phase_impl::channel_phase_impl(double center_freq_hz,
                                       double distance_m,
                                       float amplitude)
    : gr::sync_block("channel_phase",
                     gr::io_signature::make(
                         1 /* min inputs */, 1 /* max inputs */, sizeof(input_type)),
                     gr::io_signature::make(
                         1 /* min outputs */, 1 /*max outputs */, sizeof(output_type))),
      d_center_freq_hz(center_freq_hz),
      d_distance_m(distance_m),
      d_amplitude(amplitude),
      d_channel_rot(gr_complex(1.0f, 0.0f))
{
    std::lock_guard<std::mutex> lock(d_mutex);
    update_channel_rot_locked();
}

channel_phase_impl::~channel_phase_impl() {}

void channel_phase_impl::set_center_freq_hz(double center_freq_hz)
{
    std::lock_guard<std::mutex> lock(d_mutex);
    d_center_freq_hz = center_freq_hz;
    update_channel_rot_locked();
}

void channel_phase_impl::set_distance_m(double distance_m)
{
    std::lock_guard<std::mutex> lock(d_mutex);
    d_distance_m = distance_m;
    update_channel_rot_locked();
}

void channel_phase_impl::set_amplitude(float amplitude)
{
    std::lock_guard<std::mutex> lock(d_mutex);
    d_amplitude = amplitude;
    update_channel_rot_locked();
}

void channel_phase_impl::update_channel_rot_locked()
{
    const double tau = d_distance_m / SPEED_OF_LIGHT;
    const double phase = -2.0 * PI * d_center_freq_hz * tau;
    d_channel_rot = gr_complex(
        static_cast<float>(d_amplitude * std::cos(phase)),
        static_cast<float>(d_amplitude * std::sin(phase)));
}

int channel_phase_impl::work(int noutput_items,
                             gr_vector_const_void_star& input_items,
                             gr_vector_void_star& output_items)
{
    auto in = static_cast<const input_type*>(input_items[0]);
    auto out = static_cast<output_type*>(output_items[0]);
    gr_complex channel_rot;

    {
        std::lock_guard<std::mutex> lock(d_mutex);
        channel_rot = d_channel_rot;
    }

    for (int i = 0; i < noutput_items; ++i) {
        out[i] = in[i] * channel_rot;
    }

    return noutput_items;
}

} /* namespace usrp_ble */
} /* namespace gr */
