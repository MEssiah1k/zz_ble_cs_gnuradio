/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "random_phase_impl.h"
#include <gnuradio/io_signature.h>
#include <cmath>

namespace gr {
namespace usrp_ble {

using input_type = gr_complex;
using output_type = gr_complex;
static constexpr float PI = 3.14159265358979323846f;

random_phase::sptr random_phase::make(int seed, float amplitude)
{
    return gnuradio::make_block_sptr<random_phase_impl>(seed, amplitude);
}

random_phase_impl::random_phase_impl(int seed, float amplitude)
    : gr::sync_block("random_phase",
                     gr::io_signature::make(
                         1 /* min inputs */, 1 /* max inputs */, sizeof(input_type)),
                     gr::io_signature::make(
                         1 /* min outputs */, 1 /*max outputs */, sizeof(output_type))),
      d_amplitude(amplitude),
      d_has_freq(false),
      d_last_freq_hz(0.0),
      d_phase_rot(gr_complex(amplitude, 0.0f)),
      d_rng(seed),
      d_phase_dist(-PI, PI)
{
    message_port_register_in(pmt::mp("freq"));
    set_msg_handler(
        pmt::mp("freq"),
        [this](pmt::pmt_t msg) { this->handle_freq_msg(msg); });
}

random_phase_impl::~random_phase_impl() {}

void random_phase_impl::set_amplitude(float amplitude)
{
    std::lock_guard<std::mutex> lock(d_mutex);
    d_amplitude = amplitude;
    const float current_phase = std::atan2(d_phase_rot.imag(), d_phase_rot.real());
    d_phase_rot = gr_complex(
        d_amplitude * std::cos(current_phase),
        d_amplitude * std::sin(current_phase));
}

void random_phase_impl::randomize_phase_locked()
{
    const float phase = d_phase_dist(d_rng);
    d_phase_rot = gr_complex(
        d_amplitude * std::cos(phase),
        d_amplitude * std::sin(phase));
}

bool random_phase_impl::parse_freq_msg(pmt::pmt_t msg, double& freq_hz) const
{
    if (pmt::is_pair(msg) && pmt::is_symbol(pmt::car(msg))) {
        if (pmt::symbol_to_string(pmt::car(msg)) == "freq" && pmt::is_number(pmt::cdr(msg))) {
            freq_hz = pmt::to_double(pmt::cdr(msg));
            return true;
        }
    } else if (pmt::is_dict(msg)) {
        const pmt::pmt_t key = pmt::intern("freq");
        if (pmt::dict_has_key(msg, key)) {
            const pmt::pmt_t value = pmt::dict_ref(msg, key, pmt::PMT_NIL);
            if (pmt::is_number(value)) {
                freq_hz = pmt::to_double(value);
                return true;
            }
        }
    } else if (pmt::is_number(msg)) {
        freq_hz = pmt::to_double(msg);
        return true;
    }

    return false;
}

void random_phase_impl::handle_freq_msg(pmt::pmt_t msg)
{
    double freq_hz = 0.0;
    if (!parse_freq_msg(msg, freq_hz)) {
        return;
    }

    std::lock_guard<std::mutex> lock(d_mutex);
    if (!d_has_freq || freq_hz != d_last_freq_hz) {
        d_last_freq_hz = freq_hz;
        d_has_freq = true;
        randomize_phase_locked();
    }
}

int random_phase_impl::work(int noutput_items,
                            gr_vector_const_void_star& input_items,
                            gr_vector_void_star& output_items)
{
    auto in = static_cast<const input_type*>(input_items[0]);
    auto out = static_cast<output_type*>(output_items[0]);
    gr_complex phase_rot;

    {
        std::lock_guard<std::mutex> lock(d_mutex);
        phase_rot = d_phase_rot;
    }

    for (int i = 0; i < noutput_items; ++i) {
        out[i] = in[i] * phase_rot;
    }
    return noutput_items;
}

} /* namespace usrp_ble */
} /* namespace gr */
