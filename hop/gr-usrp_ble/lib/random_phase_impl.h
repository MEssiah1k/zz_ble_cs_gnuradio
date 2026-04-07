/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#ifndef INCLUDED_USRP_BLE_RANDOM_PHASE_IMPL_H
#define INCLUDED_USRP_BLE_RANDOM_PHASE_IMPL_H

#include <gnuradio/usrp_ble/random_phase.h>
#include <pmt/pmt.h>
#include <mutex>
#include <random>

namespace gr {
namespace usrp_ble {

class random_phase_impl : public random_phase
{
private:
    float d_amplitude;
    bool d_has_freq;
    double d_last_freq_hz;
    gr_complex d_phase_rot;
    std::mt19937 d_rng;
    std::uniform_real_distribution<float> d_phase_dist;
    std::mutex d_mutex;

    void handle_freq_msg(pmt::pmt_t msg);
    void randomize_phase_locked();
    bool parse_freq_msg(pmt::pmt_t msg, double& freq_hz) const;

public:
    random_phase_impl(int seed, float amplitude);
    ~random_phase_impl();

    void set_amplitude(float amplitude) override;

    int work(int noutput_items,
             gr_vector_const_void_star& input_items,
             gr_vector_void_star& output_items);
};

} // namespace usrp_ble
} // namespace gr

#endif /* INCLUDED_USRP_BLE_RANDOM_PHASE_IMPL_H */
