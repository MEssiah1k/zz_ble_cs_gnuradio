/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#ifndef INCLUDED_USRP_BLE_INTERACT_CENTER_H
#define INCLUDED_USRP_BLE_INTERACT_CENTER_H

#include <gnuradio/usrp_ble/api.h>
#include <gnuradio/sync_block.h>

namespace gr {
  namespace usrp_ble {

    /*!
     * \brief <+description of block+>
     * \ingroup usrp_ble
     *
     */
    class USRP_BLE_API interact_center : virtual public gr::sync_block
    {
     public:
      typedef std::shared_ptr<interact_center> sptr;

      /*!
       * \brief Return a shared_ptr to a new instance of usrp_ble::interact_center.
       *
       * To avoid accidental use of raw pointers, usrp_ble::interact_center's
       * constructor is in a private implementation
       * class. usrp_ble::interact_center::make is the public interface for
       * creating new instances.
       */
      static sptr make(int sample_rate,
                       bool start_btn,
                       bool stop_btn,
                       float wait_time_ms,
                       int repeat_total,
                       int start_freq_index,
                       int stop_freq_index,
                       double step_hz,
                       int capture_groups = 1,
                       float rx_tail_time_ms = 0.0f);
      
      virtual void set_start_btn(bool start_btn) = 0;
      virtual void set_stop_btn(bool stop_btn) = 0;
      virtual void set_wait_time_ms(float wait_time_ms) = 0;
      virtual void set_use_msg_clock(bool use_msg_clock) = 0;
      virtual void set_start_freq_index(int start_freq_index) = 0;
      virtual void set_stop_freq_index(int stop_freq_index) = 0;
      virtual void set_step_hz(double step_hz) = 0;
      virtual void set_capture_groups(int capture_groups) = 0;
      virtual void set_rx_tail_time_ms(float rx_tail_time_ms) = 0;
    };

  } // namespace usrp_ble
} // namespace gr

#endif /* INCLUDED_USRP_BLE_INTERACT_CENTER_H */
