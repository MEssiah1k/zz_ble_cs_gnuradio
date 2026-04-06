/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#ifndef INCLUDED_USRP_BLE_DATA_SEND_H
#define INCLUDED_USRP_BLE_DATA_SEND_H

#include <gnuradio/usrp_ble/api.h>
#include <gnuradio/sync_block.h>

namespace gr {
  namespace usrp_ble {

    /*!
     * \brief <+description of block+>
     * \ingroup usrp_ble
     *
     */
    class USRP_BLE_API data_send : virtual public gr::sync_block
    {
     public:
      typedef std::shared_ptr<data_send> sptr;

      /*!
       * \brief Return a shared_ptr to a new instance of usrp_ble::data_send.
       *
       * To avoid accidental use of raw pointers, usrp_ble::data_send's
       * constructor is in a private implementation
       * class. usrp_ble::data_send::make is the public interface for
       * creating new instances.
       */
      static sptr make(double sample_rate, double bit_duration);

      virtual void set_sample_rate(double sample_rate) = 0;
      virtual void set_bit_duration(double bit_duration) = 0;
    };

  } // namespace usrp_ble
} // namespace gr

#endif /* INCLUDED_USRP_BLE_DATA_SEND_H */
