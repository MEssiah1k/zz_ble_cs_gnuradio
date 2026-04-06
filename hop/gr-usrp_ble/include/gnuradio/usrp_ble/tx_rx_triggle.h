/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#ifndef INCLUDED_USRP_BLE_TX_RX_TRIGGLE_H
#define INCLUDED_USRP_BLE_TX_RX_TRIGGLE_H

#include <gnuradio/usrp_ble/api.h>
#include <gnuradio/sync_block.h>

namespace gr {
  namespace usrp_ble {

    /*!
     * \brief <+description of block+>
     * \ingroup usrp_ble
     *
     */
    class USRP_BLE_API tx_rx_triggle : virtual public gr::sync_block
    {
     public:
      typedef std::shared_ptr<tx_rx_triggle> sptr;

      /*!
       * \brief Return a shared_ptr to a new instance of usrp_ble::tx_rx_triggle.
       *
       * To avoid accidental use of raw pointers, usrp_ble::tx_rx_triggle's
       * constructor is in a private implementation
       * class. usrp_ble::tx_rx_triggle::make is the public interface for
       * creating new instances.
       */
      static sptr make(bool selector=true);
    };

  } // namespace usrp_ble
} // namespace gr

#endif /* INCLUDED_USRP_BLE_TX_RX_TRIGGLE_H */
