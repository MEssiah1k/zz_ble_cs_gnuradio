/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#ifndef INCLUDED_USRP_BLE_DATA_STORE_H
#define INCLUDED_USRP_BLE_DATA_STORE_H

#include <gnuradio/usrp_ble/api.h>
#include <gnuradio/sync_block.h>

namespace gr {
  namespace usrp_ble {

    /*!
     * \brief <+description of block+>
     * \ingroup usrp_ble
     *
     */
    class USRP_BLE_API data_store : virtual public gr::sync_block
    {
     public:
      typedef std::shared_ptr<data_store> sptr;

      /*!
       * \brief Return a shared_ptr to a new instance of usrp_ble::data_store.
       *
       * To avoid accidental use of raw pointers, usrp_ble::data_store's
       * constructor is in a private implementation
       * class. usrp_ble::data_store::make is the public interface for
       * creating new instances.
       */
      static sptr make(int data_len, int skip_len, const std::string& path);
    };

  } // namespace usrp_ble
} // namespace gr

#endif /* INCLUDED_USRP_BLE_DATA_STORE_H */
