/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#ifndef INCLUDED_USRP_BLE_TX_RX_TRIGGLE_IMPL_H
#define INCLUDED_USRP_BLE_TX_RX_TRIGGLE_IMPL_H

#include <gnuradio/usrp_ble/tx_rx_triggle.h>

/*
 * 文件说明：
 * 这个头文件定义了 tx_rx_triggle_impl。
 *
 * 模块整体作用：
 * 1. 从流数据角度看，它只是一个简单的直通块。
 * 2. 从控制角度看，它会在 block 启动时向外部发送一次命令消息。
 * 3. 该命令通常用于通知 USRP 或相关控制块切换天线口。
 *
 * 因此，这个模块的重点不在信号运算，而在启动阶段的设备配置动作。
 */

namespace gr {
  namespace usrp_ble {

    /*
     * 类说明：
     * tx_rx_triggle_impl 负责两件事：
     * 1. 在 start() 时发一次初始化消息。
     * 2. 在 work() 中保持输入输出样本完全一致。
     *
     * 这个块适合作为“命令触发 + 数据直通”的薄包装层。
     */
    class tx_rx_triggle_impl : public tx_rx_triggle
    {
     private:
      bool d_selector; // 天线口选择开关：true 表示 "TX/RX"，false 表示 "RX2"
      bool d_sent;     // 启动命令是否已经发送过，防止重复下发相同配置

     public:
      tx_rx_triggle_impl(bool selector);
      ~tx_rx_triggle_impl();

      /*
       * 函数说明：
       * 在 block 启动阶段发一次消息命令，用于设置外部设备状态。
       */
      bool start() override;

      /*
       * 函数说明：
       * 主处理阶段不做运算，只做数据直通。
       */
      int work(
              int noutput_items,
              gr_vector_const_void_star &input_items,
              gr_vector_void_star &output_items
      );
    };

  } // namespace usrp_ble
} // namespace gr

#endif /* INCLUDED_USRP_BLE_TX_RX_TRIGGLE_IMPL_H */
