/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#ifndef INCLUDED_USRP_BLE_DATA_SEND_IMPL_H
#define INCLUDED_USRP_BLE_DATA_SEND_IMPL_H

#include <gnuradio/usrp_ble/data_send.h>

/*
 * 文件说明：
 * 这个头文件定义了 data_send_impl，它是一个受消息控制的简单发送源。
 *
 * 功能定位：
 * 1. 该块没有流输入，只有流输出。
 * 2. 它收到 "data_start" 后开始输出预定义比特序列。
 * 3. 它收到 "data_stop" 后停止输出有效数据，改为输出全零。
 *
 * 当前实现特点：
 * 1. 比特序列是固定写死的，不是动态载荷。
 * 2. 发送形式也很直接，就是把 0/1 映射为不同幅度的复数值。
 * 3. 每个比特会持续若干采样点，时长由 sample_rate 和 bit_duration 共同决定。
 */

namespace gr {
  namespace usrp_ble {

    /*
     * 类说明：
     * data_send_impl 是 data_send 的实际实现，内部维护一个简化的比特发送状态机。
     *
     * 它关注的问题包括：
     * 1. 当前是否允许发送。
     * 2. 当前输出到第几个比特。
     * 3. 当前比特已经持续了多少个采样点。
     * 4. 一个比特总共应该持续多少个采样点。
     */
    class data_send_impl : public data_send
    {
     private:
      double _sample_rate;           // 采样率，决定单位时间内输出多少个样本
      double _bit_duration;          // 单个比特持续时间，单位是秒
      size_t _samples_per_bit;       // 经过换算后，一个比特应该输出多少个采样点
      std::vector<int> _bits;        // 固定待发比特序列，当前版本中由代码直接写死
      size_t _bit_index;             // 当前发送到 _bits 里的哪个比特
      size_t _sample_in_bit;         // 当前这个比特内部已经输出了多少个样本
      bool _send_data_enabled;       // 发送使能开关，false 时整个块输出全零静默信号

     public:
      data_send_impl(double sample_rate, double bit_duration);
      ~data_send_impl();

      /*
       * 函数说明：
       * 修改采样率配置，并同步刷新 _samples_per_bit。
       */
      void set_sample_rate(double sample_rate) override;
      /*
       * 函数说明：
       * 修改单比特时长，并同步刷新 _samples_per_bit。
       */
      void set_bit_duration(double bit_duration) override;

      /*
       * 函数说明：
       * 处理消息控制命令，负责开始发送和停止发送。
       */
      void handle_msg(pmt::pmt_t msg);

      /*
       * 函数说明：
       * 这是 block 的核心输出逻辑。
       *
       * 运行规则：
       * 1. 发送使能为 false 时，输出全零复数。
       * 2. 发送使能为 true 时，按照 _bits 顺序循环输出。
       * 3. 每个比特会维持 _samples_per_bit 个采样点后才切换。
       */
      int work(
              int noutput_items,
              gr_vector_const_void_star &input_items,
              gr_vector_void_star &output_items
      );
    };

  } // namespace usrp_ble
} // namespace gr

#endif /* INCLUDED_USRP_BLE_DATA_SEND_IMPL_H */
