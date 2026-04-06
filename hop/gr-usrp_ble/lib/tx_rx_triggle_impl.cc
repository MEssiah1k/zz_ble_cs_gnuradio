/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <gnuradio/io_signature.h>
#include <pmt/pmt.h>
#include <cstring>
#include "tx_rx_triggle_impl.h"

/*
 * 文件说明：
 * 这个实现文件提供 tx_rx_triggle_impl 的具体行为。
 *
 * 模块行为分成两部分：
 * 1. 在 start() 中发一次控制消息，让外部设备切到指定天线。
 * 2. 在 work() 中把输入 float 流原样复制到输出，不做任何加工。
 *
 * 所以这个 block 的本质是“带初始化动作的透明通道”。
 */

namespace gr {
  namespace usrp_ble {

    using input_type = float;
    using output_type = float;

    // 工厂函数：创建 tx_rx_triggle_impl 实例。
    tx_rx_triggle::sptr
    tx_rx_triggle::make(bool selector)
    {
      return gnuradio::make_block_sptr<tx_rx_triggle_impl>(
        selector); // 由具体实现类保存天线选择参数
    }

    // 构造函数：
    // 1. 声明为单输入、单输出的同步直通块
    // 2. 保存天线选择参数
    // 3. 注册消息输出端口 cmd，用于对接设备控制接口
    tx_rx_triggle_impl::tx_rx_triggle_impl(bool selector)
      : gr::sync_block("tx_rx_triggle",
              gr::io_signature::make(1 /* min inputs */, 1 /* max inputs */, sizeof(input_type)),   // 单输入口，float 流
              gr::io_signature::make(1 /* min outputs */, 1 /*max outputs */, sizeof(output_type))), // 单输出口，float 流
        d_selector(selector), // 保存用户指定的天线选择
        d_sent(false)         // 初始时尚未发送控制命令
    {
      message_port_register_out(pmt::mp("cmd")); // 注册控制消息输出端口
    }

    // 当前类没有额外资源需要释放。
    tx_rx_triggle_impl::~tx_rx_triggle_impl()
    {
    }

    bool tx_rx_triggle_impl::start()
    {
      // 只在第一次 start() 时发送一次命令，
      // 避免 flowgraph 多次调用启动流程时重复切换天线。
      if (!d_sent) {
        auto msg = pmt::make_dict();                        // 创建空字典，作为命令消息容器
        const std::string antenna = d_selector ? "TX/RX" : "RX2"; // 根据选择开关决定目标天线名
        // 按 UHD 常见命令格式组织字典消息：
        // antenna: 天线口名称
        // chan:    通道号，这里固定为 0
        msg = pmt::dict_add(msg, pmt::mp("antenna"), pmt::mp(antenna)); // 写入天线字段
        msg = pmt::dict_add(msg, pmt::mp("chan"), pmt::from_long(0));   // 写入通道字段
        message_port_pub(pmt::mp("cmd"), msg);                          // 通过 cmd 端口把字典消息发出去
        d_sent = true;                                                  // 标记已经发过，避免重复发送
      }

      return gr::sync_block::start(); // 继续执行父类标准启动流程
    }

    int
    tx_rx_triggle_impl::work(int noutput_items,
        gr_vector_const_void_star &input_items,
        gr_vector_void_star &output_items)
    {
      auto in = static_cast<const input_type*>(input_items[0]);   // 输入样本缓冲区
      auto out = static_cast<output_type*>(output_items[0]);      // 输出样本缓冲区

      // 这里不做任何信号处理，仅作为透明直通块保留原始数据。
      std::memcpy(out, in, sizeof(input_type) * noutput_items); // 按字节拷贝整段样本数据

      // 返回本轮输出的样本数。
      return noutput_items; // 告诉调度器本轮成功产生了多少输出样本
    }

  } /* namespace usrp_ble */
} /* namespace gr */
