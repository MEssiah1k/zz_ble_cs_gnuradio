/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <gnuradio/io_signature.h>
#include "data_send_impl.h"

/*
 * 文件说明：
 * 这个实现文件定义了 data_send_impl 的发送逻辑。
 *
 * 它本质上是一个非常简单的“受控波形源”：
 * 1. 不接收流输入，只负责产生输出。
 * 2. 输出内容来自内部固定比特序列。
 * 3. 是否真正输出该序列，由消息命令决定。
 *
 * 由于当前实现没有做复杂调制，所以它更像一个实验用占位发送源，
 * 适合拿来验证流程控制和时序切换是否正常。
 */

namespace gr {
  namespace usrp_ble {

    using output_type = gr_complex;

    // 工厂函数：创建 data_send_impl 实例。
    data_send::sptr
    data_send::make(double sample_rate, double bit_duration)
    {
      return gnuradio::make_block_sptr<data_send_impl>(
        sample_rate, bit_duration); // 把配置参数传入具体实现类
    }

    // 构造函数：
    // 1. 声明为零输入、单输出的源块
    // 2. 初始化发送时序参数
    // 3. 注册消息端口，用于控制发送开始与停止
    data_send_impl::data_send_impl(double sample_rate, double bit_duration)
      : gr::sync_block("data_send",
              gr::io_signature::make(0, 0, 0),                  // 没有输入口，这是一个 source
              gr::io_signature::make(1, 1, sizeof(output_type))), // 单输出口，输出类型是 gr_complex
        _sample_rate(sample_rate),      // 初始化采样率
        _bit_duration(bit_duration),    // 初始化单比特持续时间
        _bit_index(0),                  // 初始从第 0 个比特开始
        _sample_in_bit(0),              // 初始时当前比特尚未输出任何样本
        _send_data_enabled(false)       // 默认不发送，避免流图一启动就产生数据
    {
      message_port_register_in(pmt::mp("command")); // 注册控制输入端口
      set_msg_handler(pmt::mp("command"),           // 绑定控制消息处理回调
                      boost::bind(&data_send_impl::handle_msg, this, boost::placeholders::_1));

      // 根据采样率和比特持续时间计算一个比特对应多少个采样点。
      _samples_per_bit = (size_t)(_sample_rate * _bit_duration); // 秒 -> 样本数

      // 当前实现里发送的是固定的 13 个 "1"。
      // 如果后续要发送真实载荷，可以在这里改成可配置序列。
      _bits = {
        1,1,1,1,1,1,1,1,1,1,1,1,1
      };
    }

    // 当前类没有手动资源管理需求，析构函数保持为空。
    data_send_impl::~data_send_impl()
    {
    }

    void
    data_send_impl::set_sample_rate(double sample_rate)
    {
      _sample_rate = sample_rate; // 更新采样率配置
      // 采样率变化后，需要重新换算比特宽度对应的采样点数量。
      _samples_per_bit = (size_t)(_sample_rate * _bit_duration); // 重新计算每比特采样点数
    }

    void
    data_send_impl::set_bit_duration(double bit_duration)
    {
      _bit_duration = bit_duration; // 更新比特时长配置
      // 比特时长变化后，同样需要重新换算。
      _samples_per_bit = (size_t)(_sample_rate * _bit_duration); // 重新计算每比特采样点数
    }

    void
    data_send_impl::handle_msg(pmt::pmt_t msg)
    {
      // 只接受符号命令：
      // "data_start" 开始从头循环发送比特流
      // "data_stop" 停止发送并输出全零
      if (pmt::is_symbol(msg)) {
        std::string cmd = pmt::symbol_to_string(msg); // 取出字符串形式的命令字
        if (cmd == "data_start") {
          _send_data_enabled = true; // 开启发送
          _bit_index = 0;            // 从比特序列起点重新开始
          _sample_in_bit = 0;        // 当前比特内部计数清零
        } else if (cmd == "data_stop") {
          _send_data_enabled = false; // 停止发送，后续输出全零
        }
      }
    }

    int
    data_send_impl::work(int noutput_items,
        gr_vector_const_void_star &input_items,
        gr_vector_void_star &output_items)
    {
      auto out = static_cast<output_type*>(output_items[0]); // 取得输出缓冲区

      for (int i = 0; i < noutput_items; ++i) {
        if (_send_data_enabled) {
          // 当前实现采用最简单的幅度映射：
          // 比特 1 -> (1+0j)
          // 比特 0 -> (0+0j)
          // 不做成形、不做调制，仅输出基带幅度序列。
          int bit = _bits[_bit_index];                               // 读出当前比特值
          out[i] = (bit ? gr_complex(1.0, 0) : gr_complex(0.0, 0.0)); // 1 映射为幅度 1，0 映射为幅度 0

          // 在同一个比特期间持续输出 _samples_per_bit 个采样点，
          // 到达边界后再切换到下一个比特。
          _sample_in_bit++; // 当前比特又输出了一个采样点
          if (_sample_in_bit >= _samples_per_bit) {
            _sample_in_bit = 0;                        // 一个比特持续时间结束，清零局部计数
            _bit_index = (_bit_index + 1) % _bits.size(); // 切换到下一个比特，循环发送
          }
        } else {
          // 未启动时保持静默输出，避免产生残留信号。
          out[i] = gr_complex(0.0, 0.0); // 输出全零复数样本
        }
      }

      // 返回本轮实际写出的输出样本数。
      return noutput_items; // 返回本轮实际生成的输出样本数
    }

  } /* namespace usrp_ble */
} /* namespace gr */
