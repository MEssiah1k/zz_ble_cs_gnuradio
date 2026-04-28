/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <gnuradio/io_signature.h>
#include "interact_center_impl.h"
#include <algorithm>
#include <cmath>
#include <pmt/pmt.h>

/*
 * 文件说明：
 * 这个实现文件承载了整个 interact_center 的状态机控制逻辑。
 *
 * 这个模块的核心价值不在于处理输入样本本身，而在于协调多个子模块：
 * 1. 哪一组发送模块应该启动。
 * 2. 哪一组存储模块应该启动。
 * 3. 什么时候该从第一阶段切到第二阶段。
 * 4. 什么时候该切换频率并进入下一个循环。
 *
 * 这里的“时间”不是用系统时钟测的，而是通过累计输入样本数来近似表示。
 * 在一个采样率固定的流图里，这种方式简单而且容易与流图处理节奏保持一致。
 */

namespace gr {
  namespace usrp_ble {

    using input_type = gr_complex;

    // 工厂函数：创建 interact_center_impl 实例。
    interact_center::sptr
    interact_center::make(int sample_rate,
                          bool start_btn,
                          bool stop_btn,
                          float wait_time_ms,
                          int repeat_total,
                          int start_freq_index,
                          int stop_freq_index,
                          double step_hz,
                          int capture_groups)
    {
      return gnuradio::make_block_sptr<interact_center_impl>(
        sample_rate,
        start_btn,
        stop_btn,
        wait_time_ms,
        repeat_total,
        start_freq_index,
        stop_freq_index,
        step_hz,
        capture_groups); // 交由具体实现类保存配置并初始化状态机
    }

    // 构造函数：
    // 1. 声明为单输入、零输出的 sync_block
    // 2. 注册多个消息输出端口，分别控制发送块、存储块和频率块  
    // 3. 根据采样率和等待时间换算状态机阶段时长
    interact_center_impl::interact_center_impl(int sample_rate,
                                               bool start_btn,
                                               bool stop_btn,
                                               float wait_time_ms,
                                               int repeat_total,
                                               int start_freq_index,
                                               int stop_freq_index,
                                               double step_hz,
                                               int capture_groups)
      : gr::sync_block("interact_center",
              gr::io_signature::make(1 /* min inputs */, 1 /* max inputs */, sizeof(input_type)), // 单输入口，用输入样本数作为“时间推进器”
              gr::io_signature::make(0 /* min outputs */, 0 /*max outputs */, 0)),                // 无输出口，因为这里只做消息调度
        _sample_rate(sample_rate),  // 保存输入采样率配置
        _start_btn(start_btn),      // 保存开始按钮初值
        _stop_btn(stop_btn),        // 保存停止按钮初值
        _wait_time_ms(wait_time_ms),// 保存阶段等待时长
        _repeat_total(std::max(1, repeat_total)), // 至少重复 1 次，避免非法参数破坏状态机
        _repeat_index(0),           // 每次启动都从当前频点的第一次重复开始
        _capture_group_total(std::max(1, capture_groups)),
        _capture_group_index(0),
        _start_freq_index(start_freq_index),
        _stop_freq_index(stop_freq_index),
        _current_freq_index(start_freq_index),
        _step_hz(std::max(1.0, std::abs(step_hz))),
        _is_running(false),         // 初始状态机处于停止态
        _use_msg_clock(false),      // 默认沿用原来的流输入计时模式
        _phase_samples(0),
        _wait_counter(0),           // 当前阶段累计等待计数清零
        _state(state_t::idle)       // 初始状态设为空闲态
    {
        // 这些端口分别连接两个发送控制端、两个存储控制端和一个频率控制端。
        message_port_register_out(pmt::mp("send1_ctrl"));  // 第一组发送块控制口
        message_port_register_out(pmt::mp("send2_ctrl"));  // 第二组发送块控制口
        message_port_register_out(pmt::mp("store1_ctrl")); // 第一组存储块控制口
        message_port_register_out(pmt::mp("store2_ctrl")); // 第二组存储块控制口
        message_port_register_out(pmt::mp("capture_ctrl")); // 连续捕获控制口
        message_port_register_out(pmt::mp("freq_ctrl"));   // 频率控制口
        message_port_register_out(pmt::mp("phase_ctrl"));  // 当前阶段选择口：0=phase1，1=phase2
        message_port_register_in(pmt::mp("clock"));        // 可选消息计时口，用于 self_2 的双 gate 真实样本计时
        set_msg_handler(pmt::mp("clock"),
                        [this](pmt::pmt_t msg) { this->handle_clock_msg(msg); });

        // 通过输入流经过的样本数来近似计时，因此需要先把毫秒换算成采样点。
        refresh_sample_counts();
        refresh_current_freq();
    }

    // 当前类没有需要显式释放的额外资源。
    interact_center_impl::~interact_center_impl()
    {
    }
    
    void interact_center_impl::set_start_btn(bool start_btn)
    {
        bool old_val = _start_btn; // 记录旧状态，用于检测上升沿
        _start_btn = start_btn;    // 更新当前按钮状态
        // 只在上升沿触发一次启动，避免按钮保持为 true 时重复启动。
        if (!old_val && _start_btn) {
            if (!_is_running) {
                _is_running = true;          // 状态机正式进入运行态
                reset_current_freq();        // 每次重新启动都从起始频点开始
                _repeat_index = 0;           // 每次重新启动都从当前频点的第 0 次重复开始
                send_all_stop();             // 先清掉上一次可能残留的发送/存储状态
                send_all_capture_stop();
                send_capture_start_for_current_group();
                send_freq_command();
                _state = state_t::phase1;
                _wait_counter = 0;
                send_phase1_start();
            }
        }
    }

    void interact_center_impl::set_stop_btn(bool stop_btn)
    {
        bool old_val = _stop_btn; // 记录停止按钮旧值
        _stop_btn = stop_btn;     // 更新停止按钮新值
        // 停止按钮同样按上升沿处理，触发后立即中断整个流程。
        if (!old_val && _stop_btn) {
            _is_running = false; // 退出运行态
            _state = state_t::idle; // 状态机切回空闲
            _wait_counter = 0;
            send_all_stop();     // 通知所有受控模块停止
            send_all_capture_stop();
        }
    }
    
    void interact_center_impl::set_wait_time_ms(float wait_time_ms)
    {
        _wait_time_ms = wait_time_ms; // 保存新的等待时间参数
        refresh_sample_counts();
    }

    void interact_center_impl::set_capture_groups(int capture_groups)
    {
        _capture_group_total = std::max(1, capture_groups);
        if (!_is_running && _capture_group_index >= _capture_group_total) {
            _capture_group_index = 0;
        }
    }

    void interact_center_impl::set_use_msg_clock(bool use_msg_clock)
    {
        _use_msg_clock = use_msg_clock;
    }

    void interact_center_impl::set_start_freq_index(int start_freq_index)
    {
        _start_freq_index = start_freq_index;
        if (!_is_running) {
            reset_current_freq();
        }
    }

    void interact_center_impl::set_stop_freq_index(int stop_freq_index)
    {
        _stop_freq_index = stop_freq_index;
    }

    void interact_center_impl::set_step_hz(double step_hz)
    {
        _step_hz = std::max(1.0, std::abs(step_hz));
        refresh_current_freq();
    }

    void interact_center_impl::reset_current_freq()
    {
        _current_freq_index = _start_freq_index;
        refresh_current_freq();
    }

    void interact_center_impl::refresh_current_freq()
    {
        _current_freq = static_cast<double>(_current_freq_index) * _step_hz;
    }

    void interact_center_impl::refresh_sample_counts()
    {
        _phase_samples = static_cast<size_t>(
            std::max(0.0f, _wait_time_ms) * static_cast<float>(_sample_rate) / 1000.0f);
    }

    void interact_center_impl::handle_clock_msg(pmt::pmt_t msg)
    {
        if (!_use_msg_clock) {
            return;
        }

        pmt::pmt_t value = pmt::is_pair(msg) ? pmt::cdr(msg) : msg;
        if (!pmt::is_integer(value)) {
            return;
        }

        const long nitems = pmt::to_long(value);
        if (nitems > 0) {
            process_state_machine(static_cast<int>(nitems));
        }
    }

    void interact_center_impl::send_phase1_start()
    {
        // 第一阶段执行策略：
        // 1. 确保第二组发送/采集关闭
        // 2. 启动第一组发送/采集
        // 这样可以保证两个通路不会同时工作。
        message_port_pub(pmt::mp("send2_ctrl"), pmt::intern("data_stop"));   // 先停第二组发送
        message_port_pub(pmt::mp("store2_ctrl"), pmt::intern("store_stop")); // 再停第二组存储
        message_port_pub(pmt::mp("phase_ctrl"), pmt::cons(pmt::PMT_NIL, pmt::from_long(0))); // selector 的 iindex 口需要 pair 消息
        message_port_pub(pmt::mp("store1_ctrl"), make_store_start_msg()); // 启动第一组存储，并带上频点/重复编号
        message_port_pub(pmt::mp("send1_ctrl"), pmt::intern("data_start"));  // 启动第一组发送
    }
    
    void interact_center_impl::send_phase2_start()
    {
        // 第二阶段执行策略与第一阶段相反：
        // 停止第一组，启动第二组。
        message_port_pub(pmt::mp("store1_ctrl"), pmt::intern("store_stop")); // 关闭第一组存储
        message_port_pub(pmt::mp("send1_ctrl"), pmt::intern("data_stop"));   // 关闭第一组发送
        message_port_pub(pmt::mp("phase_ctrl"), pmt::cons(pmt::PMT_NIL, pmt::from_long(1))); // phase2 选择 reflector gate 输出计时
        message_port_pub(pmt::mp("store2_ctrl"), make_store_start_msg()); // 启动第二组存储，并带上频点/重复编号
        message_port_pub(pmt::mp("send2_ctrl"), pmt::intern("data_start"));   // 启动第二组发送
    }
    
    void interact_center_impl::send_all_stop()
    {
        // 一次性关闭所有受控块，用于人工停止或扫频结束。
        message_port_pub(pmt::mp("send1_ctrl"), pmt::intern("data_stop"));   // 停第一组发送
        message_port_pub(pmt::mp("send2_ctrl"), pmt::intern("data_stop"));   // 停第二组发送
        message_port_pub(pmt::mp("store1_ctrl"), pmt::intern("store_stop")); // 停第一组存储
        message_port_pub(pmt::mp("store2_ctrl"), pmt::intern("store_stop")); // 停第二组存储
    }

    void interact_center_impl::send_capture_start_for_current_group()
    {
        pmt::pmt_t msg = pmt::make_dict();
        msg = pmt::dict_add(msg, pmt::intern("cmd"), pmt::intern("capture_start"));
        msg = pmt::dict_add(msg, pmt::intern("group_index"), pmt::from_long(_capture_group_index));
        msg = pmt::dict_add(msg, pmt::intern("capture_group_index"), pmt::from_long(_capture_group_index));
        message_port_pub(pmt::mp("capture_ctrl"), msg);
    }

    void interact_center_impl::send_capture_stop_for_current_group()
    {
        pmt::pmt_t msg = pmt::make_dict();
        msg = pmt::dict_add(msg, pmt::intern("cmd"), pmt::intern("capture_stop"));
        msg = pmt::dict_add(msg, pmt::intern("group_index"), pmt::from_long(_capture_group_index));
        msg = pmt::dict_add(msg, pmt::intern("capture_group_index"), pmt::from_long(_capture_group_index));
        message_port_pub(pmt::mp("capture_ctrl"), msg);
    }

    void interact_center_impl::send_all_capture_stop()
    {
        message_port_pub(pmt::mp("capture_ctrl"), pmt::intern("capture_stop"));
    }

    void interact_center_impl::send_freq_command()
    {
        // 频率端口采用 GNU Radio 常见的 PMT 键值对形式：
        // (freq . <double>)
        // 外部块收到后应将中心频率更新为 _current_freq。
        pmt::pmt_t key = pmt::intern("freq");               // 键名固定为 "freq"
        pmt::pmt_t val = pmt::from_double(_current_freq);   // 频率值转成 PMT double
        message_port_pub(pmt::mp("freq_ctrl"), pmt::cons(key, val)); // 以 pair 形式发给频率控制口
    }

    pmt::pmt_t interact_center_impl::make_store_start_msg() const
    {
        pmt::pmt_t msg = pmt::make_dict();
        msg = pmt::dict_add(msg, pmt::intern("cmd"), pmt::intern("store_start"));
        msg = pmt::dict_add(msg, pmt::intern("freq_index"), pmt::from_long(current_freq_index()));
        msg = pmt::dict_add(msg, pmt::intern("freq_index_signed"), pmt::from_long(_current_freq_index));
        msg = pmt::dict_add(msg, pmt::intern("repeat_index"), pmt::from_long(_repeat_index));
        msg = pmt::dict_add(msg, pmt::intern("capture_group_index"), pmt::from_long(_capture_group_index));
        return msg;
    }

    int interact_center_impl::current_freq_index() const
    {
        if (_start_freq_index <= _stop_freq_index) {
            return _current_freq_index - _start_freq_index;
        }

        return _start_freq_index - _current_freq_index;
    }

    bool interact_center_impl::is_last_frequency() const
    {
        if (_start_freq_index <= _stop_freq_index) {
            return _current_freq_index >= _stop_freq_index;
        }

        return _current_freq_index <= _stop_freq_index;
    }

    void interact_center_impl::process_state_machine(int nitems)
    {
        // 通过本次 work 收到的样本数量推进状态机。
        // 这里的“等待”不是 wall clock 时间，而是“经过了多少输入采样点”。
        if (!_is_running) {
            return;
        }

        size_t items_processed = 0; // 记录本次 work 内已经处理了多少个样本
        while (items_processed < static_cast<size_t>(nitems) && _is_running) {
            size_t items_remaining = static_cast<size_t>(nitems) - items_processed;
            size_t target = 0;

            if (_state == state_t::phase1 || _state == state_t::phase2) {
                target = _phase_samples;
            } else {
                items_processed += items_remaining;
                continue;
            }

            if (target == 0 || _wait_counter + items_remaining >= target) {
                const size_t needed = (target > _wait_counter) ? (target - _wait_counter) : 0;
                items_processed += std::min(needed, items_remaining);
                _wait_counter = 0;

                if (_state == state_t::phase1) {
                    _state = state_t::phase2;
                    send_phase2_start();
                } else if (_state == state_t::phase2) {
                    if (_repeat_index + 1 < _repeat_total) {
                        _repeat_index += 1;  // 同一频点进入下一次重复
                        _state = state_t::phase1;
                        send_phase1_start();
                    } else {
                        _repeat_index = 0;
                        if (is_last_frequency()) {
                            send_all_stop();
                            send_capture_stop_for_current_group();
                            _capture_group_index = (_capture_group_index + 1) % _capture_group_total;
                            _is_running = false;
                            _state = state_t::idle;
                        } else {
                            if (_start_freq_index <= _stop_freq_index) {
                                _current_freq_index += 1;
                            } else {
                                _current_freq_index -= 1;
                            }
                            refresh_current_freq();
                            send_freq_command();
                            _state = state_t::phase1;
                            send_phase1_start();
                        }
                    }
                }
            } else {
                _wait_counter += items_remaining;
                items_processed += items_remaining;
            }
        }
    }

    int
    interact_center_impl::work(int noutput_items,
        gr_vector_const_void_star &input_items,
        gr_vector_void_star &output_items)
    {
      auto in = static_cast<const input_type*>(input_items[0]); // 取输入缓冲区，虽然数据值本身不用，但接口上仍要拿到
      (void)in; // 明确告诉编译器这里故意不使用输入值，只使用样本数量

      if (!_use_msg_clock) {
        process_state_machine(noutput_items);
      }
        
      return noutput_items; // 该块虽然无输出，但要告诉调度器本轮消耗了多少输入
    }

  } /* namespace usrp_ble */
} /* namespace gr */
