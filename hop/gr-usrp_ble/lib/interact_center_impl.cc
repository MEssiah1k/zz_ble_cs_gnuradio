/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <gnuradio/io_signature.h>
#include "interact_center_impl.h"
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
    interact_center::make(int sample_rate, bool start_btn, bool stop_btn, float wait_time_ms)
    {
      return gnuradio::make_block_sptr<interact_center_impl>(
        sample_rate, start_btn, stop_btn, wait_time_ms); // 交由具体实现类保存配置并初始化状态机
    }

    // 构造函数：
    // 1. 声明为单输入、零输出的 sync_block
    // 2. 注册多个消息输出端口，分别控制发送块、存储块和频率块  
    // 3. 根据采样率和等待时间换算状态机阶段时长
    interact_center_impl::interact_center_impl(int sample_rate, bool start_btn, bool stop_btn, float wait_time_ms)
      : gr::sync_block("interact_center",
              gr::io_signature::make(1 /* min inputs */, 1 /* max inputs */, sizeof(input_type)), // 单输入口，用输入样本数作为“时间推进器”
              gr::io_signature::make(0 /* min outputs */, 0 /*max outputs */, 0)),                // 无输出口，因为这里只做消息调度
        _sample_rate(sample_rate),  // 保存输入采样率配置
        _start_btn(start_btn),      // 保存开始按钮初值
        _stop_btn(stop_btn),        // 保存停止按钮初值
        _wait_time_ms(wait_time_ms),// 保存阶段等待时长
        _is_running(false),         // 初始状态机处于停止态
        _wait_counter(0),           // 当前阶段累计等待计数清零
        _state(0)                   // 初始状态设为空闲态
    {
        // 这些端口分别连接两个发送控制端、两个存储控制端和一个频率控制端。
        message_port_register_out(pmt::mp("send1_ctrl"));  // 第一组发送块控制口
        message_port_register_out(pmt::mp("send2_ctrl"));  // 第二组发送块控制口
        message_port_register_out(pmt::mp("store1_ctrl")); // 第一组存储块控制口
        message_port_register_out(pmt::mp("store2_ctrl")); // 第二组存储块控制口
        message_port_register_out(pmt::mp("freq_ctrl"));   // 频率控制口

        // 通过输入流经过的样本数来近似计时，因此需要先把毫秒换算成采样点。
        _samples_to_wait = (size_t)(_sample_rate * (_wait_time_ms / 1000.0f)); // 目标等待样本数
        _current_freq = -40000000.0;                                            // 初始扫频起点为 -40 MHz
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
                _current_freq = -40000000.0; // 每次重新启动都从起始频点开始
                send_freq_command();         // 先通知外部更新频率
                _state = 1;                  // 从第一阶段开始
                send_phase1_start();         // 启动第一组发送/存储
                _wait_counter = 0;           // 当前阶段计数清零
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
            _state = 0;          // 状态机切回空闲
            send_all_stop();     // 通知所有受控模块停止
        }
    }
    
    void interact_center_impl::set_wait_time_ms(float wait_time_ms)
    {
        _wait_time_ms = wait_time_ms; // 保存新的等待时间参数
        // 参数变化后立即刷新换算值，使后续阶段使用新的等待时长。
        _samples_to_wait = (size_t)(_sample_rate * (_wait_time_ms / 1000.0f)); // 毫秒重新换算为采样点
    }

    void interact_center_impl::send_phase1_start()
    {
        // 第一阶段执行策略：
        // 1. 确保第二组发送/采集关闭
        // 2. 启动第一组发送/采集
        // 这样可以保证两个通路不会同时工作。
        message_port_pub(pmt::mp("send2_ctrl"), pmt::intern("data_stop"));   // 先停第二组发送
        message_port_pub(pmt::mp("store2_ctrl"), pmt::intern("store_stop")); // 再停第二组存储
        message_port_pub(pmt::mp("send1_ctrl"), pmt::intern("data_start"));  // 启动第一组发送
        message_port_pub(pmt::mp("store1_ctrl"), pmt::intern("store_start")); // 启动第一组存储
    }
    
    void interact_center_impl::send_phase2_start()
    {
        // 第二阶段执行策略与第一阶段相反：
        // 停止第一组，启动第二组。
        message_port_pub(pmt::mp("store1_ctrl"), pmt::intern("store_stop")); // 关闭第一组存储
        message_port_pub(pmt::mp("send1_ctrl"), pmt::intern("data_stop"));   // 关闭第一组发送
        
        message_port_pub(pmt::mp("send2_ctrl"), pmt::intern("data_start"));   // 启动第二组发送
        message_port_pub(pmt::mp("store2_ctrl"), pmt::intern("store_start")); // 启动第二组存储
    }
    
    void interact_center_impl::send_all_stop()
    {
        // 一次性关闭所有受控块，用于人工停止或扫频结束。
        message_port_pub(pmt::mp("send1_ctrl"), pmt::intern("data_stop"));   // 停第一组发送
        message_port_pub(pmt::mp("send2_ctrl"), pmt::intern("data_stop"));   // 停第二组发送
        message_port_pub(pmt::mp("store1_ctrl"), pmt::intern("store_stop")); // 停第一组存储
        message_port_pub(pmt::mp("store2_ctrl"), pmt::intern("store_stop")); // 停第二组存储
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

    int
    interact_center_impl::work(int noutput_items,
        gr_vector_const_void_star &input_items,
        gr_vector_void_star &output_items)
    {
      auto in = static_cast<const input_type*>(input_items[0]); // 取输入缓冲区，虽然数据值本身不用，但接口上仍要拿到
      (void)in; // 明确告诉编译器这里故意不使用输入值，只使用样本数量

        // 通过本次 work 收到的样本数量推进状态机。
        // 这里的“等待”不是 wall clock 时间，而是“经过了多少输入采样点”。
        if (_is_running) {
             size_t items_processed = 0; // 记录本次 work 内已经处理了多少个样本
             while (items_processed < noutput_items) {
                 size_t items_remaining = noutput_items - items_processed; // 当前还剩多少样本可用于推进状态机
                 
                 if (_state == 1) { // 第一阶段等待 send1/store1 运行满设定时长
                     if (_wait_counter + items_remaining >= _samples_to_wait) {
                         // 当前批次足以跨过等待门限，进入第二阶段。
                         size_t needed = _samples_to_wait - _wait_counter; // 算出刚好补满门限还需要多少样本
                         items_processed += needed;                        // 消耗这些样本，把第一阶段走完
                         _wait_counter = 0;                                // 阶段切换前把计数器清零
                         _state = 2;                                       // 进入第二阶段
                         send_phase2_start();                              // 发送第二阶段启动命令
                     } else {
                         // 当前批次还不够，继续累加等待计数。
                         _wait_counter += items_remaining; // 累加本轮贡献的等待样本数
                         items_processed += items_remaining; // 当前批次全部用于继续等待
                     }
                 } else if (_state == 2) { // 第二阶段等待 send2/store2 运行满设定时长
                     if (_wait_counter + items_remaining >= _samples_to_wait) {
                         size_t needed = _samples_to_wait - _wait_counter; // 算出补满第二阶段门限所需样本数
                         items_processed += needed;                        // 消耗这部分样本
                         _wait_counter = 0;                                // 为下一阶段清零等待计数
                         
                         // 第二阶段结束后步进频率，形成从 -40 MHz 到 +40 MHz 的扫频。
                         _current_freq += 1000000.0; // 频率每轮增加 1 MHz
                         if (_current_freq > 40000000.0) {
                              // 扫频结束，停止整个状态机。
                              _is_running = false; // 标记状态机停止
                              _state = 0;          // 复位到空闲态
                              send_all_stop();     // 通知所有子模块收尾
                         } else {
                              // 继续下一个频点，重新回到第一阶段。
                              send_freq_command(); // 先下发新的频率
                              _state = 1;         // 切回第一阶段
                              send_phase1_start(); // 启动第一阶段通路
                         }
                     } else {
                         _wait_counter += items_remaining; // 第二阶段继续积累等待样本
                         items_processed += items_remaining; // 当前批次全部消耗掉
                     }
                 } else {
                     // 理论上运行态不应进入非法状态。
                     // 即使出现，也直接消费掉当前样本，避免卡死。
                     items_processed += items_remaining; // 容错处理：直接吃掉剩余样本
                 }
             }
        }
        
      return noutput_items; // 该块虽然无输出，但要告诉调度器本轮消耗了多少输入
    }

  } /* namespace usrp_ble */
} /* namespace gr */
