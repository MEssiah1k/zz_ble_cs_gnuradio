/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#ifndef INCLUDED_USRP_BLE_INTERACT_CENTER_IMPL_H
#define INCLUDED_USRP_BLE_INTERACT_CENTER_IMPL_H

#include <gnuradio/usrp_ble/interact_center.h>
#include <chrono>

/*
 * 文件说明：
 * 这个头文件定义了交互控制中心 interact_center_impl。
 *
 * 整体作用：
 * 1. 它不负责真正的信号处理，而是负责流程编排。
 * 2. 它根据开始/停止按钮状态管理一套时序状态机。
 * 3. 它通过多个消息端口通知其他 block 何时发送、何时存储、何时切频。
 *
 * 从系统角度看，这个块相当于“实验调度器”：
 * 1. 开始后先在一个频点执行第一阶段。
 * 2. 等待指定样本数后切到第二阶段。
 * 3. 第二阶段结束后频率步进。
 * 4. 一直循环到频率扫完整个范围或者外部按下停止。
 */

namespace gr {
  namespace usrp_ble {

    /*
     * 类说明：
     * interact_center_impl 是整个实验流程的消息控制核心。
     *
     * 它维护的核心上下文包括：
     * 1. 当前是否处于运行态。
     * 2. 当前状态机处于哪个阶段。
     * 3. 当前频率值是多少。
     * 4. 当前阶段已经等待了多少采样点。
     *
     * 这里采用“按输入采样数量计时”的方式，而不是直接读取系统时钟。
     * 这样做的好处是控制逻辑与流图调度节奏保持一致。
     */
    class interact_center_impl : public interact_center
    {
     private:
      int _sample_rate;              // 输入采样率，用于把毫秒等待时长换算成采样点数量
      bool _start_btn;               // 开始按钮当前状态，用于检测上升沿启动
      bool _stop_btn;                // 停止按钮当前状态，用于检测上升沿停止
      float _wait_time_ms;           // 每个阶段的目标等待时间，单位毫秒
      bool _is_running;              // 整个实验流程是否正在运行
      size_t _samples_to_wait;       // 当前每个阶段应等待的总采样点数量
      size_t _wait_counter;          // 当前阶段已经累计等待的采样点数量
      int _state;                    // 状态机编号：0 空闲，1 第一阶段，2 第二阶段
      double _current_freq;          // 当前扫频频点，会通过消息端口发给外部频率控制对象
      
      /*
       * 函数说明：
       * 预留的状态机推进函数声明。
       * 当前实现里状态推进逻辑直接写在 work() 中，这个接口尚未使用。
       */
      void process_state_machine(int nitems);

      /*
       * 函数说明：
       * 进入第一阶段时发送配套控制命令。
       *
       * 第一阶段的策略是：
       * 1. 关闭第二组通路。
       * 2. 启动第一组通路。
       */
      void send_phase1_start();

      /*
       * 函数说明：
       * 进入第二阶段时发送配套控制命令。
       *
       * 第二阶段与第一阶段相反：
       * 1. 关闭第一组通路。
       * 2. 启动第二组通路。
       */
      void send_phase2_start();

      /*
       * 函数说明：
       * 停止所有被调度的子模块，确保系统恢复到静默态。
       */
      void send_all_stop();

      /*
       * 函数说明：
       * 把 _current_freq 打包成消息并发给频率控制端口。
       */
      void send_freq_command();

     public:
      interact_center_impl(int sample_rate, bool start_btn, bool stop_btn, float wait_time_ms);
      ~interact_center_impl();
      
      void set_start_btn(bool start_btn) override;
      void set_stop_btn(bool stop_btn) override;
      void set_wait_time_ms(float wait_time_ms) override;

      /*
       * 函数说明：
       * work() 是这个调度器真正推进状态机的地方。
       *
       * 它不关心输入样本值本身，只关心“这次处理了多少个样本”，
       * 因为这个数量就代表状态机前进了多长时间。
       */
      int work(
              int noutput_items,
              gr_vector_const_void_star &input_items,
              gr_vector_void_star &output_items
      );
    };

  } // namespace usrp_ble
} // namespace gr

#endif /* INCLUDED_USRP_BLE_INTERACT_CENTER_IMPL_H */
