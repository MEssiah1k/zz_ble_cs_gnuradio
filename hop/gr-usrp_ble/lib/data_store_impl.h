/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#ifndef INCLUDED_USRP_BLE_DATA_STORE_IMPL_H
#define INCLUDED_USRP_BLE_DATA_STORE_IMPL_H

#include <gnuradio/usrp_ble/data_store.h>
#include <fstream>

/*
 * 文件说明：
 * 这个头文件声明了 data_store 模块的具体实现类 data_store_impl。
 *
 * 模块定位：
 * 1. 它是一个 GNU Radio sink block，只接收输入流，不产生输出流。
 * 2. 它的主要用途是在运行过程中接收消息命令，然后把一段固定长度的
 *    gr_complex 采样保存到磁盘文件里。
 * 3. 它通常会被上层控制模块驱动，例如在某个实验阶段开始时收到
 *    "store_start" 消息，结束后收到 "store_stop" 消息。
 *
 * 设计要点：
 * 1. 文件写入动作不是一直开启，而是受消息端口控制。
 * 2. 每次开始保存都会新建文件，不覆盖旧文件。
 * 3. 保存到指定样本数后会自动停止，避免无限制写盘。
 */

namespace gr {
  namespace usrp_ble {

    /*
     * 类说明：
     * data_store_impl 继承自公开接口 data_store，是实际执行保存逻辑的类。
     *
     * 这个类内部维护了完整的“单次保存任务”状态，包括：
     * 1. 当前是否正在保存。
     * 2. 当前已经写了多少个样本。
     * 3. 当前打开的是哪个文件。
     * 4. 下一个文件应使用什么编号。
     *
     * 从外部看，这个块很像一个“带开关的采样记录器”。
     */
    class data_store_impl : public data_store
    {
     private:
      int _data_len;                 // 单次保存任务的目标长度，达到该值后自动停止保存
      int _skip_len;                 // 开始写有效样本前，先跳过多少个前导样本
      std::string _path;             // 数据目录路径，生成的 .bin 文件会保存在这个目录下
      int _freq_index;               // 当前文件所属的频点编号，由 interact_center 通过消息下发
      int _repeat_index;             // 当前文件所属的重复编号，由 interact_center 通过消息下发
      int _capture_group_index;       // 当前文件所属的采集组编号，由 interact_center 通过消息下发
      int _capture_group_filter;      // 只响应指定采集组；-1 表示不过滤
      bool _is_saving;               // 当前是否处于写盘状态，false 时只消费输入不写文件
      bool _is_skipping;             // 当前是否仍处于前导过渡带跳过阶段
      int _skipped_samples_count;    // 当前这一个文件中已经跳过了多少个前导样本
      int _saved_samples_count;      // 当前这一个文件中已经累计写入了多少个采样点
      std::ofstream _file;           // 当前输出文件流对象，负责实际的二进制写盘
      int _file_index;               // 兜底自增编号，仅在没有显式元信息时用于文件名

      /*
       * 函数说明：
       * 检查目录是否存在；如果不存在，则按层级递归创建。
       *
       * 这个函数存在的原因是：
       * 1. 实验运行时不能假设目标目录已经提前建好。
       * 2. 路径可能包含多级子目录，不能只 mkdir 最后一层。
       * 3. 如果路径已存在但不是目录，需要及时报错并终止保存。
       */
      bool createDirectoryIfNotExists(const std::string& path_s);

      /*
       * 函数说明：
       * 处理输入消息端口 "command" 上收到的控制命令。
       *
       * 当前支持两类符号命令：
       * 1. "store_start"：开始一次新的保存任务。
       * 2. "store_stop"：提前中止当前保存任务。
       */
      void handle_msg(pmt::pmt_t msg);

      /*
       * 函数说明：
       * 创建新文件并切换到保存状态。
       *
       * 这个函数会完成三件事：
       * 1. 确认目录存在。
       * 2. 生成新的文件名。
       * 3. 打开文件并清零计数器。
       */
      void start_saving();

      /*
       * 函数说明：
       * 关闭当前文件并退出保存状态。
       *
       * 这个函数既可能是被外部停止命令触发，
       * 也可能是在达到目标保存长度后自动触发。
       */
      void stop_saving();

     public:
      data_store_impl(int data_len, int skip_len, const std::string& path, int capture_group_filter = -1);
      ~data_store_impl();

      /*
       * 函数说明：
       * work() 是 GNU Radio 每次调度该 block 时都会调用的主处理函数。
       *
       * 它的行为非常明确：
       * 1. 如果当前没有保存任务，就直接消费输入数据。
       * 2. 如果当前处于保存状态，就把输入流原样写入文件。
       * 3. 如果写满规定长度，就自动调用 stop_saving()。
       */
      int work(
              int noutput_items,
              gr_vector_const_void_star &input_items,
              gr_vector_void_star &output_items
      );
    };

  } // namespace usrp_ble
} // namespace gr

#endif /* INCLUDED_USRP_BLE_DATA_STORE_IMPL_H */
