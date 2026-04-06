/* -*- c++ -*- */
/*
 * Copyright 2026 lfy.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <gnuradio/io_signature.h>
#include "data_store_impl.h"
#include <sys/stat.h>
#include <iostream>
#include <cerrno>
#include <cstring>
#include <algorithm>
#include <cstdio>

/*
 * 文件说明：
 * 这个实现文件给出了 data_store_impl 的完整行为。
 *
 * 核心工作流程如下：
 * 1. 构造函数中注册消息端口，等待外部发送开始/停止命令。
 * 2. 收到 "store_start" 后，创建目录、打开新文件、开始写盘。
 * 3. work() 在每轮调度中把输入复数采样写入文件。
 * 4. 写到指定样本数后自动停掉，也可以通过 "store_stop" 手动停掉。
 *
 * 这个模块的设计目标很明确：做一个简单、可靠、容易串到实验流程里的采样记录器。
 */

namespace gr {
  namespace usrp_ble {

    using input_type = gr_complex;

    /*
     * 工厂函数说明：
     * 这是 GNU Radio 常见的 make() 包装，用于隐藏具体实现类并统一创建对象。
     */
    data_store::sptr
    data_store::make(int data_len, int skip_len, const std::string& path)
    {
      return gnuradio::make_block_sptr<data_store_impl>(
        data_len, skip_len, path); // 把外部配置参数原样传给具体实现类构造函数
    }

    // 构造函数：
    // 1. 声明该块为单输入、零输出的 sync_block
    // 2. 保存配置参数
    // 3. 注册消息输入端口，用于接收开始/停止保存命令
    data_store_impl::data_store_impl(int data_len, int skip_len, const std::string& path)
      : gr::sync_block("data_store",
              gr::io_signature::make(1 /* min inputs */, 1 /* max inputs */, sizeof(input_type)), // 单输入口，输入类型是 gr_complex
              gr::io_signature::make(0 /* min outputs */, 0 /*max outputs */, 0)),                // 零输出口，因为这是 sink block
        _data_len(data_len),            // 保存单次任务的目标样本数
        _skip_len(skip_len),            // 保存开始写有效样本前要跳过的前导样本数
        _path(path),                    // 保存目录路径
        _freq_index(-1),                // 默认没有收到频点元信息
        _repeat_index(-1),              // 默认没有收到重复元信息
        _is_saving(false),              // 初始状态下不保存
        _is_skipping(false),            // 初始时不处于跳过前导样本阶段
        _skipped_samples_count(0),      // 初始时前导样本跳过计数为 0
        _saved_samples_count(0),        // 初始时当前文件写入计数为 0
        _file_index(0)                  // 第一份文件从编号 0 开始
    {
        message_port_register_in(pmt::mp("command")); // 注册控制输入端口，名称为 "command"
        set_msg_handler(pmt::mp("command"),           // 为 "command" 端口绑定处理函数
                        boost::bind(&data_store_impl::handle_msg, this, boost::placeholders::_1));
    }

    // 析构时确保文件句柄关闭，避免缓冲区内容丢失。
    data_store_impl::~data_store_impl()
    {
        if (_file.is_open()) {
            _file.close(); // 析构前兜底关闭文件，确保缓冲区内容落盘
        }
    }

    void data_store_impl::handle_msg(pmt::pmt_t msg)
    {
        // 只处理符号类型消息，其他消息直接忽略。
        if (pmt::is_dict(msg)) {
            pmt::pmt_t cmd_key = pmt::intern("cmd");
            pmt::pmt_t cmd_val = pmt::dict_ref(msg, cmd_key, pmt::PMT_NIL);
            if (pmt::is_symbol(cmd_val) && pmt::symbol_to_string(cmd_val) == "store_start") {
                pmt::pmt_t freq_val = pmt::dict_ref(msg, pmt::intern("freq_index"), pmt::from_long(-1));
                pmt::pmt_t repeat_val = pmt::dict_ref(msg, pmt::intern("repeat_index"), pmt::from_long(-1));
                _freq_index = pmt::to_long(freq_val);
                _repeat_index = pmt::to_long(repeat_val);
                start_saving();
            }
        } else if (pmt::is_symbol(msg)) {
            std::string cmd = pmt::symbol_to_string(msg); // 把 PMT symbol 转成普通字符串命令
            if (cmd == "store_start") {
                _freq_index = -1;
                _repeat_index = -1;
                start_saving(); // 开始一次新的保存任务
            } else if (cmd == "store_stop") {
                stop_saving(); // 立即停止当前保存任务
            }
        }
    }

    bool data_store_impl::createDirectoryIfNotExists(const std::string& path_s)
    {
        // 先检查目标目录是否已经存在。
        struct stat info;                 // 保存 stat 结果，用于判断文件系统对象类型
        const char* path = path_s.data(); // 转成 C 风格字符串，便于调用 stat/mkdir

        if (stat(path, &info) == 0 && (info.st_mode & S_IFDIR)) {
            return true; // 目录已存在，直接视为成功
        }

        // 使用临时缓冲区逐级构造路径，例如：
        // /a/b/c -> 依次检查并创建 /a、/a/b、/a/b/c
        char tmp[256];      // 临时路径缓冲区，用于逐层截断路径
        char *p = nullptr;  // 遍历缓冲区中的字符，定位路径分隔符
        size_t len;         // 记录路径字符串长度

        snprintf(tmp, sizeof(tmp), "%s", path); // 拷贝原始路径到临时缓冲区
        len = strlen(tmp);                      // 计算路径长度
        if (tmp[len - 1] == '/') {
            tmp[len - 1] = 0; // 去掉末尾多余的斜杠，避免后续处理复杂化
        }

        for (p = tmp + 1; *p; p++) {
            if (*p == '/') {
                *p = 0; // 暂时截断字符串，把当前位置前面的内容当成一级目录
                if (stat(tmp, &info) != 0) {
                    if (mkdir(tmp, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) != 0) {
                        std::cerr << "Failed to create directory: " << tmp << " - " << strerror(errno) << std::endl;
                        return false;
                    }
                } else if (!(info.st_mode & S_IFDIR)) {
                    std::cerr << "Path exists but is not a directory: " << tmp << std::endl;
                    return false;
                }
                *p = '/'; // 恢复路径分隔符，继续处理下一层
            }
        }

        if (stat(tmp, &info) != 0) {
            if (mkdir(tmp, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) != 0) {
                std::cerr << "Failed to create directory: " << tmp << " - " << strerror(errno) << std::endl;
                return false;
            }
        } else if (!(info.st_mode & S_IFDIR)) {
            std::cerr << "Path exists but is not a directory: " << tmp << std::endl;
            return false;
        }

        std::cout << "Directory created successfully: " << path << std::endl; // 便于运行时确认目录创建成功
        return true;                                                          // 所有目录层级均已准备好
    }

    void data_store_impl::start_saving()
    {
        // 如果上一轮还没结束，先强制关闭，保证文件状态干净。
        if (_is_saving) {
            stop_saving(); // 如果上一次没收尾，先关闭旧文件，避免状态残留
        }
        
        // 开始写文件前，确保目标目录存在。
        if (!createDirectoryIfNotExists(_path)) {
            std::cerr << "Error: Failed to create/verify directory " << _path << std::endl;
            return; // 目录不可用时直接放弃本次保存
        }

        // 每次保存都生成一个新的文件名，避免覆盖历史数据。
        std::string filename;
        if (_freq_index >= 0 && _repeat_index >= 0) {
            char buf[64];
            std::snprintf(buf, sizeof(buf), "/data_f%02d_r%02d.bin", _freq_index, _repeat_index);
            filename = _path + buf;
        } else {
            filename = _path + "/data_" + std::to_string(_file_index++) + ".bin"; // 兼容旧控制消息时仍然使用自增文件名
        }

        _file.open(filename, std::ios::binary); // 以二进制模式打开文件，避免文本模式干扰
        if (_file.is_open()) {
            _is_saving = true;         // 打开成功后正式进入保存态
            _is_skipping = true;       // 每次开始新文件时，先进入前导过渡带跳过阶段
            _skipped_samples_count = 0;
            _saved_samples_count = 0;  // 新文件的计数重新从 0 开始
        } else {
             std::cerr << "Failed to open file: " << filename << std::endl;
        }
    }

    void data_store_impl::stop_saving()
    {
        // 停止保存时同时复位状态并关闭文件句柄。
        if (_is_saving) {
            _is_saving = false; // 先关闭保存状态，防止后续 work 继续写入
            if (_file.is_open()) {
                _file.close(); // 关闭底层文件句柄
            }
        }
    }

    int
    data_store_impl::work(int noutput_items,
        gr_vector_const_void_star &input_items,
        gr_vector_void_star &output_items)
    {
      auto in = static_cast<const input_type*>(input_items[0]); // 取得输入复数采样缓冲区

        if (_is_saving) {
          int consumed = 0; // 记录本轮输入中已经处理到的位置

          // 先丢弃固定数量的前导过渡样本，再开始记录有效样本。
          if (_is_skipping) {
              int skip_count = std::min(noutput_items, _skip_len - _skipped_samples_count);
              _skipped_samples_count += skip_count;
              consumed += skip_count;

              if (_skipped_samples_count >= _skip_len) {
                  _is_skipping = false;
              }
          }

          // 跳过前导段之后，才真正把样本写盘。
          if (!_is_skipping && consumed < noutput_items) {
              int count = noutput_items - consumed; // 当前批次剩余样本都可以参与写盘
              if (_saved_samples_count + count > _data_len) {
                  count = _data_len - _saved_samples_count; // 超出上限时，只写剩余需要的那部分
              }

              if (count > 0) {
                // 直接按原始 gr_complex 内存布局写入二进制文件，
                // 后处理读取时需要使用相同的数据类型解释。
                _file.write(reinterpret_cast<const char*>(in + consumed), count * sizeof(input_type)); // 从有效窗口起点开始写
                _saved_samples_count += count;                                                         // 累加已写入的有效样本数
              }
          }

          // 达到目标长度后自动结束当前文件写入。
          if (_saved_samples_count >= _data_len) {
              stop_saving(); // 当前文件已经满足长度要求，自动收尾
          }
      }

      // 该块没有输出口，因此这里返回的是“已消费输入样本数”。
      // 作为 sync sink，按 GNU Radio 约定直接返回 noutput_items 即可。
      return noutput_items; // 作为 sync sink，返回本轮消费的输入样本数
    }

  } /* namespace usrp_ble */
} /* namespace gr */
