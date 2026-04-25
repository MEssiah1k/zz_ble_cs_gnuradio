/*
 * Copyright 2026 Free Software Foundation, Inc.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <pybind11/complex.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace py = pybind11;

#include <gnuradio/usrp_ble/interact_center_rfhop.h>
#define D(...) ""

void bind_interact_center_rfhop(py::module& m)
{
    using interact_center_rfhop = gr::usrp_ble::interact_center_rfhop;

    py::class_<interact_center_rfhop,
               gr::sync_block,
               gr::block,
               gr::basic_block,
               std::shared_ptr<interact_center_rfhop>>(
        m, "interact_center_rfhop", D(interact_center_rfhop))
        .def(py::init(&interact_center_rfhop::make),
             py::arg("sample_rate"),
             py::arg("start_btn"),
             py::arg("stop_btn"),
             py::arg("wait_time_ms"),
             py::arg("settle_time_ms"),
             py::arg("repeat_total"),
             py::arg("start_freq_index"),
             py::arg("stop_freq_index"),
             py::arg("step_hz"),
             D(interact_center_rfhop, make))
        .def("set_start_btn",
             &interact_center_rfhop::set_start_btn,
             py::arg("start_btn"),
             D(interact_center_rfhop, set_start_btn))
        .def("set_stop_btn",
             &interact_center_rfhop::set_stop_btn,
             py::arg("stop_btn"),
             D(interact_center_rfhop, set_stop_btn))
        .def("set_wait_time_ms",
             &interact_center_rfhop::set_wait_time_ms,
             py::arg("wait_time_ms"),
             D(interact_center_rfhop, set_wait_time_ms))
        .def("set_settle_time_ms",
             &interact_center_rfhop::set_settle_time_ms,
             py::arg("settle_time_ms"),
             D(interact_center_rfhop, set_settle_time_ms))
        .def("set_start_freq_index",
             &interact_center_rfhop::set_start_freq_index,
             py::arg("start_freq_index"),
             D(interact_center_rfhop, set_start_freq_index))
        .def("set_stop_freq_index",
             &interact_center_rfhop::set_stop_freq_index,
             py::arg("stop_freq_index"),
             D(interact_center_rfhop, set_stop_freq_index))
        .def("set_step_hz",
             &interact_center_rfhop::set_step_hz,
             py::arg("step_hz"),
             D(interact_center_rfhop, set_step_hz));
}
