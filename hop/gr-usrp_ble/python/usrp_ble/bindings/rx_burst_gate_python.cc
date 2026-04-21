/*
 * Copyright 2026 Free Software Foundation, Inc.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <pybind11/pybind11.h>

namespace py = pybind11;

#include <gnuradio/usrp_ble/rx_burst_gate.h>
#define D(...) ""

void bind_rx_burst_gate(py::module& m)
{
    using rx_burst_gate = gr::usrp_ble::rx_burst_gate;

    py::class_<rx_burst_gate, gr::block, gr::basic_block,
               std::shared_ptr<rx_burst_gate>>(m, "rx_burst_gate", D(rx_burst_gate))
        .def(py::init(&rx_burst_gate::make),
             py::arg("num_channels"),
             py::arg("burst_len"),
             py::arg("skip_len") = 0,
             D(rx_burst_gate, make))
        .def("set_burst_len",
             &rx_burst_gate::set_burst_len,
             py::arg("burst_len"),
             D(rx_burst_gate, set_burst_len))
        .def("set_skip_len",
             &rx_burst_gate::set_skip_len,
             py::arg("skip_len"),
             D(rx_burst_gate, set_skip_len));
}
