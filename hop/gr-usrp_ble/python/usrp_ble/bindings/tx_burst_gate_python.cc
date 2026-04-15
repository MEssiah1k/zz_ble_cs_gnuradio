/*
 * Copyright 2026 Free Software Foundation, Inc.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <pybind11/pybind11.h>

namespace py = pybind11;

#include <gnuradio/usrp_ble/tx_burst_gate.h>
#define D(...) ""

void bind_tx_burst_gate(py::module& m)
{
    using tx_burst_gate = gr::usrp_ble::tx_burst_gate;

    py::class_<tx_burst_gate, gr::block, gr::basic_block,
               std::shared_ptr<tx_burst_gate>>(m, "tx_burst_gate", D(tx_burst_gate))
        .def(py::init(&tx_burst_gate::make),
             py::arg("num_channels"),
             py::arg("burst_len"),
             py::arg("add_uhd_tags") = true,
             D(tx_burst_gate, make))
        .def("set_burst_len",
             &tx_burst_gate::set_burst_len,
             py::arg("burst_len"),
             D(tx_burst_gate, set_burst_len));
}
