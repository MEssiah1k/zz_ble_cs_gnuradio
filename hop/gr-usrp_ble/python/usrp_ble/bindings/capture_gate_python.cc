/*
 * Copyright 2026 Free Software Foundation, Inc.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <pybind11/pybind11.h>

namespace py = pybind11;

#include <gnuradio/usrp_ble/capture_gate.h>
#define D(...) ""

void bind_capture_gate(py::module& m)
{
    using capture_gate = gr::usrp_ble::capture_gate;

    py::class_<capture_gate, gr::block, gr::basic_block, std::shared_ptr<capture_gate>>(
        m, "capture_gate", D(capture_gate))
        .def(py::init(&capture_gate::make), py::arg("num_channels"), D(capture_gate, make));
}
