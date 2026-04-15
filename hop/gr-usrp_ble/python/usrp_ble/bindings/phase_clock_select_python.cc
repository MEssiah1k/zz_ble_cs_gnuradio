/*
 * Copyright 2026 Free Software Foundation, Inc.
 *
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <pybind11/pybind11.h>

namespace py = pybind11;

#include <gnuradio/usrp_ble/phase_clock_select.h>
#define D(...) ""

void bind_phase_clock_select(py::module& m)
{
    using phase_clock_select = gr::usrp_ble::phase_clock_select;

    py::class_<phase_clock_select, gr::block, gr::basic_block,
               std::shared_ptr<phase_clock_select>>(
        m, "phase_clock_select", D(phase_clock_select))
        .def(py::init(&phase_clock_select::make), D(phase_clock_select, make));
}
