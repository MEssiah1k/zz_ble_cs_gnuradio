# Copyright 2020 Free Software Foundation, Inc.
#
# This file is part of GNU Radio
#
# SPDX-License-Identifier: GPL-3.0-or-later
#

########################################################################
# Check if there is C++ code at all
########################################################################
if(NOT usrp_ble_sources)
    MESSAGE(STATUS "No C++ sources... skipping python bindings")
    return()
endif(NOT usrp_ble_sources)

########################################################################
# Check for pygccxml
########################################################################
GR_PYTHON_CHECK_MODULE_RAW(
    "pygccxml"
    "import pygccxml"
    PYGCCXML_FOUND
    )

include(GrPybind)

########################################################################
# Python Bindings
########################################################################

list(APPEND usrp_ble_python_files
    capture_gate_python.cc
    tx_rx_triggle_python.cc
    data_send_python.cc
    data_store_python.cc
    interact_center_python.cc
    interact_center_rfhop_python.cc
    channel_phase_python.cc
    random_phase_python.cc
    rx_burst_gate_python.cc
    tx_burst_gate_python.cc
    python_bindings.cc)

GR_PYBIND_MAKE_OOT(usrp_ble
   ../../..
   gr::usrp_ble
   "${usrp_ble_python_files}")

# copy in bindings .so file for use in QA test module
add_custom_target(
  copy_bindings_for_tests ALL
  COMMAND
    ${CMAKE_COMMAND} -E copy "${CMAKE_CURRENT_BINARY_DIR}/*.so"
    ${CMAKE_BINARY_DIR}/test_modules/gnuradio/usrp_ble/
  DEPENDS usrp_ble_python)

install(TARGETS usrp_ble_python DESTINATION ${GR_PYTHON_DIR}/gnuradio/usrp_ble COMPONENT pythonapi)
