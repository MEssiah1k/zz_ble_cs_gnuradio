find_package(PkgConfig)

PKG_CHECK_MODULES(PC_GR_USRP_BLE gnuradio-usrp_ble)

FIND_PATH(
    GR_USRP_BLE_INCLUDE_DIRS
    NAMES gnuradio/usrp_ble/api.h
    HINTS $ENV{USRP_BLE_DIR}/include
        ${PC_USRP_BLE_INCLUDEDIR}
    PATHS ${CMAKE_INSTALL_PREFIX}/include
          /usr/local/include
          /usr/include
)

FIND_LIBRARY(
    GR_USRP_BLE_LIBRARIES
    NAMES gnuradio-usrp_ble
    HINTS $ENV{USRP_BLE_DIR}/lib
        ${PC_USRP_BLE_LIBDIR}
    PATHS ${CMAKE_INSTALL_PREFIX}/lib
          ${CMAKE_INSTALL_PREFIX}/lib64
          /usr/local/lib
          /usr/local/lib64
          /usr/lib
          /usr/lib64
          )

include("${CMAKE_CURRENT_LIST_DIR}/gnuradio-usrp_bleTarget.cmake")

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(GR_USRP_BLE DEFAULT_MSG GR_USRP_BLE_LIBRARIES GR_USRP_BLE_INCLUDE_DIRS)
MARK_AS_ADVANCED(GR_USRP_BLE_LIBRARIES GR_USRP_BLE_INCLUDE_DIRS)
