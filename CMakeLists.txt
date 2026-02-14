# SPDX-FileCopyrightText: 2024 - 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

cmake_minimum_required(VERSION 3.22)

option(IO_URING_ENABLED "Enable io_uring support" ON)
option(CAIRN_ENABLED "Enable cairn support" ON)
option(CHIMERA_VFS_IO_URING "Enable chimera VFS io_uring backend" ON)
option(REQUIRE_NETNS_TESTS "Fail configure if network namespace tests cannot be enabled" OFF)

if (EXISTS "/usr/local/cuda")
    set(CUDA_ENABLED true)
    set(CMAKE_CUDA_COMPILER /usr/local/cuda/bin/nvcc)
    set(CMAKE_CUDA_ARCHITECTURES 80)
else()
    set(CUDA_ENABLED false) 
endif()

if (EXISTS "/fio")
    set(FIO_ENABLED true)
else()
    set(FIO_ENABLED false)
endif()

if (CUDA_ENABLED)
    project(chimera LANGUAGES C CUDA)
else()
    project(chimera LANGUAGES C)
endif()

set(CHIMERA_VERSION "0.1.0")

execute_process(COMMAND ln -sf ${CMAKE_BINARY_DIR}/compile_commands.json ${CMAKE_SOURCE_DIR}/compile_commands.json)

add_definitions(-DCHIMERA_VERSION="${CHIMERA_VERSION}")
add_definitions(-DCHIMERA_STATE_DIR="${CMAKE_INSTALL_PREFIX}/share/state")

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/ext/libevpl/ext/prometheus-c)


set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if (NOT DISABLE_TESTS)
    enable_testing()
endif()

include_directories(3rdparty)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    message(STATUS "Enabling AddressSanitizer")
    add_definitions(-fsanitize=address -fno-omit-frame-pointer
                    -fno-optimize-sibling-calls -fstack-protector-all -DCHIMERA_SANITIZE=1)
    add_link_options(-fsanitize=address)

    message(STATUS "Enabling iovec reference tracing")
    add_definitions(-DEVPL_IOVEC_TRACE=1)
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    add_definitions(-O3)
endif()

add_definitions(-g -Wall -Werror -Wno-unused-function)
add_definitions(-fvisibility=hidden)

if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(arm.*|ARM.*|aarch64.*)")
    set(MARCH_FLAG "-march=armv8-a")
endif()

if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64)")
    set(MARCH_FLAG "-march=x86-64-v3")
endif()

add_definitions(${MARCH_FLAG})

if(CMAKE_C_COMPILER_ID MATCHES "GNU")
    add_link_options(-Wl,--no-as-needed)
endif()

if (CUDA_ENABLED)
    message(STATUS "CUDA enabled")
    #include_directories(${CUDA_INCLUDE_DIRS})
    #link_directories(${CUDA_LIBRARIES})
else()
    message(STATUS "CUDA disabled")
endif()

if (FIO_ENABLED)
    message(STATUS "FIO enabled")
else()
    message(STATUS "FIO disabled")
endif()

if (IO_URING_ENABLED)

    find_library(URING_LIB NAMES uring)
    find_path(URING_INCLUDE_DIR NAMES liburing/io_uring.h)

    if (URING_LIB AND URING_INCLUDE_DIR)
        message(STATUS "IO_URING enabled")
        if (CHIMERA_VFS_IO_URING)
            add_definitions(-DHAVE_IO_URING=1)
            message(STATUS "Chimera VFS io_uring backend enabled")
        else()
            message(STATUS "Chimera VFS io_uring backend disabled (libevpl io_uring still active)")
        endif()
    else()
        message(FATAL_ERROR "io_uring enabled but library not found.")
    endif()
else()
    message(STATUS "IO_URING disabled")
    set(CHIMERA_VFS_IO_URING OFF CACHE BOOL "Chimera VFS io_uring requires IO_URING_ENABLED" FORCE)
endif()

find_library(AIO_LIB NAMES aio)
if (AIO_LIB)
    set(HAVE_LIBAIO 1)
    message(STATUS "LIBAIO enabled")
else()
    message(STATUS "LIBAIO not found")
endif()

if (CAIRN_ENABLED)
    message(STATUS "CAIRN enabled")
    add_definitions(-DHAVE_CAIRN=1)
else()
    message(STATUS "CAIRN disabled")
endif()

# KVM testing detection
find_program(DOCKER_EXECUTABLE docker)

# Find architecture-appropriate QEMU binary
execute_process(COMMAND uname -m OUTPUT_VARIABLE HOST_ARCH OUTPUT_STRIP_TRAILING_WHITESPACE)
if(HOST_ARCH STREQUAL "aarch64")
    find_program(QEMU_EXECUTABLE qemu-system-aarch64)
else()
    find_program(QEMU_EXECUTABLE qemu-system-x86_64)
endif()

set(KVM_TESTING_AVAILABLE FALSE)
set(KVM_WINDOWS_TESTING_AVAILABLE FALSE)

if(DOCKER_EXECUTABLE AND QEMU_EXECUTABLE)
    execute_process(
        COMMAND docker info
        RESULT_VARIABLE DOCKER_INFO_RESULT
        OUTPUT_QUIET
        ERROR_QUIET
    )
    if(DOCKER_INFO_RESULT EQUAL 0)
        set(DOCKER_FUNCTIONAL TRUE)
    else()
        set(DOCKER_FUNCTIONAL FALSE)
        message(STATUS "KVM testing: docker found but not functional")
    endif()

    if(EXISTS "/dev/kvm")
        set(KVM_AVAILABLE TRUE)
    else()
        set(KVM_AVAILABLE FALSE)
        message(STATUS "KVM testing: /dev/kvm not found")
    endif()

    if(DOCKER_FUNCTIONAL AND KVM_AVAILABLE)
        set(KVM_TESTING_AVAILABLE TRUE)
    endif()
else()
    if(NOT DOCKER_EXECUTABLE)
        message(STATUS "KVM testing: docker not found")
    endif()
    if(NOT QEMU_EXECUTABLE)
        message(STATUS "KVM testing: QEMU not found for ${HOST_ARCH}")
    endif()
endif()

# Windows KVM tests require QEMU + KVM + WINDOWS_SERVER_ISO + OVMF
if(WINDOWS_SERVER_ISO AND QEMU_EXECUTABLE AND KVM_AVAILABLE)
    # Detect OVMF firmware for UEFI boot
    find_file(OVMF_CODE_PATH
        NAMES OVMF_CODE_4M.fd OVMF_CODE.fd
        PATHS /usr/share/OVMF /usr/share/edk2/ovmf /usr/share/qemu
        NO_DEFAULT_PATH
    )
    if(OVMF_CODE_PATH)
        set(KVM_WINDOWS_TESTING_AVAILABLE TRUE)
        message(STATUS "KVM Windows testing: OVMF found at ${OVMF_CODE_PATH}")
    else()
        message(STATUS "KVM Windows testing: OVMF firmware not found")
    endif()
endif()

# Enter kvm subdir if any KVM testing is available
if(KVM_TESTING_AVAILABLE OR KVM_WINDOWS_TESTING_AVAILABLE)
    if(KVM_TESTING_AVAILABLE)
        message(STATUS "KVM testing enabled (Linux)")
    endif()
    if(KVM_WINDOWS_TESTING_AVAILABLE)
        message(STATUS "KVM testing enabled (Windows)")
    endif()
    add_subdirectory(kvm)
else()
    message(STATUS "KVM testing disabled")
endif()

# Network namespace testing detection.
# Many tests run inside an isolated `ip netns` so they can bind well-known
# ports without colliding with each other or the host. Creating a netns
# requires CAP_NET_ADMIN, so a non-privileged build/test run would see a
# pile of failures. Probe whether we can create one; if not, the
# netns-using tests are silently skipped unless REQUIRE_NETNS_TESTS is ON
# (which CI should set to prevent accidental coverage loss).
execute_process(
    COMMAND bash -c "set -e; n=chimera_netns_probe_$$; ip netns add \"$n\"; ip netns delete \"$n\""
    RESULT_VARIABLE NETNS_PROBE_RESULT
    OUTPUT_QUIET
    ERROR_QUIET
)

if(NETNS_PROBE_RESULT EQUAL 0)
    set(CHIMERA_NETNS_TESTING TRUE)
    message(STATUS "Network namespace testing enabled")
else()
    set(CHIMERA_NETNS_TESTING FALSE)
    if(REQUIRE_NETNS_TESTS)
        message(FATAL_ERROR
            "REQUIRE_NETNS_TESTS=ON but `ip netns add` failed "
            "(need CAP_NET_ADMIN / root). Re-run with sudo, grant the "
            "capability, or omit -DREQUIRE_NETNS_TESTS=ON.")
    endif()
    message(STATUS "Network namespace testing disabled "
        "(no privileges to create netns; netns-requiring tests will be skipped). "
        "Pass -DREQUIRE_NETNS_TESTS=ON to make this fatal.")
endif()

# Forward result to libevpl so it doesn't probe again.
set(LIBEVPL_NETNS_TESTING ${CHIMERA_NETNS_TESTING})

add_subdirectory(ext)

include_directories(ext/libevpl/include)

# Find libwbclient (optional - for AD/Winbind integration)
find_package(PkgConfig QUIET)
if(PKG_CONFIG_FOUND)
    pkg_check_modules(WBCLIENT QUIET wbclient)
    if(WBCLIENT_FOUND)
        message(STATUS "libwbclient found - Winbind integration enabled")
        add_definitions(-DHAVE_WBCLIENT=1)
    else()
        message(STATUS "libwbclient not found - Winbind integration disabled")
    endif()
endif()

add_definitions(-DXDR_CUSTOM_IOVEC=${CHIMERA_EXT_SRC}/libevpl/src/rpc2/xdr_iovec.h)
add_definitions(-DXDR_CUSTOM_DUMP=1)

add_subdirectory(src)
