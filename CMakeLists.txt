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

if (EXISTS "/elbencho/source/backend/elbencho_plugin.h")
    set(ELBENCHO_ENABLED true)
else()
    set(ELBENCHO_ENABLED false)
endif()

if (CUDA_ENABLED)
    project(chimera LANGUAGES C CUDA)
else()
    project(chimera LANGUAGES C)
endif()

# Use ccache as the compiler launcher when it is installed.  The cache location
# is left to ccache's own CCACHE_DIR env var (not hardcoded here): the
# devcontainer points it at the persistent /build/ccache volume so all worktrees
# share one cache, and CI additionally sets an HTTP remote (CCACHE_REMOTE_STORAGE
# -> the Nexus raw repo).  A plain local build with neither set falls back to
# ccache's default (~/.cache/ccache).
find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
    set(CMAKE_C_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
    if(CUDA_ENABLED)
        set(CMAKE_CUDA_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
    endif()
    message(STATUS "ccache enabled (${CCACHE_PROGRAM})")
else()
    message(STATUS "ccache not found; building without compiler cache")
endif()

set(CHIMERA_VERSION "0.1.0")

execute_process(COMMAND ln -sf ${CMAKE_BINARY_DIR}/compile_commands.json ${CMAKE_SOURCE_DIR}/compile_commands.json)

add_definitions(-DCHIMERA_VERSION="${CHIMERA_VERSION}")
add_definitions(-DCHIMERA_STATE_DIR="${CMAKE_INSTALL_PREFIX}/share/state")

# chimera uses userspace-RCU in the QSBR flavor process-wide.  <urcu/urcu-qsbr.h>
# clears the unprefixed RCU API map (rcu_read_lock, call_rcu, rcu_barrier, ...)
# at the end of the header unless URCU_API_MAP is defined, so we set it globally:
# any TU that (even transitively) includes a chimera RCU header -- the VFS caches,
# the mount table, the identity/cred caches -- needs the map kept active.  It is
# a no-op for TUs that include no urcu headers (e.g. the fio plugin).
add_definitions(-DURCU_API_MAP)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/ext/libevpl/ext/prometheus-c)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/ext/libevpl/ext/prometheus-c/ext/stopwatch)


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

# Keep frame pointers in every build type so that perf and other
# frame-pointer-based profilers can unwind call stacks (the cost at -O3 is
# negligible and the unwind reliability is worth it for production profiling).
# Mirrors the same setting in the bundled libevpl.
add_definitions(-fno-omit-frame-pointer)

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

if (ELBENCHO_ENABLED)
    message(STATUS "elbencho plugin enabled")
else()
    message(STATUS "elbencho plugin disabled")
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
#
# Guest images are pulled (not built) from chimera-nas/kvm-test-base as OCI
# artifacts, so KVM testing needs `oras` to fetch them plus QEMU and /dev/kvm
# to run them. Docker is no longer required at build time.
find_program(ORAS_EXECUTABLE oras)

# Find architecture-appropriate QEMU binary
execute_process(COMMAND uname -m OUTPUT_VARIABLE HOST_ARCH OUTPUT_STRIP_TRAILING_WHITESPACE)
if(HOST_ARCH STREQUAL "aarch64")
    find_program(QEMU_EXECUTABLE qemu-system-aarch64)
else()
    find_program(QEMU_EXECUTABLE qemu-system-x86_64)
endif()

set(KVM_TESTING_AVAILABLE FALSE)

if(ORAS_EXECUTABLE AND QEMU_EXECUTABLE)
    if(EXISTS "/dev/kvm")
        set(KVM_AVAILABLE TRUE)
    else()
        set(KVM_AVAILABLE FALSE)
        message(STATUS "KVM testing: /dev/kvm not found")
    endif()

    if(KVM_AVAILABLE)
        set(KVM_TESTING_AVAILABLE TRUE)
    endif()
else()
    if(NOT ORAS_EXECUTABLE)
        message(STATUS "KVM testing: oras not found")
    endif()
    if(NOT QEMU_EXECUTABLE)
        message(STATUS "KVM testing: QEMU not found for ${HOST_ARCH}")
    endif()
endif()

if(KVM_TESTING_AVAILABLE)
    message(STATUS "KVM testing enabled")
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

# PyNFS testing.  Every suite is registered, but only the combinations known
# to pass are actually run; the rest are marked DISABLED (see
# pynfs/CMakeLists.txt) so a normal `ctest` stays green while still listing the
# whole matrix.  Registration is additionally gated on /opt/pynfs being present
# so builds without the pynfs checkout (most non-devcontainer images) skip it.
option(CHIMERA_ENABLE_PYNFS_TESTS "Register PyNFS test suite ctests" ON)
find_package(Python3 COMPONENTS Interpreter QUIET)
if(CHIMERA_ENABLE_PYNFS_TESTS
    AND Python3_FOUND
    AND EXISTS "/opt/pynfs/nfs4.0/testserver.py")
    message(STATUS "PyNFS testing enabled")
    add_subdirectory(pynfs)
else()
    message(STATUS "PyNFS testing disabled")
endif()

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
