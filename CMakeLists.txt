# SPDX-FileCopyrightText: 2024 - 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

cmake_minimum_required(VERSION 3.22)

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

# CUDA is enabled later with enable_language(), once the platform gate below
# has run and the toolkit probe has decided whether there is one to enable.
project(chimera LANGUAGES C)

# Platform gate.  Chimera is developed on Linux and a good deal of the stack --
# io_uring, the cairn/linux VFS backends' Linux-only syscalls, netns-isolated
# tests, KVM guest suites -- has no macOS equivalent.  Darwin is supported as a
# development/CI platform for the portable core (VFS framework, memfs, the NFS
# and S3 protocol servers, the unit tests), so everything Linux-only defaults
# off there rather than hard-failing configure.
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(CHIMERA_LINUX TRUE)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(CHIMERA_LINUX FALSE)

    # Homebrew keeps its libraries (and keg-only ones such as openssl@3 and
    # krb5) outside the default search paths.  Mirror what libevpl does so
    # find_library/find_path and the compiler locate them.
    if(EXISTS /opt/homebrew)
        set(HOMEBREW_PREFIX /opt/homebrew)
    else()
        set(HOMEBREW_PREFIX /usr/local)
    endif()

    list(APPEND CMAKE_PREFIX_PATH
        ${HOMEBREW_PREFIX}
        ${HOMEBREW_PREFIX}/opt/openssl@3
        ${HOMEBREW_PREFIX}/opt/krb5
        ${HOMEBREW_PREFIX}/opt/nghttp2)

    list(APPEND CMAKE_LIBRARY_PATH ${HOMEBREW_PREFIX}/lib ${HOMEBREW_PREFIX}/opt/krb5/lib)
    list(APPEND CMAKE_INCLUDE_PATH ${HOMEBREW_PREFIX}/include ${HOMEBREW_PREFIX}/opt/krb5/include)

    # Apple ships bison 2.3 (2006) in /usr/bin; the ndrzcc grammar needs the
    # %code directive, which is bison 3.  Prefer Homebrew's keg-only bison.
    list(APPEND CMAKE_PROGRAM_PATH ${HOMEBREW_PREFIX}/opt/bison/bin ${HOMEBREW_PREFIX}/opt/flex/bin)

    # pkg-config needs to be pointed at the keg-only .pc files explicitly.
    set(ENV{PKG_CONFIG_PATH}
        "${HOMEBREW_PREFIX}/opt/krb5/lib/pkgconfig:${HOMEBREW_PREFIX}/opt/openssl@3/lib/pkgconfig:$ENV{PKG_CONFIG_PATH}")

    include_directories(SYSTEM ${HOMEBREW_PREFIX}/include ${HOMEBREW_PREFIX}/opt/krb5/include)
    link_directories(${HOMEBREW_PREFIX}/lib ${HOMEBREW_PREFIX}/opt/krb5/lib)

    # chimera_vfs links its backend modules and the backends call back into the
    # VFS core, so the shared-library graph is deliberately cyclic; the same is
    # true of the protocol servers against chimera_server.  ELF resolves that at
    # load time, but ld64 requires every symbol to bind at link time.  Allow
    # shared libraries (only -- executables keep full link checking) to leave
    # those references to the dynamic loader, matching the ELF behaviour.
    string(APPEND CMAKE_SHARED_LINKER_FLAGS " -Wl,-undefined,dynamic_lookup")
else()
    message(FATAL_ERROR "Unsupported build platform ${CMAKE_SYSTEM_NAME}")
endif()

# Platform-dependent system libraries.  On Linux dlopen() lives in libdl, the
# unwinder in libunwind and uuid_generate() in util-linux's libuuid; Darwin
# ships all three inside libSystem, where they need no -l flag (and there is no
# libunwind.tbd to link against in the first place).
if(CHIMERA_LINUX)
    set(CHIMERA_DL_LIB dl)
    set(CHIMERA_UNWIND_LIB unwind)
    set(CHIMERA_UUID_LIB uuid)
else()
    set(CHIMERA_DL_LIB "")
    set(CHIMERA_UNWIND_LIB "")
    set(CHIMERA_UUID_LIB "")
endif()

option(IO_URING_ENABLED "Enable io_uring support" ${CHIMERA_LINUX})
option(CAIRN_ENABLED "Enable cairn support" ON)
option(CHIMERA_VFS_IO_URING "Enable chimera VFS io_uring backend" ${CHIMERA_LINUX})
option(REQUIRE_NETNS_TESTS "Fail configure if network namespace tests cannot be enabled" OFF)
option(EVPL_IOVEC_PROFILE "Enable libevpl retained-iovec stack profiling" OFF)

# Build the product only: no test executables are compiled and no ctests are
# registered, here or in the bundled libevpl (which reads the same variable).
# Declared as a cache option so it shows up in ccmake/cmake-gui alongside the
# rest; it has always been settable with -DDISABLE_TESTS=ON.
option(DISABLE_TESTS "Do not build or register any tests" OFF)

# CUDA / GPUDirect Storage support.
#
# The only CUDA-dependent part of the tree is src/cufile: a cuFile-compatible
# shim over the chimera client plus a .cu test that drives it.  Building it
# needs a full toolkit -- nvcc for the device sources, and the CUDA runtime and
# cuFile headers/libraries to link against -- so a host carrying just the
# driver, or a partial toolkit, is not enough.
#
# CUDA_SUPPORT is tri-state, matching KVM_TESTING further down:
#   AUTO (default) - enable iff a usable toolkit is present, else skip
#   ON             - require CUDA; hard fail if the toolkit is incomplete
#   OFF            - never build the CUDA bits
#
# This replaces a bare `if (EXISTS "/usr/local/cuda")`, which gave a host with
# an incomplete toolkit a confusing failure partway through the build and gave
# a host with a complete one no way to opt out.
set(CUDA_SUPPORT "AUTO" CACHE STRING "CUDA support: AUTO (enable iff available), ON (require), or OFF")
set_property(CACHE CUDA_SUPPORT PROPERTY STRINGS AUTO ON OFF)
string(TOUPPER "${CUDA_SUPPORT}" CUDA_SUPPORT)

set(CUDA_TOOLKIT_ROOT "/usr/local/cuda" CACHE PATH
    "CUDA toolkit installation to build against")

if(NOT CUDA_SUPPORT MATCHES "^(AUTO|ON|OFF)$")
    message(FATAL_ERROR "CUDA_SUPPORT must be AUTO, ON or OFF (got \"${CUDA_SUPPORT}\").")
endif()

set(CUDA_ENABLED FALSE)

if(CUDA_SUPPORT STREQUAL "OFF")
    message(STATUS "CUDA disabled (CUDA_SUPPORT=OFF)")
elseif(NOT CHIMERA_LINUX)
    # cuFile/GPUDirect Storage is Linux-only, so there is no toolkit to look
    # for here; leave ON fatal so a CI job that demands CUDA still fails loudly
    # if it is pointed at the wrong host.
    if(CUDA_SUPPORT STREQUAL "ON")
        message(FATAL_ERROR "CUDA_SUPPORT=ON but the host is ${CMAKE_SYSTEM_NAME}, not Linux.")
    endif()
    message(STATUS "CUDA disabled (host is ${CMAKE_SYSTEM_NAME}, not Linux)")
else()
    # Probe exactly the pieces src/cufile consumes, collecting whatever is
    # missing so the AUTO/ON message below can name it.  NO_DEFAULT_PATH keeps
    # the probe honest: a stray nvcc on PATH must not make us build against a
    # toolkit whose headers and libraries we then fail to find.
    find_program(CUDA_NVCC_EXECUTABLE nvcc
        HINTS ${CUDA_TOOLKIT_ROOT}/bin NO_DEFAULT_PATH)
    find_path(CUDA_CUFILE_INCLUDE_DIR cufile.h
        HINTS ${CUDA_TOOLKIT_ROOT}/include NO_DEFAULT_PATH)
    find_library(CUDA_CUFILE_LIBRARY cufile
        HINTS ${CUDA_TOOLKIT_ROOT}/lib64 ${CUDA_TOOLKIT_ROOT}/lib NO_DEFAULT_PATH)
    find_library(CUDA_CUDART_LIBRARY cudart
        HINTS ${CUDA_TOOLKIT_ROOT}/lib64 ${CUDA_TOOLKIT_ROOT}/lib NO_DEFAULT_PATH)

    set(CUDA_AVAILABLE TRUE)
    set(CUDA_UNAVAILABLE_REASON "")

    if(NOT CUDA_NVCC_EXECUTABLE)
        set(CUDA_AVAILABLE FALSE)
        string(APPEND CUDA_UNAVAILABLE_REASON " nvcc not found;")
    endif()

    if(NOT CUDA_CUFILE_INCLUDE_DIR)
        set(CUDA_AVAILABLE FALSE)
        string(APPEND CUDA_UNAVAILABLE_REASON " cufile.h not found;")
    endif()

    if(NOT CUDA_CUFILE_LIBRARY)
        set(CUDA_AVAILABLE FALSE)
        string(APPEND CUDA_UNAVAILABLE_REASON " libcufile not found;")
    endif()

    if(NOT CUDA_CUDART_LIBRARY)
        set(CUDA_AVAILABLE FALSE)
        string(APPEND CUDA_UNAVAILABLE_REASON " libcudart not found;")
    endif()

    if(CUDA_AVAILABLE)
        set(CUDA_ENABLED TRUE)
        set(CMAKE_CUDA_COMPILER ${CUDA_NVCC_EXECUTABLE})
        set(CMAKE_CUDA_ARCHITECTURES 80)
        message(STATUS "CUDA enabled (${CUDA_TOOLKIT_ROOT})")
    elseif(CUDA_SUPPORT STREQUAL "ON")
        message(FATAL_ERROR
            "CUDA_SUPPORT=ON but the CUDA toolkit at ${CUDA_TOOLKIT_ROOT} is "
            "incomplete:${CUDA_UNAVAILABLE_REASON} Install the missing pieces, point "
            "-DCUDA_TOOLKIT_ROOT at a complete toolkit, or re-run with "
            "-DCUDA_SUPPORT=AUTO/OFF.")
    else()
        message(STATUS "CUDA disabled (CUDA_SUPPORT=AUTO;${CUDA_UNAVAILABLE_REASON}). "
            "Pass -DCUDA_SUPPORT=ON to make missing prerequisites fatal.")
    endif()
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

# Turn CUDA on now that the toolkit has been probed and its compiler launcher
# picked.  Deferred to here rather than named in project() so that AUTO can
# decline gracefully instead of failing configure on a host with no toolkit.
if(CUDA_ENABLED)
    enable_language(CUDA)
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

# Coverage build: clang source-based code coverage. Instruments every TU (and,
# being directory-scoped, the bundled libevpl under ext/ too -- the report step
# filters those back out). The matching merge/report step lives in
# etc/coverage-report.sh, driven by the Makefile's test_coverage target.
if(CMAKE_BUILD_TYPE STREQUAL "Coverage")
    if(NOT CMAKE_C_COMPILER_ID MATCHES "Clang")
        message(FATAL_ERROR
            "Coverage build requires clang for source-based coverage "
            "(re-run with -DCMAKE_C_COMPILER=clang; the Makefile does this for you).")
    endif()
    message(STATUS "Enabling clang source-based code coverage")
    add_definitions(-O0 -fprofile-instr-generate -fcoverage-mapping -DCHIMERA_COVERAGE=1)
    add_link_options(-fprofile-instr-generate -fcoverage-mapping)
endif()

if(EVPL_IOVEC_PROFILE)
    message(STATUS "Enabling libevpl retained-iovec stack profiling")
    add_definitions(-DEVPL_IOVEC_PROFILE=1)
endif()

# Keep frame pointers in every build type so that perf and other
# frame-pointer-based profilers can unwind call stacks (the cost at -O3 is
# negligible and the unwind reliability is worth it for production profiling).
# Mirrors the same setting in the bundled libevpl.
add_definitions(-fno-omit-frame-pointer)

add_definitions(-g -Wall -Werror -Wno-unused-function)
add_definitions(-fvisibility=hidden)

# CMAKE_SYSTEM_PROCESSOR spells 64-bit ARM "aarch64" on Linux and "arm64" on
# Darwin, so match on the family rather than a single string.  CHIMERA_ARCH_ARM64
# is consumed further down to pick the xxhash SIMD dispatch.
if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(arm.*|ARM.*|aarch64.*)")
    set(CHIMERA_ARCH_ARM64 TRUE)
    set(MARCH_FLAG "-march=armv8-a")
else()
    set(CHIMERA_ARCH_ARM64 FALSE)
endif()

if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64)")
    set(MARCH_FLAG "-march=x86-64-v3")
endif()

# xxhash is used header-only (XXH_INLINE_ALL) by the VFS core and diskfs; pin
# its SIMD dispatch to what MARCH_FLAG above actually guarantees.
if(CHIMERA_ARCH_ARM64)
    set(CHIMERA_XXH_VECTOR XXH_NEON)
else()
    set(CHIMERA_XXH_VECTOR XXH_AVX2)
endif()

add_definitions(${MARCH_FLAG})

if(CMAKE_C_COMPILER_ID MATCHES "GNU")
    add_link_options(-Wl,--no-as-needed)
endif()

if(CMAKE_C_COMPILER_ID MATCHES "Clang")
    # GNU C constructs clang flags under -Werror where gcc -- the default
    # toolchain for Release/Debug -- stays silent.  The one customer is
    # pjd_acl_buf (pjd_common.h), which embeds the flexible-array-member
    # struct chimera_acl ahead of its backing storage, so the variable-sized
    # field is deliberately not last.  Waive that one diagnostic rather than
    # the whole -Wgnu group.  Added after -Werror above so it is not
    # re-promoted.
    #
    # Likewise tolerate clang/gcc warning-tooling differences in the test
    # sources: gcc-only `#pragma GCC diagnostic ignored` names clang doesn't
    # know (-Wunknown-warning-option, e.g. -Wstringop-truncation in
    # cthon_common.c) and clang-only diagnostics gcc lacks (-Wabsolute-value on
    # an unsigned abs() in fsx.c).
    add_definitions(-Wno-gnu-variable-sized-type-not-at-end
                    -Wno-unknown-warning-option -Wno-absolute-value)
endif()

if(NOT CHIMERA_LINUX)
    # Apple's <stdio.h> marks sprintf(3)/vsprintf(3) __deprecated, which -Werror
    # turns into ~90 hard errors across the S3 and SMB dump paths.  glibc does
    # not, so the Linux build never sees it.  Demote it here rather than let the
    # platform dictate a mass snprintf() migration; converting those call sites
    # is worth doing on its own terms, not as a side effect of the macOS port.
    # (This does not weaken -Wdeprecated-declarations on Linux.)
    add_definitions(-Wno-deprecated-declarations)
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

# KVM testing.
#
# The KVM-based suites (cthon/xfstests/pnfs) boot a real guest kernel via
# QEMU/KVM. Guest images are pulled (not built) from chimera-nas/kvm-test-base
# as OCI artifacts, so running them needs `oras` to fetch the images, an
# arch-appropriate qemu, /dev/kvm, and root.
#
# KVM_TESTING is tri-state:
#   AUTO (default) - enable iff KVM is available and we are root, else skip
#   ON             - require KVM testing; hard fail if it is not available
#   OFF            - never register the KVM suites
#
# CI sets ON for the devcontainer image (where KVM must work, so a regression
# in availability fails loudly) and OFF for the other images (which lack the
# prerequisites and should not probe). When the flag is not supplied at all the
# AUTO default makes normal users pick up KVM testing whenever their machine can
# actually run it.
set(KVM_TESTING "AUTO" CACHE STRING "KVM test suites: AUTO (enable iff available), ON (require), or OFF")
set_property(CACHE KVM_TESTING PROPERTY STRINGS AUTO ON OFF)
string(TOUPPER "${KVM_TESTING}" KVM_TESTING)

if(DISABLE_TESTS)
    # DISABLE_TESTS wins over an explicit KVM_TESTING=ON: the caller asked for
    # no tests at all, which is not the "KVM is missing" failure ON guards
    # against.
    message(STATUS "KVM testing disabled (DISABLE_TESTS=ON)")
elseif(KVM_TESTING STREQUAL "OFF")
    message(STATUS "KVM testing disabled (KVM_TESTING=OFF)")
elseif(NOT CHIMERA_LINUX)
    # No /dev/kvm and no guest images for non-Linux hosts; leave ON fatal so a
    # CI job that demands KVM still fails loudly if pointed at the wrong host.
    if(KVM_TESTING STREQUAL "ON")
        message(FATAL_ERROR "KVM_TESTING=ON but the host is ${CMAKE_SYSTEM_NAME}, not Linux.")
    endif()
    message(STATUS "KVM testing disabled (host is ${CMAKE_SYSTEM_NAME}, not Linux)")
else()
    # Probe the prerequisites and collect any that are missing.
    find_program(ORAS_EXECUTABLE oras)

    execute_process(COMMAND uname -m OUTPUT_VARIABLE HOST_ARCH OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(HOST_ARCH STREQUAL "aarch64")
        find_program(QEMU_EXECUTABLE qemu-system-aarch64)
    else()
        find_program(QEMU_EXECUTABLE qemu-system-x86_64)
    endif()

    execute_process(COMMAND id -u OUTPUT_VARIABLE HOST_UID OUTPUT_STRIP_TRAILING_WHITESPACE)

    set(KVM_TESTING_AVAILABLE TRUE)
    set(KVM_UNAVAILABLE_REASON "")

    if(NOT ORAS_EXECUTABLE)
        set(KVM_TESTING_AVAILABLE FALSE)
        string(APPEND KVM_UNAVAILABLE_REASON " oras not found;")
    endif()

    if(NOT QEMU_EXECUTABLE)
        set(KVM_TESTING_AVAILABLE FALSE)
        string(APPEND KVM_UNAVAILABLE_REASON " qemu not found for ${HOST_ARCH};")
    endif()

    if(NOT EXISTS "/dev/kvm")
        set(KVM_TESTING_AVAILABLE FALSE)
        string(APPEND KVM_UNAVAILABLE_REASON " /dev/kvm not found;")
    endif()

    if(NOT HOST_UID EQUAL 0)
        set(KVM_TESTING_AVAILABLE FALSE)
        string(APPEND KVM_UNAVAILABLE_REASON " not running as root;")
    endif()

    if(KVM_TESTING_AVAILABLE)
        message(STATUS "KVM testing enabled")
        add_subdirectory(kvm)
    elseif(KVM_TESTING STREQUAL "ON")
        message(FATAL_ERROR
            "KVM_TESTING=ON but KVM testing is not available:${KVM_UNAVAILABLE_REASON} "
            "Provide the missing prerequisites or re-run with -DKVM_TESTING=AUTO/OFF.")
    else()
        message(STATUS "KVM testing disabled (KVM_TESTING=AUTO;${KVM_UNAVAILABLE_REASON}). "
            "Pass -DKVM_TESTING=ON to make missing prerequisites fatal.")
    endif()
endif()

# Network namespace testing detection.
# Many tests run inside an isolated `ip netns` so they can bind well-known
# ports without colliding with each other or the host. Creating a netns
# requires CAP_NET_ADMIN, so a non-privileged build/test run would see a
# pile of failures. Probe whether we can create one; if not, the
# netns-using tests fall back to running on the shared host network,
# serialized under a ctest RESOURCE_LOCK (see chimera_add_netns_test below --
# the same scheme libevpl uses).
# `ip netns` is Linux-only, so on other platforms (macOS) skip the probe and
# fall back to serialized, non-isolated test execution.
if(CHIMERA_LINUX)
    execute_process(
        COMMAND bash -c "set -e; n=chimera_netns_probe_$$; ip netns add \"$n\"; ip netns delete \"$n\""
        RESULT_VARIABLE NETNS_PROBE_RESULT
        OUTPUT_QUIET
        ERROR_QUIET
    )
else()
    set(NETNS_PROBE_RESULT 1)
endif()

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
        "(no privileges to create netns; network tests run serialized on the "
        "host network). Pass -DREQUIRE_NETNS_TESTS=ON to make this fatal.")
endif()

# Forward result to libevpl so it doesn't probe again.
set(LIBEVPL_NETNS_TESTING ${CHIMERA_NETNS_TESTING})

# Raised-limit testing detection.
#
# Distinct from the netns probe above, and deliberately so.  Tests whose client
# and server share a process now talk over the in-process transport, so they
# bind no port and need no namespace to keep them apart -- but some VFS
# backends still need unlimited locked memory (io_uring queues) and a raised
# aio-max-nr (libaio contexts), which the netns wrapper had been supplying as a
# side effect of running privileged.
#
# Probing the two separately is what lets those tests run in an environment
# that grants the limits but not CAP_NET_ADMIN: previously the netns probe was
# the only gate, so no namespace meant no tests, whatever else was available.
execute_process(
    COMMAND bash -c "set -e; ulimit -l unlimited; test -w /proc/sys/fs/aio-max-nr"
    RESULT_VARIABLE LIMITS_PROBE_RESULT
    OUTPUT_QUIET
    ERROR_QUIET
)

if(NOT CHIMERA_LINUX)
    # The limits are Linux concepts backing Linux-only modules (io_uring
    # queues, libaio contexts), and no backend that needs them builds here, so
    # there is nothing to raise: the gate passes vacuously and the wrapper
    # degrades to a plain exec.  This is what lets the in-process suites run
    # on macOS.
    set(CHIMERA_LIMITS_TESTING TRUE)
    message(STATUS "Raised-limit testing enabled (no limit-needing backends on this platform)")
elseif(LIMITS_PROBE_RESULT EQUAL 0)
    set(CHIMERA_LIMITS_TESTING TRUE)
    message(STATUS "Raised-limit testing enabled")
else()
    set(CHIMERA_LIMITS_TESTING FALSE)
    message(STATUS "Raised-limit testing disabled "
        "(cannot raise memlock / aio-max-nr; backends needing them are skipped)")
endif()

# Register a test whose command needs loopback networking.  With netns
# available the command runs wrapped in an isolated namespace, so tests can
# bind well-known ports in parallel.  Without netns (macOS, or an unprivileged
# container) the test runs on the shared host network instead, serialized
# against every other such test through a ctest RESOURCE_LOCK -- the same
# fallback scheme libevpl uses for its tests (chimera_net here, evpl_net
# there).  Accepts an optional trailing WORKING_DIRECTORY <dir>.
function(chimera_add_netns_test name)
    cmake_parse_arguments(_netns "" "WORKING_DIRECTORY" "" ${ARGN})
    set(_wd "")
    if(_netns_WORKING_DIRECTORY)
        set(_wd WORKING_DIRECTORY ${_netns_WORKING_DIRECTORY})
    endif()
    if(CHIMERA_NETNS_TESTING)
        add_test(NAME ${name}
                 COMMAND ${CMAKE_SOURCE_DIR}/scripts/netns_test_wrapper.sh
                         ${_netns_UNPARSED_ARGUMENTS} ${_wd})
    else()
        add_test(NAME ${name} COMMAND ${_netns_UNPARSED_ARGUMENTS} ${_wd})
        set_tests_properties(${name} PROPERTIES RESOURCE_LOCK chimera_net)
    endif()
endfunction()

# PyNFS testing.  Every suite is registered, but only the combinations known
# to pass are actually run; the rest are marked DISABLED (see
# pynfs/CMakeLists.txt) so a normal `ctest` stays green while still listing the
# whole matrix.  Registration is additionally gated on /opt/pynfs being present
# so builds without the pynfs checkout (most non-devcontainer images) skip it.
option(CHIMERA_ENABLE_PYNFS_TESTS "Register PyNFS test suite ctests" ON)
find_package(Python3 COMPONENTS Interpreter QUIET)
if(NOT DISABLE_TESTS
    AND CHIMERA_ENABLE_PYNFS_TESTS
    AND Python3_FOUND
    AND EXISTS "/opt/pynfs/nfs4.0/testserver.py")
    message(STATUS "PyNFS testing enabled")
    add_subdirectory(pynfs)
else()
    message(STATUS "PyNFS testing disabled")
endif()

add_subdirectory(ext)

include_directories(ext/libevpl/include)

# OpenTelemetry tracing (oteltracing-c, built under libevpl/ext).  The public
# header is always available (it is header-only no-op when the library is not
# built).  When the library targets exist (protobuf-c present), wire chimera up
# to use it; otherwise compile every translation unit with OTEL_TRACING=0 so all
# embedded spans and span calls strip to nothing with no link dependency.
include_directories(ext/libevpl/ext/oteltracing-c)
if(TARGET oteltracing-c)
    message(STATUS "oteltracing-c found - chimera tracing enabled")
    add_compile_definitions(CHIMERA_HAVE_OTEL=1)
    set(CHIMERA_OTEL_LIBS oteltracing-c)
    if(TARGET oteltracing-sqlite)
        add_compile_definitions(CHIMERA_HAVE_OTEL_SQLITE=1)
        list(APPEND CHIMERA_OTEL_LIBS oteltracing-sqlite)
    endif()
    if(TARGET evpl_otel)
        add_compile_definitions(CHIMERA_HAVE_OTEL_GRPC=1)
        list(APPEND CHIMERA_OTEL_LIBS evpl_otel)
    endif()
else()
    message(STATUS "oteltracing-c not built - chimera tracing disabled")
    add_compile_definitions(OTEL_TRACING=0)
    set(CHIMERA_OTEL_LIBS "")
endif()

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

# MBT trace corpus (ext/specs): sets SPECS_BUNDLE_DIR / SPECS_BUNDLE_AVAILABLE
# for the quint suites under src/ -- must run before add_subdirectory(src).
if (NOT DISABLE_TESTS)
    include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/SpecsBundle.cmake)
endif()

add_subdirectory(src)
add_subdirectory(tools)
add_subdirectory(examples/vfs_module)
