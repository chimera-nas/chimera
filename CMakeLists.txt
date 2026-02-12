# SPDX-FileCopyrightText: 2024 - 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

cmake_minimum_required(VERSION 3.22)

option(IO_URING_ENABLED "Enable io_uring support" ON)
option(CAIRN_ENABLED "Enable cairn support" ON)

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
        add_definitions(-DHAVE_IO_URING=1)
        message(STATUS "IO_URING enabled")
    else()
        message(FATAL_ERROR "io_uring enabled but library not found.")
    endif()
else()
    message(STATUS "IO_URING disabled")
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
find_program(QEMU_EXECUTABLE qemu-system-x86_64)

set(KVM_TESTING_AVAILABLE FALSE)

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
        message(STATUS "KVM testing: qemu-system-x86_64 not found")
    endif()
endif()

if(KVM_TESTING_AVAILABLE)
    message(STATUS "KVM testing enabled")
    add_subdirectory(kvm)
else()
    message(STATUS "KVM testing disabled")
endif()

add_subdirectory(ext)

include_directories(ext/libevpl/include)

add_definitions(-DXDR_CUSTOM_IOVEC=${CHIMERA_EXT_SRC}/libevpl/src/rpc2/xdr_iovec.h)
add_definitions(-DXDR_CUSTOM_DUMP=1)

add_subdirectory(src)
