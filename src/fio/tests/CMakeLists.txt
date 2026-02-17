# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

set(FIO_TEST_BINARY ${CMAKE_CURRENT_BINARY_DIR})

# Wrapper script for running tests in a network namespace
set(NETNS_WRAPPER "${CMAKE_SOURCE_DIR}/scripts/netns_test_wrapper.sh")

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/memfs_basic.fio ${CMAKE_CURRENT_BINARY_DIR}/memfs_basic.fio)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/memfs_basic.json ${CMAKE_CURRENT_BINARY_DIR}/memfs_basic.json)

add_test(NAME chimera/fio/fio_memfs_basic COMMAND ${NETNS_WRAPPER} /usr/local/bin/fio ${CMAKE_CURRENT_BINARY_DIR}/memfs_basic.fio)

if(IO_URING_ENABLED)
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/demofs_io_uring_basic.fio ${CMAKE_CURRENT_BINARY_DIR}/demofs_io_uring_basic.fio)
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/demofs_io_uring_basic.json ${CMAKE_CURRENT_BINARY_DIR}/demofs_io_uring_basic.json)
    add_test(NAME chimera/fio/fio_demofs_io_uring_basic COMMAND ${NETNS_WRAPPER} /usr/local/bin/fio ${CMAKE_CURRENT_BINARY_DIR}/demofs_io_uring_basic.fio)
endif()

if(HAVE_LIBAIO)
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/demofs_aio_basic.fio ${CMAKE_CURRENT_BINARY_DIR}/demofs_aio_basic.fio)
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/demofs_aio_basic.json ${CMAKE_CURRENT_BINARY_DIR}/demofs_aio_basic.json)
    add_test(NAME chimera/fio/fio_demofs_aio_basic COMMAND ${NETNS_WRAPPER} /usr/local/bin/fio ${CMAKE_CURRENT_BINARY_DIR}/demofs_aio_basic.fio)
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Debug")

    if(CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
        set(ASAN_LIB_DIRS "/usr/lib/aarch64-linux-gnu" "/usr/lib64")
    else()
        set(ASAN_LIB_DIRS "/usr/lib/x86_64-linux-gnu" "/usr/lib64")
    endif()

    set(ASAN_LIB "")
    foreach(ASAN_DIR IN LISTS ASAN_LIB_DIRS)
        file(GLOB ASAN_LIB "${ASAN_DIR}/libasan.so.[0-9]")
        if(ASAN_LIB)
            break()
        endif()
    endforeach()
    if(NOT ASAN_LIB)
        message(FATAL_ERROR "ASAN library not found in any of: ${ASAN_LIB_DIRS}")
    endif()

    set_tests_properties(chimera/fio/fio_memfs_basic PROPERTIES
        ENVIRONMENT "LD_PRELOAD=${ASAN_LIB};LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/suppressions.txt"
    )

    if(IO_URING_ENABLED)
        set_tests_properties(chimera/fio/fio_demofs_io_uring_basic PROPERTIES
            ENVIRONMENT "LD_PRELOAD=${ASAN_LIB};LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/suppressions.txt"
        )
    endif()

    if(HAVE_LIBAIO)
        set_tests_properties(chimera/fio/fio_demofs_aio_basic PROPERTIES
            ENVIRONMENT "LD_PRELOAD=${ASAN_LIB};LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/suppressions.txt"
        )
    endif()
endif()
