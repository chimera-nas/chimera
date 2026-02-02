# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

set(FIO_TEST_BINARY ${CMAKE_CURRENT_BINARY_DIR})

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/memfs_basic.fio ${CMAKE_CURRENT_BINARY_DIR}/memfs_basic.fio)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/demofs_basic.fio ${CMAKE_CURRENT_BINARY_DIR}/demofs_basic.fio)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/memfs_basic.json ${CMAKE_CURRENT_BINARY_DIR}/memfs_basic.json)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/demofs_basic.json ${CMAKE_CURRENT_BINARY_DIR}/demofs_basic.json)

add_test(NAME chimera/fio/fio_memfs_basic COMMAND /usr/local/bin/fio ${CMAKE_CURRENT_BINARY_DIR}/memfs_basic.fio)
add_test(NAME chimera/fio/fio_demofs_basic COMMAND /usr/local/bin/fio ${CMAKE_CURRENT_BINARY_DIR}/demofs_basic.fio)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")

    if(CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
        set(ASAN_LIB "/usr/lib/aarch64-linux-gnu/libasan.so.8")
    else()
        set(ASAN_LIB "/usr/lib/x86_64-linux-gnu/libasan.so.8")
    endif()

    if(NOT EXISTS ${ASAN_LIB})
        message(FATAL_ERROR "ASAN library not found at ${ASAN_LIB}")
    endif()

    set_tests_properties(chimera/fio/fio_memfs_basic PROPERTIES
        ENVIRONMENT "LD_PRELOAD=${ASAN_LIB};LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/suppressions.txt"
    )

    set_tests_properties(chimera/fio/fio_demofs_basic PROPERTIES
        ENVIRONMENT "LD_PRELOAD=${ASAN_LIB};LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/suppressions.txt"
    )
endif()