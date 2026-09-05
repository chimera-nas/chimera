# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

set(FIO_TEST_BINARY ${CMAKE_CURRENT_BINARY_DIR})

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/memfs_basic.fio ${CMAKE_CURRENT_BINARY_DIR}/memfs_basic.fio)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/memfs_basic.json ${CMAKE_CURRENT_BINARY_DIR}/memfs_basic.json)

chimera_add_netns_test(chimera/fio/fio_memfs_basic /usr/local/bin/fio ${CMAKE_CURRENT_BINARY_DIR}/memfs_basic.fio)

if(IO_URING_ENABLED)
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/diskfs_io_uring_basic.fio ${CMAKE_CURRENT_BINARY_DIR}/diskfs_io_uring_basic.fio)
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/diskfs_io_uring_basic.json ${CMAKE_CURRENT_BINARY_DIR}/diskfs_io_uring_basic.json)
    chimera_add_netns_test(chimera/fio/fio_diskfs_io_uring_basic /usr/local/bin/fio ${CMAKE_CURRENT_BINARY_DIR}/diskfs_io_uring_basic.fio)
endif()

if(HAVE_LIBAIO)
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/diskfs_aio_basic.fio ${CMAKE_CURRENT_BINARY_DIR}/diskfs_aio_basic.fio)
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/diskfs_aio_basic.json ${CMAKE_CURRENT_BINARY_DIR}/diskfs_aio_basic.json)
    chimera_add_netns_test(chimera/fio/fio_diskfs_aio_basic /usr/local/bin/fio ${CMAKE_CURRENT_BINARY_DIR}/diskfs_aio_basic.fio)
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
        ENVIRONMENT "CHIMERA_TEST_LD_PRELOAD=${ASAN_LIB};LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/suppressions.txt"
    )

    if(IO_URING_ENABLED)
        set_tests_properties(chimera/fio/fio_diskfs_io_uring_basic PROPERTIES
            ENVIRONMENT "CHIMERA_TEST_LD_PRELOAD=${ASAN_LIB};LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/suppressions.txt"
        )
    endif()

    if(HAVE_LIBAIO)
        set_tests_properties(chimera/fio/fio_diskfs_aio_basic PROPERTIES
            ENVIRONMENT "CHIMERA_TEST_LD_PRELOAD=${ASAN_LIB};LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/suppressions.txt"
        )
    endif()
endif()
