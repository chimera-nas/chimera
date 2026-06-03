# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

set(ELBENCHO_TEST_BINARY ${CMAKE_CURRENT_BINARY_DIR})

# elbencho host binary built in the devcontainer at /elbencho (see Dockerfile).
find_program(ELBENCHO_BIN elbencho PATHS /elbencho/bin /usr/local/bin)

# Wrapper script for running tests in a network namespace. Although the plugin
# drives the VFS in-process (no networking), initializing the chimera VFS brings
# up the io_uring module, whose queue allocation needs an unlimited RLIMIT_MEMLOCK
# (and a raised fs.aio-max-nr); the wrapper provides those and handles the ASan
# LD_PRELOAD for the test process. Mirrors src/fio/tests.
set(NETNS_WRAPPER "${CMAKE_SOURCE_DIR}/scripts/netns_test_wrapper.sh")

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/memfs_basic.json
               ${CMAKE_CURRENT_BINARY_DIR}/memfs_basic.json)

if(ELBENCHO_BIN AND CHIMERA_NETNS_TESTING)
    # Sequential write -> read with integrity verification (--verify), which proves
    # byte correctness through the plugin. iodepth 1 exercises the synchronous
    # pread/pwrite path; iodepth 8 exercises the async aio_submit/aio_reap path.
    add_test(NAME chimera/elbencho/memfs_seq_verify_iodepth1
        COMMAND ${NETNS_WRAPPER} ${ELBENCHO_BIN}
            --backend-plugin ${ELBENCHO_PLUGIN_PATH}
            --backend-plugin-config ${CMAKE_CURRENT_BINARY_DIR}/memfs_basic.json
            -w -r -t 1 -b 128k -s 4m --iodepth 1 --verify 12345
            /memfs/elbencho_seq1)

    add_test(NAME chimera/elbencho/memfs_seq_verify_iodepth8
        COMMAND ${NETNS_WRAPPER} ${ELBENCHO_BIN}
            --backend-plugin ${ELBENCHO_PLUGIN_PATH}
            --backend-plugin-config ${CMAKE_CURRENT_BINARY_DIR}/memfs_basic.json
            -w -r -t 1 -b 128k -s 8m --iodepth 8 --verify 12345
            /memfs/elbencho_seq2)

    # Random-offset file mode (the classic iodepth workload), multi-thread.
    add_test(NAME chimera/elbencho/memfs_rand_iodepth8
        COMMAND ${NETNS_WRAPPER} ${ELBENCHO_BIN}
            --backend-plugin ${ELBENCHO_PLUGIN_PATH}
            --backend-plugin-config ${CMAKE_CURRENT_BINARY_DIR}/memfs_basic.json
            -w -r --rand -t 2 -b 128k -s 4m --iodepth 8
            /memfs/elbencho_rand_a /memfs/elbencho_rand_b)

    # Dir mode (many files): mkdirs -> write -> stat -> read -> delete files+dirs,
    # with integrity verification, exercising the plugin metadata ops + data path.
    add_test(NAME chimera/elbencho/memfs_dirmode_verify
        COMMAND ${NETNS_WRAPPER} ${ELBENCHO_BIN}
            --backend-plugin ${ELBENCHO_PLUGIN_PATH}
            --backend-plugin-config ${CMAKE_CURRENT_BINARY_DIR}/memfs_basic.json
            -d -w -r --stat --delfiles --deldirs -t 4 -n 4 -N 16 -s 128k -b 16k
            --iodepth 4 --verify 999
            /memfs)

    # Exercise the size/preallocate ops (--trunctosize -> op_ftruncate,
    # --preallocfile -> op_fallocate), with integrity verification.
    add_test(NAME chimera/elbencho/memfs_prealloc_verify
        COMMAND ${NETNS_WRAPPER} ${ELBENCHO_BIN}
            --backend-plugin ${ELBENCHO_PLUGIN_PATH}
            --backend-plugin-config ${CMAKE_CURRENT_BINARY_DIR}/memfs_basic.json
            -d -w -r --trunctosize --preallocfile -t 2 -n 2 -N 8 -s 128k -b 16k
            --iodepth 4 --verify 555
            /memfs)

    set(ELBENCHO_TEST_NAMES
        chimera/elbencho/memfs_seq_verify_iodepth1
        chimera/elbencho/memfs_seq_verify_iodepth8
        chimera/elbencho/memfs_rand_iodepth8
        chimera/elbencho/memfs_dirmode_verify
        chimera/elbencho/memfs_prealloc_verify)

    # diskfs over io_uring is a genuinely asynchronous backend (unlike the in-memory
    # memfs, which completes inline), so it's the one that proves the plugin keeps
    # multiple chimera ops in flight at once. verify_async.sh asserts the plugin's
    # reported peak concurrency reaches the requested iodepth.
    if(IO_URING_ENABLED)
        configure_file(${CMAKE_CURRENT_SOURCE_DIR}/diskfs_io_uring.json
                       ${CMAKE_CURRENT_BINARY_DIR}/diskfs_io_uring.json)

        # iodepth 16 random workload: require peak concurrency >= 8 (fully async proof).
        add_test(NAME chimera/elbencho/diskfs_async_concurrency
            COMMAND ${NETNS_WRAPPER} bash ${CMAKE_CURRENT_SOURCE_DIR}/verify_async.sh 8 ${ELBENCHO_BIN}
                --backend-plugin ${ELBENCHO_PLUGIN_PATH}
                --backend-plugin-config ${CMAKE_CURRENT_BINARY_DIR}/diskfs_io_uring.json
                -w -r --rand -t 1 -b 64k -s 64m --iodepth 16
                /diskfs/elbencho_async)

        # integrity verification on the async backend (correctness + peak >= 4).
        add_test(NAME chimera/elbencho/diskfs_verify
            COMMAND ${NETNS_WRAPPER} bash ${CMAKE_CURRENT_SOURCE_DIR}/verify_async.sh 4 ${ELBENCHO_BIN}
                --backend-plugin ${ELBENCHO_PLUGIN_PATH}
                --backend-plugin-config ${CMAKE_CURRENT_BINARY_DIR}/diskfs_io_uring.json
                -w -r -t 2 -b 128k -s 32m --iodepth 8 --verify 4242
                /diskfs/elbencho_v1 /diskfs/elbencho_v2)

        # both diskfs tests share one device image (reformatted on each run), so don't
        # let ctest run them concurrently.
        set_tests_properties(
            chimera/elbencho/diskfs_async_concurrency
            chimera/elbencho/diskfs_verify
            PROPERTIES RESOURCE_LOCK "elbencho_diskfs_image")

        list(APPEND ELBENCHO_TEST_NAMES
            chimera/elbencho/diskfs_async_concurrency
            chimera/elbencho/diskfs_verify)
    endif()

    # In Debug builds the plugin .so is ASan-instrumented but the elbencho host
    # is not, so preload the ASan runtime (the netns wrapper restores LD_PRELOAD
    # for the test process only). Mirrors src/fio/tests.
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

        if(ASAN_LIB)
            set_tests_properties(${ELBENCHO_TEST_NAMES}
                PROPERTIES ENVIRONMENT
                "LD_PRELOAD=${ASAN_LIB};ASAN_OPTIONS=detect_leaks=0")
        endif()
    endif()
endif()
