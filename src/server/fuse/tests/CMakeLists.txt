# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# Deterministic unit test of the nodeid<->file-handle table; needs no
# /dev/fuse and runs everywhere the FUSE server builds.
add_executable(fuse_nodeid_test fuse_nodeid_test.c ../fuse_node_table.c)
target_link_libraries(fuse_nodeid_test chimera_common pthread)

add_test(chimera/fuse/nodeid_table fuse_nodeid_test)

# End-to-end tests need to actually establish kernel FUSE mounts, which the
# test environment may not permit (the CI configure and test containers
# differ, so this is probed at test time): fuse_probe checks /dev/fuse +
# mount capability and scripts/fuse_test_wrapper.sh turns its failure into a
# ctest skip -- or a hard failure under REQUIRE_FUSE_TESTS.  No netns or
# port lock: FUSE binds no ports and mountpoints live in private tempdirs.
add_executable(fuse_probe fuse_probe.c)

add_executable(fuse_posix_test fuse_posix_test.c)

add_executable(fuse_lock_test fuse_lock_test.c)

add_executable(fuse_coherence_test fuse_coherence_test.c)

add_executable(fuse_sync_test fuse_sync_test.c)

if(REQUIRE_FUSE_TESTS)
    set(_fuse_require 1)
else()
    set(_fuse_require 0)
endif()

set(_fuse_wrapper ${CMAKE_SOURCE_DIR}/scripts/fuse_test_wrapper.sh)
# CHIMERA_FUSE_URING_DEPTH: ring entries reserve ~1 MiB each per CPU queue, so
# the tests keep the depth low -- which fuse_lock_test also relies on to park
# more waiters on one queue than it has entries.
set(_fuse_env
    "FUSE_PROBE=$<TARGET_FILE:fuse_probe>;CHIMERA_REQUIRE_FUSE=${_fuse_require};CHIMERA_FUSE_URING_DEPTH=2")

# memfs only: the FUSE protocol path is backend-agnostic, and the linux
# backend cannot serve a container tempdir (overlayfs rejects
# name_to_handle_at) -- the same limitation the posix suites have.  The
# scripts take the backend as an argument, so other backends can be driven
# by hand on a capable host.
foreach(backend memfs)
    add_test(NAME chimera/fuse/mount_${backend}
             COMMAND ${_fuse_wrapper} bash
                     ${CMAKE_CURRENT_SOURCE_DIR}/fuse_mount_test.sh
                     $<TARGET_FILE:chimera> ${backend})

    set_tests_properties(chimera/fuse/mount_${backend} PROPERTIES
        SKIP_RETURN_CODE 77
        TIMEOUT 120
        ENVIRONMENT "${_fuse_env}")

    # Pinned to plain /dev/fuse reads so that path stays covered on hosts
    # whose fuse module offers io_uring; posix_uring_* covers the ring.
    add_test(NAME chimera/fuse/posix_${backend}
             COMMAND ${_fuse_wrapper} bash
                     ${CMAKE_CURRENT_SOURCE_DIR}/fuse_posix_test.sh
                     $<TARGET_FILE:chimera> ${backend}
                     $<TARGET_FILE:fuse_posix_test>
                     "uring_depth=0")

    set_tests_properties(chimera/fuse/posix_${backend} PROPERTIES
        SKIP_RETURN_CODE 77
        TIMEOUT 120
        ENVIRONMENT "${_fuse_env}")

    # The same assertions carried over FUSE-over-io_uring.  Skips unless the
    # fuse module was loaded with enable_uring=1.
    add_test(NAME chimera/fuse/posix_uring_${backend}
             COMMAND ${_fuse_wrapper} bash
                     ${CMAKE_CURRENT_SOURCE_DIR}/fuse_posix_test.sh
                     $<TARGET_FILE:chimera> ${backend}
                     $<TARGET_FILE:fuse_posix_test>)

    set_tests_properties(chimera/fuse/posix_uring_${backend} PROPERTIES
        SKIP_RETURN_CODE 77
        TIMEOUT 120
        ENVIRONMENT "${_fuse_env};FUSE_EXPECT_URING=1")

    # The same POSIX assertions with the kernel's page cache taken out of the
    # path entirely (direct_io), so forced direct I/O cannot quietly change
    # read/write/truncate semantics.  parallel_direct_writes rides along: it
    # is the combination a throughput deployment would actually run.
    add_test(NAME chimera/fuse/posix_directio_${backend}
             COMMAND ${_fuse_wrapper} bash
                     ${CMAKE_CURRENT_SOURCE_DIR}/fuse_posix_test.sh
                     $<TARGET_FILE:chimera> ${backend}
                     $<TARGET_FILE:fuse_posix_test>
                     "direct_io,parallel_direct_writes")

    set_tests_properties(chimera/fuse/posix_directio_${backend} PROPERTIES
        SKIP_RETURN_CODE 77
        TIMEOUT 120
        ENVIRONMENT "${_fuse_env}")
endforeach()

# Locks and cross-mount coherence run against TWO FUSE mounts of one share.
# fuse_coherence_test polls (proving the async invalidation pushes);
# fuse_sync_test never polls (proving the coherence=sync completion gates).
foreach(tbin fuse_lock_test fuse_coherence_test fuse_sync_test)
    add_test(NAME chimera/fuse/${tbin}
             COMMAND ${_fuse_wrapper} bash
                     ${CMAKE_CURRENT_SOURCE_DIR}/fuse_multimount_test.sh
                     $<TARGET_FILE:chimera>
                     $<TARGET_FILE:${tbin}>)

    set_tests_properties(chimera/fuse/${tbin} PROPERTIES
        SKIP_RETURN_CODE 77
        TIMEOUT 120
        ENVIRONMENT "${_fuse_env}")
endforeach()

# Blocking locks over io_uring: a parked SETLKW holds its ring entry until
# the conflicting lock goes away, which is what the replacement entries in
# fuse_uring.c exist for.
add_test(NAME chimera/fuse/fuse_lock_test_uring
         COMMAND ${_fuse_wrapper} bash
                 ${CMAKE_CURRENT_SOURCE_DIR}/fuse_multimount_test.sh
                 $<TARGET_FILE:chimera>
                 $<TARGET_FILE:fuse_lock_test>)

set_tests_properties(chimera/fuse/fuse_lock_test_uring PROPERTIES
    SKIP_RETURN_CODE 77
    TIMEOUT 120
    ENVIRONMENT "${_fuse_env};FUSE_EXPECT_URING=1")

get_property(_fuse_tests DIRECTORY PROPERTY TESTS)
if(_fuse_tests)
    set_property(TEST ${_fuse_tests} APPEND PROPERTY LABELS fuse)
endif()

# In-process harness (no /dev/fuse, no mount, no privileges).
add_subdirectory(quint)
