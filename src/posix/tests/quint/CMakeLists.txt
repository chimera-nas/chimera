# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# These are the model-based tests, so they make up the quick tier that plain
# `ctest` runs.  See the test-tier block in the top-level CMakeLists.txt.
set(CHIMERA_TEST_TIER quick)

# POSIX model-based tests: the Quint specification (posix.qnt and friends) and
# its trace generation live in the chimera-nas/specs repo (submodule ext/specs).
# This directory owns the replay harness -- posix_mbt_replay.c (a pure-C
# replayer that runs the model oracle in-process against chimera's POSIX client
# over an in-process memfs mount) and posix_driver.c / posix_replay.py (the
# JSON line-protocol driver + Python replayer used for the NFS-loopback
# backends) -- which drive that corpus against chimera and fail on any
# unregistered divergence.
#
# The trace corpus is resolved once, at the top level, by cmake/SpecsBundle.cmake
# into SPECS_BUNDLE_DIR.  The specs `posix` suite generates every backend
# profile into one directory, distinguished by filename prefix (memfs has none;
# nfs3_/nfs4_/disk_ for the loopback and diskfs/cairn profiles); the replay
# selects a profile with --trace-dir plus --exclude-prefix.

add_executable(posix_quint_driver posix_driver.c)
target_link_libraries(posix_quint_driver
    chimera_posix chimera_client chimera_server chimera_metrics jansson)

# Pure-C replayer: reuses posix_driver.c's op engine in-process (no Python, no
# line-protocol subprocess) to load ITF traces and run the model oracle.
add_executable(posix_mbt_replay posix_mbt_replay.c)
target_include_directories(posix_mbt_replay PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(posix_mbt_replay
    chimera_posix chimera_client chimera_server chimera_metrics jansson)

if(NOT SPECS_BUNDLE_AVAILABLE)
    message(STATUS "posix MBT replay ctest skipped (no specs trace corpus)")
    return()
endif()

# One batched replay: a single posix_mbt_replay runs the model oracle in-process
# across the whole memfs corpus, cycling a fresh uniquely-named filesystem per
# trace ("newfs"), so the dominant per-trace cost -- driver server/client init +
# mount -- is paid once instead of once per trace.  Traces whose LInit profile
# does not match the backend skip individually; the batch reports SKIP (77) only
# if every trace skips.
#
# The specs `posix` corpus carries per-backend profiles distinguished by
# filename prefix: memfs (no prefix), the NFS loopbacks (nfs3_/nfs4_), the
# on-disk backends (disk_) and FUSE (fuse_).  Each registered batch selects
# its profile with --exclude-prefix.  Still unregistered: nfs4_ (the nfs4
# client needs its own round of the fixes that took nfs3 to green -- see the
# ND* registry in posix_mbt_replay.c) and disk_ (diskfs needs libaio;
# runs in the container/CI like the other diskfs batches).
add_test(NAME chimera/posix/mbt/batch_memfs
    COMMAND $<TARGET_FILE:posix_mbt_replay>
            --backend memfs
            --trace-dir ${SPECS_BUNDLE_DIR}/posix
            --exclude-prefix nfs3_ --exclude-prefix nfs4_ --exclude-prefix disk_
            --exclude-prefix fuse_)
set_tests_properties(chimera/posix/mbt/batch_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=posix_batch_memfs"
    LABELS "quint;posix_mbt;memfs"
    SKIP_RETURN_CODE 77
    TIMEOUT 300)

# The same corpus replayed through the NFS loopback: an in-process chimera
# server exports a memfs filesystem over the inproc transport and the posix
# client mounts it through the nfs proxy module (src/vfs/nfs) with vers=3 --
# the whole client stack, wire included, under the POSIX model.  The nfs3_
# profile pins the NFS-flavored capabilities (strict atime, no copy/clone/
# seek offload); the client-behavior deviations a real NFS client cannot
# avoid (silly rename and friends) reconcile in the replayer as the ND*
# registry.  The per-trace recycle unmounts the client, cycles the
# server-side filesystem, and remounts, so every trace also exercises the
# proxy's mount/umount paths.
add_test(NAME chimera/posix/mbt/batch_nfs3_memfs
    COMMAND $<TARGET_FILE:posix_mbt_replay>
            --backend nfs3_memfs
            --trace-dir ${SPECS_BUNDLE_DIR}/posix
            --exclude-prefix step --exclude-prefix disk_
            --exclude-prefix fuse_ --exclude-prefix nfs4_)
set_tests_properties(chimera/posix/mbt/batch_nfs3_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=posix_batch_nfs3_memfs"
    LABELS "quint;posix_mbt;memfs;nfs3_mbt"
    SKIP_RETURN_CODE 77
    TIMEOUT 600)

# The same POSIX surface through the NFSv4.1 loopback: stateful opens,
# sessions/slots, and the pseudo-fs export junction all sit on the path.
# The nfs4_ trace profile models search permissions, so the DAC answers
# are asserted directly rather than reconciled.
add_test(NAME chimera/posix/mbt/batch_nfs4_memfs
    COMMAND $<TARGET_FILE:posix_mbt_replay>
            --backend nfs4_memfs
            --trace-dir ${SPECS_BUNDLE_DIR}/posix
            --exclude-prefix step --exclude-prefix disk_
            --exclude-prefix fuse_ --exclude-prefix nfs3_)
set_tests_properties(chimera/posix/mbt/batch_nfs4_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=posix_batch_nfs4_memfs"
    LABELS "quint;posix_mbt;memfs;nfs4_mbt"
    SKIP_RETURN_CODE 77
    TIMEOUT 600)
