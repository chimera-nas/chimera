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
# filename prefix: memfs (no prefix, and also what diskfs replays), the NFS
# loopbacks (nfs3_/nfs4_), the no-clone profile (disk_, shared by cairn and
# the linux/io_uring passthroughs) and FUSE (fuse_).  Each registered batch
# selects its profile with --exclude-prefix.
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

# The memfs-profile corpus against diskfs: a chimera-native backend
# (model-conformant DAC, no host-kernel deviations), so it replays the memfs
# profile rather than the disk_ one, and the per-trace newfs recycle
# mkfs-cycles it exactly like memfs.  The replay process self-provisions its
# scratch device image under its own mkdtemp session dir (posix_env_setup) and
# takes whichever libevpl block backend the platform has
# (CHIMERA_DISKFS_DEVICE_TYPE).  Five traces are declined BY NAME in
# posix_mbt_declines (posix_mbt_replay.c), each naming the diskfs bug that
# retires the entry.
#
add_test(NAME chimera/posix/mbt/batch_diskfs
    COMMAND $<TARGET_FILE:posix_mbt_replay>
            --backend diskfs
            --trace-dir ${SPECS_BUNDLE_DIR}/posix
            --exclude-prefix nfs3_ --exclude-prefix nfs4_ --exclude-prefix disk_
            --exclude-prefix fuse_)
set_tests_properties(chimera/posix/mbt/batch_diskfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=posix_batch_diskfs"
    LABELS "quint;posix_mbt;diskfs"
    SKIP_RETURN_CODE 77
    TIMEOUT 600)

# cairn, the other on-disk backend, completing the engine-backend matrix.  It
# is the one engine backend the memfs profile does not fit: cairn advertises
# neither CHIMERA_VFS_CAP_COPY_RANGE nor CHIMERA_VFS_CAP_CLONE_RANGE, and
# while the VFS supplies a generic copy fallback for the former there is no
# clone fallback -- clone answers ENOTSUP.  So cairn replays the `disk_`
# profile (posix_run.qnt's posixDisk instance, F_CLONE_RANGE = FeatOff), the
# same profile the linux/io_uring passthrough batches below replay, and
# selects it the same way: exclude the unprefixed memfs traces (they all
# begin "step") and the three prefixed profiles, leaving disk_ alone.
#
# diskfs stays on the memfs profile above rather than joining cairn here: it
# does implement clone, so holding it to the model-conformant profile is what
# exercises that path (and is what found the clone defects behind its two
# remaining declines).
add_test(NAME chimera/posix/mbt/batch_cairn
    COMMAND $<TARGET_FILE:posix_mbt_replay>
            --backend cairn
            --trace-dir ${SPECS_BUNDLE_DIR}/posix
            --exclude-prefix step --exclude-prefix nfs3_
            --exclude-prefix nfs4_ --exclude-prefix fuse_)
set_tests_properties(chimera/posix/mbt/batch_cairn PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=posix_batch_cairn"
    LABELS "quint;posix_mbt;cairn"
    SKIP_RETURN_CODE 77
    TIMEOUT 600)

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

# The disk_ profile replayed against the host-passthrough backends.  The
# backing store is a fresh directory per trace under $CHIMERA_MBT_SCRATCH
# (default: the build tree), whose filesystem must support name_to_handle_at
# -- tmpfs/overlayfs do not, and the driver turns that ENOTSUP into a clean
# SKIP.  The modules run with per-operation credential impersonation, so the
# batch needs root (CI containers; skipped otherwise via the mount failure).
if(CHIMERA_LINUX)
    add_test(NAME chimera/posix/mbt/batch_linux
        COMMAND $<TARGET_FILE:posix_mbt_replay>
                --backend linux
                --trace-dir ${SPECS_BUNDLE_DIR}/posix
                --exclude-prefix step --exclude-prefix nfs3_
                --exclude-prefix nfs4_ --exclude-prefix fuse_)
    set_tests_properties(chimera/posix/mbt/batch_linux PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=posix_batch_linux"
        LABELS "quint;posix_mbt;linux"
        SKIP_RETURN_CODE 77
        TIMEOUT 600)
endif()

if(CHIMERA_VFS_IO_URING)
    add_test(NAME chimera/posix/mbt/batch_io_uring
        COMMAND $<TARGET_FILE:posix_mbt_replay>
                --backend io_uring
                --trace-dir ${SPECS_BUNDLE_DIR}/posix
                --exclude-prefix step --exclude-prefix nfs3_
                --exclude-prefix nfs4_ --exclude-prefix fuse_)
    set_tests_properties(chimera/posix/mbt/batch_io_uring PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=posix_batch_io_uring"
        LABELS "quint;posix_mbt;io_uring"
        SKIP_RETURN_CODE 77
        TIMEOUT 600)
endif()

# The NFS loopback profiles replayed with a passthrough backend under the
# server: the full client stack (posix client -> nfs proxy -> wire -> chimera
# NFS server) on top of the host-kernel filesystem semantics.  The backing
# store and skip behavior are as for the direct passthrough batches above.
set(POSIX_MBT_PT_LOOPBACKS "")
if(CHIMERA_LINUX)
    list(APPEND POSIX_MBT_PT_LOOPBACKS linux)
endif()
if(CHIMERA_VFS_IO_URING)
    list(APPEND POSIX_MBT_PT_LOOPBACKS io_uring)
endif()

foreach(ptmod ${POSIX_MBT_PT_LOOPBACKS})
    add_test(NAME chimera/posix/mbt/batch_nfs3_${ptmod}
        COMMAND $<TARGET_FILE:posix_mbt_replay>
                --backend nfs3_${ptmod}
                --trace-dir ${SPECS_BUNDLE_DIR}/posix
                --exclude-prefix step --exclude-prefix disk_
                --exclude-prefix fuse_ --exclude-prefix nfs4_)
    set_tests_properties(chimera/posix/mbt/batch_nfs3_${ptmod} PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=posix_batch_nfs3_${ptmod}"
        LABELS "quint;posix_mbt;${ptmod};nfs3_mbt"
        SKIP_RETURN_CODE 77
        TIMEOUT 600)

    add_test(NAME chimera/posix/mbt/batch_nfs4_${ptmod}
        COMMAND $<TARGET_FILE:posix_mbt_replay>
                --backend nfs4_${ptmod}
                --trace-dir ${SPECS_BUNDLE_DIR}/posix
                --exclude-prefix step --exclude-prefix disk_
                --exclude-prefix fuse_ --exclude-prefix nfs3_)
    set_tests_properties(chimera/posix/mbt/batch_nfs4_${ptmod} PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=posix_batch_nfs4_${ptmod}"
        LABELS "quint;posix_mbt;${ptmod};nfs4_mbt"
        SKIP_RETURN_CODE 77
        TIMEOUT 600)
endforeach()
