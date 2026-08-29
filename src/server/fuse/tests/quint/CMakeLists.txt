# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# These are the model-based tests, so they make up the quick tier that plain
# `ctest` runs.  See the test-tier block in the top-level CMakeLists.txt.
set(CHIMERA_TEST_TIER quick)

# In-process FUSE harness: drives the FUSE server over a socketpair standing
# in for /dev/fuse, so it needs no device, no mount, and no privileges and
# runs unconditionally rather than behind the fuse_probe capability gate.
add_executable(fuse_sim_probe fuse_sim_probe.c)
target_link_libraries(fuse_sim_probe chimera_server chimera_vfs chimera_common chimera_metrics)

add_test(NAME chimera/fuse/sim/probe COMMAND fuse_sim_probe)

set_tests_properties(chimera/fuse/sim/probe PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=fuse_sim_probe"
    TIMEOUT 60
    LABELS "fuse")

# JSON line-protocol driver: same contract as posix_quint_driver, but every
# operation executes as FUSE requests against the server over the socketpair.
# Lets posix_replay.py drive the identical POSIX trace corpus through the
# FUSE server -- coverage no existing corpus reaches.
add_executable(fuse_quint_driver fuse_quint_driver.c)
target_link_libraries(fuse_quint_driver
    chimera_server chimera_vfs chimera_common chimera_metrics jansson)

if(NOT SPECS_BUNDLE_AVAILABLE)
    message(STATUS "fuse MBT replay ctest skipped (no specs trace corpus)")
    return()
endif()

find_package(Python3 COMPONENTS Interpreter REQUIRED)

# One batched replay of the specs `posixFuse` corpus: the same POSIX model,
# the same replayer, and a driver that turns every operation into FUSE
# requests against the server.  The profile differs from posixMemfs only where
# the FUSE transport forces it to (no reflink ioctl, strict atime, no chgrp to
# a supplementary group), so a divergence here is the FUSE server's and not
# the model's or the backend's.
#
# Needs no /dev/fuse, no mount and no privileges -- the harness stands a
# socketpair in for the device -- so it runs everywhere the build does, ahead
# of the mount-backed tests behind the fuse_probe gate.
add_test(NAME chimera/fuse/mbt/batch_memfs
    COMMAND ${Python3_EXECUTABLE}
            ${CMAKE_SOURCE_DIR}/src/posix/tests/quint/posix_replay.py
            --backend fuse_memfs
            --driver $<TARGET_FILE:fuse_quint_driver>
            --trace-dir ${SPECS_BUNDLE_DIR}/posix
            --include-prefix fuse_)
set_tests_properties(chimera/fuse/mbt/batch_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=fuse_batch_memfs"
    LABELS "quint;fuse;posix_mbt;memfs"
    SKIP_RETURN_CODE 77
    TIMEOUT 600)
