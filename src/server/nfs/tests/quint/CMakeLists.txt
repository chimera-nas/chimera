# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# NFS3/NFS4 model-based tests: the Quint specifications and their trace
# generation live in the chimera-nas/specs repo (submodule ext/specs); this
# directory owns only the C replay harness that drives that corpus against the
# live chimera server.  The replayers (nfs3_mbt_replay.c, nfs4_mbt_replay.c)
# embed the chimera server (memfs backend) in the test process and replay each
# trace over libevpl's inproc transport, failing on any divergence.  No test
# binds a port, needs a network namespace or resource lock, or spawns a daemon
# -- every trace runs fully parallel on any host.
#
# The trace corpus is resolved once, at the top level, by cmake/SpecsBundle.cmake
# into SPECS_BUNDLE_DIR (a fetched prebuilt bundle when the submodule is clean,
# else an in-tree build of ext/specs).  The replay ctests point --trace-dir at
# ${SPECS_BUNDLE_DIR}/nfs3 and /nfs4; the pure-C deviation/delegation probes need
# no corpus and always run.

# The C replayers + probes: chimera server + rpc2 client in one process over the
# inproc transport (see nfs3_mbt_common.h).  They build regardless of whether a
# trace corpus is available.
add_executable(nfs3_mbt_replay nfs3_mbt_replay.c)
target_include_directories(nfs3_mbt_replay PRIVATE
    ${CMAKE_BINARY_DIR}/src/nfs_common)
target_link_libraries(nfs3_mbt_replay
    chimera_server chimera_nfs_common jansson)

add_executable(nfs3_mbt_probe nfs3_mbt_probe.c)
target_include_directories(nfs3_mbt_probe PRIVATE
    ${CMAKE_BINARY_DIR}/src/nfs_common)
target_link_libraries(nfs3_mbt_probe
    chimera_server chimera_nfs_common jansson)

add_executable(nfs4_deleg_probe nfs4_deleg_probe.c)
target_include_directories(nfs4_deleg_probe PRIVATE
    ${CMAKE_BINARY_DIR}/src/nfs_common)
target_link_libraries(nfs4_deleg_probe
    chimera_server chimera_nfs_common jansson)

add_executable(nfs4_mbt_replay nfs4_mbt_replay.c)
target_include_directories(nfs4_mbt_replay PRIVATE
    ${CMAKE_BINARY_DIR}/src/nfs_common)
target_link_libraries(nfs4_mbt_replay
    chimera_server chimera_nfs_common jansson)

# Deviation probe: asserts each documented chimera deviation (DEVIATIONS.md)
# still reproduces, and that the two fixed deadlocks (LINK/RENAME self-alias)
# do not hang.  Goes red when chimera is fixed -- the signal to retire a
# deviation -- or if a deadlock regresses.  Needs no trace corpus.
add_test(NAME chimera/server/nfs/mbt/deviation_probe_memfs
    COMMAND $<TARGET_FILE:nfs3_mbt_probe>)
set_tests_properties(chimera/server/nfs/mbt/deviation_probe_memfs PROPERTIES
    LABELS "nfs3_mbt;memfs"
    TIMEOUT 60)

# Delegation policy probe: pins the deterministic grant/recall behavior the
# delegation-enabled instances encode (see nfs4_deleg_probe.c).  Needs no corpus.
add_test(NAME chimera/server/nfs/mbt4/deleg_probe_memfs
    COMMAND $<TARGET_FILE:nfs4_deleg_probe>)
set_tests_properties(chimera/server/nfs/mbt4/deleg_probe_memfs PROPERTIES
    LABELS "nfs4_mbt;multiproto;memfs"
    TIMEOUT 90)

if(NOT SPECS_BUNDLE_AVAILABLE)
    message(STATUS "nfs MBT replay ctests skipped (no specs trace corpus)")
    return()
endif()

# One batched replay per suite: a single replayer process opens the server +
# rpc2 client once and replays every trace in the corpus back-to-back, standing
# up a fresh, uniquely-named memfs (mkfs) per trace so the server's attr/name/
# open caches cannot leak state across traces -- isolation is preserved while
# the dominant per-process server/client init is amortized over the whole
# corpus.  The NFS4 replayer additionally cycles a fresh client identity per
# trace.  --trace-dir consumes every *.itf.json the specs build/bundle produced
# for the suite.
add_test(NAME chimera/server/nfs/mbt/batch_memfs
    COMMAND $<TARGET_FILE:nfs3_mbt_replay> --trace-dir ${SPECS_BUNDLE_DIR}/nfs3)
set_tests_properties(chimera/server/nfs/mbt/batch_memfs PROPERTIES
    LABELS "nfs3_mbt;memfs"
    TIMEOUT 300)

# SKIP_RETURN_CODE 77 still applies to NFS4: a trace whose capability profile
# does not match the live server exits 77 individually; the batch reports SKIP
# only if every trace skips, otherwise the runnable traces gate it.
add_test(NAME chimera/server/nfs/mbt4/batch_memfs
    COMMAND $<TARGET_FILE:nfs4_mbt_replay> --trace-dir ${SPECS_BUNDLE_DIR}/nfs4)
set_tests_properties(chimera/server/nfs/mbt4/batch_memfs PROPERTIES
    LABELS "nfs4_mbt;memfs"
    SKIP_RETURN_CODE 77
    TIMEOUT 300)
