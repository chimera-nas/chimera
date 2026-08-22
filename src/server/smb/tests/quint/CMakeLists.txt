# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# SMB2/3 model-based tests: the Quint specification (smb2.qnt and its pure
# smb2_state.qnt / smb2_ops.qnt modules, on top of the protocol-neutral
# filesystem core the NFSv4 model shares) and its trace generation live in the
# chimera-nas/specs repo (submodule ext/specs).  This directory owns the
# in-process C replay harness that drives that corpus against the live chimera
# SMB server, plus the ground-truth probes that need no corpus at all.
#
# The trace corpus is resolved once, at the top level, by cmake/SpecsBundle.cmake
# into SPECS_BUNDLE_DIR; the batched replay points --trace-dir at
# ${SPECS_BUNDLE_DIR}/smb2.

# ---------------------------------------------------------------------------
# The in-process SMB2 wire harness + smoke probe (ROADMAP-SMB.md Increment 0).
# Independent of any trace corpus: it embeds the chimera SMB server (memfs
# backend) over libevpl's inproc transport and drives it from a hand-rolled
# SMB2 client (smb2_mbt_common.h) -- the SMB sibling of nfs3_mbt_common.h.  SMB
# is not ONC-RPC/XDR, so the client hand-builds each PDU following the server's
# own parser rather than a generated stub.
# ---------------------------------------------------------------------------
add_executable(smb2_smoke_probe smb2_smoke_probe.c)
target_include_directories(smb2_smoke_probe PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(smb2_smoke_probe chimera_server chimera_metrics)

add_test(NAME chimera/server/smb/mbt/smoke_probe_memfs
    COMMAND $<TARGET_FILE:smb2_smoke_probe>)
set_tests_properties(chimera/server/smb/mbt/smoke_probe_memfs PROPERTIES
    LABELS "smb_mbt;memfs"
    TIMEOUT 60)

# Oplock/lease ground-truth probe (Increment 1): pins chimera's grant ->
# break -> acknowledge behavior against MS-SMB2/MS-FSA, two connections
# (holder + conflicting opener) on one in-process server.  Findings triaged
# into DEVIATIONS-SMB.md.
add_executable(smb2_oplock_probe smb2_oplock_probe.c)
target_include_directories(smb2_oplock_probe PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(smb2_oplock_probe chimera_server chimera_metrics)

add_test(NAME chimera/server/smb/mbt/oplock_probe_memfs
    COMMAND $<TARGET_FILE:smb2_oplock_probe>)
set_tests_properties(chimera/server/smb/mbt/oplock_probe_memfs PROPERTIES
    LABELS "smb_mbt;memfs"
    TIMEOUT 90)

# ---------------------------------------------------------------------------
# Trace replay (ROADMAP-SMB.md Increment 3).  A single smb2_mbt_replay process
# opens the server + client evpl once (capability profile taken from the first
# trace -- every trace in a batch shares it, since a profile is fixed per
# generation instance) and replays every trace in the corpus, standing up a
# fresh uniquely-named memfs per trace AND opening fresh connections/sessions
# per trace (smb2_conn_reset drops the prior trace's sessions/opens), so neither
# the filesystem nor the SMB session/tree/handle state leaks across traces.
# ---------------------------------------------------------------------------
add_executable(smb2_mbt_replay smb2_mbt_replay.c)
target_include_directories(smb2_mbt_replay PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(smb2_mbt_replay chimera_server chimera_metrics jansson)

if(NOT SPECS_BUNDLE_AVAILABLE)
    message(STATUS "smb MBT replay ctest skipped (no specs trace corpus)")
    return()
endif()

add_test(NAME chimera/server/smb/mbt/batch_memfs
    COMMAND $<TARGET_FILE:smb2_mbt_replay> --trace-dir ${SPECS_BUNDLE_DIR}/smb2)
set_tests_properties(chimera/server/smb/mbt/batch_memfs PROPERTIES
    LABELS "smb_mbt;memfs"
    TIMEOUT 120)
