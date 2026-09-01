# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# These are the model-based tests, so they make up the quick tier that plain
# `ctest` runs.  See the test-tier block in the top-level CMakeLists.txt.
set(CHIMERA_TEST_TIER quick)

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
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=smb2_smoke_probe_memfs"
    LABELS "quint;smb_mbt;memfs"
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
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=smb2_oplock_probe_memfs"
    LABELS "quint;smb_mbt;memfs"
    TIMEOUT 90)

# Durable/persistent-handle ground-truth probe: pins the durable grant matrix,
# the granted Timeout policy, park/reclaim across a transport drop, the reclaim
# denial matrix, v1 (DHnQ/DHnC) handling, expiry, replay eligibility,
# FSCTL_LMR_REQUEST_RESILIENCY and persistent handles on a CA share.
add_executable(smb2_durable_probe smb2_durable_probe.c)
target_include_directories(smb2_durable_probe PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(smb2_durable_probe chimera_server chimera_metrics)

add_test(NAME chimera/server/smb/mbt/durable_probe_memfs
    COMMAND $<TARGET_FILE:smb2_durable_probe>)
set_tests_properties(chimera/server/smb/mbt/durable_probe_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=smb2_durable_probe_memfs"
    LABELS "quint;smb_mbt;memfs"
    TIMEOUT 120)

# Replay / ChannelSequence ground-truth probe: pins the exactly-once layer --
# the command-sequence window, what makes a CREATE replayable, the four
# create_guid classification outcomes, and the ChannelSequence accept/reject
# window at every checked operation.  Every exactly-once claim is asserted on
# the EFFECT (file content, size, open count), never on the status alone.
add_executable(smb2_replay_probe smb2_replay_probe.c)
target_include_directories(smb2_replay_probe PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(smb2_replay_probe chimera_server chimera_metrics)

add_test(NAME chimera/server/smb/mbt/replay_probe_memfs
    COMMAND $<TARGET_FILE:smb2_replay_probe>)
set_tests_properties(chimera/server/smb/mbt/replay_probe_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=smb2_replay_probe_memfs"
    LABELS "quint;smb_mbt;memfs"
    TIMEOUT 120)

# Wire-protection ground-truth probe: walks a matrix of connection profiles
# (dialect ceiling x NTLMv2 auth x signing algorithm x cipher) and asserts a
# full session works over each.  This is the only test that reaches the
# server's protection layer at all -- signing, SMB3 key derivation, the
# TRANSFORM header, 3.1.1 negotiate contexts and algorithm selection, and real
# NTLMv2 credential validation -- which every other test crosses unprotected.
add_executable(smb2_wire_probe smb2_wire_probe.c)
target_include_directories(smb2_wire_probe PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(smb2_wire_probe chimera_server chimera_metrics crypto)

add_test(NAME chimera/server/smb/mbt/wire_probe_memfs
    COMMAND $<TARGET_FILE:smb2_wire_probe>)
set_tests_properties(chimera/server/smb/mbt/wire_probe_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=smb2_wire_probe_memfs"
    LABELS "quint;smb_mbt;memfs"
    TIMEOUT 180)

# FSCTL ground-truth probe: server-side copy, copy-offload tokens, block
# cloning, reparse points and the sparse-file trio.  No trace in the corpus
# emits an IOCTL, so this is the only test that reaches src/server/smb's
# largest dark region.  Every assertion is on the FSCTL's effect (bytes at a
# destination, a range reading back as zeroes, a link target surviving a round
# trip), never on the status alone.
add_executable(smb2_fsctl_probe smb2_fsctl_probe.c)
target_include_directories(smb2_fsctl_probe PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(smb2_fsctl_probe chimera_server chimera_metrics crypto)

add_test(NAME chimera/server/smb/mbt/fsctl_probe_memfs
    COMMAND $<TARGET_FILE:smb2_fsctl_probe>)
set_tests_properties(chimera/server/smb/mbt/fsctl_probe_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=smb2_fsctl_probe_memfs"
    LABELS "quint;smb_mbt;memfs"
    TIMEOUT 120)

# Information-class probe: the QUERY_INFO / SET_INFO surface.  The corpus asks
# for one file-info class and sets three, so most of the class table and the
# smb_attr.h marshallers never run.  The probe sweeps every implemented class
# on both a file and a directory, then checks the classes AGAINST EACH OTHER --
# a marshaller that writes the right byte count into the wrong field passes a
# size check and fails an agreement check.
add_executable(smb2_info_probe smb2_info_probe.c)
target_include_directories(smb2_info_probe PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(smb2_info_probe chimera_server chimera_metrics crypto)

add_test(NAME chimera/server/smb/mbt/info_probe_memfs
    COMMAND $<TARGET_FILE:smb2_info_probe>)
set_tests_properties(chimera/server/smb/mbt/info_probe_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=smb2_info_probe_memfs"
    LABELS "quint;smb_mbt;memfs"
    TIMEOUT 120)

# Transport-compression ground-truth probe: the MS-XCA codecs (LZ77, LZNT1,
# LZ77+Huffman) against Microsoft reference vectors, and the whole
# COMPRESSION_TRANSFORM path -- negotiate context, algorithm selection, the
# per-connection compression context, unchained and chained framing -- over a
# real 3.1.1 connection.  No trace can reach any of it: compression is a
# property of the connection, which the model does not describe, and the
# server compresses only a READ reply that actually shrinks while the corpus
# reads 1-4 bytes at a time.  The probe picks its own payload, so it can force
# real compressed traffic and assert it HAPPENED, not merely that it was
# negotiated.
add_executable(smb2_compress_probe smb2_compress_probe.c)
target_include_directories(smb2_compress_probe PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(smb2_compress_probe chimera_server chimera_smb
    chimera_metrics crypto)

add_test(NAME chimera/server/smb/mbt/compress_probe_memfs
    COMMAND $<TARGET_FILE:smb2_compress_probe>)
set_tests_properties(chimera/server/smb/mbt/compress_probe_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=smb2_compress_probe_memfs"
    LABELS "quint;smb_mbt;memfs"
    TIMEOUT 120)

# ---------------------------------------------------------------------------
# Trace replay (ROADMAP-SMB.md Increment 3).  A single smb2_mbt_replay process
# opens the server + client evpl once per CAPABILITY PROFILE (the corpus spans
# several generation instances -- smb2Base has leases off, smb2Durable has
# durable on -- so the replayer groups adjacent traces by profile and restarts
# the server when it changes) and replays every trace in the corpus, standing up a
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
# The whole generated corpus in one ctest, not a single case.  ext/specs
# generates every instance and flavor unconditionally -- nothing is withheld on
# account of what chimera does with it -- so this runs ~70 traces at depth
# 300-500, each standing up and tearing down its own filesystem, against an
# ASAN build on a loaded container.
#
# The batches chimera cannot drive yet are declined BY NAME in
# smb2_mbt_deviations.h (smb2_mbt_trace_limits), each with the chimera bug that
# would retire the entry, and reported as SKIP lines.  SKIP_RETURN_CODE turns a
# run where nothing was drivable into a ctest SKIP rather than a false pass.
set_tests_properties(chimera/server/smb/mbt/batch_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=smb2_batch_memfs"
    LABELS "quint;smb_mbt;memfs"
    TIMEOUT 1800
    SKIP_RETURN_CODE 77)

# The same corpus over a PROTECTED connection: SMB 3.1.1, a real NTLMv2 logon,
# and every request signed with AES-GMAC.  The model says nothing about wire
# protection -- it is the same SMB2 conversation either way -- so the traces
# and their expected results are identical; what changes is that the server's
# signing path, 3.1.1 key derivation and negotiate-context handling are now
# crossed by every packet of every trace, including the compound replies and
# break notifications that no single-request probe reaches.  A divergence here
# that does not reproduce in batch_memfs is a protection-layer bug.
add_test(NAME chimera/server/smb/mbt/batch_memfs_signed
    COMMAND $<TARGET_FILE:smb2_mbt_replay> --trace-dir ${SPECS_BUNDLE_DIR}/smb2
            --wire signed311)
set_tests_properties(chimera/server/smb/mbt/batch_memfs_signed PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=smb2_batch_memfs_signed"
    LABELS "quint;smb_mbt;memfs"
    TIMEOUT 1800
    SKIP_RETURN_CODE 77)
