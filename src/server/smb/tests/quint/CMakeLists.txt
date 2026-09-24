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
# The trace corpus is CONFIG-DRIVEN.  configs/*.json each fix every constant
# the smb2 model declares (which caching features the server under test has,
# which of several legal behaviours it chose) and carry that corpus's batch
# list, so one config fully determines one corpus.  specs_corpus() registers
# each as a cell with its own trace directory, and the replay ctests point
# --trace-dir at those directories -- no filename-prefix carving.

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
    LABELS "functional;probe;smb_mbt;memfs"
    TIMEOUT 60)

# Hold a backend rename across TREE_DISCONNECT to exercise a completion after
# the requesting tree has released its share.
add_executable(smb2_rename_disconnect_probe smb2_rename_disconnect_probe.c)
target_include_directories(smb2_rename_disconnect_probe PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(smb2_rename_disconnect_probe chimera_server chimera_metrics
    chimera_vfs_memfs evpl)

add_test(NAME chimera/server/smb/mbt/rename_disconnect_probe_memfs
    COMMAND $<TARGET_FILE:smb2_rename_disconnect_probe>)
set_tests_properties(chimera/server/smb/mbt/rename_disconnect_probe_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=smb2_rename_disconnect_probe_memfs"
    LABELS "functional;probe;smb_mbt;memfs"
    TIMEOUT 60)

# Closing a lease holder must remove its cache before waking a pending CREATE.
add_executable(smb2_close_claim_probe smb2_close_claim_probe.c)
target_include_directories(smb2_close_claim_probe PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(smb2_close_claim_probe chimera_server chimera_smb chimera_vfs)
add_test(NAME chimera/server/smb/mbt/close_claim_probe
    COMMAND $<TARGET_FILE:smb2_close_claim_probe>)
set_tests_properties(chimera/server/smb/mbt/close_claim_probe PROPERTIES
    LABELS "functional;probe;smb_mbt" TIMEOUT 30)

# Same object-store caching key across distinct protocol lease records.
add_executable(smb2_lease_identity_probe smb2_lease_identity_probe.c)
target_include_directories(smb2_lease_identity_probe PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(smb2_lease_identity_probe chimera_server chimera_metrics)

add_test(NAME chimera/server/smb/mbt/lease_identity_probe_memfs
    COMMAND $<TARGET_FILE:smb2_lease_identity_probe>)
set_tests_properties(chimera/server/smb/mbt/lease_identity_probe_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=smb2_lease_identity_probe_memfs"
    LABELS "functional;probe;smb_mbt;memfs"
    TIMEOUT 90)

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
    LABELS "functional;probe;smb_mbt;memfs"
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
    LABELS "functional;probe;smb_mbt;memfs"
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
    LABELS "functional;probe;smb_mbt;memfs"
    TIMEOUT 120)

# Wire-protection ground-truth probe: walks a matrix of connection profiles
# (dialect ceiling x NTLMv2 auth x signing algorithm x cipher) and asserts a
# full session works over each.  This is the only test that reaches the
# server's protection layer at all -- signing, SMB3 key derivation, the
# TRANSFORM header, 3.1.1 negotiate contexts and algorithm selection, and real
# NTLMv2 credential validation -- which every other test crosses unprotected.
add_executable(smb2_wire_probe smb2_wire_probe.c)
target_include_directories(smb2_wire_probe PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(smb2_wire_probe chimera_server chimera_metrics chimera_crypto)

add_test(NAME chimera/server/smb/mbt/wire_probe_memfs
    COMMAND $<TARGET_FILE:smb2_wire_probe>)
set_tests_properties(chimera/server/smb/mbt/wire_probe_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=smb2_wire_probe_memfs"
    LABELS "functional;probe;smb_mbt;memfs"
    TIMEOUT 180)

# FSCTL ground-truth probe: server-side copy, copy-offload tokens, block
# cloning, reparse points and the sparse-file trio.  No trace in the corpus
# emits an IOCTL, so this is the only test that reaches src/server/smb's
# largest dark region.  Every assertion is on the FSCTL's effect (bytes at a
# destination, a range reading back as zeroes, a link target surviving a round
# trip), never on the status alone.
add_executable(smb2_fsctl_probe smb2_fsctl_probe.c)
target_include_directories(smb2_fsctl_probe PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(smb2_fsctl_probe chimera_server chimera_metrics chimera_crypto)

add_test(NAME chimera/server/smb/mbt/fsctl_probe_memfs
    COMMAND $<TARGET_FILE:smb2_fsctl_probe>)
set_tests_properties(chimera/server/smb/mbt/fsctl_probe_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=smb2_fsctl_probe_memfs"
    LABELS "functional;probe;smb_mbt;memfs"
    TIMEOUT 120)

# Information-class probe: the QUERY_INFO / SET_INFO surface.  The corpus asks
# for one file-info class and sets three, so most of the class table and the
# smb_attr.h marshallers never run.  The probe sweeps every implemented class
# on both a file and a directory, then checks the classes AGAINST EACH OTHER --
# a marshaller that writes the right byte count into the wrong field passes a
# size check and fails an agreement check.
add_executable(smb2_info_probe smb2_info_probe.c)
target_include_directories(smb2_info_probe PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(smb2_info_probe chimera_server chimera_metrics chimera_crypto)

add_test(NAME chimera/server/smb/mbt/info_probe_memfs
    COMMAND $<TARGET_FILE:smb2_info_probe>)
set_tests_properties(chimera/server/smb/mbt/info_probe_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=smb2_info_probe_memfs"
    LABELS "functional;probe;smb_mbt;memfs"
    TIMEOUT 120)

# Alternate-data-stream (ADS / named stream) probe: the whole named-stream
# lifecycle the corpus never enables -- CREATE with the file:stream syntax,
# independent stream WRITE/READ, FILE_STREAM_INFORMATION enumeration, and
# delete-on-close of a single stream -- exercising the shared VFS named-stream
# storage that NFSv4 named attributes also project onto.
add_executable(smb2_stream_probe smb2_stream_probe.c)
target_include_directories(smb2_stream_probe PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(smb2_stream_probe chimera_server chimera_metrics chimera_crypto)

add_test(NAME chimera/server/smb/mbt/stream_probe_memfs
    COMMAND $<TARGET_FILE:smb2_stream_probe>)
set_tests_properties(chimera/server/smb/mbt/stream_probe_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=smb2_stream_probe_memfs"
    LABELS "functional;probe;smb_mbt;memfs"
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
    chimera_metrics chimera_crypto)

add_test(NAME chimera/server/smb/mbt/compress_probe_memfs
    COMMAND $<TARGET_FILE:smb2_compress_probe>)
set_tests_properties(chimera/server/smb/mbt/compress_probe_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=smb2_compress_probe_memfs"
    LABELS "functional;probe;smb_mbt;memfs"
    TIMEOUT 120)

# Creator-SID seeding probe: whose native owner SID a CREATE stores, and which
# CREATEs must store none.  Every other security test runs an ANONYMOUS session
# against a server with no identity authority, so the seeding path is dark: both
# identity probes miss and nothing is ever stamped.  This probe registers
# accounts that carry real SIDs, which is the only configuration where the code
# runs at all -- and then spends most of its assertions on the negative cases (a
# non-owner's overwrite, a stream create on another user's file, a client-named
# owner), because a seed that fires on a path that does not create the object
# rewrites an existing file's owner and trips the setattr chown gate.
add_executable(smb2_sidseed_probe smb2_sidseed_probe.c)
target_include_directories(smb2_sidseed_probe PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(smb2_sidseed_probe chimera_server chimera_metrics chimera_crypto)

add_test(NAME chimera/server/smb/mbt/sidseed_probe_memfs
    COMMAND $<TARGET_FILE:smb2_sidseed_probe>)
set_tests_properties(chimera/server/smb/mbt/sidseed_probe_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=smb2_sidseed_probe_memfs"
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

if(NOT CHIMERA_MBT_AVAILABLE)
    message(STATUS "smb MBT replay ctest skipped (no specs trace corpus)")
    return()
endif()

# One cell per distinct corpus.  These are exactly the five generation
# instances the corpus had before configs existed -- smb2Base, smb2Leases,
# smb2ForceL2, smb2Durable, smb2Replay -- with the same flavors, depths, trace
# counts and seeds, and the same trace basenames (each config's traceStem is
# the old instance name, which is also what ext/specs/quint/smb2/coverage.py
# parses its (instance, flavor) coverage groups out of).
#
# Each config also names the KNOWN DEVIATIONS chimera still carries, and the
# model predicts them: there is no reply-level deviation registry in the
# harness any more.  A deviation is a branch in
# ext/specs/quint/smb2/smb2_ops.qnt gated on the DEVS set, declared with its
# MS-SMB2 / MS-FSA citation in that family's corpus.schema.json.  That is what
# makes a STATE-MUTATING divergence expressible at all: CD-3 (a truncating
# CREATE refused after it had already emptied the file) had an identical reply
# on both sides and differed only in state, so the old registry entry could only
# abandon the trace -- while the model branch let the rest of the trace keep
# testing, which is how the bug stayed measurable until it was fixed.  No cell
# declares one today: CD-5 (a lease grant keyed without a client term, so a
# second client's RqLs open landed on the first client's lease) went the same
# way.
# smb2/base additionally registers a STRICT TWIN: the identical batch list
# regenerated with DEVS = Set(), i.e. every expectation the MS-FSA-correct one.
# With every cell's deviation list now empty the twin passes, and it keeps
# running so the day a deviation is added the debt it stands for is re-measured
# on every run. Each family below has its own replay and coverage result.
specs_corpus(NAME           smb2/base
             FAMILY         smb2
             CONFIG         ${CMAKE_CURRENT_SOURCE_DIR}/configs/base.json
             OUT_DIR        smb2_base_traces
             STRICT_OUT_DIR smb2_base_strict)
specs_corpus(NAME    smb2/durable
             FAMILY  smb2
             CONFIG  ${CMAKE_CURRENT_SOURCE_DIR}/configs/durable.json
             OUT_DIR smb2_durable_traces)
specs_corpus(NAME    smb2/force_l2
             FAMILY  smb2
             CONFIG  ${CMAKE_CURRENT_SOURCE_DIR}/configs/force_l2.json
             OUT_DIR smb2_force_l2_traces)
specs_corpus(NAME    smb2/leases
             FAMILY  smb2
             CONFIG  ${CMAKE_CURRENT_SOURCE_DIR}/configs/leases.json
             OUT_DIR smb2_leases_traces)
specs_corpus(NAME    smb2/replay
             FAMILY  smb2
             CONFIG  ${CMAKE_CURRENT_SOURCE_DIR}/configs/replay.json
             OUT_DIR smb2_replay_traces)

# Execute each declared family independently. A lease timeout must not stop
# durable/replay traces from running, and no family may pass by skipping all
# of its traces. Plain and signed connections use the identical model corpus.
foreach(cell base durable force_l2 leases replay)
    foreach(wire plain signed311)
        set(wire_args "")
        if(NOT wire STREQUAL "plain")
            set(wire_args --wire ${wire})
        endif()
        add_test(NAME chimera/server/smb/mbt/${cell}_memfs_${wire}
            COMMAND $<TARGET_FILE:smb2_mbt_replay>
                    --trace-dir ${smb2_${cell}_traces} ${wire_args})
        set_tests_properties(chimera/server/smb/mbt/${cell}_memfs_${wire} PROPERTIES
            ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=smb2_${cell}_${wire}"
            LABELS "quint;smb_mbt;memfs;${cell}" TIMEOUT 600)
    endforeach()
    if(Python3_FOUND)
        add_test(NAME chimera/server/smb/mbt/coverage_${cell}
            COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/ext/specs/quint/smb2/coverage.py
                    ${smb2_${cell}_traces})
        set_tests_properties(chimera/server/smb/mbt/coverage_${cell} PROPERTIES
            LABELS "quint;model_coverage;smb_mbt" TIMEOUT 120)
    endif()
endforeach()

# ---- the deviation machinery ----------------------------------------------

set(SPECS_DEVLIVENESS ${CMAKE_SOURCE_DIR}/ext/specs/tools/devliveness.py)

# The strict twin of smb2/base regenerates the same batches with DEVS = Set().
# It passes today and gates in quick, keeping strict conformance visible on
# every run rather than withholding the replay until extended testing.
add_test(NAME chimera/server/smb/mbt/strict_memfs
    COMMAND $<TARGET_FILE:smb2_mbt_replay> --trace-dir ${smb2_base_strict})
set_tests_properties(chimera/server/smb/mbt/strict_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=smb2_strict_memfs"
    LABELS "quint;smb_mbt;memfs;strict"
    TIMEOUT 1800
    SKIP_RETURN_CODE 77)

# The liveness gate: a cell may not enable a deviation its own corpus never
# exercises.  Either the deviation is dead (retire it from the config and
# delete its branch in the model) or the generator quietly lost the coverage
# that used to reach it.  Both need a human, so both fail here -- in the quick
# tier, because a stale claim is a blindfold from the moment it goes stale.
if(Python3_FOUND)
    foreach(cell base durable force_l2 leases replay)
        add_test(NAME chimera/server/smb/mbt/devliveness_${cell}
            COMMAND ${Python3_EXECUTABLE} ${SPECS_DEVLIVENESS}
                    ${CMAKE_CURRENT_SOURCE_DIR}/configs/${cell}.json
                    ${SPECS_CORPUS_ROOT}/smb2/${cell})
        set_tests_properties(chimera/server/smb/mbt/devliveness_${cell}
            PROPERTIES LABELS "quint;smb_mbt;devliveness" TIMEOUT 120)
    endforeach()
else()
    message(STATUS "smb MBT deviation-liveness gate skipped (no python3)")
endif()
