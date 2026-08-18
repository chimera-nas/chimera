# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# SMB2/3 model-based tests: a Quint specification (smb2.qnt and its pure
# smb2_state.qnt / smb2_ops.qnt modules) modeling an SMB2/3 server on top of
# the SAME protocol-neutral filesystem core the NFSv4 model uses
# (../../../nfs/tests/quint/nfs4_fs.qnt).  The shared substrate is the whole
# point: SMB share modes, oplocks/leases and byte-range locks are the SMB
# face of the very VFS state NFSv4 OPEN share/deny, delegations and locks
# drive, so the two models meet in one place (see DESIGN-SMB.md,
# DESIGN-MULTIPROTOCOL.md, ROADMAP-SMB.md).
#
# This increment lands the model plus its deterministic self-test.  The
# self-test runs at build time and gates (in later increments) trace
# generation: it is a deterministic function of the model source alone, so
# it reruns exactly when the model changes, and a broken model fails the
# build.  The in-process C replay harness (smb2_mbt_replay.c) and the
# cross-protocol coherency probe are later increments per ROADMAP-SMB.md.

# ---------------------------------------------------------------------------
# The in-process SMB2 wire harness + smoke probe (ROADMAP-SMB.md Increment 0).
# Independent of quint: it embeds the chimera SMB server (memfs backend) over
# libevpl's inproc transport and drives it from a hand-rolled SMB2 client
# (smb2_mbt_common.h) -- the SMB sibling of nfs3_mbt_common.h.  SMB is not
# ONC-RPC/XDR, so the client hand-builds each PDU following the server's own
# parser rather than a generated stub.
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
# Model self-test (needs quint); the C harness above does not.
# ---------------------------------------------------------------------------
find_program(QUINT_BIN quint)

if(NOT QUINT_BIN)
    message(STATUS "quint not found - SMB2 model self-test will be "
                   "skipped (npm install -g @informalsystems/quint)")
    return()
endif()

# The model source set: every file the self-test depends on (including the
# reused NFSv4 filesystem core, so a change there re-triggers the SMB
# self-test too).
set(SMB2_MBT_DEPS
    ${CMAKE_CURRENT_SOURCE_DIR}/smb2.qnt
    ${CMAKE_CURRENT_SOURCE_DIR}/smb2_ops.qnt
    ${CMAKE_CURRENT_SOURCE_DIR}/smb2_state.qnt
    ${CMAKE_CURRENT_SOURCE_DIR}/smb2Test.qnt
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../nfs/tests/quint/nfs4_fs.qnt)

# Model self-check (quint test): validates the CREATE matrix, share
# arbitration, the oplock/lease break lifecycle, byte-range locks,
# delete-on-close and compound threading, plus random-walk invariant
# preservation -- all without a server.  Runs at build time and, like the
# NFS4 self-test stamp, reruns only when the model source changes.
set(SMB2_MBT_SELFTEST ${CMAKE_CURRENT_BINARY_DIR}/smb2_model_selftest.stamp)
add_custom_command(OUTPUT ${SMB2_MBT_SELFTEST}
    COMMAND ${QUINT_BIN} test --max-samples=200 --main=smb2Test
            ${CMAKE_CURRENT_SOURCE_DIR}/smb2Test.qnt > /dev/null
    COMMAND ${CMAKE_COMMAND} -E touch ${SMB2_MBT_SELFTEST}
    DEPENDS ${SMB2_MBT_DEPS}
    COMMENT "SMB2 model self-test (quint test --main=smb2Test)"
    VERBATIM)

add_custom_target(smb2_mbt_selftest ALL DEPENDS ${SMB2_MBT_SELFTEST})

# ---------------------------------------------------------------------------
# Trace generation + replay (Increment 3).  Each generated ITF trace is one
# ctest replayed by smb2_mbt_replay.c against the in-process server; a trace
# whose every reply matches the model passes.  Traces are regenerated at
# build time from fixed seeds and gated on the model self-test, so they always
# match the current model source (determinism from the pinned quint version).
# ---------------------------------------------------------------------------
set(SMB2_MBT_RUN ${CMAKE_CURRENT_SOURCE_DIR}/smb2_run.qnt)
set(SMB2_MBT_TRACE_DIR ${CMAKE_CURRENT_BINARY_DIR}/traces)
file(MAKE_DIRECTORY ${SMB2_MBT_TRACE_DIR})

add_executable(smb2_mbt_replay smb2_mbt_replay.c)
target_include_directories(smb2_mbt_replay PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(smb2_mbt_replay chimera_server chimera_metrics jansson)

set(SMB2_MBT_GEN_DEPS
    ${SMB2_MBT_RUN}
    ${CMAKE_CURRENT_SOURCE_DIR}/smb2.qnt
    ${CMAKE_CURRENT_SOURCE_DIR}/smb2_ops.qnt
    ${CMAKE_CURRENT_SOURCE_DIR}/smb2_state.qnt
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../nfs/tests/quint/nfs4_fs.qnt)

set(SMB2_MBT_TRACE_FILES)

# Clone of nfs4_mbt_add_batch: generate `ntraces` traces of `inst`/`flavor`
# and register one replay ctest per trace.
function(smb2_mbt_add_batch inst flavor steps ntraces seed)
    set(traces)
    math(EXPR last "${ntraces} - 1")
    foreach(seq RANGE 0 ${last})
        list(APPEND traces
             ${SMB2_MBT_TRACE_DIR}/${inst}_${flavor}_${steps}_${seq}.itf.json)
    endforeach()

    add_custom_command(OUTPUT ${traces}
        COMMAND ${QUINT_BIN} run ${SMB2_MBT_RUN}
                --main=${inst} --step=${flavor} --max-steps=${steps}
                --max-samples=${ntraces} --n-traces=${ntraces} --seed=${seed}
                --invariant=inv
                --out-itf=${SMB2_MBT_TRACE_DIR}/${inst}_${flavor}_${steps}_{seq}.itf.json > /dev/null
        DEPENDS ${SMB2_MBT_GEN_DEPS} ${SMB2_MBT_SELFTEST}
        COMMENT "Generating ${ntraces} SMB2 MBT traces ${inst}/${flavor} steps=${steps}"
        VERBATIM)

    list(APPEND SMB2_MBT_TRACE_FILES ${traces})
    set(SMB2_MBT_TRACE_FILES ${SMB2_MBT_TRACE_FILES} PARENT_SCOPE)

    foreach(trace ${traces})
        get_filename_component(tname ${trace} NAME_WE)
        set(name chimera/server/smb/mbt/${tname}_memfs)
        add_test(NAME ${name}
            COMMAND $<TARGET_FILE:smb2_mbt_replay> --trace ${trace})
        set_tests_properties(${name} PROPERTIES
            LABELS "smb_mbt;memfs"
            TIMEOUT 90)
    endforeach()
endfunction()

# The deterministic filesystem core (no caching, no delete-on-close): the
# CREATE disposition/type matrix, share arbitration, I/O and compounds.
smb2_mbt_add_batch(smb2Base stepCore 60 6 0x7)

add_custom_target(smb2_mbt_traces ALL DEPENDS ${SMB2_MBT_TRACE_FILES})

# Behavior-bucket coverage gate: fails if the generated corpus never exercises
# a CREATE disposition, CreateAction or coherency-relevant status, so uniform
# stepping cannot silently starve a create-matrix corner (clone of the NFS
# `coverage` ctest).
find_package(Python3 COMPONENTS Interpreter)
if(Python3_Interpreter_FOUND)
    add_test(NAME chimera/server/smb/mbt/coverage
        COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/coverage.py
                ${SMB2_MBT_TRACE_DIR})
    set_tests_properties(chimera/server/smb/mbt/coverage PROPERTIES
        LABELS "smb_mbt;memfs")
endif()
