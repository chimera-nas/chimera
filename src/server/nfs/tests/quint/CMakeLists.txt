# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# NFS3/NFS4 model-based tests: Quint specifications (nfs3.qnt, nfs4.qnt)
# generate traces of NFS RPCs with expected replies baked into every step;
# the replayers (nfs3_mbt_replay.c, nfs4_mbt_replay.c) embed the chimera
# server (memfs backend) in the test process and replay each trace over
# libevpl's inproc transport, failing on any divergence.  No test binds a
# port, needs a network namespace or resource lock, or spawns a daemon --
# every trace runs fully parallel on any host.
#
# Traces are generated at build time from fixed seeds, so they always match
# the current model source; determinism comes from the pinned quint version
# (see .devcontainer/Dockerfile) plus the fixed seed per trace.  The model
# self-tests run at build time too, gating trace generation: they are
# deterministic functions of the model source alone, so they rerun exactly
# when the model changed, and a broken model fails the build before it can
# emit a corpus.

find_program(QUINT_BIN quint)
find_package(Python3 COMPONENTS Interpreter)

if(NOT QUINT_BIN)
    message(STATUS "quint not found - NFS3/NFS4 model-based tests will be "
                   "skipped (npm install -g @informalsystems/quint)")
    return()
endif()

set(NFS3_MBT_MODEL ${CMAKE_CURRENT_SOURCE_DIR}/nfs3.qnt)
set(NFS3_MBT_TRACE_DIR ${CMAKE_CURRENT_BINARY_DIR}/traces)

file(MAKE_DIRECTORY ${NFS3_MBT_TRACE_DIR})

# The C replayers and deviation probe: chimera server + rpc2 client in one
# process over the inproc transport (see nfs3_mbt_common.h).
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

# Model self-check (deterministic run definitions plus a bounded random
# walk); reruns only when the model source changes, and trace generation
# depends on it, so a model regression stops the corpus at the source.
set(NFS3_MBT_SELFTEST ${CMAKE_CURRENT_BINARY_DIR}/nfs3_model_selftest.stamp)
add_custom_command(OUTPUT ${NFS3_MBT_SELFTEST}
    COMMAND ${QUINT_BIN} test --max-samples=500
            ${CMAKE_CURRENT_SOURCE_DIR}/nfs3Test.qnt > /dev/null
    COMMAND ${CMAKE_COMMAND} -E touch ${NFS3_MBT_SELFTEST}
    DEPENDS ${NFS3_MBT_MODEL} ${CMAKE_CURRENT_SOURCE_DIR}/nfs3Test.qnt
    COMMENT "NFS3 model self-test (quint test nfs3Test.qnt)"
    VERBATIM)

set(NFS3_MBT_TRACE_FILES)

# Generate a batch of N traces for one (flavor, steps) with a SINGLE quint
# invocation, then register one CTest entry per trace.  `flavor` selects the
# model's step distribution (--step): "step" is the uniform mix;
# stepChurn/stepDir/stepExcl bias toward namespace churn, empty-directory
# operations, and exclusive-create retries respectively.  The blend is chosen
# to saturate coverage.py's behavior buckets (uniform stepping alone starves
# successful-rmdir, dir-into-empty-dir rename, and idempotent exclusive-create
# retries).
#
# Batching matters: quint's ~1.5s per-invocation cost is node startup + model
# parse, not the (millisecond) simulation, so `--n-traces=N --out-itf=..{seq}..`
# amortizes that fixed cost across all N traces of a group -- one invocation
# emits N distinct walks from the seed's sample stream in about the time one
# trace used to take.  --invariant=inv still gates every generated trace, so a
# model bug fails the build rather than producing a nonsense corpus.
function(nfs3_mbt_add_batch flavor steps ntraces seed)
    set(traces)
    math(EXPR last "${ntraces} - 1")
    foreach(seq RANGE 0 ${last})
        list(APPEND traces ${NFS3_MBT_TRACE_DIR}/${flavor}_${steps}_${seq}.itf.json)
    endforeach()

    add_custom_command(OUTPUT ${traces}
        COMMAND ${QUINT_BIN} run ${NFS3_MBT_MODEL}
                --step=${flavor} --max-steps=${steps}
                --max-samples=${ntraces} --n-traces=${ntraces} --seed=${seed}
                --invariant=inv
                --out-itf=${NFS3_MBT_TRACE_DIR}/${flavor}_${steps}_{seq}.itf.json > /dev/null
        DEPENDS ${NFS3_MBT_MODEL} ${NFS3_MBT_SELFTEST}
        COMMENT "Generating ${ntraces} NFS3 MBT traces ${flavor} steps=${steps}"
        VERBATIM)

    list(APPEND NFS3_MBT_TRACE_FILES ${traces})
    set(NFS3_MBT_TRACE_FILES ${NFS3_MBT_TRACE_FILES} PARENT_SCOPE)

    foreach(trace ${traces})
        get_filename_component(tname ${trace} NAME_WE)
        set(name chimera/server/nfs/mbt/${tname}_memfs)
        add_test(NAME ${name}
            COMMAND $<TARGET_FILE:nfs3_mbt_replay> --trace ${trace})
        set_tests_properties(${name} PROPERTIES
            LABELS "nfs3_mbt;memfs"
            TIMEOUT 60)
    endforeach()
endfunction()

# Coverage-derived blend (see coverage.py): the uniform flavor gives broad
# op/attr/IO coverage; the biased flavors fill the buckets it starves.  One
# invocation per (flavor, steps) group; the seed makes the batch reproducible.
nfs3_mbt_add_batch(step      128 10 0x1)
nfs3_mbt_add_batch(step      256  3 0x100)
nfs3_mbt_add_batch(stepChurn 128  6 0x1)
nfs3_mbt_add_batch(stepChurn 256  2 0x200)
nfs3_mbt_add_batch(stepDir   128  4 0x1)
nfs3_mbt_add_batch(stepExcl  128  3 0x1)

add_custom_target(nfs3_mbt_traces ALL DEPENDS ${NFS3_MBT_TRACE_FILES})

# Coverage gate: classify every step of the generated corpus into behavior
# buckets and fail if any bucket is never exercised.  This catches a
# generation blend (or a model change) that silently stops covering a
# scenario -- the report also prints the LOW buckets worth more seeds.  No
# server needed; it reads the same traces the replays consume.
if(Python3_FOUND)
    add_test(NAME chimera/server/nfs/mbt/coverage
        COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/coverage.py
                --fail-on-starved --glob "${NFS3_MBT_TRACE_DIR}/*.itf.json")
    set_tests_properties(chimera/server/nfs/mbt/coverage PROPERTIES
        LABELS "nfs3_mbt"
        ENVIRONMENT "PYTHONUNBUFFERED=1"
        TIMEOUT 120)
endif()

# Deviation probe: asserts each documented chimera deviation (DEVIATIONS.md)
# still reproduces, and that the two fixed deadlocks (LINK/RENAME self-alias)
# do not hang.  Goes red when chimera is fixed -- the signal to retire a
# deviation -- or if a deadlock regresses.
add_test(NAME chimera/server/nfs/mbt/deviation_probe_memfs
    COMMAND $<TARGET_FILE:nfs3_mbt_probe>)
set_tests_properties(chimera/server/nfs/mbt/deviation_probe_memfs PROPERTIES
    LABELS "nfs3_mbt;memfs"
    TIMEOUT 60)

# ---------------------------------------------------------------------------
# NFSv4 model-based tests: same architecture as the v3 suite above, driven
# by the RFC-first model in nfs4.qnt (see DESIGN-NFS4.md).  One trace = one
# ctest entry replayed by nfs4_mbt_replay.c.  A trace whose capability
# profile does not match the live server exits 77 and is reported as SKIP,
# per the FeatIfAdvertised contract.
# ---------------------------------------------------------------------------

set(NFS4_MBT_RUN ${CMAKE_CURRENT_SOURCE_DIR}/nfs4_run.qnt)
set(NFS4_MBT_TRACE_DIR ${CMAKE_CURRENT_BINARY_DIR}/traces4)
set(NFS4_MBT_DEPS
    ${CMAKE_CURRENT_SOURCE_DIR}/nfs4_run.qnt
    ${CMAKE_CURRENT_SOURCE_DIR}/nfs4.qnt
    ${CMAKE_CURRENT_SOURCE_DIR}/nfs4_ops.qnt
    ${CMAKE_CURRENT_SOURCE_DIR}/nfs4_state.qnt
    ${CMAKE_CURRENT_SOURCE_DIR}/nfs4_fs.qnt)

file(MAKE_DIRECTORY ${NFS4_MBT_TRACE_DIR})

# Model self-checks per instance family, gating generation like the v3
# stamp above.  These are CPU-heavy quint runs (~160s each), which is
# exactly why they belong at build time: they rerun only on a model
# change instead of on every test invocation, and ninja runs them
# concurrently with the rest of the build.
set(NFS4_MBT_SELFTESTS)
foreach(main nfs4Test42 nfs4Test40)
    set(stamp ${CMAKE_CURRENT_BINARY_DIR}/${main}_selftest.stamp)
    add_custom_command(OUTPUT ${stamp}
        COMMAND ${QUINT_BIN} test --max-samples=200 --main=${main}
                ${CMAKE_CURRENT_SOURCE_DIR}/nfs4Test.qnt > /dev/null
        COMMAND ${CMAKE_COMMAND} -E touch ${stamp}
        DEPENDS ${NFS4_MBT_DEPS} ${CMAKE_CURRENT_SOURCE_DIR}/nfs4Test.qnt
        COMMENT "NFS4 model self-test (quint test --main=${main})"
        VERBATIM)
    list(APPEND NFS4_MBT_SELFTESTS ${stamp})
endforeach()

set(NFS4_MBT_TRACE_FILES)

function(nfs4_mbt_add_batch inst flavor steps ntraces seed)
    set(traces)
    math(EXPR last "${ntraces} - 1")
    foreach(seq RANGE 0 ${last})
        list(APPEND traces
             ${NFS4_MBT_TRACE_DIR}/${inst}_${flavor}_${steps}_${seq}.itf.json)
    endforeach()

    add_custom_command(OUTPUT ${traces}
        COMMAND ${QUINT_BIN} run ${NFS4_MBT_RUN}
                --main=${inst} --step=${flavor} --max-steps=${steps}
                --max-samples=${ntraces} --n-traces=${ntraces} --seed=${seed}
                --invariant=inv
                --out-itf=${NFS4_MBT_TRACE_DIR}/${inst}_${flavor}_${steps}_{seq}.itf.json > /dev/null
        DEPENDS ${NFS4_MBT_DEPS} ${NFS4_MBT_SELFTESTS}
        COMMENT "Generating ${ntraces} NFS4 MBT traces ${inst}/${flavor} steps=${steps}"
        VERBATIM)

    list(APPEND NFS4_MBT_TRACE_FILES ${traces})
    set(NFS4_MBT_TRACE_FILES ${NFS4_MBT_TRACE_FILES} PARENT_SCOPE)

    foreach(trace ${traces})
        get_filename_component(tname ${trace} NAME_WE)
        set(name chimera/server/nfs/mbt4/${tname}_memfs)
        add_test(NAME ${name}
            COMMAND $<TARGET_FILE:nfs4_mbt_replay> --trace ${trace})
        set_tests_properties(${name} PROPERTIES
            LABELS "nfs4_mbt;memfs"
            SKIP_RETURN_CODE 77
            TIMEOUT 180)
    endforeach()
endfunction()

# Three memfs-tuned instances (minorversion 2/1/0), four step flavors
# each; fixed seeds keep the corpus reproducible for a pinned quint
# version.  These pin every optional feature to what chimera + memfs
# actually does, so the corpus contains no not-applicable traces (no
# spurious capability skips).  The generic FeatIfAdvertised instances
# stay in nfs4_run.qnt for a capabilities-unknown server and the
# all-mandatory conformance gate.
foreach(inst nfs4Memfs42 nfs4Memfs41 nfs4Memfs40)
    nfs4_mbt_add_batch(${inst} step          80 6 0x7)
    nfs4_mbt_add_batch(${inst} stepState     80 6 0x7)
    nfs4_mbt_add_batch(${inst} stepNamespace 80 6 0x7)
    nfs4_mbt_add_batch(${inst} stepData      80 6 0x7)
endforeach()

# Delegation-enabled instances: the stepDeleg flavor drives the
# grant/recall/return lifecycle across the two model clients; a uniform
# batch keeps broad coverage under delegations too.
foreach(inst nfs4Memfs42Deleg nfs4Memfs41Deleg)
    nfs4_mbt_add_batch(${inst} stepDeleg 80 10 0x7)
endforeach()

add_custom_target(nfs4_mbt_traces ALL DEPENDS ${NFS4_MBT_TRACE_FILES})

# Delegation policy probe: pins the deterministic grant/recall behavior
# the delegation-enabled instances encode (see nfs4_deleg_probe.c).
add_test(NAME chimera/server/nfs/mbt4/deleg_probe_memfs
    COMMAND $<TARGET_FILE:nfs4_deleg_probe>)
set_tests_properties(chimera/server/nfs/mbt4/deleg_probe_memfs PROPERTIES
    LABELS "nfs4_mbt;multiproto;memfs"
    TIMEOUT 90)
