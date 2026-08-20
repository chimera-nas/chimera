# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# POSIX model-based tests: the Quint specification (posix.qnt and friends)
# generates traces of POSIX file-API calls with expected results baked into
# every step; posix_replay.py replays them against chimera's POSIX client
# backed by an in-process memfs mount, through the JSON line protocol served
# by posix_driver, and fails on any unregistered divergence.
#
# Traces are generated at build time from fixed seeds against the posixMemfs
# instance (capability/policy profile pinned to memfs's probed behavior, see
# posix_run.qnt), so every trace's LInit profile matches the live backend and
# no trace is skipped by profile reconciliation.

add_executable(posix_quint_driver posix_driver.c)
target_link_libraries(posix_quint_driver
    chimera_posix chimera_client chimera_server jansson)

find_program(QUINT_BIN quint)
find_package(Python3 COMPONENTS Interpreter)

if(NOT QUINT_BIN OR NOT Python3_FOUND)
    message(STATUS "posix MBT tests skipped (need quint + python3)")
    return()
endif()

set(POSIX_MBT_MODEL ${CMAKE_CURRENT_SOURCE_DIR}/posix_run.qnt)
set(POSIX_MBT_REPLAY ${CMAKE_CURRENT_SOURCE_DIR}/posix_replay.py)
set(POSIX_MBT_TRACE_DIR ${CMAKE_CURRENT_BINARY_DIR}/traces)

file(MAKE_DIRECTORY ${POSIX_MBT_TRACE_DIR})

# Model files the corpus and the self-tests both depend on.
set(POSIX_MBT_DEPS
    ${POSIX_MBT_MODEL}
    ${CMAKE_CURRENT_SOURCE_DIR}/posix.qnt
    ${CMAKE_CURRENT_SOURCE_DIR}/posix_ops.qnt
    ${CMAKE_CURRENT_SOURCE_DIR}/posix_fs.qnt
    ${CMAKE_CURRENT_SOURCE_DIR}/posix_state.qnt)

# Model self-checks (deterministic run definitions plus bounded random walks)
# as BUILD-TIME jobs that gate trace generation, exactly like the NFS/SMB
# suites.  They are deterministic functions of the model source alone, so they
# rerun only when the model changes -- not on every ctest invocation -- and a
# broken model fails the build before it can emit a corpus; ninja runs them
# concurrently with the rest of the build.
set(POSIX_MBT_SELFTESTS)
foreach(main posixTestStd posixTestRoot)
    set(stamp ${CMAKE_CURRENT_BINARY_DIR}/${main}_selftest.stamp)
    add_custom_command(OUTPUT ${stamp}
        COMMAND ${QUINT_BIN} test --max-samples=100 --main=${main}
                ${CMAKE_CURRENT_SOURCE_DIR}/posixTest.qnt > /dev/null
        COMMAND ${CMAKE_COMMAND} -E touch ${stamp}
        DEPENDS ${POSIX_MBT_DEPS} ${CMAKE_CURRENT_SOURCE_DIR}/posixTest.qnt
        COMMENT "POSIX model self-test (quint test --main=${main})"
        VERBATIM)
    list(APPEND POSIX_MBT_SELFTESTS ${stamp})
endforeach()
add_custom_target(posix_mbt_selftest ALL DEPENDS ${POSIX_MBT_SELFTESTS})

set(POSIX_MBT_TRACE_FILES)

# TEMPORARY: restrict the POSIX MBT replays to the memfs backend while it is
# being driven to 100%.  Set OFF (or delete) to restore the full backend matrix
# (memfs + cairn + diskfs + nfs3_memfs + nfs4_memfs).
set(POSIX_MBT_MEMFS_ONLY ON)

# Generate a batch of N traces for one (instance, flavor, steps) with a
# single quint invocation (same amortization as the NFS3 suite: node startup
# + parse dominate, so --n-traces batches are nearly free).  --invariant=inv
# gates every trace, so a model bug fails the build.  Extra arguments name
# the backends the batch replays against (one add_test per backend); the
# prefix keeps trace files from different instances apart.
function(posix_mbt_add_batch instance prefix flavor steps ntraces seed)
    set(traces)
    math(EXPR last "${ntraces} - 1")
    foreach(seq RANGE 0 ${last})
        list(APPEND traces
             ${POSIX_MBT_TRACE_DIR}/${prefix}${flavor}_${steps}_${seed}_${seq}.itf.json)
    endforeach()

    # quint exits 0 without writing anything if the output directory is
    # missing (e.g. after an rm -rf of the trace dir, which only configure
    # recreates), so make the directory part of the recipe.
    add_custom_command(OUTPUT ${traces}
        COMMAND ${CMAKE_COMMAND} -E make_directory ${POSIX_MBT_TRACE_DIR}
        COMMAND ${QUINT_BIN} run ${POSIX_MBT_MODEL} --main=${instance}
                --step=${flavor} --max-steps=${steps}
                --max-samples=${ntraces} --n-traces=${ntraces} --seed=${seed}
                --invariant=inv
                --out-itf=${POSIX_MBT_TRACE_DIR}/${prefix}${flavor}_${steps}_${seed}_{seq}.itf.json > /dev/null
        DEPENDS ${POSIX_MBT_DEPS} ${POSIX_MBT_SELFTESTS}
        COMMENT "Generating ${ntraces} POSIX MBT traces ${instance}/${flavor} steps=${steps}"
        VERBATIM)

    list(APPEND POSIX_MBT_TRACE_FILES ${traces})
    set(POSIX_MBT_TRACE_FILES ${POSIX_MBT_TRACE_FILES} PARENT_SCOPE)

    # TEMPORARY (POSIX_MBT_MEMFS_ONLY): while driving the memfs backend to 100%,
    # register replays only for memfs; the other backends' traces are still
    # generated but not run.  Flip the toggle below OFF to restore the matrix.
    set(_backends ${ARGN})
    if(POSIX_MBT_MEMFS_ONLY)
        list(FILTER _backends INCLUDE REGEX "^memfs$")
    endif()

    # Accumulate this batch's traces into a per-backend GLOBAL list; a single
    # batched replay per backend is registered after all batches are collected
    # (see below), so the whole corpus shares one long-lived driver.
    foreach(backend ${_backends})
        set_property(GLOBAL APPEND PROPERTY posix_mbt_backends ${backend})
        set_property(GLOBAL APPEND PROPERTY
                     posix_mbt_traces_${backend} ${traces})
    endforeach()
endfunction()

posix_mbt_add_batch(posixMemfs "" step            128 10 0x1   memfs)
posix_mbt_add_batch(posixMemfs "" step            128  6 0x2   memfs)
posix_mbt_add_batch(posixMemfs "" step            256  3 0x100 memfs)
posix_mbt_add_batch(posixMemfs "" step            512  2 0x5   memfs)
posix_mbt_add_batch(posixMemfs "" stepNamespace   128  4 0x1   memfs)
posix_mbt_add_batch(posixMemfs "" stepNamespace   256  2 0x9   memfs)
posix_mbt_add_batch(posixMemfs "" stepDescriptors 128  4 0x1   memfs)
posix_mbt_add_batch(posixMemfs "" stepMetadata    128  4 0x1   memfs)
posix_mbt_add_batch(posixMemfs "" stepDirs        128  3 0x1   memfs)
posix_mbt_add_batch(posixMemfs "" stepData        128  4 0x1   memfs)
posix_mbt_add_batch(posixMemfs "" stepLocks       128  2 0x1   memfs)
posix_mbt_add_batch(posixMemfs "" stepPerms       128  5 0x1   memfs)
posix_mbt_add_batch(posixMemfs "" stepLinks       128  5 0x1   memfs)
posix_mbt_add_batch(posixMemfs "" stepFds         128  5 0x1   memfs)
posix_mbt_add_batch(posixMemfs "" stepTooLong     128  4 0x1   memfs)
posix_mbt_add_batch(posixMemfs "" stepPrefix      128  5 0x1   memfs)
posix_mbt_add_batch(posixMemfs "" stepFallocate   128  4 0x1   memfs)

# NFS loopback paths (client -> in-process server -> backend): the probed
# profile is version-keyed and identical across the three backends behind
# each version, so one corpus per NFS version replays against all three.
posix_mbt_add_batch(posixNfs3 nfs3_ step            128 10 0x1   nfs3_memfs nfs3_diskfs nfs3_cairn)
posix_mbt_add_batch(posixNfs3 nfs3_ step            128  6 0x2   nfs3_memfs nfs3_diskfs nfs3_cairn)
posix_mbt_add_batch(posixNfs3 nfs3_ step            256  3 0x100 nfs3_memfs nfs3_diskfs nfs3_cairn)
posix_mbt_add_batch(posixNfs3 nfs3_ stepNamespace   128  4 0x1   nfs3_memfs nfs3_diskfs nfs3_cairn)
posix_mbt_add_batch(posixNfs3 nfs3_ stepDescriptors 128  4 0x1   nfs3_memfs nfs3_diskfs nfs3_cairn)
posix_mbt_add_batch(posixNfs3 nfs3_ stepMetadata    128  4 0x1   nfs3_memfs nfs3_diskfs nfs3_cairn)
posix_mbt_add_batch(posixNfs3 nfs3_ stepDirs        128  3 0x1   nfs3_memfs nfs3_diskfs nfs3_cairn)
posix_mbt_add_batch(posixNfs3 nfs3_ stepData        128  4 0x1   nfs3_memfs nfs3_diskfs nfs3_cairn)
posix_mbt_add_batch(posixNfs3 nfs3_ stepPerms       128  5 0x1   nfs3_memfs nfs3_diskfs nfs3_cairn)
posix_mbt_add_batch(posixNfs3 nfs3_ stepLinks       128  5 0x1   nfs3_memfs nfs3_diskfs nfs3_cairn)
posix_mbt_add_batch(posixNfs3 nfs3_ stepFds         128  5 0x1   nfs3_memfs nfs3_diskfs nfs3_cairn)
posix_mbt_add_batch(posixNfs4 nfs4_ step            128 10 0x1   nfs4_memfs nfs4_diskfs nfs4_cairn)
posix_mbt_add_batch(posixNfs4 nfs4_ step            128  6 0x2   nfs4_memfs nfs4_diskfs nfs4_cairn)
posix_mbt_add_batch(posixNfs4 nfs4_ step            256  3 0x100 nfs4_memfs nfs4_diskfs nfs4_cairn)
posix_mbt_add_batch(posixNfs4 nfs4_ stepNamespace   128  4 0x1   nfs4_memfs nfs4_diskfs nfs4_cairn)
posix_mbt_add_batch(posixNfs4 nfs4_ stepDescriptors 128  4 0x1   nfs4_memfs nfs4_diskfs nfs4_cairn)
posix_mbt_add_batch(posixNfs4 nfs4_ stepMetadata    128  4 0x1   nfs4_memfs nfs4_diskfs nfs4_cairn)
posix_mbt_add_batch(posixNfs4 nfs4_ stepDirs        128  3 0x1   nfs4_memfs nfs4_diskfs nfs4_cairn)
posix_mbt_add_batch(posixNfs4 nfs4_ stepData        128  4 0x1   nfs4_memfs nfs4_diskfs nfs4_cairn)
posix_mbt_add_batch(posixNfs4 nfs4_ stepPerms       128  5 0x1   nfs4_memfs nfs4_diskfs nfs4_cairn)
posix_mbt_add_batch(posixNfs4 nfs4_ stepLinks       128  5 0x1   nfs4_memfs nfs4_diskfs nfs4_cairn)
posix_mbt_add_batch(posixNfs4 nfs4_ stepFds         128  5 0x1   nfs4_memfs nfs4_diskfs nfs4_cairn)

# diskfs and cairn probe identically (memfs profile minus clone_range), so
# one posixDisk trace corpus replays against both backends.
posix_mbt_add_batch(posixDisk disk_ step            128 10 0x1   diskfs cairn)
posix_mbt_add_batch(posixDisk disk_ step            128  6 0x2   diskfs cairn)
posix_mbt_add_batch(posixDisk disk_ step            256  3 0x100 diskfs cairn)
posix_mbt_add_batch(posixDisk disk_ step            512  2 0x5   diskfs cairn)
posix_mbt_add_batch(posixDisk disk_ stepNamespace   128  4 0x1   diskfs cairn)
posix_mbt_add_batch(posixDisk disk_ stepNamespace   256  2 0x9   diskfs cairn)
posix_mbt_add_batch(posixDisk disk_ stepDescriptors 128  4 0x1   diskfs cairn)
posix_mbt_add_batch(posixDisk disk_ stepMetadata    128  4 0x1   diskfs cairn)
posix_mbt_add_batch(posixDisk disk_ stepDirs        128  3 0x1   diskfs cairn)
posix_mbt_add_batch(posixDisk disk_ stepData        128  4 0x1   diskfs cairn)
posix_mbt_add_batch(posixDisk disk_ stepLocks       128  2 0x1   diskfs cairn)
posix_mbt_add_batch(posixDisk disk_ stepPerms       128  5 0x1   diskfs cairn)
posix_mbt_add_batch(posixDisk disk_ stepLinks       128  5 0x1   diskfs cairn)
posix_mbt_add_batch(posixDisk disk_ stepFds         128  5 0x1   diskfs cairn)

add_custom_target(posix_mbt_traces ALL DEPENDS ${POSIX_MBT_TRACE_FILES})

# One batched replay per backend: a single posix_replay.py drives one
# long-lived posix_quint_driver across the whole corpus, cycling a fresh
# uniquely-named filesystem per trace (driver "newfs"), so the dominant
# per-trace cost -- driver server/client init + mount -- is paid once instead
# of ~237 times.  Replaces the former one-ctest-per-(trace,backend) matrix.
# Traces whose LInit profile does not match the backend skip individually; the
# batch reports SKIP (77) only if every trace skips.
get_property(_posix_mbt_backends GLOBAL PROPERTY posix_mbt_backends)
if(_posix_mbt_backends)
    list(REMOVE_DUPLICATES _posix_mbt_backends)
endif()
foreach(backend ${_posix_mbt_backends})
    get_property(_traces GLOBAL PROPERTY posix_mbt_traces_${backend})
    set(_args)
    foreach(trace ${_traces})
        list(APPEND _args --trace ${trace})
    endforeach()
    # NFS-loopback replays add server spin-up and RPC round trips per trace.
    if(backend MATCHES "^nfs")
        set(ttimeout 600)
    else()
        set(ttimeout 300)
    endif()
    add_test(NAME chimera/posix/mbt/batch_${backend}
        COMMAND ${Python3_EXECUTABLE} ${POSIX_MBT_REPLAY}
                --backend ${backend}
                --driver $<TARGET_FILE:posix_quint_driver>
                ${_args})
    set_tests_properties(chimera/posix/mbt/batch_${backend} PROPERTIES
        LABELS "posix_mbt;${backend}"
        ENVIRONMENT "PYTHONUNBUFFERED=1"
        SKIP_RETURN_CODE 77
        TIMEOUT ${ttimeout})
endforeach()
