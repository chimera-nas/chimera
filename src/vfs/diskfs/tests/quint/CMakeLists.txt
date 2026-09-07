# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# Model-based tests: the quick tier a plain `ctest` runs.
set(CHIMERA_TEST_TIER quick)

# The diskfs stress model lives HERE rather than in the specs submodule
# (ext/specs).  That submodule holds models of published protocols -- POSIX,
# NFS, SMB2 -- worth stating independently of any implementation.  This model
# describes nothing observable from outside: it is a generator of bulk workloads
# aimed squarely at diskfs's own on-disk internals (b+tree rebalance, space-map
# fragmentation / reclaim, intent-log crash recovery), and its oracle is the
# white-box invariant SDK (diskfs_test.h) rather than a protocol contract.  So,
# like the control-plane (ctl) model, its corpus is generated at build time from
# the .qnt files here and the replay ctest is registered only where quint is
# available.
#
#   diskfs.qnt       the bulk-operation workload state machine + step flavors
#   diskfs_run.qnt   the profiles (reference depth vs. the deep-tree profile)
#   diskfsTest.qnt   deterministic self-checks, run as a build gate

find_package(PkgConfig REQUIRED)
pkg_check_modules(JANSSON REQUIRED jansson)

add_executable(diskfs_mbt_replay diskfs_mbt_replay.c)
target_include_directories(diskfs_mbt_replay PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/..       # diskfs_test_harness.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../..    # diskfs_test.h
    ${JANSSON_INCLUDE_DIRS})
target_link_libraries(diskfs_mbt_replay
    chimera_vfs chimera_vfs_diskfs chimera_vfs_memkv evpl ${JANSSON_LIBRARIES})

# ---------------------------------------------------------------------------
# Trace corpus (generated at build time by quint; see rest/tests/quint for the
# rationale and the rust-backend note).  When quint is absent the smoke test in
# the parent directory still builds and runs -- only the replay batch is skipped,
# with a message.  -DREQUIRE_DISKFS_MBT=ON makes the absence a configure error.
# ---------------------------------------------------------------------------

find_program(QUINT_BIN quint)
find_package(Python3 COMPONENTS Interpreter QUIET)

if(NOT QUINT_BIN)
    if(REQUIRE_DISKFS_MBT)
        message(FATAL_ERROR
            "REQUIRE_DISKFS_MBT=ON but quint is not on PATH.  The diskfs stress "
            "model lives in this repo rather than ext/specs, so its corpus must "
            "be generated here: install quint (the version in "
            "ext/specs/.quint-version), or omit -DREQUIRE_DISKFS_MBT=ON.")
    endif()
    message(STATUS
        "diskfs MBT: quint not on PATH; chimera/vfs/diskfs/mbt/batch will NOT "
        "run (-DREQUIRE_DISKFS_MBT=ON makes this fatal)")
    return()
endif()

set(DFS_Q ${CMAKE_CURRENT_SOURCE_DIR})
set(DFS_T ${CMAKE_CURRENT_BINARY_DIR}/traces)
set(DFS_QNT ${DFS_Q}/diskfs.qnt ${DFS_Q}/diskfs_run.qnt)

# Model self-tests gate generation.
set(DFS_SELFTEST ${CMAKE_CURRENT_BINARY_DIR}/diskfsTest.stamp)
add_custom_command(OUTPUT ${DFS_SELFTEST}
    COMMAND ${QUINT_BIN} test --backend=rust
            --main=diskfsTestRef ${DFS_Q}/diskfsTest.qnt > /dev/null
    COMMAND ${CMAKE_COMMAND} -E touch ${DFS_SELFTEST}
    DEPENDS ${DFS_QNT} ${DFS_Q}/diskfsTest.qnt
    COMMENT "model self-test: diskfsTest"
    VERBATIM)

# profile | flavor | steps | ntraces | seed
#
# These traces expand into real device I/O (thousands of creates each), so the
# corpus is deliberately small and the counts modest.  stepGrow drives leaf
# split / merge / borrow / root-collapse; stepFragment drives space-map
# fragmentation + reclaim + extent-map growth; stepDurable interleaves work with
# remounts and crashes (intent-log replay).  The single diskfsDeep trace drives
# one directory past the second split into a height-3 tree (interior-node
# rebalance), which is expensive, so there is just one, short.
set(DFS_BATCHES
    "diskfsRef|stepGrow|14|2|0x1"
    "diskfsRef|stepFragment|16|2|0x2"
    "diskfsRef|stepDurable|12|2|0x3"
    "diskfsRef|step|18|2|0x4"
    "diskfsDeep|stepGrow|10|1|0x5"
    "diskfsRef|stepOps|24|2|0x6"
)

set(DFS_TRACES)
foreach(b ${DFS_BATCHES})
    string(REPLACE "|" ";" parts "${b}")
    list(GET parts 0 prof)
    list(GET parts 1 flavor)
    list(GET parts 2 steps)
    list(GET parts 3 ntraces)
    list(GET parts 4 seed)

    set(naming "${prof}_${flavor}_${steps}_${seed}_{seq}")
    set(batch)
    math(EXPR last "${ntraces} - 1")
    foreach(seq RANGE 0 ${last})
        string(REPLACE "{seq}" "${seq}" nm "${naming}")
        list(APPEND batch ${DFS_T}/${nm}.itf.json)
    endforeach()

    add_custom_command(OUTPUT ${batch}
        COMMAND ${CMAKE_COMMAND} -E make_directory ${DFS_T}
        COMMAND ${QUINT_BIN} run --backend=rust
                ${DFS_Q}/diskfs_run.qnt --main=${prof}
                --step=${flavor} --max-steps=${steps}
                --max-samples=${ntraces} --n-traces=${ntraces} --seed=${seed}
                --invariant=inv
                --out-itf=${DFS_T}/${naming}.itf.json > /dev/null
        DEPENDS ${DFS_QNT} ${DFS_SELFTEST}
        COMMENT "generate ${ntraces} diskfs traces ${prof}/${flavor} steps=${steps}"
        VERBATIM)
    list(APPEND DFS_TRACES ${batch})
endforeach()

add_custom_target(diskfs_traces ALL DEPENDS ${DFS_TRACES})
add_dependencies(diskfs_mbt_replay diskfs_traces)

add_test(NAME chimera/vfs/diskfs/mbt/batch
    COMMAND $<TARGET_FILE:diskfs_mbt_replay> --trace-dir ${DFS_T})
set_tests_properties(chimera/vfs/diskfs/mbt/batch PROPERTIES
    LABELS "quint;diskfs_mbt"
    TIMEOUT 900)
