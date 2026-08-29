# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# Control-plane model-based tests: the REST API, the Prometheus scrape
# endpoint, and their effect on the data protocols, all driven in-process over
# libevpl's inproc transport (ctl_http.h).
#
# Nothing here binds a port, so every test runs fully parallel on any host --
# unlike the older REST tests in the parent directory, which shell out to curl
# against a real socket and therefore serialize under the netns resource lock.
#
# The Quint model lives HERE rather than in the specs submodule (ext/specs).
# That submodule holds models of published protocols -- POSIX, NFS, SMB2 --
# which are worth stating independently of any implementation and are useful
# to anyone testing a server.  Chimera's administrative API is chimera's own,
# so its specification belongs with it, and there is no second implementation
# for it to be shared with.  The practical consequence is that the corpus is
# generated at build time from the .qnt files below rather than fetched as a
# prebuilt bundle, so the replay ctest is registered only where quint is
# available.
#
#   ctl_api.qnt   the resources and what each REST call does to them
#   ctl.qnt       the state machine: the API plus the filehandles a client
#                 holds, which is what lets it say what deleting an export
#                 does to them
#   ctl_run.qnt   the profile
#   ctlTest.qnt   deterministic self-checks, run as a build gate

find_package(PkgConfig REQUIRED)
pkg_check_modules(JANSSON REQUIRED jansson)

add_executable(ctl_smoke_probe ctl_smoke_probe.c)
target_include_directories(ctl_smoke_probe PRIVATE
    ${CMAKE_SOURCE_DIR}/src ${JANSSON_INCLUDE_DIRS})
target_link_libraries(ctl_smoke_probe
    chimera_server chimera_metrics ${JANSSON_LIBRARIES})

# Ground truth for the control plane: the routing edges, the admin lifecycle
# over HTTP, the config reconstruction, and the scrape endpoint.  Needs no
# trace corpus and always runs.
add_test(NAME chimera/server/rest/ctl/smoke_probe
    COMMAND $<TARGET_FILE:ctl_smoke_probe>)
set_tests_properties(chimera/server/rest/ctl/smoke_probe PROPERTIES
    LABELS "quint;ctl_mbt;memfs"
    TIMEOUT 120)

# The control plane's effect on the data protocols: one process running the
# NFS server, the REST API and the scrape endpoint on inproc, with an rpc2
# client and an HTTP client on one event loop.  Reuses the NFS harness
# (nfs3_mbt_common.h) for the protocol half rather than duplicating it.
add_executable(ctl_proto_probe ctl_proto_probe.c)
target_include_directories(ctl_proto_probe PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/src/server/nfs/tests/quint
    ${CMAKE_BINARY_DIR}/src/nfs_common
    ${JANSSON_INCLUDE_DIRS})
target_link_libraries(ctl_proto_probe
    chimera_server chimera_nfs_common chimera_metrics ${JANSSON_LIBRARIES})

add_test(NAME chimera/server/rest/ctl/proto_probe
    COMMAND $<TARGET_FILE:ctl_proto_probe>)
set_tests_properties(chimera/server/rest/ctl/proto_probe PROPERTIES
    LABELS "quint;ctl_mbt;memfs"
    TIMEOUT 120)

# The corpus replayer.  Drives REST calls and NFS requests from one trace
# against one in-process server, so an administrative change and the very next
# protocol request are ordered the way the model says.
add_executable(ctl_mbt_replay ctl_mbt_replay.c)
target_include_directories(ctl_mbt_replay PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/src/server/nfs/tests/quint
    ${CMAKE_BINARY_DIR}/src/nfs_common
    ${JANSSON_INCLUDE_DIRS})
target_link_libraries(ctl_mbt_replay
    chimera_server chimera_nfs_common chimera_metrics ${JANSSON_LIBRARIES})

# ---------------------------------------------------------------------------
# Trace corpus
# ---------------------------------------------------------------------------
#
# Generated at build time by quint.  When quint is not installed the probes
# above still build and run -- they need no corpus -- and only the replay
# ctest is skipped, with a message rather than in silence.

find_program(QUINT_BIN quint)
find_package(Python3 COMPONENTS Interpreter QUIET)

if(NOT QUINT_BIN OR NOT Python3_Interpreter_FOUND)
    message(STATUS "ctl MBT replay skipped (needs quint and python3)")
    return()
endif()

set(CTL_Q ${CMAKE_CURRENT_SOURCE_DIR})
set(CTL_T ${CMAKE_CURRENT_BINARY_DIR}/traces)
set(CTL_QNT
    ${CTL_Q}/ctl_api.qnt
    ${CTL_Q}/ctl.qnt
    ${CTL_Q}/ctl_run.qnt)

# The model self-tests gate generation, so a model regression stops the corpus
# at its source instead of producing traces that assert the wrong thing.
set(CTL_SELFTEST ${CMAKE_CURRENT_BINARY_DIR}/ctlTest.stamp)
add_custom_command(OUTPUT ${CTL_SELFTEST}
    COMMAND ${QUINT_BIN} test --main=ctlTestRef ${CTL_Q}/ctlTest.qnt > /dev/null
    COMMAND ${CMAKE_COMMAND} -E touch ${CTL_SELFTEST}
    DEPENDS ${CTL_QNT} ${CTL_Q}/ctlTest.qnt
    COMMENT "model self-test: ctlTest"
    VERBATIM)

# profile | flavor | steps | ntraces | seed
#
# Three flavors, because one walk cannot weight everything at once.  `step`
# gives the interleaving this suite is about; `stepProtoHeavy` produces long
# runs of protocol requests across an export's lifetime, which is where a
# policy that stopped applying shows up; `stepAdminOnly` reaches the
# referential refusals, which a mixed walk hits only by accident.
set(CTL_BATCHES
    "ctlRef|step|40|8|0x1"
    "ctlRef|step|80|4|0x2"
    "ctlRef|stepProtoHeavy|60|6|0x3"
    "ctlRef|stepAdminOnly|40|4|0x4"
    "ctlRef|stepPolicy|60|6|0x5"
    "ctlRef|stepPolicy|80|4|0x6"
)

set(CTL_TRACES)
foreach(b ${CTL_BATCHES})
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
        list(APPEND batch ${CTL_T}/${nm}.itf.json)
    endforeach()

    add_custom_command(OUTPUT ${batch}
        COMMAND ${CMAKE_COMMAND} -E make_directory ${CTL_T}
        COMMAND ${QUINT_BIN} run ${CTL_Q}/ctl_run.qnt --main=${prof}
                --step=${flavor} --max-steps=${steps}
                --max-samples=${ntraces} --n-traces=${ntraces} --seed=${seed}
                --invariant=inv
                --out-itf=${CTL_T}/${naming}.itf.json > /dev/null
        DEPENDS ${CTL_QNT} ${CTL_SELFTEST}
        COMMENT "generate ${ntraces} ctl traces ${prof}/${flavor} steps=${steps}"
        VERBATIM)
    list(APPEND CTL_TRACES ${batch})
endforeach()

add_custom_target(ctl_traces ALL DEPENDS ${CTL_TRACES})
add_dependencies(ctl_mbt_replay ctl_traces)

# --paranoid is on deliberately.  Comparing statuses alone cannot see a
# control-plane call that reported success and did nothing: a create that
# answers 201 and a create that answers 201 and registers the resource are the
# same reply.  The flag re-reads every listing and the config reconstruction
# after each step and compares them against the model's own state, so such a
# call is caught where it happened.
add_test(NAME chimera/server/rest/ctl/batch_memfs
    COMMAND $<TARGET_FILE:ctl_mbt_replay> --paranoid --trace-dir ${CTL_T})
set_tests_properties(chimera/server/rest/ctl/batch_memfs PROPERTIES
    LABELS "quint;ctl_mbt;memfs"
    TIMEOUT 600)

# The corpus gate.  A control-plane suite is unusually easy to pass vacuously
# -- a corpus that only ever creates things and never sends a protocol request
# replays green against a server whose exports API stops working after the
# create -- so this asserts the traces actually reach the states the model was
# written to describe.  See coverage.py for what each bucket is for.
add_test(NAME chimera/server/rest/ctl/coverage
    COMMAND ${Python3_EXECUTABLE} ${CTL_Q}/coverage.py ${CTL_T})
set_tests_properties(chimera/server/rest/ctl/coverage PROPERTIES
    LABELS "quint;ctl_mbt"
    TIMEOUT 120)
