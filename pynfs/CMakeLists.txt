# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

set(CHIMERA_BINARY ${CMAKE_BINARY_DIR}/src/daemon/chimera)
set(PYNFS_WRAPPER ${CMAKE_SOURCE_DIR}/scripts/pynfs_test_wrapper.sh)
set(PYNFS_DIR /opt/pynfs)
set(PYNFS_STABLE_PASS_FILE ${CMAKE_CURRENT_SOURCE_DIR}/stable-passing-tests.txt)
option(PYNFS_ENABLE_UNSTABLE_TESTS "Enable all generated pynfs tests, including tests outside the stable pass list" OFF)

set(PYNFS_BACKENDS memfs linux io_uring diskfs_io_uring)

if(HAVE_LIBAIO)
    list(APPEND PYNFS_BACKENDS diskfs_aio)
endif()

if(CAIRN_ENABLED)
    list(APPEND PYNFS_BACKENDS cairn)
endif()

# NFSv4.0 test flags
set(PYNFS_FLAGS_0
    access acl close commit compound create delegations getattr getfh link lock
    lockt locku lookup lookupp nverify open openconfirm opendowngrade putfh
    putpubfh putrootfh read readdir readlink releaselockowner remove rename renew
    replay restorefh savefh secinfo setattr setclientid setclientidconfirm verify
    write)

# NFSv4.1 test flags
set(PYNFS_FLAGS_1
    compound courteous create_session currentstateid deleg destroy_clientid
    destroy_session exchange_id getfh lookup lookupp open putfh reclaim_complete
    rename sequence verify xattr)

# NFSv4.2 test flags (4.1 flags plus copy and sparse)
set(PYNFS_FLAGS_2 ${PYNFS_FLAGS_1} copy sparse)

set(PYNFS_MINOR_VERSIONS 0 1 2)

# Exact CTest names validated as stable by repeated full-matrix pynfs runs.
# Everything else is still registered but marked DISABLED, so the full matrix
# remains visible and failed/flaky tests can be promoted individually.
file(STRINGS ${PYNFS_STABLE_PASS_FILE} PYNFS_STABLE_PASS_TESTS)

# Extra pynfs flag tokens for specific suites.  pynfs excludes any test tagged
# with a "no<flag>" token, so this drops tests a suite pulls in that the server
# does not support.  currentstateid's CSID7 is tagged pnfs (LAYOUTGET); chimera
# is not a pNFS server, so exclude it.
set(PYNFS_EXTRA_currentstateid nopnfs)

# Per-code NFSv4 lease-time override (seconds).  Delegation recall-handshake
# tests open a file as one client, then drive a *second* client's conflicting
# OPEN to force a CB_RECALL.  Establishing that second client (SETCLIENTID /
# EXCHANGE_ID + CREATE_SESSION) does not renew the first client's lease, so under
# ctest -j load the default 5s lease can lapse mid-test and the delegation is
# revoked before the recall round-trips -- a load-dependent false failure.  These
# tests perform no lease-scaled sleeps, so a longer lease adds no wall-clock cost.
# Keyed by pynfs flag; listed codes get PYNFS_LEASE_OVERRIDE_SECONDS.  DELEG6
# (4.0 testRenew) also scales its own sleeps to the lease period, but it first
# drives an unanswered CB_RECALL; with the wrapper's 5s default lease, Chimera's
# 15s recall-failure path can expire the delegation holder before the test sends
# RENEW.  Give it a lease beyond the recall window and a matching timeout.
set(PYNFS_LEASE_OVERRIDE_SECONDS 30)
set(PYNFS_LEASE_OVERRIDE_delegations DELEG1 DELEG4 DELEG5 DELEG6 DELEG7 DELEG9 DELEG14)
set(PYNFS_LEASE_OVERRIDE_SECONDS_delegations 40)
set(PYNFS_TIMEOUT_delegations 180)
set(PYNFS_LEASE_OVERRIDE_deleg DELEG8)

# Courteous-server tests (st_courtesy.py, COUR1-COUR7) need a longer lease for a
# different reason, and -- unlike the delegation tests above -- they DO scale
# their own sleeps to the lease (env.sleep(lease+10), or lease*2 for COUR1),
# since they deliberately drive a client past lease expiry.  The flake: COUR7
# (testExpiringManyClients) establishes 1000 clients back-to-back, and under
# ctest -j load a single EXCHANGE_ID -> CREATE_SESSION handshake can slip past
# the 5s default lease.  The sweep then expires that still-unconfirmed client,
# so its own CREATE_SESSION (correctly, per RFC 8881 18.35.3 / EID9
# testLeasePeriod) returns NFS4ERR_STALE_CLIENTID and the test fails.  A longer
# lease makes that setup race effectively impossible.  Because the sleeps scale
# with the lease, use a *modest* override (not the 30s above): 20s gives a 4x
# margin over the observed race (a handshake gap would have to exceed 20s,
# impossible across the ~8-10s setup seen in CI) without bloating the deliberate
# sleeps (COUR7 -> ~40s, COUR1 -> ~40s in CI).  Per-flag
# PYNFS_LEASE_OVERRIDE_SECONDS_<flag> overrides the shared default above.
#
# The flake is load-correlated -- a heavy ctest -j run is exactly when the 1000
# client setup is slowest, pushing total runtime toward the 60s default wrapper
# timeout (pynfs_test_wrapper.sh) just as the longer sleeps already cost more.
# Raise the courteous wrapper timeout (PYNFS_TIMEOUT_<flag>, plumbed below) to
# absorb that variance; 90s leaves ample headroom (COUR1's lease*2 = 40s sleep
# plus even a badly inflated setup) while still flagging a genuine hang.
set(PYNFS_LEASE_OVERRIDE_courteous COUR1 COUR2 COUR3 COUR4 COUR5 COUR6 COUR7)
set(PYNFS_LEASE_OVERRIDE_SECONDS_courteous 20)
set(PYNFS_TIMEOUT_courteous 90)

# Per-code delegation-enable override.  The wrapper enables NFSv4 protocol
# delegations only for the deleg/delegations suites, so the other suites
# exercise the default (delegations-off) behavior.  DSESS9002/DSESS9003 live in
# the destroy_session suite but need delegations on: each destroys and recreates
# a client's session and drives a CB_RECALL to verify the backchannel survives a
# DESTROY_SESSION/CREATE_SESSION cycle.  DSESS9002 grants the delegation over the
# new session; DSESS9003 grants it over the old session, leaves the recall
# unanswered, then expects the server to re-drive that recall over the recreated
# session (see nfs4_cb_resend_recalls_on_rebind).  Keyed by pynfs flag; listed
# codes get PYNFS_FORCE_DELEG=1.  These recall tests share the delegation suite's
# lease/timeout sensitivity: DSESS9003 in particular drives an *unanswered*
# CB_RECALL (so chimera's 15s recall-failure window is in play) and then a recall
# round-trip over the recreated session, which under a heavy ctest -j ASan run
# can run long.  Mirror the delegations suite -- a 40s lease (beyond the recall
# window) and a 180s wrapper timeout -- so the handshake has headroom and the
# first client's lease cannot lapse mid-test.
set(PYNFS_FORCE_DELEG_destroy_session DSESS9002 DSESS9003)
set(PYNFS_LEASE_OVERRIDE_destroy_session DSESS9002 DSESS9003)
set(PYNFS_LEASE_OVERRIDE_SECONDS_destroy_session 40)
set(PYNFS_TIMEOUT_destroy_session 180)

function(pynfs_codes_for_flag flag minor out_var)
    if("${minor}" STREQUAL "0")
        set(testserver ${PYNFS_DIR}/nfs4.0/testserver.py)
    else()
        set(testserver ${PYNFS_DIR}/nfs4.1/testserver.py)
    endif()

    execute_process(
        COMMAND ${CMAKE_COMMAND} -E env "PYTHONPATH=${PYNFS_DIR}"
                python3 ${testserver} --showcodesflags
        RESULT_VARIABLE result
        OUTPUT_VARIABLE output
        ERROR_VARIABLE error)

    if(NOT result EQUAL 0)
        message(FATAL_ERROR
            "Failed to query pynfs test codes from ${testserver}: ${error}")
    endif()

    string(REPLACE "\n" ";" lines "${output}")
    set(codes)
    foreach(line IN LISTS lines)
        if(line MATCHES "^([^ ]+) FLAGS: (.*)$")
            set(code ${CMAKE_MATCH_1})
            set(code_flags_text ${CMAKE_MATCH_2})
            string(REPLACE ", " ";" code_flags "${code_flags_text}")

            set(selected FALSE)
            if("${flag}" IN_LIST code_flags)
                set(selected TRUE)
            endif()

            foreach(extra IN LISTS PYNFS_EXTRA_${flag})
                if(extra MATCHES "^no(.+)$")
                    set(excluded_flag ${CMAKE_MATCH_1})
                    if("${excluded_flag}" IN_LIST code_flags)
                        set(selected FALSE)
                    endif()
                endif()
            endforeach()

            if(selected)
                list(APPEND codes ${code})
            endif()
        endif()
    endforeach()

    list(REMOVE_DUPLICATES codes)
    set(${out_var} ${codes} PARENT_SCOPE)
endfunction()

# Register one PyNFS code per CTest entry, enabling it only when the exact
# generated test name is on the validated stable-pass list.
function(pynfs_add_code_test flag code backend minor)
    set(name chimera/pynfs/${flag}/${code}_${backend}_nfs4.${minor})
    add_test(NAME ${name}
        COMMAND bash ${PYNFS_WRAPPER}
                ${CHIMERA_BINARY} ${backend} ${minor} ${PYNFS_DIR} ${code})

    set(test_env "PYTHONUNBUFFERED=1" "PYNFS_RUNDEPS=1")

    if(DEFINED PYNFS_TIMEOUT_${flag})
        list(APPEND test_env "PYNFS_TIMEOUT=${PYNFS_TIMEOUT_${flag}}")
    endif()

    # Lease-sensitive tests: see PYNFS_LEASE_OVERRIDE_* above.  A flag may set
    # its own PYNFS_LEASE_OVERRIDE_SECONDS_<flag>; otherwise the shared default
    # applies.
    if("${code}" IN_LIST PYNFS_LEASE_OVERRIDE_${flag})
        set(lease_seconds ${PYNFS_LEASE_OVERRIDE_SECONDS})
        if(DEFINED PYNFS_LEASE_OVERRIDE_SECONDS_${flag})
            set(lease_seconds ${PYNFS_LEASE_OVERRIDE_SECONDS_${flag}})
        endif()
        list(APPEND test_env "PYNFS_NFS4_LEASE_TIME=${lease_seconds}")
    endif()

    # Delegation-sensitive tests outside the deleg suites: see
    # PYNFS_FORCE_DELEG_* above.
    if("${code}" IN_LIST PYNFS_FORCE_DELEG_${flag})
        list(APPEND test_env "PYNFS_FORCE_DELEG=1")
    endif()

    set_tests_properties(${name} PROPERTIES
        LABELS pynfs
        ENVIRONMENT "${test_env}")

    if(NOT PYNFS_ENABLE_UNSTABLE_TESTS AND NOT "${name}" IN_LIST PYNFS_STABLE_PASS_TESTS)
        set_tests_properties(${name} PROPERTIES DISABLED TRUE)
    endif()
endfunction()

foreach(minor ${PYNFS_MINOR_VERSIONS})
    foreach(flag ${PYNFS_FLAGS_${minor}})
        pynfs_codes_for_flag(${flag} ${minor} codes)
        foreach(backend ${PYNFS_BACKENDS})
            foreach(code ${codes})
                pynfs_add_code_test(${flag} ${code} ${backend} ${minor})
            endforeach()
        endforeach()
    endforeach()
endforeach()

# pNFS flex-files (RFC 8435) suite.  Unlike the suites above it needs a pNFS
# metadata server *and* a separate data server, so it uses a dedicated wrapper
# that brings up both (memfs MDS on :2049 + memfs data_server on :2050) and runs
# pynfs's `flex` flag against the MDS.  memfs only.
set(PNFS_WRAPPER ${CMAKE_SOURCE_DIR}/scripts/pynfs_pnfs_test_wrapper.sh)
pynfs_codes_for_flag(flex 1 PYNFS_FLEX_CODES)
foreach(code ${PYNFS_FLEX_CODES})
    set(name chimera/pynfs/flex/${code}_memfs_nfs4.1)
    add_test(NAME ${name}
        COMMAND bash ${PNFS_WRAPPER} ${CHIMERA_BINARY} ${PYNFS_DIR} ${code})
    set_tests_properties(${name} PROPERTIES
        LABELS pynfs
        ENVIRONMENT "PYTHONUNBUFFERED=1;PYNFS_RUNDEPS=1")
    if(NOT PYNFS_ENABLE_UNSTABLE_TESTS AND NOT "${name}" IN_LIST PYNFS_STABLE_PASS_TESTS)
        set_tests_properties(${name} PROPERTIES DISABLED TRUE)
    endif()
endforeach()
