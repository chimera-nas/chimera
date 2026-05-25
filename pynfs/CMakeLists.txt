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
# (4.0 testRenew) is deliberately excluded: it scales its own sleeps to the lease
# period, so a longer lease would slow it down / trip the per-test timeout.
set(PYNFS_LEASE_OVERRIDE_SECONDS 30)
set(PYNFS_LEASE_OVERRIDE_delegations DELEG1 DELEG4 DELEG5 DELEG7 DELEG14)
set(PYNFS_LEASE_OVERRIDE_deleg DELEG8)

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

    # Lease-sensitive delegation recall tests: see PYNFS_LEASE_OVERRIDE_* above.
    if("${code}" IN_LIST PYNFS_LEASE_OVERRIDE_${flag})
        list(APPEND test_env "PYNFS_NFS4_LEASE_TIME=${PYNFS_LEASE_OVERRIDE_SECONDS}")
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
