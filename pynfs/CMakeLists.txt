# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

set(CHIMERA_BINARY ${CMAKE_BINARY_DIR}/src/daemon/chimera)
set(PYNFS_WRAPPER ${CMAKE_SOURCE_DIR}/scripts/pynfs_test_wrapper.sh)
set(PYNFS_DIR /opt/pynfs)

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
    bind_conn_to_session compound courteous create_session currentstateid deleg
    destroy_clientid destroy_session exchange_id getfh lookup lookupp open putfh
    reclaim_complete rename sequence verify xattr)

# NFSv4.2 test flags (4.1 flags plus copy and sparse)
set(PYNFS_FLAGS_2 ${PYNFS_FLAGS_1} copy sparse)

set(PYNFS_MINOR_VERSIONS 0 1 2)

# Flags whose tests deliberately drive lease/clientid/delegation/session expiry
# -- they sleep through a full lease period (st_*.py: getLeaseTime()-scaled
# sleeps) and/or reclaim state across a second client.  These CANNOT be folded
# into the combined run below: pynfs shares one client across a run, so a
# deliberate expiry lapses that shared client and a second client's conflicting
# op reclaims its state (correct courteous-server behaviour, but it then poisons
# the rest of the run with NFS4ERR_EXPIRED).  Each stays broken out as one CTest
# entry per *code* (a fresh daemon each), carrying its lease/deleg/timeout knobs.
# Every OTHER flag has no such sleep, so all of them collapse into a single
# combined run per (backend, minor) -- one daemon + one netns for ~500 codes.
set(PYNFS_ISOLATED_FLAGS
    # 4.0 expiry/lease-scaled-sleep flags (st_lock/locku/close/renew/replay/
    # delegation + the lock family that drives a second conflicting client)
    lock lockt locku close renew replay delegations
    # 4.1/4.2 expiry flags (st_courtesy/delegation/exchange_id + session teardown)
    deleg courteous destroy_session exchange_id)

# Per-suite JUnit (one <testcase> per code).  ctest --output-junit only sees the
# combined run as a single testcase, so each combined run also writes a pynfs
# JUnit here; CI ingests these for per-code PR visibility (the broken-out
# per-code entries need none -- ctest's own JUnit already names them per code).
set(PYNFS_JUNIT_DIR ${CMAKE_BINARY_DIR}/pynfs-junit
    CACHE PATH "Directory for per-suite pynfs JUnit XML output.")

# Extra pynfs flag tokens for specific suites.  pynfs excludes any test tagged
# with a "no<flag>" token, so this drops tests a suite pulls in that the server
# does not support.  currentstateid's CSID7 is tagged pnfs (LAYOUTGET); chimera
# is not a pNFS server, so exclude it.
set(PYNFS_EXTRA_currentstateid nopnfs)

# Per-code backend skip list: (code -> backends) pairs that are not even
# registered as CTest entries (so they do not appear at all, rather than as
# noisy DISABLED entries).  WRT18 (testChangeGranularityWrite) and SATT15
# (testChangeGranularity) require the change attribute to advance on *every*
# rapid write/setattr.  For the passthrough backends (linux, io_uring) the change
# attribute is the host filesystem's ctime, which only advances per-rapid-op when
# the kernel provides multigrain timestamps (Linux >= 6.13 on an FS_MGTIME
# filesystem).  Our CI runner's kernel predates multigrain, so these cannot pass
# there.  They are still registered and pass on memfs/cairn/diskfs (which carry
# their own fine-grained change clock) and on modern-kernel deployments.  Keyed
# by pynfs code; WRT18/SATT15 are nfs4.0-only, so the code names are unambiguous.
set(PYNFS_SKIP_BACKENDS_WRT18  linux io_uring)
set(PYNFS_SKIP_BACKENDS_SATT15 linux io_uring)

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
# client setup is slowest, pushing total runtime toward the wrapper timeout just
# as the longer sleeps already cost more.  COUR7's budget is:
#   setup (1000 clients) + sleep(lease+10 = 30s) + conflicting open
# The 90s ceiling that appeared sufficient when the fix was written (with ~8-10s
# setup observed then) proved too tight after the devcontainer test job was split
# into per-category shards: all pynfs tests now run concurrently in one shard,
# and the heavier intra-shard parallelism pushes the setup past 60s under CI
# load, hitting the 90s wall.  The whole family fails in lockstep -- the
# classic all-backends-at-once load-induced timeout signature.  Raise to 180s,
# matching the headroom already given to the other load-sensitive pynfs suites
# (delegations, destroy_session, readdir).
set(PYNFS_LEASE_OVERRIDE_courteous COUR1 COUR2 COUR3 COUR4 COUR5 COUR6 COUR7)
set(PYNFS_LEASE_OVERRIDE_SECONDS_courteous 20)
set(PYNFS_TIMEOUT_courteous 180)

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

# The readdir suite carries one CPU-bound code, RDDR8b
# (st_readdir.testDircountVarious), which loops 300 sequential READDIR
# compounds through pynfs's pure-Python RPC client.  On amd64 it runs in
# ~20s wall clock; on the emulated-arm64 devcontainer CI runner (Python
# under QEMU TCG, 3-5x slower) it bumps against the wrapper's 60s default
# and all six backends in that job time out together at ~62s.  Server side
# is irrelevant -- chimera's READDIR finishes in microseconds for these
# parameters; the budget is purely the client's Python-cost x 300 calls.
# Raise the readdir-suite timeout to give RDDR8b headroom on the slow
# runner; the other readdir codes finish in well under 1s, so a higher
# cap is a no-op for them.  Matches the headroom used for the (similarly
# pynfs-client-bound) delegations and destroy_session suites.
set(PYNFS_TIMEOUT_readdir 180)

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

# Register one CTest entry per PyNFS code/backend, except (code, backend) pairs
# in PYNFS_SKIP_BACKENDS_<code>, which are not registered at all (see above).
function(pynfs_add_code_test flag code backend minor)
    if("${backend}" IN_LIST PYNFS_SKIP_BACKENDS_${code})
        return()
    endif()

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

    # Label each test along every dimension encoded in its name so the suite
    # can be sharded independently: the whole suite (pynfs), the category/flag
    # (pynfs_rename, pynfs_courteous, ...), the VFS backend (memfs, linux, ...),
    # and the NFSv4 minor version (nfs4.0, nfs4.1, nfs4.2).
    set_tests_properties(${name} PROPERTIES
        LABELS "pynfs;pynfs_${flag};${backend};nfs4.${minor}"
        ENVIRONMENT "${test_env}")
endfunction()

# One combined CTest entry per (backend, minor): every NON-isolated flag for that
# minor in a single testserver invocation -- one daemon + one netns for ~500
# codes, instead of one netns per code.  Positive flags first, then the
# PYNFS_EXTRA / no<CODE> deselections (run_filter is last-match-wins).
function(pynfs_add_combined_suite backend minor)
    set(flags)
    set(excludes)
    # Each isolated flag that exists in THIS minor is deselected from the
    # combined run with a trailing no<flag>, so a code tagged with both an
    # isolated flag and a non-isolated one (pynfs tags some delegation/callback
    # tests, e.g. testReadDeleg/testCBSecParms, with `deleg` AND open/sequence)
    # does not leak into the combined run -- which has delegations off -- and
    # fail.  run_filter is last-match-wins, so the trailing no<flag> wins; the
    # code still runs in its isolated per-code entry.  Only minor-valid flags are
    # emitted (a 4.0-only flag like `lock` is not a valid token for the 4.1
    # testserver, which would abort with "Unknown code or flag").
    foreach(flag ${PYNFS_FLAGS_${minor}})
        if(NOT "${flag}" IN_LIST PYNFS_ISOLATED_FLAGS)
            list(APPEND flags ${flag})
            foreach(extra IN LISTS PYNFS_EXTRA_${flag})
                list(APPEND excludes ${extra})
            endforeach()
        else()
            list(APPEND excludes "no${flag}")
        endif()
    endforeach()

    # Codes that cannot pass on the passthrough backends are deselected here
    # (the per-code path drops them from registration; in a shared run we use
    # pynfs's no<CODE> token instead).  WRT18 (write) / SATT15 (setattr) are
    # NFSv4.0-only codes, so only emit their no<CODE> token in the 4.0 run -- the
    # 4.1/4.2 testserver aborts with "Unknown code or flag" on an unknown token.
    if("${minor}" STREQUAL "0")
        if("${backend}" IN_LIST PYNFS_SKIP_BACKENDS_WRT18)
            list(APPEND excludes noWRT18)
        endif()
        if("${backend}" IN_LIST PYNFS_SKIP_BACKENDS_SATT15)
            list(APPEND excludes noSATT15)
        endif()
    endif()

    set(name chimera/pynfs/combined_${backend}_nfs4.${minor})
    add_test(NAME ${name}
        COMMAND bash ${PYNFS_WRAPPER}
                ${CHIMERA_BINARY} ${backend} ${minor} ${PYNFS_DIR} ${flags} ${excludes})

    # No deliberate-expiry flags here, so nothing lapses the shared client during
    # the run -- but a lease comfortably above the longest non-renewing gap
    # (readdir's RDDR8b loops ~300 READDIRs) keeps it alive, and there are no
    # lease-scaled sleeps to bloat.  ~500 codes run through one Python client, so
    # give a generous timeout (the wrapper's fires first, with diagnostics).
    set_tests_properties(${name} PROPERTIES
        LABELS pynfs
        TIMEOUT 900
        ENVIRONMENT
            "PYTHONUNBUFFERED=1;PYNFS_RUNDEPS=1;PYNFS_TIMEOUT=800;PYNFS_NFS4_LEASE_TIME=90;PYNFS_JUNIT_FILE=${PYNFS_JUNIT_DIR}/combined_${backend}_nfs4.${minor}.xml")
endfunction()

foreach(minor ${PYNFS_MINOR_VERSIONS})
    # Combined run for the safe (non-isolated) flags.
    foreach(backend ${PYNFS_BACKENDS})
        pynfs_add_combined_suite(${backend} ${minor})
    endforeach()
    # Expiry flags stay broken out, one CTest entry per code (fresh daemon each).
    foreach(flag ${PYNFS_FLAGS_${minor}})
        if("${flag}" IN_LIST PYNFS_ISOLATED_FLAGS)
            pynfs_codes_for_flag(${flag} ${minor} codes)
            foreach(backend ${PYNFS_BACKENDS})
                foreach(code ${codes})
                    pynfs_add_code_test(${flag} ${code} ${backend} ${minor})
                endforeach()
            endforeach()
        endif()
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
        LABELS "pynfs;pynfs_flex;memfs;nfs4.1"
        ENVIRONMENT "PYTHONUNBUFFERED=1;PYNFS_RUNDEPS=1")
endforeach()

# RPCSEC_GSS / Kerberos (sec=krb5) end-to-end smoke.  Stands up an MIT KDC and a
# chimera NFS server with Kerberos enabled inside a netns, then drives pynfs 4.1
# with sec=krb5 (pynfs 4.1 derives the GSS target as nfs@<server>, so it works
# unmodified).  Exercises the full RPCSEC_GSS path: context establishment, the
# per-request call/reply verifiers, the replay window, and -- via BIND -- a GSS
# context shared across trunked connections.  Skips (77) without krb5 tooling or
# python gssapi.  memfs only; gated like the other netns tests.
set(NFS_KRB_WRAPPER ${CMAKE_SOURCE_DIR}/scripts/nfs_kerberos_test_wrapper.sh)
set(NFS_KRB_CODES EID1 EID2 CSESS1 CSESS2 SEQ1 SEQ4 BIND1 BIND2 BIND3 RECC1 OPEN1 OPEN2)
add_test(NAME chimera/pynfs/kerberos/krb5_smoke_memfs_nfs4.1
    COMMAND bash ${NFS_KRB_WRAPPER} ${CHIMERA_BINARY} memfs ${PYNFS_DIR} ${NFS_KRB_CODES})
set_tests_properties(chimera/pynfs/kerberos/krb5_smoke_memfs_nfs4.1 PROPERTIES
    LABELS "pynfs;pynfs_kerberos;rpcsec_gss;memfs;nfs4.1"
    SKIP_RETURN_CODE 77
    ENVIRONMENT "PYTHONUNBUFFERED=1")

# Per-export sec= policy (Phase 2).  Restrict the export to krb5/krb5i and
# verify (a) an allowed flavor (krb5) still works end-to-end through the
# enforcement path, and (b) a disallowed flavor (sys) is rejected with
# NFS4ERR_WRONGSEC at the export boundary.
add_test(NAME chimera/pynfs/kerberos/sec_krb5only_allow_memfs_nfs4.1
    COMMAND bash ${NFS_KRB_WRAPPER} ${CHIMERA_BINARY} memfs ${PYNFS_DIR} ${NFS_KRB_CODES})
set_tests_properties(chimera/pynfs/kerberos/sec_krb5only_allow_memfs_nfs4.1 PROPERTIES
    LABELS "pynfs;pynfs_kerberos;rpcsec_gss;memfs;nfs4.1"
    SKIP_RETURN_CODE 77
    ENVIRONMENT "PYTHONUNBUFFERED=1;NFS_EXPORT_SEC=krb5,krb5i;NFS_KRB_SEC=krb5")

add_test(NAME chimera/pynfs/kerberos/sec_krb5only_deny_sys_memfs_nfs4.1
    COMMAND bash ${NFS_KRB_WRAPPER} ${CHIMERA_BINARY} memfs ${PYNFS_DIR} EID1 CSESS1)
set_tests_properties(chimera/pynfs/kerberos/sec_krb5only_deny_sys_memfs_nfs4.1 PROPERTIES
    LABELS "pynfs;pynfs_kerberos;rpcsec_gss;memfs;nfs4.1"
    SKIP_RETURN_CODE 77
    ENVIRONMENT "PYTHONUNBUFFERED=1;NFS_EXPORT_SEC=krb5;NFS_KRB_SEC=sys;NFS_EXPECT_WRONGSEC=1")
