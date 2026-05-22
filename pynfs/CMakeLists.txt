# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

set(CHIMERA_BINARY ${CMAKE_BINARY_DIR}/src/daemon/chimera)
set(PYNFS_WRAPPER ${CMAKE_SOURCE_DIR}/scripts/pynfs_test_wrapper.sh)
set(PYNFS_DIR /opt/pynfs)

set(PYNFS_BACKENDS memfs linux io_uring demofs_io_uring)

if(HAVE_LIBAIO)
    list(APPEND PYNFS_BACKENDS demofs_aio)
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

# Suites known to pass cleanly today.  Everything registered but not on these
# lists is marked DISABLED so `ctest` neither runs nor fails it (the suite is
# still listed, so the full matrix is visible and suites can be promoted here
# as the server gains conformance).  Keep these sorted.
set(PYNFS_PASS_0
    access commit create getattr getfh link lockt lookup lookupp nverify
    putpubfh putrootfh readdir readlink remove rename restorefh savefh verify)
set(PYNFS_PASS_1
    currentstateid destroy_clientid getfh lookup putfh rename verify xattr)
set(PYNFS_PASS_2
    destroy_clientid getfh lookup rename sparse verify xattr)

# Backends on which the pass-lists above have been validated.  Other backends
# register the same suites but leave them DISABLED until validated.
set(PYNFS_PASS_BACKENDS memfs)

# Extra pynfs flag tokens for specific suites.  pynfs excludes any test tagged
# with a "no<flag>" token, so this drops tests a suite pulls in that the server
# does not support.  currentstateid's CSID7 is tagged pnfs (LAYOUTGET); chimera
# is not a pNFS server, so exclude it.
set(PYNFS_EXTRA_currentstateid nopnfs)

# Per-suite wall-clock timeout overrides for pynfs_test_wrapper.sh.  The
# readdir group includes RDDR8b, which issues 300 READDIR compounds over a
# 100-entry long-name directory.  It passes functionally, but slow arm64 CI
# runners can exceed the wrapper's 60s default while still making progress.
set(PYNFS_TIMEOUT_readdir 180)

# Register one suite, enabling it only when both the backend and the
# flag/minor pair are on the validated pass-lists; otherwise mark it DISABLED.
function(pynfs_add_test flag backend minor)
    set(name chimera/pynfs/${flag}_${backend}_nfs4.${minor})
    add_test(NAME ${name}
        COMMAND bash ${PYNFS_WRAPPER}
                ${CHIMERA_BINARY} ${backend} ${minor} ${PYNFS_DIR} ${flag}
                ${PYNFS_EXTRA_${flag}})
    set_tests_properties(${name} PROPERTIES
        LABELS pynfs
        ENVIRONMENT "PYTHONUNBUFFERED=1")

    if(DEFINED PYNFS_TIMEOUT_${flag})
        set_tests_properties(${name} PROPERTIES
            ENVIRONMENT "PYTHONUNBUFFERED=1;PYNFS_TIMEOUT=${PYNFS_TIMEOUT_${flag}}")
    endif()

    set(enabled FALSE)
    if(("${backend}" IN_LIST PYNFS_PASS_BACKENDS) AND
       ("${flag}" IN_LIST PYNFS_PASS_${minor}))
        set(enabled TRUE)
    endif()

    if(NOT enabled)
        set_tests_properties(${name} PROPERTIES DISABLED TRUE)
    endif()
endfunction()

foreach(minor ${PYNFS_MINOR_VERSIONS})
    foreach(flag ${PYNFS_FLAGS_${minor}})
        foreach(backend ${PYNFS_BACKENDS})
            pynfs_add_test(${flag} ${backend} ${minor})
        endforeach()
    endforeach()
endforeach()

# pNFS flex-files (RFC 8435) suite.  Unlike the suites above it needs a pNFS
# metadata server *and* a separate data server, so it uses a dedicated wrapper
# that brings up both (memfs MDS on :2049 + memfs data_server on :2050) and runs
# pynfs's `flex` flag against the MDS.  memfs only.
set(PNFS_WRAPPER ${CMAKE_SOURCE_DIR}/scripts/pynfs_pnfs_test_wrapper.sh)
add_test(NAME chimera/pynfs/flex_memfs_nfs4.1
    COMMAND bash ${PNFS_WRAPPER} ${CHIMERA_BINARY} ${PYNFS_DIR} flex)
set_tests_properties(chimera/pynfs/flex_memfs_nfs4.1 PROPERTIES
    LABELS pynfs
    ENVIRONMENT "PYTHONUNBUFFERED=1")
