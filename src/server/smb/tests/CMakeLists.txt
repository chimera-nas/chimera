# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only


# SMB authentication test program
add_executable(smb_auth_test smb_auth_test.c)
target_link_libraries(smb_auth_test chimera_vfs urcu urcu-cds)
target_include_directories(smb_auth_test PRIVATE ${CMAKE_SOURCE_DIR}/src)

add_test(NAME chimera/server/smb/auth_test
    COMMAND ${CMAKE_CURRENT_BINARY_DIR}/smb_auth_test --mode=local)

# SMB smbclient authentication test
add_executable(smbclient_auth_test smbclient_auth_test.c)
target_link_libraries(smbclient_auth_test chimera_server chimera_metrics jansson)
target_include_directories(smbclient_auth_test PRIVATE ${CMAKE_SOURCE_DIR}/src)

if(CHIMERA_NETNS_TESTING)
    add_test(NAME chimera/server/smb/smbclient_ntlm
        COMMAND ${CMAKE_SOURCE_DIR}/scripts/netns_test_wrapper.sh
                ${CMAKE_CURRENT_BINARY_DIR}/smbclient_auth_test --mode=ntlm)
endif()

# kerberos_test_wrapper.sh and ad_test_wrapper.sh both set up an isolated
# netns internally, so they need the same privilege as the netns wrapper.
if(CHIMERA_NETNS_TESTING)
    add_test(NAME chimera/server/smb/smbclient_kerberos
        COMMAND ${CMAKE_SOURCE_DIR}/scripts/kerberos_test_wrapper.sh
                ${CMAKE_CURRENT_BINARY_DIR}/smbclient_auth_test --mode=kerberos)
    set_tests_properties(chimera/server/smb/smbclient_kerberos PROPERTIES
        SKIP_RETURN_CODE 77)

    find_program(SAMBA_TOOL samba-tool)
    if(SAMBA_TOOL)
        add_test(NAME chimera/server/smb/smbclient_winbind
            COMMAND ${CMAKE_SOURCE_DIR}/scripts/ad_test_wrapper.sh
                    ${CMAKE_CURRENT_BINARY_DIR}/smbclient_auth_test --mode=winbind)
    endif()
endif()

# SMB share mode test
add_executable(smb_sharemode_test smb_sharemode_test.c)
target_link_libraries(smb_sharemode_test chimera_server chimera_smb)
target_include_directories(smb_sharemode_test PRIVATE ${CMAKE_SOURCE_DIR}/src)

add_test(NAME chimera/server/smb/sharemode_test
    COMMAND ${CMAKE_CURRENT_BINARY_DIR}/smb_sharemode_test)

# Phase 0: negotiate-context and CREATE-context dispatch
add_executable(phase0_contexts_test phase0_contexts_test.c)
target_link_libraries(phase0_contexts_test chimera_server chimera_smb)
target_include_directories(phase0_contexts_test PRIVATE ${CMAKE_SOURCE_DIR}/src)

add_test(NAME chimera/server/smb/phase0_contexts_test
    COMMAND ${CMAKE_CURRENT_BINARY_DIR}/phase0_contexts_test)

# REST API user/share manipulation test
add_executable(rest_user_manip_test rest_user_manip_test.c)
target_link_libraries(rest_user_manip_test chimera_server chimera_metrics jansson)
target_include_directories(rest_user_manip_test PRIVATE ${CMAKE_SOURCE_DIR}/src)

if(CHIMERA_NETNS_TESTING)
    add_test(NAME chimera/server/smb/rest_user_manip
        COMMAND ${CMAKE_SOURCE_DIR}/scripts/netns_test_wrapper.sh
                ${CMAKE_CURRENT_BINARY_DIR}/rest_user_manip_test)
endif()

# ---------------------------------------------------------------------------
# Microsoft Windows Protocol Test Suites (WPTS) -- MS-SMB2 server conformance
#
# WPTS speaks SMB2 from its own managed (.NET) stack, so it runs host-side in
# a network namespace against the chimera daemon -- no KVM/kernel CIFS client
# needed (unlike the cthon/xfstests KVM suites). The .NET runtime and the
# extracted FileServer "Bin" directory are external prerequisites, located via
# cache variables (or auto-detected on PATH / common install dirs).
#
#   cmake -DWPTS_FILESERVER_DIR=/opt/wpts/Bin -DWPTS_DOTNET=/opt/dotnet/dotnet ...
#
# The chimera daemon binary is required to launch the SUT.
find_program(WPTS_DOTNET dotnet HINTS /opt/dotnet /usr/lib/dotnet /usr/share/dotnet)
find_file(WPTS_SMB2_DLL MS-SMB2_ServerTestSuite.dll
          HINTS ${WPTS_FILESERVER_DIR} /opt/wpts/Bin
          NO_DEFAULT_PATH)

# The WPTS .NET test host only runs reliably on x86_64. The arm64 CI builds run
# under emulation, where the managed test host crashes ("Test host process
# crashed") even though the chimera daemon is healthy. Gate to x86_64 until a
# native arm64 runner is available.
set(WPTS_ARCH_SUPPORTED FALSE)
if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64|AMD64)$")
    set(WPTS_ARCH_SUPPORTED TRUE)
endif()

if(CHIMERA_NETNS_TESTING AND WPTS_DOTNET AND WPTS_SMB2_DLL AND WPTS_ARCH_SUPPORTED)
    get_filename_component(WPTS_BIN_DIR "${WPTS_SMB2_DLL}" DIRECTORY)
    set(CHIMERA_DAEMON $<TARGET_FILE:chimera>)

    # Allowlist: MS-SMB2 cases confirmed green across repeated runs against the
    # current SMB2 server (memfs backend). Grow this as features land. Tests
    # that only pass with an unmerged server fix (e.g. the QUERY_DIRECTORY
    # double-completion fix for QueryDir_Reopen_OnFile) are intentionally
    # enabled in that fix's PR, not here.
    set(WPTS_SMB2_ALLOWLIST
        BVT_Compound_RelatedRequests
        BVT_Compound_UnRelatedRequests
        BVT_CreateClose_CreateAndCloseDirectory
        BVT_CreateClose_CreateAndCloseFile
        BVT_Negotiate_Compatible_Wildcard
        BVT_Negotiate_SMB21
        BVT_Negotiate_SMB30
        BVT_Negotiate_SMB302
        BVT_Negotiate_SigningEnabled
        BVT_SMB2Basic_ChangeNotify_ChangeAttributes
        BVT_SMB2Basic_ChangeNotify_ChangeCreation
        BVT_SMB2Basic_ChangeNotify_ChangeDirName
        BVT_SMB2Basic_ChangeNotify_ChangeFileName
        BVT_SMB2Basic_ChangeNotify_ChangeLastAccess
        BVT_SMB2Basic_ChangeNotify_ChangeLastWrite
        BVT_SMB2Basic_ChangeNotify_NonDirectoryFile
        BVT_SMB2Basic_QueryAndSet_FileInfo
        BVT_SMB2Basic_QueryDir_Reopen_OnDir
        BVT_SMB2Basic_QueryDir_Reopen_OnEmptyDir_NoFilesReturned
        BVT_SMB2Basic_Query_FileAllInformation
        BVT_SessionMgmt_NewSession
        BVT_SessionMgmt_Reauthentication
        BVT_SessionMgmt_ReconnectSessionSetup
        BVT_Signing
        BVT_TreeMgmt_TreeConnectAndDisconnect
        BVT_ValidateNegotiateInfo
        CreateClose_CreateAndCloseDirWithDotDir
        CreateClose_CreateAndCloseFileWithDotDir
        Negotiate_SMB311_ContextID_NetName
        Negotiate_SMB311_InvalidCipher
        Negotiate_SMB311_IsTransportCapabilitiesSupported
        Signing_VerifyAesGmacSigning
        TreeMgmt_SMB311_CLUSTER_RECONNECT
        ValidateNegotiateInfo_Negative_InvalidCapabilities
        ValidateNegotiateInfo_Negative_InvalidDialects_CommonDialectNotExpected
        ValidateNegotiateInfo_Negative_InvalidDialects_NoCommonDialect
        ValidateNegotiateInfo_Negative_InvalidGuid
        ValidateNegotiateInfo_Negative_InvalidMaxOutputResponse
        ValidateNegotiateInfo_Negative_InvalidSecurityMode)

    foreach(tc ${WPTS_SMB2_ALLOWLIST})
        add_test(NAME chimera/server/smb/wpts/memfs/${tc}
            COMMAND ${CMAKE_SOURCE_DIR}/scripts/wpts_smb_test_wrapper.sh
                    ${CHIMERA_DAEMON} memfs "Name=${tc}")
        # The wrapper stages our ptfconfig into the shared WPTS Bin dir, so
        # serialize WPTS tests against each other to avoid a write race.
        set_tests_properties(chimera/server/smb/wpts/memfs/${tc} PROPERTIES
            LABELS "smb;wpts"
            TIMEOUT 180
            RESOURCE_LOCK wpts_smb_bin
            ENVIRONMENT "WPTS_BIN_DIR=${WPTS_BIN_DIR};DOTNET=${WPTS_DOTNET}")
    endforeach()

    message(STATUS "WPTS MS-SMB2 tests: enabled (${WPTS_BIN_DIR}, ${WPTS_DOTNET})")
elseif(CHIMERA_NETNS_TESTING AND WPTS_DOTNET AND WPTS_SMB2_DLL AND NOT WPTS_ARCH_SUPPORTED)
    message(STATUS "WPTS MS-SMB2 tests: disabled "
            "(unsupported arch ${CMAKE_SYSTEM_PROCESSOR}; .NET test host needs x86_64)")
else()
    message(STATUS "WPTS MS-SMB2 tests: disabled "
            "(need CHIMERA_NETNS_TESTING + dotnet + -DWPTS_FILESERVER_DIR=<Bin>)")
endif()

# SMB smbtorture integration tests
add_executable(smbtorture_test smbtorture_test.c)
target_link_libraries(smbtorture_test chimera_server chimera_metrics jansson)
target_include_directories(smbtorture_test PRIVATE ${CMAKE_SOURCE_DIR}/src)

set(SMBTORTURE_LSAN "LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/suppressions.txt")
set(SMBTORTURE_CMD ${CMAKE_CURRENT_BINARY_DIR}/smbtorture_test)

set(SMBTORTURE_SUITES
    smb2.acls
    smb2.acls_non_canonical
    smb2.async_dosmode
    smb2.change_notify_disabled
    smb2.charset
    smb2.check-sharemode
    smb2.compound
    smb2.compound_async
    smb2.compound_find
    smb2.connect
    smb2.create
    smb2.create_no_streams
    smb2.credits
    smb2.delete-on-close-perms
    smb2.deny
    smb2.dir
    smb2.dosmode
    smb2.durable-open
    smb2.durable-open-disconnect
    smb2.durable-v2-delay
    smb2.durable-v2-open
    smb2.ea
    smb2.fileid
    smb2.getinfo
    smb2.ioctl
    smb2.ioctl-on-stream
    smb2.kernel-oplocks
    smb2.lease
    smb2.lock
    smb2.maximum_allowed
    smb2.mkdir
    smb2.name-mangling
    smb2.notify
    smb2.notify-inotify
    smb2.openattr
    smb2.oplock
    smb2.read
    smb2.rename
    smb2.replay
    smb2.rw
    smb2.samba3misc
    smb2.sdread
    smb2.secleak
    smb2.session
    smb2.session-id
    smb2.session-require-signing
    smb2.session.signing-aes-128-cmac
    smb2.set-sparse-ioctl
    smb2.setinfo
    smb2.sharemode
    smb2.streams
    smb2.tcon
    smb2.timestamp_resolution
    smb2.timestamps
    smb2.twrp
    smb2.winattr
    smb2.winattr2
    smb2.zero-data-ioctl
)

# Per-backend allowlist of suites that currently pass end-to-end against the
# SMB2 server.  Every suite in SMBTORTURE_SUITES is still registered as a ctest
# for every backend so the full conformance surface stays visible, but any
# suite NOT in the backend's allowlist is marked DISABLED -- ctest reports it as
# "Disabled" (never run, never a failure), representing unfinished work.
#
# To bring a suite to green: implement the feature, confirm the suite passes on
# the backend, then add it to that backend's SMBTORTURE_ENABLED_<backend> list
# in the same change.  This lets the remaining work be parallelized across
# branches without a perpetually-red test run.
#
# Only the memfs backend is seeded today (mirroring the WPTS suite, which also
# runs memfs-only).  The other backends start fully disabled; populate their
# lists as suites are verified against them.
set(SMBTORTURE_ENABLED_memfs
    smb2.check-sharemode
    smb2.create_no_streams
    smb2.notify-inotify
    smb2.secleak
    smb2.session.signing-aes-128-cmac
    smb2.set-sparse-ioctl
    smb2.sharemode
    smb2.winattr2
    # smb2.acls_non_canonical and smb2.async_dosmode pass on x86_64 but fail on
    # arm64 CI; left disabled until the arch difference is understood.
)
set(SMBTORTURE_ENABLED_linux)
set(SMBTORTURE_ENABLED_demofs_io_uring)
set(SMBTORTURE_ENABLED_demofs_aio)
set(SMBTORTURE_ENABLED_cairn)
set(SMBTORTURE_ENABLED_io_uring)

macro(add_smbtorture_backend_tests backend)
    foreach(SUITE ${SMBTORTURE_SUITES})
        string(REPLACE "." "_" SUITE_ID ${SUITE})
        set(TEST_NAME chimera/server/smb/smbtorture_${SUITE_ID}_${backend})
        add_test(NAME ${TEST_NAME}
            COMMAND ${CMAKE_SOURCE_DIR}/scripts/netns_test_wrapper.sh
                    ${SMBTORTURE_CMD} -b ${backend} ${SUITE})
        set_tests_properties(${TEST_NAME} PROPERTIES
            ENVIRONMENT "${SMBTORTURE_LSAN}"
            # smbtorture_test exits 77 when the smbtorture client is missing at
            # runtime; report that as Skipped rather than a failure.
            SKIP_RETURN_CODE 77)
        if(NOT "${SUITE}" IN_LIST SMBTORTURE_ENABLED_${backend})
            # Known-incomplete on this backend: keep it visible but don't run
            # or fail on it.
            set_tests_properties(${TEST_NAME} PROPERTIES DISABLED TRUE)
        endif()
    endforeach()
endmacro()

# The suites drive the Samba `smbtorture` client, which is only installed in the
# netns test environment (devcontainer image, via samba-testsuite).  Distro CI
# Test jobs build + run the unit tests without it, so only register the suites
# when the client is actually present -- otherwise every suite would fail with
# "smbtorture not found in PATH".
find_program(SMBTORTURE_EXECUTABLE smbtorture)

if(CHIMERA_NETNS_TESTING AND SMBTORTURE_EXECUTABLE)
    add_smbtorture_backend_tests(memfs)
    add_smbtorture_backend_tests(linux)
    add_smbtorture_backend_tests(demofs_io_uring)
    if(HAVE_LIBAIO)
        add_smbtorture_backend_tests(demofs_aio)
    endif()
    if(CAIRN_ENABLED)
        add_smbtorture_backend_tests(cairn)
    endif()
    if(IO_URING_ENABLED)
        add_smbtorture_backend_tests(io_uring)
    endif()
elseif(CHIMERA_NETNS_TESTING)
    message(STATUS "smbtorture tests: disabled (smbtorture client not found; "
            "install samba-testsuite to enable)")
endif()
