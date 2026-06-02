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

    # SMB3 durable / persistent handle cases.  These need the daemon run with
    # smb_persistent_handles enabled (the wrapper sets it from CHIMERA_SMB_PERSISTENT,
    # which also flips the persistent-handle / CA-share ptfconfig capabilities and
    # marks FileShare as the CA share).  Allowlist = every applicable
    # durable/persistent case confirmed green against the current server.  Left
    # out: DurableHandleV1_Reconnect_AfterServerDisconnect (needs
    # negotiate-after-negotiate to drop the connection — an unrelated protocol
    # gap) and *_WithDifferentDurableOwner / BVT_PersistentHandles (need a second
    # user account / scale-out config the harness doesn't provide).
    set(WPTS_SMB2_PERSISTENT_ALLOWLIST
        BVT_DurableHandleV1_Reconnect_WithBatchOplock
        BVT_DurableHandleV1_Reconnect_WithLeaseV1
        BVT_DurableHandleV2_Reconnect_WithBatchOplock
        BVT_DurableHandleV2_Reconnect_WithLeaseV1
        BVT_PersistentHandle_Reconnect
        DurableHandleV1_Reconnect_IncludeCreateDurableHandle
        DurableHandleV1_Reconnect_IncludeReconnectV2
        DurableHandleV1_Reconnect_IncludeRequestV2
        DurableHandleV1_Reconnect_LeaseIsNull
        DurableHandleV1_Reconnect_SessionIsNotNull
        DurableHandleV1_Reconnect_WithDifferentClientGuid
        DurableHandleV1_Reconnect_WithDifferentLeaseKey
        DurableHandleV1_Reconnect_WithLeaseV1_WithDifferentFileName
        DurableHandleV1_Reconnect_WithoutDurableHandle
        DurableHandleV1_Reconnect_WithoutFileIdPersistent
        DurableHandleV1_Reconnect_WithoutLogoffOrDisconnect_WithoutPreviousSessionIdSet
        DurableHandleV1_Reconnect_WithoutRequestLease
        DurableHandleV1_WithLeaseV1_WithoutHandleCaching
        DurableHandleV2_NoPersistenceGrantedOnNonCAShare
        DurableHandleV2_Reconnect_WithLeaseV1_WithDifferentFileName
        DurableHandleV2_Reconnect_WithoutPersistence
        DurableHandleV2_WithLeaseV1AndV2_WithDifferentLeaseKey
        DurableHandleV2_WithLeaseV2_WithoutHandleCaching
        PersistentHandle_ReOpenFromDiffClient)

    foreach(tc ${WPTS_SMB2_PERSISTENT_ALLOWLIST})
        add_test(NAME chimera/server/smb/wpts/memfs_persistent/${tc}
            COMMAND ${CMAKE_SOURCE_DIR}/scripts/wpts_smb_test_wrapper.sh
                    ${CHIMERA_DAEMON} memfs "Name=${tc}")
        set_tests_properties(chimera/server/smb/wpts/memfs_persistent/${tc} PROPERTIES
            LABELS "smb;wpts;persistent"
            TIMEOUT 180
            RESOURCE_LOCK wpts_smb_bin
            ENVIRONMENT "WPTS_BIN_DIR=${WPTS_BIN_DIR};DOTNET=${WPTS_DOTNET};CHIMERA_SMB_PERSISTENT=1")
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

# The smb2.acls suite is registered both as the whole suite (kept visible but
# disabled -- see below) and as individual subtests.  Every ACL subtest passes,
# but the suite as a single run shares one server process and trips over an
# unrelated robustness gap: smb2_deltree's per-test cleanup believes its
# delete-on-close of a non-empty directory succeeded, so a leftover directory
# collides with the next subtest's setup.  Running each subtest in its own
# server process (one ctest each) avoids that, so the ACL behaviour is fully
# covered in CI; greening the combined run is tracked as a deltree follow-up.
set(SMBTORTURE_ACLS_SUBTESTS
    smb2.acls.CREATOR
    smb2.acls.GENERIC
    smb2.acls.OWNER
    smb2.acls.INHERITANCE
    smb2.acls.INHERITFLAGS
    smb2.acls.SDFLAGSVSCHOWN
    smb2.acls.DYNAMIC
    smb2.acls.ACCESSBASED
    smb2.acls.OWNER-RIGHTS
    smb2.acls.OWNER-RIGHTS-DENY
    smb2.acls.OWNER-RIGHTS-DENY1
    smb2.acls.DENY1
    smb2.acls.MXAC-NOT-GRANTED
    smb2.acls.OVERWRITE_READ_ONLY_FILE
)

# Like smb2.acls, the combined smb2.notify suite shares one server process and
# trips over the same smb2_deltree per-subtest cleanup cascade (a non-empty
# directory whose delete-on-close is believed to have succeeded lingers and the
# stale state breaks later subtests).  Each subtest passes cleanly in its own
# server process, so the notify behaviour is covered by registering the
# individual subtests as separate ctests.  The combined suite stays visible but
# disabled (see below); greening it is tracked as the shared deltree follow-up.
set(SMBTORTURE_NOTIFY_SUBTESTS
    smb2.notify.tcon
    smb2.notify.dir
    smb2.notify.mask
    smb2.notify.tdis
    smb2.notify.tdis1
    smb2.notify.close
    smb2.notify.logoff
    smb2.notify.invalid-reauth
    smb2.notify.basedir
    smb2.notify.double
    smb2.notify.file
    smb2.notify.tcp
    smb2.notify.overflow
    smb2.notify.rmdir1
    smb2.notify.rmdir2
    smb2.notify.rmdir3
    smb2.notify.rmdir4
    smb2.notify.handle-permissions
)

set(SMBTORTURE_SUITES
    smb2.acls
    ${SMBTORTURE_ACLS_SUBTESTS}
    ${SMBTORTURE_NOTIFY_SUBTESTS}
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
# The memfs and passthrough (linux/io_uring) backends are seeded today.  The
# remaining backends start fully disabled; populate their lists as suites are
# verified against them.
set(SMBTORTURE_ENABLED_memfs
    smb2.change_notify_disabled
    smb2.maximum_allowed
    smb2.mkdir
    smb2.openattr
    # Path-based set/get of the DOS attribute combinations round-trips through
    # the persisted dos_attributes; a file cleared to NORMAL reads back NORMAL
    # (no synthesized ARCHIVE), and the create/setinfo paths stamp ARCHIVE.
    smb2.winattr
    smb2.read
    smb2.rw
    smb2.check-sharemode
    smb2.compound_find
    smb2.connect
    smb2.create_no_streams
    # SMB3 durable/persistent handle suites that pass end-to-end on memfs with
    # the in-memory durable registry (the harness runs the server with
    # persistent handles enabled).  smb2.durable-open / -v2-open still have
    # individual gaps and stay disabled until fully green.
    smb2.durable-open-disconnect
    smb2.durable-v2-delay
    smb2.notify-inotify
    smb2.secleak
    smb2.session-id
    smb2.session-require-signing
    smb2.session.signing-aes-128-cmac
    smb2.set-sparse-ioctl
    # FILE_ZERO_DATA on an existing file maps to FALLOC_FL_PUNCH_HOLE in the
    # VFS; the smbtorture wrapper pre-creates torture_test_file so the test
    # exercises the FSCTL itself rather than bailing at the OPEN-existing step.
    smb2.zero-data-ioctl
    # FSCTL_CREATE_OR_GET_OBJECT_ID returns a synthesised 64-byte
    # FILE_OBJECTID_BUFFER derived from the open handle's fh_hash; the test
    # opens a named stream, which is memfs-only today.
    smb2.ioctl-on-stream
    smb2.sharemode
    smb2.tcon
    smb2.timestamps
    smb2.winattr2
    # Named-stream (ADS) support is implemented on memfs (config-gated
    # smb_named_streams + CHIMERA_VFS_CAP_NAMED_STREAMS): create/read/write/
    # enumerate (FileStreamInformation)/delete of named streams all work and
    # several smb2.streams subtests pass (sharemodes, rename, create-disposition,
    # zero-byte).  The full smb2.streams / smb2.sdread suites are NOT yet
    # green -- remaining gaps are stream share-mode/sharing-violation
    # semantics, FILE_ATTRIBUTE reporting on stream handles, stream-name
    # validation edge cases, directory-stream handling, and rename-with-an-
    # open-stream -- so those suites stay disabled until fully passing.  The
    # test harness (smbtorture_test.c) already flips smb_named_streams on for
    # them.
    # Unified-ACL coverage: every smb2.acls subtest runs green, both
    # individually and as the combined suite (after PR #622's
    # FileNamesInformation marshalling fix unblocked smbtorture's deltree
    # cleanup).
    smb2.acls
    ${SMBTORTURE_ACLS_SUBTESTS}
    # CHANGE_NOTIFY coverage: these notify subtests pass individually on memfs.
    # The combined smb2.notify suite stays disabled (residual deltree cascade
    # specific to that suite).  Remaining notify subtests (valid-req,
    # mask-change, session-reconnect, tree, rec) need deeper features
    # (recursive-buffer accuracy, per-session preauth for PreviousSessionId)
    # and are left disabled.
    ${SMBTORTURE_NOTIFY_SUBTESTS}
    # Non-canonical AUTO_INHERITED semantics: with the
    # smb_acl_inherited_canonicalize knob set to 0 (the harness flips it for
    # this suite only), SECINFO_DACL set+get round-trips the AUTO_INHERITED
    # bit verbatim instead of stripping it when AUTO_INHERIT_REQ is absent
    # (Samba's "acl flag inherited canonicalization = no" mode).
    smb2.acls_non_canonical
    # smb2.async_dosmode passes on x86_64 but fails on arm64 CI; left
    # disabled here until the arch difference is understood.
)
# The linux and io_uring passthrough backends share an implementation and pass
# the same set.  They expose the share root as a real local directory whose
# filesystem must support name_to_handle_at(2) (see smbtorture_test.c).
# smb2.timestamps stays disabled here: the suite sets an arbitrary create time
# (incl. far-future / pre-1970 values) and reads it back, but a real Linux
# filesystem has no API to set a file's birth time, so the create-time
# round-trip is unsupportable on the passthrough backends (it works on the
# native backends, which persist btime in their own inode metadata).
# smb2.openattr and smb2.winattr stay disabled for the same reason: they
# round-trip DOS attributes (READONLY/HIDDEN/SYSTEM/ARCHIVE) that a real Linux
# filesystem has no native API to store, so they cannot be persisted on a
# passthrough mount.  In particular winattr clears a file to NORMAL and expects
# it back, but a passthrough file has no stored DOS set, so it falls back to a
# synthesized ARCHIVE (it works on the native backends, which persist
# dos_attributes in their inode metadata).
set(SMBTORTURE_ENABLED_linux
    smb2.change_notify_disabled
    smb2.mkdir
    smb2.read
    smb2.rw
    smb2.check-sharemode
    smb2.compound_find
    smb2.connect
    smb2.create_no_streams
    smb2.deny
    smb2.notify-inotify
    smb2.secleak
    smb2.session-id
    smb2.session-require-signing
    smb2.session.signing-aes-128-cmac
    smb2.set-sparse-ioctl
    # FALLOC_FL_PUNCH_HOLE on the underlying filesystem.
    smb2.zero-data-ioctl
    smb2.sharemode
    smb2.tcon
    smb2.winattr2
    # CHANGE_NOTIFY subtests run cleanly in isolation; the combined smb2.notify
    # suite still trips the shared-process deltree cascade.
    smb2.notify.basedir
    smb2.notify.close
    smb2.notify.dir
    smb2.notify.double
    smb2.notify.file
    smb2.notify.handle-permissions
    smb2.notify.invalid-reauth
    smb2.notify.logoff
    smb2.notify.mask
    smb2.notify.overflow
    smb2.notify.rmdir1
    smb2.notify.rmdir2
    smb2.notify.rmdir3
    smb2.notify.rmdir4
    smb2.notify.tcon
    smb2.notify.tcp
    smb2.notify.tdis
    smb2.notify.tdis1
    # smb2.acls_non_canonical stays disabled here: the passthrough backends
    # don't persist ACLs (set is projected to POSIX mode, get is synthesised
    # from mode bits), so the AUTO_INHERITED control bit can't round-trip.
)
set(SMBTORTURE_ENABLED_io_uring
    smb2.change_notify_disabled
    smb2.mkdir
    smb2.read
    smb2.rw
    smb2.check-sharemode
    smb2.compound_find
    smb2.connect
    smb2.create_no_streams
    smb2.deny
    smb2.notify-inotify
    smb2.secleak
    smb2.session-id
    smb2.session-require-signing
    smb2.session.signing-aes-128-cmac
    smb2.set-sparse-ioctl
    smb2.zero-data-ioctl
    smb2.sharemode
    smb2.tcon
    smb2.winattr2
    # io_uring passes a subset of the CHANGE_NOTIFY subtests; the rest
    # (notify.close/dir/invalid-reauth/logoff/mask/rmdir1/rmdir3) still fail
    # here despite passing on the plain linux passthrough, pending a follow-up.
    smb2.notify.basedir
    smb2.notify.double
    smb2.notify.file
    smb2.notify.handle-permissions
    smb2.notify.overflow
    smb2.notify.rmdir2
    smb2.notify.rmdir4
    smb2.notify.tcon
    smb2.notify.tcp
    smb2.notify.tdis
    smb2.notify.tdis1
)
# The engine backends store their own data and stamp a new object's owner from
# the calling credential, so they behave consistently regardless of the server's
# own uid (unlike the setfsuid/personality passthrough backends).  The suites
# below were verified green end-to-end once the SMB CREATE owner access check was
# fixed (see chimera_smb_create_check_access).  cairn stores ACLs natively in its
# KV (like diskfs), so it also runs the smb2.acls subtests now that set_security
# (cairn_setattr SET-ACL) persists the descriptor.
set(SMBTORTURE_ENABLED_cairn
    smb2.change_notify_disabled
    smb2.mkdir
    # Native DOS-attribute storage in the cairn inode lets the SMB
    # create/overwrite-truncate attribute round-trip pass end-to-end; the
    # async_dosmode variant exercises the same code paths concurrently.
    smb2.openattr
    smb2.async_dosmode
    # Path-based DOS attribute set/get round-trip (incl. clear-to-NORMAL).
    smb2.winattr
    smb2.read
    smb2.rw
    smb2.check-sharemode
    smb2.compound_find
    smb2.connect
    smb2.create_no_streams
    smb2.notify-inotify
    smb2.secleak
    smb2.session-id
    smb2.session-require-signing
    smb2.session.signing-aes-128-cmac
    smb2.set-sparse-ioctl
    smb2.zero-data-ioctl
    smb2.sharemode
    smb2.tcon
    # Native btime storage in the cairn inode lets the SMB create-time round-trip
    # and the LastWriteTime write-time/sticky semantics pass end-to-end.
    smb2.timestamps
    smb2.winattr2
    # CHANGE_NOTIFY subtests run cleanly in isolation (combined smb2.notify
    # suite still trips the shared-process deltree cascade).
    smb2.notify.basedir
    smb2.notify.close
    smb2.notify.dir
    smb2.notify.double
    smb2.notify.file
    smb2.notify.handle-permissions
    smb2.notify.invalid-reauth
    smb2.notify.logoff
    smb2.notify.mask
    smb2.notify.overflow
    smb2.notify.rmdir1
    smb2.notify.rmdir2
    smb2.notify.rmdir3
    smb2.notify.rmdir4
    smb2.notify.tcon
    smb2.notify.tcp
    smb2.notify.tdis
    smb2.notify.tdis1
    # Unified-ACL coverage: every smb2.acls subtest runs green, both
    # individually and as the combined suite (PR #622 unblocked the
    # smbtorture deltree cleanup that was the gate).
    smb2.acls
    ${SMBTORTURE_ACLS_SUBTESTS}
    smb2.acls_non_canonical
)
# diskfs is a native-ACL backend: it stores each inode's ACL as a
# DISKFS_REC_ACL b+tree record and seeds/inherits it on create, so it adds the
# smb2.acls subtests on top of the suites enabled above.  smb2.set-sparse-ioctl
# stays disabled (no FSCTL_SET_SPARSE support yet); both block backends share
# the diskfs module and enable the same set.
set(SMBTORTURE_ENABLED_diskfs_common
    smb2.change_notify_disabled
    smb2.mkdir
    # Native DOS-attribute storage in the diskfs inode (in-memory + on-disk
    # dinode) lets the SMB create/overwrite-truncate attribute round-trip
    # pass; the async_dosmode variant exercises the same code paths
    # concurrently.
    smb2.openattr
    smb2.async_dosmode
    # Path-based DOS attribute set/get round-trip (incl. clear-to-NORMAL).
    smb2.winattr
    smb2.read
    smb2.rw
    smb2.check-sharemode
    smb2.compound_find
    smb2.connect
    smb2.create_no_streams
    smb2.notify-inotify
    smb2.secleak
    smb2.session-id
    smb2.session-require-signing
    smb2.session.signing-aes-128-cmac
    smb2.set-sparse-ioctl
    smb2.zero-data-ioctl
    smb2.sharemode
    smb2.tcon
    # Native btime storage in the diskfs inode (in-memory + on-disk dinode) lets
    # the SMB create-time round-trip and LastWriteTime semantics pass; both block
    # backends share the diskfs module.
    smb2.timestamps
    smb2.winattr2
    # CHANGE_NOTIFY subtests run cleanly in isolation (combined smb2.notify
    # suite still trips the shared-process deltree cascade).
    smb2.notify.basedir
    smb2.notify.close
    smb2.notify.dir
    smb2.notify.double
    smb2.notify.file
    smb2.notify.handle-permissions
    smb2.notify.invalid-reauth
    smb2.notify.logoff
    smb2.notify.mask
    smb2.notify.overflow
    smb2.notify.rmdir1
    smb2.notify.rmdir2
    smb2.notify.rmdir3
    smb2.notify.rmdir4
    smb2.notify.tcon
    smb2.notify.tcp
    smb2.notify.tdis
    smb2.notify.tdis1
    # Unified-ACL coverage: subtests + combined suite + non-canonical-ACL
    # variant all pass (PR #622 unblocked the smbtorture deltree cleanup).
    smb2.acls
    ${SMBTORTURE_ACLS_SUBTESTS}
    smb2.acls_non_canonical
)
set(SMBTORTURE_ENABLED_diskfs_io_uring ${SMBTORTURE_ENABLED_diskfs_common})
set(SMBTORTURE_ENABLED_diskfs_aio ${SMBTORTURE_ENABLED_diskfs_common})

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
    add_smbtorture_backend_tests(diskfs_io_uring)
    if(HAVE_LIBAIO)
        add_smbtorture_backend_tests(diskfs_aio)
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
