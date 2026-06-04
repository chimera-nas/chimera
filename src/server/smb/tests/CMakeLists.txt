# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only


# SMB authentication test program
add_executable(smb_auth_test smb_auth_test.c)
target_link_libraries(smb_auth_test chimera_vfs urcu-qsbr urcu-cds)
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


# smbtorture SMB2 conformance — exploded matrix
#
# Every individual smb2.* subtest the Samba smbtorture client knows about is
# registered as its own ctest per backend (one server process per subtest).
# Running atomically prevents one failing subtest from masking the rest of a
# combined suite, and gives each failure its own clear log.
#
# The catalog was generated by `smbtorture --list smb2.*` (655 subtests) and
# the per-backend allowlists below were derived from a clean matrix run
# against the smbtorture binary in the Samba 4.23 devcontainer image.  Run
# `tools/smbtorture/regen.sh` to regenerate after a Samba upgrade or when
# adding backends.

# >>> SMBTORTURE_BLOCK_BEGIN (regenerate with tools/smbtorture/regen.sh)
# Every smb2.* atomic subtest the Samba smbtorture client knows about,
# one ctest per (subtest, backend).  Generated by tools/smbtorture/regen.sh.
set(SMBTORTURE_SUITES
    # smb2.acls
    smb2.acls.ACCESSBASED
    smb2.acls.CREATOR
    smb2.acls.DENY1
    smb2.acls.DYNAMIC
    smb2.acls.GENERIC
    smb2.acls.INHERITANCE
    smb2.acls.INHERITFLAGS
    smb2.acls.MXAC-NOT-GRANTED
    smb2.acls.OVERWRITE_READ_ONLY_FILE
    smb2.acls.OWNER
    smb2.acls.OWNER-RIGHTS
    smb2.acls.OWNER-RIGHTS-DENY
    smb2.acls.OWNER-RIGHTS-DENY1
    smb2.acls.SDFLAGSVSCHOWN
    # smb2.acls_non_canonical
    smb2.acls_non_canonical.flags
    # smb2.aio_delay
    smb2.aio_delay.aio_cancel
    # smb2.async_dosmode
    smb2.async_dosmode
    # smb2.bench
    smb2.bench.echo
    smb2.bench.oplock1
    smb2.bench.path-contention-shared
    smb2.bench.read
    smb2.bench.session-setup
    # smb2.change_notify_disabled
    smb2.change_notify_disabled.notfiy_disabled
    # smb2.charset
    "smb2.charset.Testing composite character (a umlaut)"
    "smb2.charset.Testing naked diacritical (umlaut)"
    "smb2.charset.Testing partial surrogate"
    "smb2.charset.Testing wide-a"
    # smb2.check-sharemode
    smb2.check-sharemode
    # smb2.compound
    smb2.compound.compound-break
    smb2.compound.compound-padding
    smb2.compound.create-write-close
    smb2.compound.interim1
    smb2.compound.interim2
    smb2.compound.interim3
    smb2.compound.invalid1
    smb2.compound.invalid2
    smb2.compound.invalid3
    smb2.compound.invalid4
    smb2.compound.related1
    smb2.compound.related2
    smb2.compound.related3
    smb2.compound.related4
    smb2.compound.related5
    smb2.compound.related6
    smb2.compound.related7
    smb2.compound.related8
    smb2.compound.related9
    smb2.compound.unrelated1
    # smb2.compound_async
    smb2.compound_async.create_lease_break_async
    smb2.compound_async.flush_close
    smb2.compound_async.flush_flush
    smb2.compound_async.getinfo_middle
    smb2.compound_async.read_read
    smb2.compound_async.rename_last
    smb2.compound_async.rename_middle
    smb2.compound_async.rename_non_compound_no_async
    smb2.compound_async.rename_same_srcdst_non_compound_no_async
    smb2.compound_async.write_write
    # smb2.compound_find
    smb2.compound_find.compound_find_close
    smb2.compound_find.compound_find_related
    smb2.compound_find.compound_find_unrelated
    # smb2.connect
    smb2.connect
    # smb2.create
    smb2.create.acldir
    smb2.create.aclfile
    smb2.create.bench-path-contention-shared
    smb2.create.blob
    smb2.create.brlocked
    smb2.create.delete
    smb2.create.dir-alloc-size
    smb2.create.dosattr_tmp_dir
    smb2.create.gentest
    smb2.create.impersonation
    smb2.create.leading-slash
    smb2.create.mkdir-dup
    smb2.create.mkdir-visible
    smb2.create.multi
    smb2.create.nulldacl
    smb2.create.open
    smb2.create.path-length
    smb2.create.quota-fake-file
    # smb2.create_no_streams
    smb2.create_no_streams.no_stream
    # smb2.credits
    smb2.credits.1conn_ipc_max_async_credits
    smb2.credits.1conn_notify_max_async_credits
    smb2.credits.2conn_ipc_max_async_credits
    smb2.credits.2conn_notify_max_async_credits
    smb2.credits.ipc_max_data_zero
    smb2.credits.multichannel_ipc_max_async_credits
    smb2.credits.multichannel_max_async_credits
    smb2.credits.session_setup_credits_granted
    smb2.credits.single_req_credits_granted
    smb2.credits.skipped_mid
    # smb2.delete-on-close-perms
    smb2.delete-on-close-perms.BUG14427
    smb2.delete-on-close-perms.CREATE
    "smb2.delete-on-close-perms.CREATE Existing"
    smb2.delete-on-close-perms.CREATE_IF
    "smb2.delete-on-close-perms.CREATE_IF Existing"
    smb2.delete-on-close-perms.FIND_and_set_DOC
    smb2.delete-on-close-perms.OVERWRITE_IF
    "smb2.delete-on-close-perms.OVERWRITE_IF Existing"
    smb2.delete-on-close-perms.READONLY
    # smb2.deny
    smb2.deny.deny1
    smb2.deny.deny2
    # smb2.dir
    smb2.dir.1kfiles_rename
    smb2.dir.file-index
    smb2.dir.find
    smb2.dir.fixed
    smb2.dir.large-files
    smb2.dir.many
    smb2.dir.modify
    smb2.dir.one
    smb2.dir.sorted
    # smb2.dirlease
    smb2.dirlease.hardlink
    smb2.dirlease.leases
    smb2.dirlease.oplocks
    smb2.dirlease.overwrite
    smb2.dirlease.rename
    smb2.dirlease.rename_dst_parent
    smb2.dirlease.setatime
    smb2.dirlease.setbtime
    smb2.dirlease.setctime
    smb2.dirlease.setdos
    smb2.dirlease.seteof
    smb2.dirlease.setmtime
    smb2.dirlease.unlink_different_initial_and_close
    smb2.dirlease.unlink_different_set_and_close
    smb2.dirlease.unlink_same_initial_and_close
    smb2.dirlease.unlink_same_set_and_close
    smb2.dirlease.v2_request
    smb2.dirlease.v2_request_parent
    # smb2.dosmode
    smb2.dosmode
    # smb2.durable-open
    smb2.durable-open.alloc-size
    smb2.durable-open.delete_on_close1
    smb2.durable-open.delete_on_close2
    smb2.durable-open.file-position
    smb2.durable-open.lease
    smb2.durable-open.lock-lease
    smb2.durable-open.lock-noW-lease
    smb2.durable-open.lock-oplock
    smb2.durable-open.open-lease
    smb2.durable-open.open-oplock
    smb2.durable-open.open2-lease
    smb2.durable-open.open2-oplock
    smb2.durable-open.oplock
    smb2.durable-open.read-only
    smb2.durable-open.reopen1
    smb2.durable-open.reopen1a
    smb2.durable-open.reopen1a-lease
    smb2.durable-open.reopen2
    smb2.durable-open.reopen2-lease
    smb2.durable-open.reopen2-lease-v2
    smb2.durable-open.reopen2a
    smb2.durable-open.reopen3
    smb2.durable-open.reopen4
    smb2.durable-open.reopen5
    smb2.durable-open.reopen6
    smb2.durable-open.stat-open
    # smb2.durable-open-disconnect
    smb2.durable-open-disconnect.open-oplock-disconnect
    # smb2.durable-v2-delay
    smb2.durable-v2-delay.durable_v2_reconnect_delay
    smb2.durable-v2-delay.durable_v2_reconnect_delay_msec
    # smb2.durable-v2-open
    smb2.durable-v2-open.app-instance
    smb2.durable-v2-open.create-blob
    smb2.durable-v2-open.durable-v2-setinfo
    smb2.durable-v2-open.keep-disconnected-rh-with-rh-open
    smb2.durable-v2-open.keep-disconnected-rh-with-rwh-open
    smb2.durable-v2-open.keep-disconnected-rh-with-stat-open
    smb2.durable-v2-open.keep-disconnected-rwh-with-stat-open
    smb2.durable-v2-open.lock-lease
    smb2.durable-v2-open.lock-noW-lease
    smb2.durable-v2-open.lock-oplock
    smb2.durable-v2-open.nonstat-and-lease
    smb2.durable-v2-open.open-lease
    smb2.durable-v2-open.open-oplock
    smb2.durable-v2-open.persistent-open-lease
    smb2.durable-v2-open.persistent-open-oplock
    smb2.durable-v2-open.purge-disconnected-rh-with-rename
    smb2.durable-v2-open.purge-disconnected-rh-with-share-none-open
    smb2.durable-v2-open.purge-disconnected-rh-with-write
    smb2.durable-v2-open.purge-disconnected-rwh-with-rh-open
    smb2.durable-v2-open.purge-disconnected-rwh-with-rwh-open
    smb2.durable-v2-open.reconnect-twice
    smb2.durable-v2-open.reopen1
    smb2.durable-v2-open.reopen1a
    smb2.durable-v2-open.reopen1a-lease
    smb2.durable-v2-open.reopen2
    smb2.durable-v2-open.reopen2-lease
    smb2.durable-v2-open.reopen2-lease-v2
    smb2.durable-v2-open.reopen2b
    smb2.durable-v2-open.reopen2c
    smb2.durable-v2-open.stat-and-lease
    smb2.durable-v2-open.statRH-and-lease
    smb2.durable-v2-open.two-different-lease
    smb2.durable-v2-open.two-same-lease
    # smb2.durable-v2-regressions
    smb2.durable-v2-regressions.durable_v2_reconnect_bug15624
    # smb2.ea
    smb2.ea.acl_xattr
    # smb2.fileid
    smb2.fileid.fileid
    smb2.fileid.fileid-dir
    smb2.fileid.unique
    smb2.fileid.unique-dir
    # smb2.getinfo
    smb2.getinfo.complex
    smb2.getinfo.fsinfo
    smb2.getinfo.getinfo_access
    smb2.getinfo.granted
    smb2.getinfo.normalized
    smb2.getinfo.qfile_buffercheck
    smb2.getinfo.qfs_buffercheck
    smb2.getinfo.qsec_buffercheck
    # smb2.hold-oplock
    smb2.hold-oplock
    # smb2.hold-sharemode
    smb2.hold-sharemode
    # smb2.ioctl
    smb2.ioctl.bug14607
    smb2.ioctl.bug14769
    smb2.ioctl.bug14788.NETWORK_INTERFACE
    smb2.ioctl.bug14788.VALIDATE_NEGOTIATE
    smb2.ioctl.compress_create_with_attr
    smb2.ioctl.compress_dir_inherit
    smb2.ioctl.compress_file_flag
    smb2.ioctl.compress_inherit_disable
    smb2.ioctl.compress_invalid_buf
    smb2.ioctl.compress_invalid_format
    smb2.ioctl.compress_notsup_get
    smb2.ioctl.compress_notsup_set
    smb2.ioctl.compress_perms
    smb2.ioctl.compress_query_file_attr
    smb2.ioctl.compress_set_file_attr
    "smb2.ioctl.copy-chunk streams"
    smb2.ioctl.copy_chunk_across_shares
    smb2.ioctl.copy_chunk_across_shares2
    smb2.ioctl.copy_chunk_across_shares3
    smb2.ioctl.copy_chunk_append
    smb2.ioctl.copy_chunk_bad_access
    smb2.ioctl.copy_chunk_bad_key
    smb2.ioctl.copy_chunk_bug15644
    smb2.ioctl.copy_chunk_dest_lock
    smb2.ioctl.copy_chunk_limits
    smb2.ioctl.copy_chunk_max_output_sz
    smb2.ioctl.copy_chunk_multi
    smb2.ioctl.copy_chunk_overwrite
    smb2.ioctl.copy_chunk_simple
    smb2.ioctl.copy_chunk_sparse_dest
    smb2.ioctl.copy_chunk_src_exceed
    smb2.ioctl.copy_chunk_src_exceed_multi
    smb2.ioctl.copy_chunk_src_is_dest
    smb2.ioctl.copy_chunk_src_is_dest_overlap
    smb2.ioctl.copy_chunk_src_lock
    smb2.ioctl.copy_chunk_tiny
    smb2.ioctl.copy_chunk_write_access
    smb2.ioctl.copy_chunk_zero_length
    smb2.ioctl.dup_extents_bad_handle
    smb2.ioctl.dup_extents_compressed_dest
    smb2.ioctl.dup_extents_compressed_src
    smb2.ioctl.dup_extents_dest_lock
    smb2.ioctl.dup_extents_len_beyond_dest
    smb2.ioctl.dup_extents_len_beyond_src
    smb2.ioctl.dup_extents_len_zero
    smb2.ioctl.dup_extents_simple
    smb2.ioctl.dup_extents_sparse_both
    smb2.ioctl.dup_extents_sparse_dest
    smb2.ioctl.dup_extents_sparse_src
    smb2.ioctl.dup_extents_src_is_dest
    smb2.ioctl.dup_extents_src_is_dest_overlap
    smb2.ioctl.dup_extents_src_lock
    smb2.ioctl.network_interface_info
    smb2.ioctl.req_resume_key
    smb2.ioctl.req_two_resume_keys
    smb2.ioctl.shadow_copy
    smb2.ioctl.sparse_compressed
    smb2.ioctl.sparse_copy_chunk
    smb2.ioctl.sparse_dir_flag
    smb2.ioctl.sparse_file_attr
    smb2.ioctl.sparse_file_flag
    smb2.ioctl.sparse_hole_dealloc
    smb2.ioctl.sparse_lock
    smb2.ioctl.sparse_perms
    smb2.ioctl.sparse_punch
    smb2.ioctl.sparse_punch_invalid
    smb2.ioctl.sparse_qar
    smb2.ioctl.sparse_qar_malformed
    smb2.ioctl.sparse_qar_multi
    smb2.ioctl.sparse_qar_ob1
    smb2.ioctl.sparse_qar_overflow
    smb2.ioctl.sparse_qar_truncated
    smb2.ioctl.sparse_set_nobuf
    smb2.ioctl.sparse_set_oversize
    smb2.ioctl.trim_simple
    # smb2.ioctl-on-stream
    smb2.ioctl-on-stream
    # smb2.kernel-oplocks
    smb2.kernel-oplocks.kernel_oplocks1
    smb2.kernel-oplocks.kernel_oplocks2
    smb2.kernel-oplocks.kernel_oplocks3
    smb2.kernel-oplocks.kernel_oplocks4
    smb2.kernel-oplocks.kernel_oplocks5
    smb2.kernel-oplocks.kernel_oplocks6
    smb2.kernel-oplocks.kernel_oplocks7
    smb2.kernel-oplocks.kernel_oplocks8
    # smb2.lease
    smb2.lease.break
    smb2.lease.break_twice
    smb2.lease.breaking1
    smb2.lease.breaking2
    smb2.lease.breaking3
    smb2.lease.breaking4
    smb2.lease.breaking5
    smb2.lease.breaking6
    smb2.lease.complex1
    smb2.lease.duplicate_create
    smb2.lease.duplicate_open
    smb2.lease.dynamic_share
    smb2.lease.initial_delete_disconnect
    smb2.lease.initial_delete_logoff
    smb2.lease.initial_delete_tdis
    smb2.lease.lease-epoch
    smb2.lease.lock1
    smb2.lease.multibreak
    smb2.lease.nobreakself
    smb2.lease.oplock
    smb2.lease.rename_dir_openfile
    smb2.lease.rename_wait
    smb2.lease.request
    smb2.lease.statopen
    smb2.lease.statopen2
    smb2.lease.statopen3
    smb2.lease.statopen4
    smb2.lease.timeout
    smb2.lease.timeout-disconnect
    smb2.lease.unlink
    smb2.lease.upgrade
    smb2.lease.upgrade2
    smb2.lease.upgrade3
    smb2.lease.v1_bug15148
    smb2.lease.v2_breaking3
    smb2.lease.v2_bug15148
    smb2.lease.v2_complex1
    smb2.lease.v2_complex2
    smb2.lease.v2_epoch1
    smb2.lease.v2_epoch2
    smb2.lease.v2_epoch3
    smb2.lease.v2_flags_breaking
    smb2.lease.v2_flags_parentkey
    smb2.lease.v2_rename
    smb2.lease.v2_rename_target_overwrite
    # smb2.lock
    smb2.lock.async
    smb2.lock.auto-unlock
    smb2.lock.cancel
    smb2.lock.cancel-logoff
    smb2.lock.cancel-tdis
    smb2.lock.contend
    smb2.lock.context
    smb2.lock.ctdb-delrec-deadlock
    smb2.lock.errorcode
    smb2.lock.lock
    smb2.lock.multiple-unlock
    smb2.lock.open-brlock-deadlock
    smb2.lock.overlap
    smb2.lock.range
    smb2.lock.replay_broken_windows
    smb2.lock.replay_smb3_specification_durable
    smb2.lock.replay_smb3_specification_multi
    smb2.lock.rw-exclusive
    smb2.lock.rw-none
    smb2.lock.rw-shared
    smb2.lock.stacking
    smb2.lock.truncate
    smb2.lock.unlock
    smb2.lock.valid-request
    smb2.lock.zerobytelength
    smb2.lock.zerobyteread
    # smb2.maxfid
    smb2.maxfid
    # smb2.maximum_allowed
    smb2.maximum_allowed.maximum_allowed
    smb2.maximum_allowed.read_only
    # smb2.mkdir
    smb2.mkdir
    # smb2.multichannel
    smb2.multichannel.bugs.bug_15346
    smb2.multichannel.generic.interface_info
    smb2.multichannel.generic.num_channels
    smb2.multichannel.leases.test1
    smb2.multichannel.leases.test2
    smb2.multichannel.leases.test3
    smb2.multichannel.leases.test4
    smb2.multichannel.oplocks.test1
    smb2.multichannel.oplocks.test2
    smb2.multichannel.oplocks.test3_specification
    smb2.multichannel.oplocks.test3_windows
    # smb2.name-mangling
    smb2.name-mangling.mangle
    smb2.name-mangling.mangled-mask
    # smb2.notify
    smb2.notify.basedir
    smb2.notify.close
    smb2.notify.dir
    smb2.notify.double
    smb2.notify.file
    smb2.notify.handle-permissions
    smb2.notify.invalid-reauth
    smb2.notify.logoff
    smb2.notify.mask
    smb2.notify.mask-change
    smb2.notify.overflow
    smb2.notify.rec
    smb2.notify.rmdir1
    smb2.notify.rmdir2
    smb2.notify.rmdir3
    smb2.notify.rmdir4
    smb2.notify.session-reconnect
    smb2.notify.tcon
    smb2.notify.tcp
    smb2.notify.tdis
    smb2.notify.tdis1
    smb2.notify.tree
    smb2.notify.valid-req
    # smb2.notify-inotify
    smb2.notify-inotify.inotify-rename
    # smb2.openattr
    smb2.openattr
    # smb2.oplock
    smb2.oplock.batch1
    smb2.oplock.batch10
    smb2.oplock.batch11
    smb2.oplock.batch12
    smb2.oplock.batch13
    smb2.oplock.batch14
    smb2.oplock.batch15
    smb2.oplock.batch16
    smb2.oplock.batch19
    smb2.oplock.batch2
    smb2.oplock.batch20
    smb2.oplock.batch21
    smb2.oplock.batch22a
    smb2.oplock.batch22b
    smb2.oplock.batch23
    smb2.oplock.batch24
    smb2.oplock.batch25
    smb2.oplock.batch26
    smb2.oplock.batch3
    smb2.oplock.batch4
    smb2.oplock.batch5
    smb2.oplock.batch6
    smb2.oplock.batch7
    smb2.oplock.batch8
    smb2.oplock.batch9
    smb2.oplock.batch9a
    smb2.oplock.brl1
    smb2.oplock.brl2
    smb2.oplock.brl3
    smb2.oplock.doc
    smb2.oplock.exclusive1
    smb2.oplock.exclusive2
    smb2.oplock.exclusive3
    smb2.oplock.exclusive4
    smb2.oplock.exclusive5
    smb2.oplock.exclusive6
    smb2.oplock.exclusive9
    smb2.oplock.levelii500
    smb2.oplock.levelii501
    smb2.oplock.levelii502
    smb2.oplock.statopen1
    smb2.oplock.stream1
    # smb2.read
    smb2.read.access
    smb2.read.bug14607
    smb2.read.dir
    smb2.read.eof
    smb2.read.position
    # smb2.rename
    smb2.rename.close-full-information
    smb2.rename.msword
    smb2.rename.no_share_delete_but_delete_access
    smb2.rename.no_share_delete_no_delete_access
    smb2.rename.no_sharing
    smb2.rename.rename-open
    smb2.rename.rename_dir_bench
    smb2.rename.rename_dir_openfile
    smb2.rename.share_delete_and_delete_access
    smb2.rename.share_delete_no_delete_access
    smb2.rename.simple
    smb2.rename.simple_modtime
    smb2.rename.simple_nodelete
    # smb2.replay
    smb2.replay.channel-sequence
    smb2.replay.dhv2-pending1l-vs-lease-sane
    smb2.replay.dhv2-pending1l-vs-lease-windows
    smb2.replay.dhv2-pending1l-vs-oplock-sane
    smb2.replay.dhv2-pending1l-vs-oplock-windows
    smb2.replay.dhv2-pending1n-vs-lease-sane
    smb2.replay.dhv2-pending1n-vs-lease-windows
    smb2.replay.dhv2-pending1n-vs-oplock-sane
    smb2.replay.dhv2-pending1n-vs-oplock-windows
    smb2.replay.dhv2-pending1n-vs-violation-lease-ack-sane
    smb2.replay.dhv2-pending1n-vs-violation-lease-ack-windows
    smb2.replay.dhv2-pending1n-vs-violation-lease-close-sane
    smb2.replay.dhv2-pending1n-vs-violation-lease-close-windows
    smb2.replay.dhv2-pending1o-vs-lease-sane
    smb2.replay.dhv2-pending1o-vs-lease-windows
    smb2.replay.dhv2-pending1o-vs-oplock-sane
    smb2.replay.dhv2-pending1o-vs-oplock-windows
    smb2.replay.dhv2-pending2l-vs-lease-sane
    smb2.replay.dhv2-pending2l-vs-lease-windows
    smb2.replay.dhv2-pending2l-vs-oplock-sane
    smb2.replay.dhv2-pending2l-vs-oplock-windows
    smb2.replay.dhv2-pending2n-vs-lease-sane
    smb2.replay.dhv2-pending2n-vs-lease-windows
    smb2.replay.dhv2-pending2n-vs-oplock-sane
    smb2.replay.dhv2-pending2n-vs-oplock-windows
    smb2.replay.dhv2-pending2o-vs-lease-sane
    smb2.replay.dhv2-pending2o-vs-lease-windows
    smb2.replay.dhv2-pending2o-vs-oplock-sane
    smb2.replay.dhv2-pending2o-vs-oplock-windows
    smb2.replay.dhv2-pending3l-vs-lease-sane
    smb2.replay.dhv2-pending3l-vs-lease-windows
    smb2.replay.dhv2-pending3l-vs-oplock-sane
    smb2.replay.dhv2-pending3l-vs-oplock-windows
    smb2.replay.dhv2-pending3n-vs-lease-sane
    smb2.replay.dhv2-pending3n-vs-lease-windows
    smb2.replay.dhv2-pending3n-vs-oplock-sane
    smb2.replay.dhv2-pending3n-vs-oplock-windows
    smb2.replay.dhv2-pending3o-vs-lease-sane
    smb2.replay.dhv2-pending3o-vs-lease-windows
    smb2.replay.dhv2-pending3o-vs-oplock-sane
    smb2.replay.dhv2-pending3o-vs-oplock-windows
    smb2.replay.durable-reconnect-replay1
    smb2.replay.durable-reconnect-replay2
    smb2.replay.durable-reconnect-replay3
    smb2.replay.replay-commands
    smb2.replay.replay-dhv2-lease-oplock
    smb2.replay.replay-dhv2-lease1
    smb2.replay.replay-dhv2-lease2
    smb2.replay.replay-dhv2-lease3
    smb2.replay.replay-dhv2-oplock-lease
    smb2.replay.replay-dhv2-oplock1
    smb2.replay.replay-dhv2-oplock2
    smb2.replay.replay-dhv2-oplock3
    smb2.replay.replay-regular
    smb2.replay.replay-twice-durable
    smb2.replay.replay3
    smb2.replay.replay4
    smb2.replay.replay5
    smb2.replay.replay6
    smb2.replay.replay7
    # smb2.rw
    smb2.rw.append
    smb2.rw.invalid
    smb2.rw.rw1
    smb2.rw.rw2
    # smb2.samba3misc
    smb2.samba3misc.localposixlock1
    # smb2.scan
    smb2.scan.find
    smb2.scan.getinfo
    smb2.scan.scan
    smb2.scan.setinfo
    # smb2.sdread
    smb2.sdread
    # smb2.secleak
    smb2.secleak
    # smb2.session
    smb2.session.anon-encryption1
    smb2.session.anon-encryption2
    smb2.session.anon-encryption3
    smb2.session.anon-signing1
    smb2.session.anon-signing2
    smb2.session.bind1
    smb2.session.bind2
    smb2.session.bind_different_user
    smb2.session.bind_invalid_auth
    smb2.session.bind_negative_smb202
    smb2.session.bind_negative_smb210d
    smb2.session.bind_negative_smb210s
    smb2.session.bind_negative_smb2to3d
    smb2.session.bind_negative_smb2to3s
    smb2.session.bind_negative_smb3encGtoCd
    smb2.session.bind_negative_smb3encGtoCs
    smb2.session.bind_negative_smb3signC30toGd
    smb2.session.bind_negative_smb3signC30toGs
    smb2.session.bind_negative_smb3signCtoGd
    smb2.session.bind_negative_smb3signCtoGs
    smb2.session.bind_negative_smb3signCtoHd
    smb2.session.bind_negative_smb3signCtoHs
    smb2.session.bind_negative_smb3signGtoC30d
    smb2.session.bind_negative_smb3signGtoC30s
    smb2.session.bind_negative_smb3signGtoCd
    smb2.session.bind_negative_smb3signGtoCs
    smb2.session.bind_negative_smb3signGtoH2Xd
    smb2.session.bind_negative_smb3signGtoH2Xs
    smb2.session.bind_negative_smb3signGtoHd
    smb2.session.bind_negative_smb3signGtoHs
    smb2.session.bind_negative_smb3signH2XtoGd
    smb2.session.bind_negative_smb3signH2XtoGs
    smb2.session.bind_negative_smb3signHtoCd
    smb2.session.bind_negative_smb3signHtoCs
    smb2.session.bind_negative_smb3signHtoGd
    smb2.session.bind_negative_smb3signHtoGs
    smb2.session.bind_negative_smb3sneCtoGd
    smb2.session.bind_negative_smb3sneCtoGs
    smb2.session.bind_negative_smb3sneGtoCd
    smb2.session.bind_negative_smb3sneGtoCs
    smb2.session.bind_negative_smb3sneGtoHd
    smb2.session.bind_negative_smb3sneGtoHs
    smb2.session.bind_negative_smb3sneHtoGd
    smb2.session.bind_negative_smb3sneHtoGs
    smb2.session.bind_negative_smb3to2d
    smb2.session.bind_negative_smb3to2s
    smb2.session.bind_negative_smb3to3d
    smb2.session.bind_negative_smb3to3s
    smb2.session.encryption-aes-128-ccm
    smb2.session.encryption-aes-128-gcm
    smb2.session.encryption-aes-256-ccm
    smb2.session.encryption-aes-256-gcm
    smb2.session.expire1e
    smb2.session.expire1n
    smb2.session.expire1s
    smb2.session.expire2e
    smb2.session.expire2s
    smb2.session.expire_disconnect
    smb2.session.ntlmssp_bug14932
    smb2.session.reauth1
    smb2.session.reauth2
    smb2.session.reauth3
    smb2.session.reauth4
    smb2.session.reauth5
    smb2.session.reauth6
    smb2.session.reconnect1
    smb2.session.reconnect2
    smb2.session.signing-aes-128-cmac
    smb2.session.signing-aes-128-gmac
    smb2.session.signing-hmac-sha-256
    smb2.session.two_logoff
    # smb2.session-id
    smb2.session-id
    # smb2.session-require-signing
    smb2.session-require-signing.bug15397
    # smb2.set-sparse-ioctl
    smb2.set-sparse-ioctl
    # smb2.setinfo
    smb2.setinfo
    # smb2.sharemode
    smb2.sharemode.access-sharemode
    smb2.sharemode.bug14375
    smb2.sharemode.sharemode-access
    # smb2.stream-inherit-perms
    smb2.stream-inherit-perms
    # smb2.streams
    smb2.streams.attributes1
    smb2.streams.attributes2
    smb2.streams.basefile-rename-with-open-stream
    smb2.streams.create-disposition
    smb2.streams.delete
    smb2.streams.dir
    smb2.streams.io
    smb2.streams.names
    smb2.streams.names2
    smb2.streams.names3
    smb2.streams.rename
    smb2.streams.rename2
    smb2.streams.sharemodes
    smb2.streams.zero-byte
    # smb2.tcon
    smb2.tcon
    # smb2.timestamp_resolution
    smb2.timestamp_resolution.resolution1
    # smb2.timestamps
    smb2.timestamps.delayed-1write
    smb2.timestamps.delayed-2write
    smb2.timestamps.delayed-write-vs-flush
    smb2.timestamps.delayed-write-vs-setbasic
    smb2.timestamps.delayed-write-vs-seteof
    smb2.timestamps.freeze-thaw
    smb2.timestamps.modern_write_time_update-1
    smb2.timestamps.modern_write_time_update-2
    smb2.timestamps.test_close_not_attrib
    smb2.timestamps.time_t_-1
    smb2.timestamps.time_t_-2
    smb2.timestamps.time_t_0
    smb2.timestamps.time_t_1
    smb2.timestamps.time_t_10000000000
    smb2.timestamps.time_t_15032385535
    smb2.timestamps.time_t_1968
    smb2.timestamps.time_t_4294967295
    # smb2.twrp
    smb2.twrp.listdir
    smb2.twrp.openroot
    smb2.twrp.stream
    smb2.twrp.write
    # smb2.winattr
    smb2.winattr
    # smb2.winattr2
    smb2.winattr2
    # smb2.zero-data-ioctl
    smb2.zero-data-ioctl
)

set(SMBTORTURE_ENABLED_memfs
    # smb2.acls
    smb2.acls.ACCESSBASED
    smb2.acls.CREATOR
    smb2.acls.DENY1
    smb2.acls.DYNAMIC
    smb2.acls.GENERIC
    smb2.acls.INHERITANCE
    smb2.acls.INHERITFLAGS
    smb2.acls.MXAC-NOT-GRANTED
    smb2.acls.OVERWRITE_READ_ONLY_FILE
    smb2.acls.OWNER
    smb2.acls.OWNER-RIGHTS
    smb2.acls.OWNER-RIGHTS-DENY
    smb2.acls.OWNER-RIGHTS-DENY1
    smb2.acls.SDFLAGSVSCHOWN
    # smb2.async_dosmode
    smb2.async_dosmode
    # smb2.bench
    # smb2.change_notify_disabled
    smb2.change_notify_disabled.notfiy_disabled
    # smb2.check-sharemode
    smb2.check-sharemode
    # smb2.compound
    smb2.compound.create-write-close
    smb2.compound.invalid1
    smb2.compound.invalid2
    smb2.compound.invalid3
    smb2.compound.invalid4
    smb2.compound.related1
    smb2.compound.related2
    smb2.compound.related3
    smb2.compound.related5
    smb2.compound.related6
    smb2.compound.related9
    smb2.compound.unrelated1
    # smb2.compound_find
    smb2.compound_find.compound_find_close
    smb2.compound_find.compound_find_related
    smb2.compound_find.compound_find_unrelated
    # smb2.connect
    smb2.connect
    # smb2.create
    smb2.create.acldir
    smb2.create.aclfile
    smb2.create.bench-path-contention-shared
    smb2.create.brlocked
    smb2.create.delete
    smb2.create.dir-alloc-size
    smb2.create.leading-slash
    smb2.create.mkdir-dup
    smb2.create.multi
    smb2.create.path-length
    # smb2.create_no_streams
    smb2.create_no_streams.no_stream
    # smb2.credits
    smb2.credits.session_setup_credits_granted
    smb2.credits.single_req_credits_granted
    smb2.credits.skipped_mid
    # smb2.delete-on-close-perms
    smb2.delete-on-close-perms.BUG14427
    smb2.delete-on-close-perms.CREATE
    smb2.delete-on-close-perms.CREATE_IF
    smb2.delete-on-close-perms.FIND_and_set_DOC
    smb2.delete-on-close-perms.OVERWRITE_IF
    # smb2.deny
    smb2.deny.deny1
    smb2.deny.deny2
    # smb2.dir
    smb2.dir.1kfiles_rename
    smb2.dir.file-index
    smb2.dir.find
    smb2.dir.fixed
    smb2.dir.many
    smb2.dir.sorted
    # smb2.durable-open-disconnect
    smb2.durable-open-disconnect.open-oplock-disconnect
    # smb2.durable-open
    # smb2.durable-v2-delay
    smb2.durable-v2-delay.durable_v2_reconnect_delay
    smb2.durable-v2-delay.durable_v2_reconnect_delay_msec
    # smb2.durable-v2-open
    # smb2.durable-v2-regressions
    smb2.durable-v2-regressions.durable_v2_reconnect_bug15624
    # smb2.fileid
    smb2.fileid.unique
    smb2.fileid.unique-dir
    # smb2.ioctl-on-stream
    smb2.ioctl-on-stream
    # smb2.ioctl
    smb2.ioctl.bug14607
    smb2.ioctl.bug14788.VALIDATE_NEGOTIATE
    smb2.ioctl.compress_create_with_attr
    smb2.ioctl.compress_dir_inherit
    smb2.ioctl.compress_file_flag
    smb2.ioctl.compress_inherit_disable
    smb2.ioctl.compress_invalid_buf
    smb2.ioctl.compress_invalid_format
    smb2.ioctl.compress_perms
    smb2.ioctl.compress_query_file_attr
    smb2.ioctl.compress_set_file_attr
    smb2.ioctl.copy_chunk_across_shares2
    smb2.ioctl.copy_chunk_append
    smb2.ioctl.copy_chunk_bad_key
    smb2.ioctl.copy_chunk_bug15644
    smb2.ioctl.copy_chunk_multi
    smb2.ioctl.copy_chunk_overwrite
    smb2.ioctl.copy_chunk_simple
    smb2.ioctl.copy_chunk_sparse_dest
    smb2.ioctl.copy_chunk_src_is_dest
    smb2.ioctl.copy_chunk_tiny
    smb2.ioctl.copy_chunk_write_access
    smb2.ioctl.dup_extents_bad_handle
    smb2.ioctl.dup_extents_compressed_dest
    smb2.ioctl.dup_extents_compressed_src
    smb2.ioctl.dup_extents_dest_lock
    smb2.ioctl.dup_extents_len_beyond_dest
    smb2.ioctl.dup_extents_len_beyond_src
    smb2.ioctl.dup_extents_len_zero
    smb2.ioctl.dup_extents_simple
    smb2.ioctl.dup_extents_sparse_both
    smb2.ioctl.dup_extents_sparse_dest
    smb2.ioctl.dup_extents_sparse_src
    smb2.ioctl.dup_extents_src_is_dest
    smb2.ioctl.dup_extents_src_is_dest_overlap
    smb2.ioctl.dup_extents_src_lock
    smb2.ioctl.req_resume_key
    smb2.ioctl.req_two_resume_keys
    smb2.ioctl.shadow_copy
    smb2.ioctl.sparse_compressed
    smb2.ioctl.sparse_copy_chunk
    smb2.ioctl.sparse_dir_flag
    smb2.ioctl.sparse_file_attr
    smb2.ioctl.sparse_file_flag
    smb2.ioctl.sparse_hole_dealloc
    smb2.ioctl.sparse_lock
    smb2.ioctl.sparse_perms
    smb2.ioctl.sparse_punch
    smb2.ioctl.sparse_punch_invalid
    smb2.ioctl.sparse_qar
    smb2.ioctl.sparse_qar_malformed
    smb2.ioctl.sparse_qar_multi
    smb2.ioctl.sparse_qar_ob1
    smb2.ioctl.sparse_qar_overflow
    smb2.ioctl.sparse_qar_truncated
    smb2.ioctl.sparse_set_nobuf
    smb2.ioctl.sparse_set_oversize
    # smb2.kernel-oplocks
    smb2.kernel-oplocks.kernel_oplocks8
    # smb2.lock
    smb2.lock.contend
    smb2.lock.context
    smb2.lock.ctdb-delrec-deadlock
    smb2.lock.open-brlock-deadlock
    smb2.lock.rw-exclusive
    smb2.lock.rw-none
    smb2.lock.rw-shared
    smb2.lock.truncate
    smb2.lock.unlock
    # smb2.maxfid
    smb2.maxfid
    # smb2.maximum_allowed
    smb2.maximum_allowed.maximum_allowed
    smb2.maximum_allowed.read_only
    # smb2.mkdir
    smb2.mkdir
    # smb2.notify-inotify
    smb2.notify-inotify.inotify-rename
    # smb2.notify
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
    # smb2.openattr
    smb2.openattr
    # smb2.oplock
    smb2.oplock.batch10
    # smb2.read
    smb2.read.access
    smb2.read.bug14607
    smb2.read.dir
    smb2.read.eof
    smb2.read.position
    # smb2.rename
    smb2.rename.close-full-information
    smb2.rename.msword
    smb2.rename.no_sharing
    smb2.rename.rename-open
    smb2.rename.rename_dir_bench
    smb2.rename.share_delete_no_delete_access
    smb2.rename.simple
    smb2.rename.simple_modtime
    smb2.rename.simple_nodelete
    # smb2.replay
    smb2.replay.replay-commands
    smb2.replay.replay-regular
    smb2.replay.replay5
    smb2.replay.replay7
    # smb2.rw
    smb2.rw.append
    smb2.rw.invalid
    smb2.rw.rw1
    smb2.rw.rw2
    # smb2.scan
    smb2.scan.setinfo
    # smb2.secleak
    smb2.secleak
    # smb2.session-id
    smb2.session-id
    # smb2.session-require-signing
    smb2.session-require-signing.bug15397
    # smb2.session
    smb2.session.bind_different_user
    smb2.session.bind_negative_smb3signC30toGd
    smb2.session.bind_negative_smb3signC30toGs
    smb2.session.bind_negative_smb3signCtoGd
    smb2.session.bind_negative_smb3signCtoGs
    smb2.session.bind_negative_smb3signCtoHd
    smb2.session.bind_negative_smb3signCtoHs
    smb2.session.bind_negative_smb3signGtoC30d
    smb2.session.bind_negative_smb3signGtoC30s
    smb2.session.bind_negative_smb3signGtoCd
    smb2.session.bind_negative_smb3signGtoCs
    smb2.session.bind_negative_smb3signGtoH2Xd
    smb2.session.bind_negative_smb3signGtoH2Xs
    smb2.session.bind_negative_smb3signGtoHd
    smb2.session.bind_negative_smb3signGtoHs
    smb2.session.bind_negative_smb3signH2XtoGd
    smb2.session.bind_negative_smb3signH2XtoGs
    smb2.session.bind_negative_smb3signHtoCd
    smb2.session.bind_negative_smb3signHtoCs
    smb2.session.bind_negative_smb3signHtoGd
    smb2.session.bind_negative_smb3signHtoGs
    smb2.session.bind_negative_smb3sneCtoGd
    smb2.session.bind_negative_smb3sneCtoGs
    smb2.session.bind_negative_smb3sneGtoCd
    smb2.session.bind_negative_smb3sneGtoCs
    smb2.session.bind_negative_smb3sneGtoHd
    smb2.session.bind_negative_smb3sneGtoHs
    smb2.session.bind_negative_smb3sneHtoGd
    smb2.session.bind_negative_smb3sneHtoGs
    smb2.session.encryption-aes-128-ccm
    smb2.session.encryption-aes-128-gcm
    smb2.session.encryption-aes-256-ccm
    smb2.session.encryption-aes-256-gcm
    smb2.session.expire1e
    smb2.session.expire1n
    smb2.session.expire1s
    smb2.session.expire2e
    smb2.session.expire2s
    smb2.session.expire_disconnect
    smb2.session.signing-aes-128-cmac
    smb2.session.signing-aes-128-gmac
    smb2.session.signing-hmac-sha-256
    smb2.session.two_logoff
    # smb2.set-sparse-ioctl
    smb2.set-sparse-ioctl
    # smb2.sharemode
    smb2.sharemode.access-sharemode
    smb2.sharemode.bug14375
    smb2.sharemode.sharemode-access
    # smb2.streams
    smb2.streams.basefile-rename-with-open-stream
    smb2.streams.create-disposition
    smb2.streams.rename
    smb2.streams.sharemodes
    smb2.streams.zero-byte
    # smb2.tcon
    smb2.tcon
    # smb2.timestamps
    smb2.timestamps.delayed-1write
    smb2.timestamps.delayed-2write
    smb2.timestamps.delayed-write-vs-flush
    smb2.timestamps.delayed-write-vs-setbasic
    smb2.timestamps.delayed-write-vs-seteof
    smb2.timestamps.freeze-thaw
    smb2.timestamps.modern_write_time_update-1
    smb2.timestamps.modern_write_time_update-2
    smb2.timestamps.test_close_not_attrib
    smb2.timestamps.time_t_-1
    smb2.timestamps.time_t_-2
    smb2.timestamps.time_t_0
    smb2.timestamps.time_t_1
    smb2.timestamps.time_t_10000000000
    smb2.timestamps.time_t_15032385535
    smb2.timestamps.time_t_1968
    smb2.timestamps.time_t_4294967295
    # smb2.twrp
    smb2.twrp.openroot
    smb2.twrp.stream
    smb2.twrp.write
    # smb2.winattr
    smb2.winattr
    # smb2.winattr2
    smb2.winattr2
    # smb2.zero-data-ioctl
    smb2.zero-data-ioctl
)

set(SMBTORTURE_ENABLED_linux
    # smb2.bench
    # smb2.change_notify_disabled
    smb2.change_notify_disabled.notfiy_disabled
    # smb2.check-sharemode
    smb2.check-sharemode
    # smb2.compound
    smb2.compound.create-write-close
    smb2.compound.invalid1
    smb2.compound.invalid2
    smb2.compound.invalid3
    smb2.compound.invalid4
    smb2.compound.related1
    smb2.compound.related2
    smb2.compound.related3
    smb2.compound.related5
    smb2.compound.related6
    smb2.compound.related9
    smb2.compound.unrelated1
    # smb2.compound_find
    smb2.compound_find.compound_find_close
    smb2.compound_find.compound_find_related
    smb2.compound_find.compound_find_unrelated
    # smb2.connect
    smb2.connect
    # smb2.create
    smb2.create.bench-path-contention-shared
    smb2.create.brlocked
    smb2.create.delete
    smb2.create.dir-alloc-size
    smb2.create.leading-slash
    smb2.create.mkdir-dup
    smb2.create.multi
    smb2.create.path-length
    # smb2.create_no_streams
    smb2.create_no_streams.no_stream
    # smb2.credits
    smb2.credits.session_setup_credits_granted
    smb2.credits.single_req_credits_granted
    smb2.credits.skipped_mid
    # smb2.delete-on-close-perms
    smb2.delete-on-close-perms.CREATE
    smb2.delete-on-close-perms.CREATE_IF
    smb2.delete-on-close-perms.FIND_and_set_DOC
    smb2.delete-on-close-perms.OVERWRITE_IF
    # smb2.deny
    smb2.deny.deny1
    smb2.deny.deny2
    # smb2.dir
    smb2.dir.1kfiles_rename
    smb2.dir.file-index
    smb2.dir.find
    smb2.dir.fixed
    smb2.dir.sorted
    # smb2.durable-open
    # smb2.durable-v2-delay
    smb2.durable-v2-delay.durable_v2_reconnect_delay
    smb2.durable-v2-delay.durable_v2_reconnect_delay_msec
    # smb2.durable-v2-open
    # smb2.durable-v2-regressions
    smb2.durable-v2-regressions.durable_v2_reconnect_bug15624
    # smb2.fileid
    smb2.fileid.unique
    smb2.fileid.unique-dir
    # smb2.ioctl
    smb2.ioctl.bug14607
    smb2.ioctl.bug14788.VALIDATE_NEGOTIATE
    smb2.ioctl.compress_create_with_attr
    smb2.ioctl.compress_dir_inherit
    smb2.ioctl.compress_file_flag
    smb2.ioctl.compress_inherit_disable
    smb2.ioctl.compress_invalid_buf
    smb2.ioctl.compress_invalid_format
    smb2.ioctl.compress_perms
    smb2.ioctl.compress_query_file_attr
    smb2.ioctl.compress_set_file_attr
    smb2.ioctl.copy_chunk_across_shares2
    smb2.ioctl.copy_chunk_append
    smb2.ioctl.copy_chunk_bad_key
    smb2.ioctl.copy_chunk_bug15644
    smb2.ioctl.copy_chunk_multi
    smb2.ioctl.copy_chunk_overwrite
    smb2.ioctl.copy_chunk_simple
    smb2.ioctl.copy_chunk_sparse_dest
    smb2.ioctl.copy_chunk_src_is_dest
    smb2.ioctl.copy_chunk_tiny
    smb2.ioctl.copy_chunk_write_access
    smb2.ioctl.dup_extents_bad_handle
    smb2.ioctl.dup_extents_compressed_dest
    smb2.ioctl.dup_extents_compressed_src
    smb2.ioctl.dup_extents_dest_lock
    smb2.ioctl.dup_extents_len_beyond_dest
    smb2.ioctl.dup_extents_len_beyond_src
    smb2.ioctl.dup_extents_len_zero
    smb2.ioctl.dup_extents_simple
    smb2.ioctl.dup_extents_sparse_both
    smb2.ioctl.dup_extents_sparse_dest
    smb2.ioctl.dup_extents_sparse_src
    smb2.ioctl.dup_extents_src_is_dest
    smb2.ioctl.dup_extents_src_is_dest_overlap
    smb2.ioctl.dup_extents_src_lock
    smb2.ioctl.req_resume_key
    smb2.ioctl.req_two_resume_keys
    smb2.ioctl.shadow_copy
    smb2.ioctl.sparse_compressed
    smb2.ioctl.sparse_copy_chunk
    smb2.ioctl.sparse_dir_flag
    smb2.ioctl.sparse_file_attr
    smb2.ioctl.sparse_file_flag
    smb2.ioctl.sparse_hole_dealloc
    smb2.ioctl.sparse_lock
    smb2.ioctl.sparse_perms
    smb2.ioctl.sparse_punch
    smb2.ioctl.sparse_punch_invalid
    smb2.ioctl.sparse_qar
    smb2.ioctl.sparse_qar_malformed
    smb2.ioctl.sparse_qar_multi
    smb2.ioctl.sparse_qar_ob1
    smb2.ioctl.sparse_qar_overflow
    smb2.ioctl.sparse_qar_truncated
    smb2.ioctl.sparse_set_nobuf
    smb2.ioctl.sparse_set_oversize
    # smb2.lock
    smb2.lock.contend
    smb2.lock.context
    smb2.lock.ctdb-delrec-deadlock
    smb2.lock.open-brlock-deadlock
    smb2.lock.rw-exclusive
    smb2.lock.rw-none
    smb2.lock.rw-shared
    smb2.lock.truncate
    smb2.lock.unlock
    # smb2.maxfid
    smb2.maxfid
    # smb2.maximum_allowed
    smb2.maximum_allowed.read_only
    # smb2.mkdir
    smb2.mkdir
    # smb2.notify-inotify
    smb2.notify-inotify.inotify-rename
    # smb2.notify
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
    # smb2.oplock
    smb2.oplock.batch10
    # smb2.read
    smb2.read.access
    smb2.read.bug14607
    smb2.read.dir
    smb2.read.eof
    smb2.read.position
    # smb2.rename
    smb2.rename.close-full-information
    smb2.rename.msword
    smb2.rename.no_sharing
    smb2.rename.rename-open
    smb2.rename.rename_dir_bench
    smb2.rename.share_delete_no_delete_access
    smb2.rename.simple
    smb2.rename.simple_modtime
    smb2.rename.simple_nodelete
    # smb2.replay
    smb2.replay.replay-commands
    smb2.replay.replay5
    smb2.replay.replay7
    # smb2.rw
    smb2.rw.append
    smb2.rw.invalid
    smb2.rw.rw1
    smb2.rw.rw2
    # smb2.secleak
    smb2.secleak
    # smb2.session-id
    smb2.session-id
    # smb2.session-require-signing
    smb2.session-require-signing.bug15397
    # smb2.session
    smb2.session.bind_different_user
    smb2.session.bind_negative_smb3signC30toGd
    smb2.session.bind_negative_smb3signC30toGs
    smb2.session.bind_negative_smb3signCtoGd
    smb2.session.bind_negative_smb3signCtoGs
    smb2.session.bind_negative_smb3signCtoHd
    smb2.session.bind_negative_smb3signCtoHs
    smb2.session.bind_negative_smb3signGtoC30d
    smb2.session.bind_negative_smb3signGtoC30s
    smb2.session.bind_negative_smb3signGtoCd
    smb2.session.bind_negative_smb3signGtoCs
    smb2.session.bind_negative_smb3signGtoH2Xd
    smb2.session.bind_negative_smb3signGtoH2Xs
    smb2.session.bind_negative_smb3signGtoHd
    smb2.session.bind_negative_smb3signGtoHs
    smb2.session.bind_negative_smb3signH2XtoGd
    smb2.session.bind_negative_smb3signH2XtoGs
    smb2.session.bind_negative_smb3signHtoCd
    smb2.session.bind_negative_smb3signHtoCs
    smb2.session.bind_negative_smb3signHtoGd
    smb2.session.bind_negative_smb3signHtoGs
    smb2.session.bind_negative_smb3sneCtoGd
    smb2.session.bind_negative_smb3sneCtoGs
    smb2.session.bind_negative_smb3sneGtoCd
    smb2.session.bind_negative_smb3sneGtoCs
    smb2.session.bind_negative_smb3sneGtoHd
    smb2.session.bind_negative_smb3sneGtoHs
    smb2.session.bind_negative_smb3sneHtoGd
    smb2.session.bind_negative_smb3sneHtoGs
    smb2.session.encryption-aes-128-ccm
    smb2.session.encryption-aes-128-gcm
    smb2.session.encryption-aes-256-ccm
    smb2.session.encryption-aes-256-gcm
    smb2.session.expire1e
    smb2.session.expire1n
    smb2.session.expire1s
    smb2.session.expire2e
    smb2.session.expire2s
    smb2.session.expire_disconnect
    smb2.session.signing-aes-128-cmac
    smb2.session.signing-aes-128-gmac
    smb2.session.signing-hmac-sha-256
    smb2.session.two_logoff
    # smb2.set-sparse-ioctl
    smb2.set-sparse-ioctl
    # smb2.sharemode
    smb2.sharemode.access-sharemode
    smb2.sharemode.bug14375
    smb2.sharemode.sharemode-access
    # smb2.tcon
    smb2.tcon
    # smb2.timestamps
    smb2.timestamps.test_close_not_attrib
    # smb2.twrp
    smb2.twrp.openroot
    smb2.twrp.stream
    smb2.twrp.write
    # smb2.winattr2
    smb2.winattr2
    # smb2.zero-data-ioctl
    smb2.zero-data-ioctl
)

set(SMBTORTURE_ENABLED_io_uring
    # smb2.bench
    # smb2.change_notify_disabled
    smb2.change_notify_disabled.notfiy_disabled
    # smb2.check-sharemode
    smb2.check-sharemode
    # smb2.compound
    smb2.compound.create-write-close
    smb2.compound.invalid1
    smb2.compound.invalid2
    smb2.compound.invalid3
    smb2.compound.invalid4
    smb2.compound.related1
    smb2.compound.related2
    smb2.compound.related3
    smb2.compound.related5
    smb2.compound.related6
    smb2.compound.related9
    smb2.compound.unrelated1
    # smb2.compound_find
    smb2.compound_find.compound_find_close
    smb2.compound_find.compound_find_related
    smb2.compound_find.compound_find_unrelated
    # smb2.connect
    smb2.connect
    # smb2.create
    smb2.create.bench-path-contention-shared
    smb2.create.brlocked
    smb2.create.delete
    smb2.create.dir-alloc-size
    smb2.create.leading-slash
    smb2.create.mkdir-dup
    smb2.create.multi
    smb2.create.path-length
    # smb2.create_no_streams
    smb2.create_no_streams.no_stream
    # smb2.credits
    smb2.credits.session_setup_credits_granted
    smb2.credits.single_req_credits_granted
    smb2.credits.skipped_mid
    # smb2.delete-on-close-perms
    smb2.delete-on-close-perms.CREATE
    smb2.delete-on-close-perms.CREATE_IF
    smb2.delete-on-close-perms.FIND_and_set_DOC
    smb2.delete-on-close-perms.OVERWRITE_IF
    # smb2.deny
    smb2.deny.deny1
    smb2.deny.deny2
    # smb2.dir
    smb2.dir.1kfiles_rename
    smb2.dir.file-index
    smb2.dir.find
    smb2.dir.fixed
    smb2.dir.many
    smb2.dir.sorted
    # smb2.durable-open
    # smb2.durable-v2-delay
    smb2.durable-v2-delay.durable_v2_reconnect_delay
    smb2.durable-v2-delay.durable_v2_reconnect_delay_msec
    # smb2.durable-v2-open
    # smb2.durable-v2-regressions
    smb2.durable-v2-regressions.durable_v2_reconnect_bug15624
    # smb2.fileid
    smb2.fileid.unique
    smb2.fileid.unique-dir
    # smb2.ioctl
    smb2.ioctl.bug14607
    smb2.ioctl.bug14788.VALIDATE_NEGOTIATE
    smb2.ioctl.compress_create_with_attr
    smb2.ioctl.compress_dir_inherit
    smb2.ioctl.compress_file_flag
    smb2.ioctl.compress_inherit_disable
    smb2.ioctl.compress_invalid_buf
    smb2.ioctl.compress_invalid_format
    smb2.ioctl.compress_perms
    smb2.ioctl.compress_query_file_attr
    smb2.ioctl.compress_set_file_attr
    smb2.ioctl.copy_chunk_across_shares2
    smb2.ioctl.copy_chunk_append
    smb2.ioctl.copy_chunk_bad_key
    smb2.ioctl.copy_chunk_bug15644
    smb2.ioctl.copy_chunk_multi
    smb2.ioctl.copy_chunk_overwrite
    smb2.ioctl.copy_chunk_simple
    smb2.ioctl.copy_chunk_sparse_dest
    smb2.ioctl.copy_chunk_src_is_dest
    smb2.ioctl.copy_chunk_tiny
    smb2.ioctl.copy_chunk_write_access
    smb2.ioctl.dup_extents_bad_handle
    smb2.ioctl.dup_extents_compressed_dest
    smb2.ioctl.dup_extents_compressed_src
    smb2.ioctl.dup_extents_dest_lock
    smb2.ioctl.dup_extents_len_beyond_dest
    smb2.ioctl.dup_extents_len_beyond_src
    smb2.ioctl.dup_extents_len_zero
    smb2.ioctl.dup_extents_simple
    smb2.ioctl.dup_extents_sparse_both
    smb2.ioctl.dup_extents_sparse_dest
    smb2.ioctl.dup_extents_sparse_src
    smb2.ioctl.dup_extents_src_is_dest
    smb2.ioctl.dup_extents_src_is_dest_overlap
    smb2.ioctl.dup_extents_src_lock
    smb2.ioctl.req_resume_key
    smb2.ioctl.req_two_resume_keys
    smb2.ioctl.shadow_copy
    smb2.ioctl.sparse_compressed
    smb2.ioctl.sparse_copy_chunk
    smb2.ioctl.sparse_dir_flag
    smb2.ioctl.sparse_file_attr
    smb2.ioctl.sparse_file_flag
    smb2.ioctl.sparse_hole_dealloc
    smb2.ioctl.sparse_lock
    smb2.ioctl.sparse_perms
    smb2.ioctl.sparse_punch
    smb2.ioctl.sparse_punch_invalid
    smb2.ioctl.sparse_qar
    smb2.ioctl.sparse_qar_malformed
    smb2.ioctl.sparse_qar_multi
    smb2.ioctl.sparse_qar_ob1
    smb2.ioctl.sparse_qar_overflow
    smb2.ioctl.sparse_qar_truncated
    smb2.ioctl.sparse_set_nobuf
    smb2.ioctl.sparse_set_oversize
    # smb2.lock
    smb2.lock.contend
    smb2.lock.context
    smb2.lock.ctdb-delrec-deadlock
    smb2.lock.open-brlock-deadlock
    smb2.lock.rw-exclusive
    smb2.lock.rw-none
    smb2.lock.rw-shared
    smb2.lock.truncate
    smb2.lock.unlock
    # smb2.maxfid
    smb2.maxfid
    # smb2.maximum_allowed
    smb2.maximum_allowed.read_only
    # smb2.mkdir
    smb2.mkdir
    # smb2.notify-inotify
    smb2.notify-inotify.inotify-rename
    # smb2.notify
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
    # smb2.oplock
    smb2.oplock.batch10
    # smb2.read
    smb2.read.access
    smb2.read.bug14607
    smb2.read.dir
    smb2.read.eof
    smb2.read.position
    # smb2.rename
    smb2.rename.close-full-information
    smb2.rename.msword
    smb2.rename.no_sharing
    smb2.rename.rename-open
    smb2.rename.rename_dir_bench
    smb2.rename.share_delete_no_delete_access
    smb2.rename.simple
    smb2.rename.simple_modtime
    smb2.rename.simple_nodelete
    # smb2.replay
    smb2.replay.replay-commands
    smb2.replay.replay5
    smb2.replay.replay7
    # smb2.rw
    smb2.rw.append
    smb2.rw.invalid
    smb2.rw.rw1
    smb2.rw.rw2
    # smb2.secleak
    smb2.secleak
    # smb2.session-id
    smb2.session-id
    # smb2.session-require-signing
    smb2.session-require-signing.bug15397
    # smb2.session
    smb2.session.bind_different_user
    smb2.session.bind_negative_smb3signC30toGd
    smb2.session.bind_negative_smb3signC30toGs
    smb2.session.bind_negative_smb3signCtoGd
    smb2.session.bind_negative_smb3signCtoGs
    smb2.session.bind_negative_smb3signCtoHd
    smb2.session.bind_negative_smb3signCtoHs
    smb2.session.bind_negative_smb3signGtoC30d
    smb2.session.bind_negative_smb3signGtoC30s
    smb2.session.bind_negative_smb3signGtoCd
    smb2.session.bind_negative_smb3signGtoCs
    smb2.session.bind_negative_smb3signGtoH2Xd
    smb2.session.bind_negative_smb3signGtoH2Xs
    smb2.session.bind_negative_smb3signGtoHd
    smb2.session.bind_negative_smb3signGtoHs
    smb2.session.bind_negative_smb3signH2XtoGd
    smb2.session.bind_negative_smb3signH2XtoGs
    smb2.session.bind_negative_smb3signHtoCd
    smb2.session.bind_negative_smb3signHtoCs
    smb2.session.bind_negative_smb3signHtoGd
    smb2.session.bind_negative_smb3signHtoGs
    smb2.session.bind_negative_smb3sneCtoGd
    smb2.session.bind_negative_smb3sneCtoGs
    smb2.session.bind_negative_smb3sneGtoCd
    smb2.session.bind_negative_smb3sneGtoCs
    smb2.session.bind_negative_smb3sneGtoHd
    smb2.session.bind_negative_smb3sneGtoHs
    smb2.session.bind_negative_smb3sneHtoGd
    smb2.session.bind_negative_smb3sneHtoGs
    smb2.session.encryption-aes-128-ccm
    smb2.session.encryption-aes-128-gcm
    smb2.session.encryption-aes-256-ccm
    smb2.session.encryption-aes-256-gcm
    smb2.session.expire1e
    smb2.session.expire1n
    smb2.session.expire1s
    smb2.session.expire2e
    smb2.session.expire2s
    smb2.session.expire_disconnect
    smb2.session.signing-aes-128-cmac
    smb2.session.signing-aes-128-gmac
    smb2.session.signing-hmac-sha-256
    smb2.session.two_logoff
    # smb2.set-sparse-ioctl
    smb2.set-sparse-ioctl
    # smb2.sharemode
    smb2.sharemode.access-sharemode
    smb2.sharemode.bug14375
    smb2.sharemode.sharemode-access
    # smb2.tcon
    smb2.tcon
    # smb2.timestamps
    smb2.timestamps.test_close_not_attrib
    # smb2.twrp
    smb2.twrp.openroot
    smb2.twrp.stream
    smb2.twrp.write
    # smb2.winattr2
    smb2.winattr2
    # smb2.zero-data-ioctl
    smb2.zero-data-ioctl
)

set(SMBTORTURE_ENABLED_cairn
    # smb2.acls
    smb2.acls.ACCESSBASED
    smb2.acls.CREATOR
    smb2.acls.DENY1
    smb2.acls.DYNAMIC
    smb2.acls.GENERIC
    smb2.acls.INHERITANCE
    smb2.acls.INHERITFLAGS
    smb2.acls.MXAC-NOT-GRANTED
    smb2.acls.OVERWRITE_READ_ONLY_FILE
    smb2.acls.OWNER
    smb2.acls.OWNER-RIGHTS
    smb2.acls.OWNER-RIGHTS-DENY
    smb2.acls.OWNER-RIGHTS-DENY1
    smb2.acls.SDFLAGSVSCHOWN
    # smb2.async_dosmode
    smb2.async_dosmode
    # smb2.bench
    # smb2.change_notify_disabled
    smb2.change_notify_disabled.notfiy_disabled
    # smb2.check-sharemode
    smb2.check-sharemode
    # smb2.compound
    smb2.compound.create-write-close
    smb2.compound.invalid1
    smb2.compound.invalid2
    smb2.compound.invalid3
    smb2.compound.invalid4
    smb2.compound.related1
    smb2.compound.related2
    smb2.compound.related3
    smb2.compound.related5
    smb2.compound.related6
    smb2.compound.related9
    smb2.compound.unrelated1
    # smb2.compound_find
    smb2.compound_find.compound_find_close
    smb2.compound_find.compound_find_related
    smb2.compound_find.compound_find_unrelated
    # smb2.connect
    smb2.connect
    # smb2.create
    smb2.create.bench-path-contention-shared
    smb2.create.brlocked
    smb2.create.delete
    smb2.create.dir-alloc-size
    smb2.create.leading-slash
    smb2.create.mkdir-dup
    smb2.create.multi
    smb2.create.path-length
    # smb2.create_no_streams
    smb2.create_no_streams.no_stream
    # smb2.credits
    smb2.credits.session_setup_credits_granted
    smb2.credits.single_req_credits_granted
    smb2.credits.skipped_mid
    # smb2.delete-on-close-perms
    smb2.delete-on-close-perms.BUG14427
    smb2.delete-on-close-perms.CREATE
    smb2.delete-on-close-perms.CREATE_IF
    smb2.delete-on-close-perms.FIND_and_set_DOC
    smb2.delete-on-close-perms.OVERWRITE_IF
    # smb2.deny
    smb2.deny.deny1
    smb2.deny.deny2
    # smb2.dir
    smb2.dir.1kfiles_rename
    smb2.dir.file-index
    smb2.dir.find
    smb2.dir.sorted
    # smb2.durable-open-disconnect
    smb2.durable-open-disconnect.open-oplock-disconnect
    # smb2.durable-open
    # smb2.durable-v2-delay
    smb2.durable-v2-delay.durable_v2_reconnect_delay
    smb2.durable-v2-delay.durable_v2_reconnect_delay_msec
    # smb2.durable-v2-open
    # smb2.durable-v2-regressions
    smb2.durable-v2-regressions.durable_v2_reconnect_bug15624
    # smb2.fileid
    smb2.fileid.unique
    smb2.fileid.unique-dir
    # smb2.ioctl
    smb2.ioctl.bug14607
    smb2.ioctl.bug14788.VALIDATE_NEGOTIATE
    smb2.ioctl.compress_create_with_attr
    smb2.ioctl.compress_dir_inherit
    smb2.ioctl.compress_file_flag
    smb2.ioctl.compress_inherit_disable
    smb2.ioctl.compress_invalid_buf
    smb2.ioctl.compress_invalid_format
    smb2.ioctl.compress_perms
    smb2.ioctl.compress_query_file_attr
    smb2.ioctl.compress_set_file_attr
    smb2.ioctl.copy_chunk_across_shares2
    smb2.ioctl.copy_chunk_bad_key
    smb2.ioctl.copy_chunk_bug15644
    smb2.ioctl.dup_extents_bad_handle
    smb2.ioctl.dup_extents_compressed_dest
    smb2.ioctl.dup_extents_compressed_src
    smb2.ioctl.dup_extents_dest_lock
    smb2.ioctl.dup_extents_len_beyond_dest
    smb2.ioctl.dup_extents_len_beyond_src
    smb2.ioctl.dup_extents_len_zero
    smb2.ioctl.dup_extents_simple
    smb2.ioctl.dup_extents_sparse_both
    smb2.ioctl.dup_extents_sparse_dest
    smb2.ioctl.dup_extents_sparse_src
    smb2.ioctl.dup_extents_src_is_dest
    smb2.ioctl.dup_extents_src_is_dest_overlap
    smb2.ioctl.dup_extents_src_lock
    smb2.ioctl.req_resume_key
    smb2.ioctl.req_two_resume_keys
    smb2.ioctl.shadow_copy
    smb2.ioctl.sparse_compressed
    smb2.ioctl.sparse_copy_chunk
    smb2.ioctl.sparse_dir_flag
    smb2.ioctl.sparse_file_attr
    smb2.ioctl.sparse_file_flag
    smb2.ioctl.sparse_hole_dealloc
    smb2.ioctl.sparse_lock
    smb2.ioctl.sparse_perms
    smb2.ioctl.sparse_punch
    smb2.ioctl.sparse_punch_invalid
    smb2.ioctl.sparse_qar
    smb2.ioctl.sparse_qar_malformed
    smb2.ioctl.sparse_qar_multi
    smb2.ioctl.sparse_qar_ob1
    smb2.ioctl.sparse_qar_overflow
    smb2.ioctl.sparse_qar_truncated
    smb2.ioctl.sparse_set_nobuf
    smb2.ioctl.sparse_set_oversize
    # smb2.kernel-oplocks
    smb2.kernel-oplocks.kernel_oplocks8
    # smb2.lock
    smb2.lock.contend
    smb2.lock.context
    smb2.lock.ctdb-delrec-deadlock
    smb2.lock.open-brlock-deadlock
    smb2.lock.rw-exclusive
    smb2.lock.rw-none
    smb2.lock.rw-shared
    smb2.lock.truncate
    smb2.lock.unlock
    # smb2.maxfid
    # smb2.maximum_allowed
    smb2.maximum_allowed.maximum_allowed
    smb2.maximum_allowed.read_only
    # smb2.mkdir
    smb2.mkdir
    # smb2.notify-inotify
    smb2.notify-inotify.inotify-rename
    # smb2.notify
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
    # smb2.openattr
    smb2.openattr
    # smb2.read
    smb2.read.access
    smb2.read.bug14607
    smb2.read.dir
    smb2.read.eof
    smb2.read.position
    # smb2.rename
    smb2.rename.close-full-information
    smb2.rename.msword
    smb2.rename.no_sharing
    smb2.rename.rename-open
    smb2.rename.rename_dir_bench
    smb2.rename.share_delete_no_delete_access
    smb2.rename.simple
    smb2.rename.simple_modtime
    smb2.rename.simple_nodelete
    # smb2.replay
    smb2.replay.replay-commands
    smb2.replay.replay-regular
    smb2.replay.replay5
    smb2.replay.replay7
    # smb2.rw
    smb2.rw.append
    smb2.rw.invalid
    smb2.rw.rw1
    smb2.rw.rw2
    # smb2.scan
    smb2.scan.setinfo
    # smb2.secleak
    smb2.secleak
    # smb2.session-id
    smb2.session-id
    # smb2.session-require-signing
    smb2.session-require-signing.bug15397
    # smb2.session
    smb2.session.bind_different_user
    smb2.session.bind_negative_smb3signC30toGd
    smb2.session.bind_negative_smb3signC30toGs
    smb2.session.bind_negative_smb3signCtoGd
    smb2.session.bind_negative_smb3signCtoGs
    smb2.session.bind_negative_smb3signCtoHd
    smb2.session.bind_negative_smb3signCtoHs
    smb2.session.bind_negative_smb3signGtoC30d
    smb2.session.bind_negative_smb3signGtoC30s
    smb2.session.bind_negative_smb3signGtoCd
    smb2.session.bind_negative_smb3signGtoCs
    smb2.session.bind_negative_smb3signGtoH2Xd
    smb2.session.bind_negative_smb3signGtoH2Xs
    smb2.session.bind_negative_smb3signGtoHd
    smb2.session.bind_negative_smb3signGtoHs
    smb2.session.bind_negative_smb3signH2XtoGd
    smb2.session.bind_negative_smb3signH2XtoGs
    smb2.session.bind_negative_smb3signHtoCd
    smb2.session.bind_negative_smb3signHtoCs
    smb2.session.bind_negative_smb3signHtoGd
    smb2.session.bind_negative_smb3signHtoGs
    smb2.session.bind_negative_smb3sneCtoGd
    smb2.session.bind_negative_smb3sneCtoGs
    smb2.session.bind_negative_smb3sneGtoCd
    smb2.session.bind_negative_smb3sneGtoCs
    smb2.session.bind_negative_smb3sneGtoHd
    smb2.session.bind_negative_smb3sneGtoHs
    smb2.session.bind_negative_smb3sneHtoGd
    smb2.session.bind_negative_smb3sneHtoGs
    smb2.session.encryption-aes-128-ccm
    smb2.session.encryption-aes-128-gcm
    smb2.session.encryption-aes-256-ccm
    smb2.session.encryption-aes-256-gcm
    smb2.session.expire1e
    smb2.session.expire1n
    smb2.session.expire1s
    smb2.session.expire2e
    smb2.session.expire2s
    smb2.session.expire_disconnect
    smb2.session.signing-aes-128-cmac
    smb2.session.signing-aes-128-gmac
    smb2.session.signing-hmac-sha-256
    smb2.session.two_logoff
    # smb2.set-sparse-ioctl
    smb2.set-sparse-ioctl
    # smb2.sharemode
    smb2.sharemode.access-sharemode
    smb2.sharemode.bug14375
    smb2.sharemode.sharemode-access
    # smb2.tcon
    smb2.tcon
    # smb2.timestamps
    smb2.timestamps.delayed-1write
    smb2.timestamps.delayed-2write
    smb2.timestamps.delayed-write-vs-flush
    smb2.timestamps.delayed-write-vs-setbasic
    smb2.timestamps.delayed-write-vs-seteof
    smb2.timestamps.freeze-thaw
    smb2.timestamps.modern_write_time_update-1
    smb2.timestamps.modern_write_time_update-2
    smb2.timestamps.test_close_not_attrib
    smb2.timestamps.time_t_-1
    smb2.timestamps.time_t_-2
    smb2.timestamps.time_t_0
    smb2.timestamps.time_t_1
    smb2.timestamps.time_t_10000000000
    smb2.timestamps.time_t_15032385535
    smb2.timestamps.time_t_1968
    smb2.timestamps.time_t_4294967295
    # smb2.twrp
    smb2.twrp.openroot
    smb2.twrp.stream
    smb2.twrp.write
    # smb2.winattr
    smb2.winattr
    # smb2.winattr2
    smb2.winattr2
    # smb2.zero-data-ioctl
    smb2.zero-data-ioctl
)

set(SMBTORTURE_ENABLED_diskfs_io_uring
    # smb2.acls
    smb2.acls.ACCESSBASED
    smb2.acls.CREATOR
    smb2.acls.DENY1
    smb2.acls.DYNAMIC
    smb2.acls.GENERIC
    smb2.acls.INHERITANCE
    smb2.acls.INHERITFLAGS
    smb2.acls.MXAC-NOT-GRANTED
    smb2.acls.OVERWRITE_READ_ONLY_FILE
    smb2.acls.OWNER
    smb2.acls.OWNER-RIGHTS
    smb2.acls.OWNER-RIGHTS-DENY
    smb2.acls.OWNER-RIGHTS-DENY1
    smb2.acls.SDFLAGSVSCHOWN
    # smb2.async_dosmode
    smb2.async_dosmode
    # smb2.bench
    # smb2.change_notify_disabled
    smb2.change_notify_disabled.notfiy_disabled
    # smb2.check-sharemode
    smb2.check-sharemode
    # smb2.compound
    smb2.compound.create-write-close
    smb2.compound.invalid1
    smb2.compound.invalid2
    smb2.compound.invalid3
    smb2.compound.invalid4
    smb2.compound.related1
    smb2.compound.related2
    smb2.compound.related3
    smb2.compound.related5
    smb2.compound.related6
    smb2.compound.related9
    smb2.compound.unrelated1
    # smb2.compound_find
    smb2.compound_find.compound_find_close
    smb2.compound_find.compound_find_related
    smb2.compound_find.compound_find_unrelated
    # smb2.connect
    smb2.connect
    # smb2.create
    smb2.create.acldir
    smb2.create.aclfile
    smb2.create.bench-path-contention-shared
    smb2.create.brlocked
    smb2.create.delete
    smb2.create.dir-alloc-size
    smb2.create.leading-slash
    smb2.create.mkdir-dup
    smb2.create.multi
    smb2.create.path-length
    # smb2.create_no_streams
    smb2.create_no_streams.no_stream
    # smb2.credits
    smb2.credits.session_setup_credits_granted
    smb2.credits.single_req_credits_granted
    smb2.credits.skipped_mid
    # smb2.delete-on-close-perms
    smb2.delete-on-close-perms.BUG14427
    smb2.delete-on-close-perms.CREATE
    smb2.delete-on-close-perms.CREATE_IF
    smb2.delete-on-close-perms.FIND_and_set_DOC
    smb2.delete-on-close-perms.OVERWRITE_IF
    # smb2.deny
    smb2.deny.deny1
    smb2.deny.deny2
    # smb2.dir
    smb2.dir.1kfiles_rename
    smb2.dir.file-index
    smb2.dir.find
    smb2.dir.fixed
    smb2.dir.many
    smb2.dir.sorted
    # smb2.durable-open-disconnect
    smb2.durable-open-disconnect.open-oplock-disconnect
    # smb2.durable-open
    # smb2.durable-v2-delay
    smb2.durable-v2-delay.durable_v2_reconnect_delay
    smb2.durable-v2-delay.durable_v2_reconnect_delay_msec
    # smb2.durable-v2-open
    # smb2.durable-v2-regressions
    smb2.durable-v2-regressions.durable_v2_reconnect_bug15624
    # smb2.fileid
    smb2.fileid.unique
    smb2.fileid.unique-dir
    # smb2.ioctl
    smb2.ioctl.bug14607
    smb2.ioctl.bug14788.VALIDATE_NEGOTIATE
    smb2.ioctl.compress_create_with_attr
    smb2.ioctl.compress_dir_inherit
    smb2.ioctl.compress_file_flag
    smb2.ioctl.compress_inherit_disable
    smb2.ioctl.compress_invalid_buf
    smb2.ioctl.compress_invalid_format
    smb2.ioctl.compress_perms
    smb2.ioctl.compress_query_file_attr
    smb2.ioctl.compress_set_file_attr
    smb2.ioctl.copy_chunk_across_shares2
    smb2.ioctl.copy_chunk_bad_key
    smb2.ioctl.copy_chunk_bug15644
    smb2.ioctl.dup_extents_bad_handle
    smb2.ioctl.dup_extents_compressed_dest
    smb2.ioctl.dup_extents_compressed_src
    smb2.ioctl.dup_extents_dest_lock
    smb2.ioctl.dup_extents_len_beyond_dest
    smb2.ioctl.dup_extents_len_beyond_src
    smb2.ioctl.dup_extents_len_zero
    smb2.ioctl.dup_extents_simple
    smb2.ioctl.dup_extents_sparse_both
    smb2.ioctl.dup_extents_sparse_dest
    smb2.ioctl.dup_extents_sparse_src
    smb2.ioctl.dup_extents_src_is_dest
    smb2.ioctl.dup_extents_src_is_dest_overlap
    smb2.ioctl.dup_extents_src_lock
    smb2.ioctl.req_resume_key
    smb2.ioctl.req_two_resume_keys
    smb2.ioctl.shadow_copy
    smb2.ioctl.sparse_compressed
    smb2.ioctl.sparse_copy_chunk
    smb2.ioctl.sparse_dir_flag
    smb2.ioctl.sparse_file_attr
    smb2.ioctl.sparse_file_flag
    smb2.ioctl.sparse_hole_dealloc
    smb2.ioctl.sparse_lock
    smb2.ioctl.sparse_perms
    smb2.ioctl.sparse_punch
    smb2.ioctl.sparse_punch_invalid
    smb2.ioctl.sparse_qar
    smb2.ioctl.sparse_qar_malformed
    smb2.ioctl.sparse_qar_multi
    smb2.ioctl.sparse_qar_ob1
    smb2.ioctl.sparse_qar_overflow
    smb2.ioctl.sparse_qar_truncated
    smb2.ioctl.sparse_set_nobuf
    smb2.ioctl.sparse_set_oversize
    # smb2.kernel-oplocks
    smb2.kernel-oplocks.kernel_oplocks8
    # smb2.lock
    smb2.lock.contend
    smb2.lock.context
    smb2.lock.ctdb-delrec-deadlock
    smb2.lock.open-brlock-deadlock
    smb2.lock.rw-exclusive
    smb2.lock.rw-none
    smb2.lock.rw-shared
    smb2.lock.truncate
    smb2.lock.unlock
    # smb2.maxfid
    # smb2.maximum_allowed
    smb2.maximum_allowed.maximum_allowed
    smb2.maximum_allowed.read_only
    # smb2.mkdir
    smb2.mkdir
    # smb2.notify-inotify
    smb2.notify-inotify.inotify-rename
    # smb2.notify
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
    # smb2.openattr
    smb2.openattr
    # smb2.read
    smb2.read.access
    smb2.read.bug14607
    smb2.read.dir
    smb2.read.eof
    smb2.read.position
    # smb2.rename
    smb2.rename.close-full-information
    smb2.rename.msword
    smb2.rename.no_sharing
    smb2.rename.rename-open
    smb2.rename.rename_dir_bench
    smb2.rename.share_delete_no_delete_access
    smb2.rename.simple
    smb2.rename.simple_modtime
    smb2.rename.simple_nodelete
    # smb2.replay
    smb2.replay.replay-commands
    smb2.replay.replay-regular
    smb2.replay.replay5
    smb2.replay.replay7
    # smb2.rw
    smb2.rw.append
    smb2.rw.invalid
    smb2.rw.rw1
    smb2.rw.rw2
    # smb2.scan
    smb2.scan.setinfo
    # smb2.secleak
    smb2.secleak
    # smb2.session-id
    smb2.session-id
    # smb2.session-require-signing
    smb2.session-require-signing.bug15397
    # smb2.session
    smb2.session.bind_different_user
    smb2.session.bind_negative_smb3signC30toGd
    smb2.session.bind_negative_smb3signC30toGs
    smb2.session.bind_negative_smb3signCtoGd
    smb2.session.bind_negative_smb3signCtoGs
    smb2.session.bind_negative_smb3signCtoHd
    smb2.session.bind_negative_smb3signCtoHs
    smb2.session.bind_negative_smb3signGtoC30d
    smb2.session.bind_negative_smb3signGtoC30s
    smb2.session.bind_negative_smb3signGtoCd
    smb2.session.bind_negative_smb3signGtoCs
    smb2.session.bind_negative_smb3signGtoH2Xd
    smb2.session.bind_negative_smb3signGtoH2Xs
    smb2.session.bind_negative_smb3signGtoHd
    smb2.session.bind_negative_smb3signGtoHs
    smb2.session.bind_negative_smb3signH2XtoGd
    smb2.session.bind_negative_smb3signH2XtoGs
    smb2.session.bind_negative_smb3signHtoCd
    smb2.session.bind_negative_smb3signHtoCs
    smb2.session.bind_negative_smb3signHtoGd
    smb2.session.bind_negative_smb3signHtoGs
    smb2.session.bind_negative_smb3sneCtoGd
    smb2.session.bind_negative_smb3sneCtoGs
    smb2.session.bind_negative_smb3sneGtoCd
    smb2.session.bind_negative_smb3sneGtoCs
    smb2.session.bind_negative_smb3sneGtoHd
    smb2.session.bind_negative_smb3sneGtoHs
    smb2.session.bind_negative_smb3sneHtoGd
    smb2.session.bind_negative_smb3sneHtoGs
    smb2.session.encryption-aes-128-ccm
    smb2.session.encryption-aes-128-gcm
    smb2.session.encryption-aes-256-ccm
    smb2.session.encryption-aes-256-gcm
    smb2.session.expire1e
    smb2.session.expire1n
    smb2.session.expire1s
    smb2.session.expire2e
    smb2.session.expire2s
    smb2.session.expire_disconnect
    smb2.session.signing-aes-128-cmac
    smb2.session.signing-aes-128-gmac
    smb2.session.signing-hmac-sha-256
    smb2.session.two_logoff
    # smb2.set-sparse-ioctl
    smb2.set-sparse-ioctl
    # smb2.sharemode
    smb2.sharemode.access-sharemode
    smb2.sharemode.bug14375
    smb2.sharemode.sharemode-access
    # smb2.tcon
    smb2.tcon
    # smb2.timestamps
    smb2.timestamps.delayed-1write
    smb2.timestamps.delayed-2write
    smb2.timestamps.delayed-write-vs-flush
    smb2.timestamps.delayed-write-vs-setbasic
    smb2.timestamps.delayed-write-vs-seteof
    smb2.timestamps.freeze-thaw
    smb2.timestamps.modern_write_time_update-1
    smb2.timestamps.modern_write_time_update-2
    smb2.timestamps.test_close_not_attrib
    smb2.timestamps.time_t_-1
    smb2.timestamps.time_t_-2
    smb2.timestamps.time_t_0
    smb2.timestamps.time_t_1
    smb2.timestamps.time_t_10000000000
    smb2.timestamps.time_t_15032385535
    smb2.timestamps.time_t_1968
    smb2.timestamps.time_t_4294967295
    # smb2.twrp
    smb2.twrp.openroot
    smb2.twrp.stream
    smb2.twrp.write
    # smb2.winattr
    smb2.winattr
    # smb2.winattr2
    smb2.winattr2
    # smb2.zero-data-ioctl
    smb2.zero-data-ioctl
)

set(SMBTORTURE_ENABLED_diskfs_aio
    # smb2.acls
    smb2.acls.ACCESSBASED
    smb2.acls.CREATOR
    smb2.acls.DENY1
    smb2.acls.DYNAMIC
    smb2.acls.GENERIC
    smb2.acls.INHERITANCE
    smb2.acls.INHERITFLAGS
    smb2.acls.MXAC-NOT-GRANTED
    smb2.acls.OVERWRITE_READ_ONLY_FILE
    smb2.acls.OWNER
    smb2.acls.OWNER-RIGHTS
    smb2.acls.OWNER-RIGHTS-DENY
    smb2.acls.OWNER-RIGHTS-DENY1
    smb2.acls.SDFLAGSVSCHOWN
    # smb2.async_dosmode
    smb2.async_dosmode
    # smb2.bench
    # smb2.change_notify_disabled
    smb2.change_notify_disabled.notfiy_disabled
    # smb2.check-sharemode
    smb2.check-sharemode
    # smb2.compound
    smb2.compound.create-write-close
    smb2.compound.invalid1
    smb2.compound.invalid2
    smb2.compound.invalid3
    smb2.compound.invalid4
    smb2.compound.related1
    smb2.compound.related2
    smb2.compound.related3
    smb2.compound.related5
    smb2.compound.related6
    smb2.compound.related9
    smb2.compound.unrelated1
    # smb2.compound_find
    smb2.compound_find.compound_find_close
    smb2.compound_find.compound_find_related
    smb2.compound_find.compound_find_unrelated
    # smb2.connect
    smb2.connect
    # smb2.create
    smb2.create.acldir
    smb2.create.aclfile
    smb2.create.bench-path-contention-shared
    smb2.create.brlocked
    smb2.create.delete
    smb2.create.dir-alloc-size
    smb2.create.leading-slash
    smb2.create.mkdir-dup
    smb2.create.multi
    smb2.create.path-length
    # smb2.create_no_streams
    smb2.create_no_streams.no_stream
    # smb2.credits
    smb2.credits.session_setup_credits_granted
    smb2.credits.single_req_credits_granted
    smb2.credits.skipped_mid
    # smb2.delete-on-close-perms
    smb2.delete-on-close-perms.BUG14427
    smb2.delete-on-close-perms.CREATE
    smb2.delete-on-close-perms.CREATE_IF
    smb2.delete-on-close-perms.FIND_and_set_DOC
    smb2.delete-on-close-perms.OVERWRITE_IF
    # smb2.deny
    smb2.deny.deny1
    smb2.deny.deny2
    # smb2.dir
    smb2.dir.1kfiles_rename
    smb2.dir.file-index
    smb2.dir.find
    smb2.dir.fixed
    smb2.dir.many
    smb2.dir.sorted
    # smb2.durable-open-disconnect
    smb2.durable-open-disconnect.open-oplock-disconnect
    # smb2.durable-open
    # smb2.durable-v2-delay
    smb2.durable-v2-delay.durable_v2_reconnect_delay
    smb2.durable-v2-delay.durable_v2_reconnect_delay_msec
    # smb2.durable-v2-open
    # smb2.durable-v2-regressions
    smb2.durable-v2-regressions.durable_v2_reconnect_bug15624
    # smb2.fileid
    smb2.fileid.unique
    smb2.fileid.unique-dir
    # smb2.ioctl
    smb2.ioctl.bug14607
    smb2.ioctl.bug14788.VALIDATE_NEGOTIATE
    smb2.ioctl.compress_create_with_attr
    smb2.ioctl.compress_dir_inherit
    smb2.ioctl.compress_file_flag
    smb2.ioctl.compress_inherit_disable
    smb2.ioctl.compress_invalid_buf
    smb2.ioctl.compress_invalid_format
    smb2.ioctl.compress_perms
    smb2.ioctl.compress_query_file_attr
    smb2.ioctl.compress_set_file_attr
    smb2.ioctl.copy_chunk_across_shares2
    smb2.ioctl.copy_chunk_bad_key
    smb2.ioctl.copy_chunk_bug15644
    smb2.ioctl.dup_extents_bad_handle
    smb2.ioctl.dup_extents_compressed_dest
    smb2.ioctl.dup_extents_compressed_src
    smb2.ioctl.dup_extents_dest_lock
    smb2.ioctl.dup_extents_len_beyond_dest
    smb2.ioctl.dup_extents_len_beyond_src
    smb2.ioctl.dup_extents_len_zero
    smb2.ioctl.dup_extents_simple
    smb2.ioctl.dup_extents_sparse_both
    smb2.ioctl.dup_extents_sparse_dest
    smb2.ioctl.dup_extents_sparse_src
    smb2.ioctl.dup_extents_src_is_dest
    smb2.ioctl.dup_extents_src_is_dest_overlap
    smb2.ioctl.dup_extents_src_lock
    smb2.ioctl.req_resume_key
    smb2.ioctl.req_two_resume_keys
    smb2.ioctl.shadow_copy
    smb2.ioctl.sparse_compressed
    smb2.ioctl.sparse_copy_chunk
    smb2.ioctl.sparse_dir_flag
    smb2.ioctl.sparse_file_attr
    smb2.ioctl.sparse_file_flag
    smb2.ioctl.sparse_hole_dealloc
    smb2.ioctl.sparse_lock
    smb2.ioctl.sparse_perms
    smb2.ioctl.sparse_punch
    smb2.ioctl.sparse_punch_invalid
    smb2.ioctl.sparse_qar
    smb2.ioctl.sparse_qar_malformed
    smb2.ioctl.sparse_qar_multi
    smb2.ioctl.sparse_qar_ob1
    smb2.ioctl.sparse_qar_overflow
    smb2.ioctl.sparse_qar_truncated
    smb2.ioctl.sparse_set_nobuf
    smb2.ioctl.sparse_set_oversize
    # smb2.kernel-oplocks
    smb2.kernel-oplocks.kernel_oplocks8
    # smb2.lock
    smb2.lock.contend
    smb2.lock.context
    smb2.lock.ctdb-delrec-deadlock
    smb2.lock.open-brlock-deadlock
    smb2.lock.rw-exclusive
    smb2.lock.rw-none
    smb2.lock.rw-shared
    smb2.lock.truncate
    smb2.lock.unlock
    # smb2.maxfid
    # smb2.maximum_allowed
    smb2.maximum_allowed.maximum_allowed
    smb2.maximum_allowed.read_only
    # smb2.mkdir
    smb2.mkdir
    # smb2.notify-inotify
    smb2.notify-inotify.inotify-rename
    # smb2.notify
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
    # smb2.openattr
    smb2.openattr
    # smb2.read
    smb2.read.access
    smb2.read.bug14607
    smb2.read.dir
    smb2.read.eof
    smb2.read.position
    # smb2.rename
    smb2.rename.close-full-information
    smb2.rename.msword
    smb2.rename.no_sharing
    smb2.rename.rename-open
    smb2.rename.rename_dir_bench
    smb2.rename.share_delete_no_delete_access
    smb2.rename.simple
    smb2.rename.simple_modtime
    smb2.rename.simple_nodelete
    # smb2.replay
    smb2.replay.replay-commands
    smb2.replay.replay-regular
    smb2.replay.replay5
    smb2.replay.replay7
    # smb2.rw
    smb2.rw.append
    smb2.rw.invalid
    smb2.rw.rw1
    smb2.rw.rw2
    # smb2.scan
    smb2.scan.setinfo
    # smb2.secleak
    smb2.secleak
    # smb2.session-id
    smb2.session-id
    # smb2.session-require-signing
    smb2.session-require-signing.bug15397
    # smb2.session
    smb2.session.bind_different_user
    smb2.session.bind_negative_smb3signC30toGd
    smb2.session.bind_negative_smb3signC30toGs
    smb2.session.bind_negative_smb3signCtoGd
    smb2.session.bind_negative_smb3signCtoGs
    smb2.session.bind_negative_smb3signCtoHd
    smb2.session.bind_negative_smb3signCtoHs
    smb2.session.bind_negative_smb3signGtoC30d
    smb2.session.bind_negative_smb3signGtoC30s
    smb2.session.bind_negative_smb3signGtoCd
    smb2.session.bind_negative_smb3signGtoCs
    smb2.session.bind_negative_smb3signGtoH2Xd
    smb2.session.bind_negative_smb3signGtoH2Xs
    smb2.session.bind_negative_smb3signGtoHd
    smb2.session.bind_negative_smb3signGtoHs
    smb2.session.bind_negative_smb3signH2XtoGd
    smb2.session.bind_negative_smb3signH2XtoGs
    smb2.session.bind_negative_smb3signHtoCd
    smb2.session.bind_negative_smb3signHtoCs
    smb2.session.bind_negative_smb3signHtoGd
    smb2.session.bind_negative_smb3signHtoGs
    smb2.session.bind_negative_smb3sneCtoGd
    smb2.session.bind_negative_smb3sneCtoGs
    smb2.session.bind_negative_smb3sneGtoCd
    smb2.session.bind_negative_smb3sneGtoCs
    smb2.session.bind_negative_smb3sneGtoHd
    smb2.session.bind_negative_smb3sneGtoHs
    smb2.session.bind_negative_smb3sneHtoGd
    smb2.session.bind_negative_smb3sneHtoGs
    smb2.session.encryption-aes-128-ccm
    smb2.session.encryption-aes-128-gcm
    smb2.session.encryption-aes-256-ccm
    smb2.session.encryption-aes-256-gcm
    smb2.session.expire1e
    smb2.session.expire1n
    smb2.session.expire1s
    smb2.session.expire2e
    smb2.session.expire2s
    smb2.session.expire_disconnect
    smb2.session.signing-aes-128-cmac
    smb2.session.signing-aes-128-gmac
    smb2.session.signing-hmac-sha-256
    smb2.session.two_logoff
    # smb2.set-sparse-ioctl
    smb2.set-sparse-ioctl
    # smb2.sharemode
    smb2.sharemode.access-sharemode
    smb2.sharemode.bug14375
    smb2.sharemode.sharemode-access
    # smb2.tcon
    smb2.tcon
    # smb2.timestamps
    smb2.timestamps.delayed-1write
    smb2.timestamps.delayed-2write
    smb2.timestamps.delayed-write-vs-flush
    smb2.timestamps.delayed-write-vs-setbasic
    smb2.timestamps.delayed-write-vs-seteof
    smb2.timestamps.freeze-thaw
    smb2.timestamps.modern_write_time_update-1
    smb2.timestamps.modern_write_time_update-2
    smb2.timestamps.test_close_not_attrib
    smb2.timestamps.time_t_-1
    smb2.timestamps.time_t_-2
    smb2.timestamps.time_t_0
    smb2.timestamps.time_t_1
    smb2.timestamps.time_t_10000000000
    smb2.timestamps.time_t_15032385535
    smb2.timestamps.time_t_1968
    smb2.timestamps.time_t_4294967295
    # smb2.twrp
    smb2.twrp.openroot
    smb2.twrp.stream
    smb2.twrp.write
    # smb2.winattr
    smb2.winattr
    # smb2.winattr2
    smb2.winattr2
    # smb2.zero-data-ioctl
    smb2.zero-data-ioctl
)

# >>> SMBTORTURE_BLOCK_END

macro(add_smbtorture_backend_tests backend)
    foreach(SUITE ${SMBTORTURE_SUITES})
        # Sanitize the suite name into a ctest-safe identifier: a handful of
        # smbtorture subtests carry spaces / parens / hyphens in their name
        # (e.g. `smb2.charset.Testing composite character (a umlaut)`), but the
        # SUITE arg to smbtorture itself must keep them verbatim.
        string(REGEX REPLACE "[^A-Za-z0-9]" "_" SUITE_ID ${SUITE})
        set(TEST_NAME chimera/server/smb/smbtorture_${SUITE_ID}_${backend})
        add_test(NAME ${TEST_NAME}
            COMMAND ${CMAKE_SOURCE_DIR}/scripts/netns_test_wrapper.sh
                    ${SMBTORTURE_CMD} -b ${backend} ${SUITE})
        set_tests_properties(${TEST_NAME} PROPERTIES
            ENVIRONMENT "${SMBTORTURE_LSAN}"
            # smbtorture_test exits 77 when the smbtorture client is missing at
            # runtime; report that as Skipped rather than a failure.
            SKIP_RETURN_CODE 77
            # Override the project-wide 30s default.  Most smbtorture
            # subtests finish in under 10s, but a few (smb2.notify.*,
            # smb2.maxfid, smb2.lock.cancel, ...) involve real-time
            # waits or large file-handle counts and can stretch to 90-180s
            # on slow ARC runners under ctest -j 8.  300s matches the
            # tools/smbtorture/regen.sh matrix budget; anything that
            # genuinely runs longer than that is broken, not slow.
            TIMEOUT 300)
        # The smb2.durable-open.* / smb2.durable-v2-open.* families are
        # lease/oplock-break state-machine tests; their pass/fail asserts on
        # timing that gets tight when CPU is contended on slow CI runners,
        # so serialize them against each other via a shared ctest lock.  They
        # still run alongside unrelated tests, just not against each other.
        if(SUITE MATCHES "^smb2\\.durable-(v2-)?open\\.")
            set_tests_properties(${TEST_NAME} PROPERTIES
                RESOURCE_LOCK smb_durable_oplock_family)
        endif()
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
