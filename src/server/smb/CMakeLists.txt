# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_library(chimera_smb ${CHIMERA_LIBRARY_TYPE}
    smb.c smb_proc_negotiate.c smb_proc_session_setup.c smb_dump.c smb_trace.c
    smb_proc_tree_connect.c smb_proc_create.c smb_proc_close.c
    smb_proc_set_info.c smb_proc_tree_disconnect.c smb_proc_logoff.c
    smb_proc_write.c smb_proc_read.c smb_proc_query_info.c
    smb_proc_query_directory.c smb_proc_ioctl.c smb_proc_flush.c smb_proc_echo.c
    smb_rpc.c smb_rpc_host.c smb_lsarpc.c smb_srvsvc.c smb_samr.c smb_wkssvc.c smb_sign_request.c smb_secure_send.c smb_proc_set_info_rename.c smb_proc_security.c
    smb_proc_reparse.c smb_proc_object_id.c smb_proc_change_notify.c smb_proc_cancel.c
    smb_proc_sparse.c smb_proc_copychunk.c smb_proc_copyoffload.c
    smb_proc_lock.c smb_proc_oplock_break.c
    smb_notify.c smb_async_interim.c smb_sharemode.c smb_ntlm.c smb_durable.c
    smb_wbclient.c smb_auth.c ${CHIMERA_SMB_GSS_SOURCE} smb_kerberos_identity.c
)

# DCE/RPC for the srvsvc / wkssvc / lsarpc / samr named pipes comes from
# libdcerpc (ext/libdcerpc.cmake), linked in statically.
target_link_libraries(chimera_smb chimera_server_config chimera_smb_common chimera_vfs evpl dcerpc ${CHIMERA_GSS_LIB})

# SMB string conversion needs iconv even when Kerberos is disabled.
if(NOT WIN32)
    find_package(Iconv REQUIRED)
    target_link_libraries(chimera_smb Iconv::Iconv)
endif()

# Conditionally link libwbclient for Winbind integration
if(WBCLIENT_FOUND)
    target_include_directories(chimera_smb PRIVATE ${WBCLIENT_INCLUDE_DIRS})
    target_link_libraries(chimera_smb ${WBCLIENT_LIBRARIES})
endif()

install(TARGETS chimera_smb DESTINATION lib)

if (NOT DISABLE_TESTS)
    add_subdirectory(tests)
endif()

# Exports its data rather than importing it (common/export.h).
target_compile_definitions(chimera_smb PRIVATE CHIMERA_SMB_BUILD)
