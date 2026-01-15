# SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_library(chimera_smb SHARED
    smb.c smb_proc_negotiate.c smb_proc_session_setup.c smb_dump.c
    smb_proc_tree_connect.c smb_proc_create.c smb_proc_close.c
    smb_proc_set_info.c smb_proc_tree_disconnect.c smb_proc_logoff.c
    smb_proc_write.c smb_proc_read.c smb_proc_query_info.c
    smb_proc_query_directory.c smb_proc_ioctl.c smb_proc_flush.c smb_proc_echo.c
    smb_lsarpc.c smb_signing.c smb_proc_set_info_rename.c
    #idl/ndr_dcerpc.c
)

target_link_libraries(chimera_smb chimera_vfs evpl gssapi_krb5)

install(TARGETS chimera_smb DESTINATION lib)

if (NOT DISABLE_TESTS)
    add_subdirectory(tests)
endif()