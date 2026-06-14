# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_library(chimera_smb SHARED
    smb.c smb_proc_negotiate.c smb_proc_session_setup.c smb_dump.c
    smb_proc_tree_connect.c smb_proc_create.c smb_proc_close.c
    smb_proc_set_info.c smb_proc_tree_disconnect.c smb_proc_logoff.c
    smb_proc_write.c smb_proc_read.c smb_proc_query_info.c
    smb_proc_query_directory.c smb_proc_ioctl.c smb_proc_flush.c smb_proc_echo.c
    smb_lsarpc.c smb_srvsvc.c smb_samr.c smb_wkssvc.c smb_signing.c smb_encrypt.c smb_compress.c smb_proc_set_info_rename.c smb_proc_security.c
    smb_proc_reparse.c smb_proc_change_notify.c smb_proc_cancel.c
    smb_proc_sparse.c smb_proc_copychunk.c
    smb_proc_lock.c smb_proc_oplock_break.c
    smb_notify.c smb_async_interim.c smb_sharemode.c smb_ntlm.c smb_durable.c
    smb_wbclient.c smb_auth.c smb_gssapi.c
)

# Generate NDR marshalling for the named-pipe RPC interfaces from their .idl
# definitions via ndrzcc (exported as ${NDRZCC} by ext/ndrzcc).
set(NDR_IDL_DIR ${CMAKE_CURRENT_SOURCE_DIR}/idl)
set(NDR_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/idl)
file(MAKE_DIRECTORY ${NDR_GEN_DIR})

foreach(iface lsarpc srvsvc samr wkssvc)
    add_custom_command(
        OUTPUT  ${NDR_GEN_DIR}/${iface}_ndr.c ${NDR_GEN_DIR}/${iface}_ndr.h
        COMMAND ${NDRZCC} ${NDR_IDL_DIR}/${iface}.idl
                ${NDR_GEN_DIR}/${iface}_ndr.c ${NDR_GEN_DIR}/${iface}_ndr.h
        DEPENDS ${NDR_IDL_DIR}/${iface}.idl ${NDRZCC}
        COMMENT "Compiling ${iface}.idl")
    list(APPEND NDR_GEN_SRCS ${NDR_GEN_DIR}/${iface}_ndr.c)
endforeach()

target_sources(chimera_smb PRIVATE ${NDR_GEN_SRCS})
target_include_directories(chimera_smb PRIVATE ${NDR_GEN_DIR})
set_source_files_properties(${NDR_GEN_SRCS}
    PROPERTIES COMPILE_OPTIONS "-Wno-unused;-Wno-format-truncation")
add_dependencies(chimera_smb ndrzcc)

target_link_libraries(chimera_smb chimera_vfs evpl gssapi_krb5)

# Conditionally link libwbclient for Winbind integration
if(WBCLIENT_FOUND)
    target_include_directories(chimera_smb PRIVATE ${WBCLIENT_INCLUDE_DIRS})
    target_link_libraries(chimera_smb ${WBCLIENT_LIBRARIES})
endif()

install(TARGETS chimera_smb DESTINATION lib)

if (NOT DISABLE_TESTS)
    # Unit test for the generated LSARPC marshalling.  Defined here (not in
    # tests/) so the generated lsarpc_ndr.c custom command is in scope; links
    # the generated marshaller directly, with no server dependency.
    add_executable(smb_lsarpc_test tests/smb_lsarpc_test.c ${NDR_GEN_DIR}/lsarpc_ndr.c)
    target_include_directories(smb_lsarpc_test PRIVATE ${NDR_GEN_DIR})
    set_source_files_properties(${NDR_GEN_DIR}/lsarpc_ndr.c
        PROPERTIES COMPILE_OPTIONS "-Wno-unused;-Wno-format-truncation")
    add_dependencies(smb_lsarpc_test ndrzcc)
    add_test(NAME chimera/server/smb/lsarpc COMMAND smb_lsarpc_test)
    set_tests_properties(chimera/server/smb/lsarpc PROPERTIES LABELS "smb")

    add_subdirectory(tests)
endif()
