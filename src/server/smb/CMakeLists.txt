add_library(chimera_smb SHARED
    smb.c smb_proc_negotiate.c smb_proc_session_setup.c smb_dump.c 
    smb_proc_tree_connect.c smb_proc_create.c smb_proc_close.c
    smb_proc_set_info.c smb_proc_tree_disconnect.c smb_proc_logoff.c
    smb_proc_write.c smb_proc_read.c smb_proc_query_info.c
    smb_proc_query_directory.c)

target_link_libraries(chimera_smb chimera_vfs evpl) #gssapi_krb5)

install(TARGETS chimera_smb DESTINATION lib)

if (NOT DISABLE_TESTS)
    add_subdirectory(tests)
endif()