

add_executable(libnfs_nfs4_mount libnfs_nfs4_mount.c)
target_link_libraries(libnfs_nfs4_mount nfs chimera_server)

add_test(NAME chimera/server/libnfs_nfs4_mount COMMAND ${CMAKE_CURRENT_BINARY_DIR}/libnfs_nfs4_mount)
