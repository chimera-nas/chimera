

add_executable(libnfs_stat_root libnfs_stat_root.c)
target_link_libraries(libnfs_stat_root nfs chimera_server)

add_test(NAME chimera/server/libnfs_stat_root COMMAND ${CMAKE_CURRENT_BINARY_DIR}/libnfs_stat_root)