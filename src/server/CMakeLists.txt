add_library(chimera_server SHARED server.c)
target_link_libraries(chimera_server chimera_nfs chimera_vfs evpl)

add_subdirectory(tests)