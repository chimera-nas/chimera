
add_executable(chimera daemon.c)

target_link_libraries(chimera chimera_server)
