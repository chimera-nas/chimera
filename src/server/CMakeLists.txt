add_library(chimera_server SHARED server.c)
target_link_libraries(chimera_server chimera_nfs chimera_vfs evpl)

install(TARGETS chimera_server DESTINATION lib)

if (NOT DISABLE_TESTS)
    add_subdirectory(tests)
endif()