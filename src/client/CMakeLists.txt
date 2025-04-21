add_library(chimera_client SHARED client.c client_open.c client_mkdir.c client_close.c)
target_link_libraries(chimera_client chimera_vfs evpl)

install(TARGETS chimera_client DESTINATION lib)

if (NOT DISABLE_TESTS)
    add_subdirectory(tests)
endif()