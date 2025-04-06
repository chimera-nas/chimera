add_library(chimera_client SHARED client.c)
target_link_libraries(chimera_client chimera_vfs evpl)

install(TARGETS chimera_client DESTINATION lib)

if (NOT DISABLE_TESTS)
    add_subdirectory(tests)
endif()