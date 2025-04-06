add_library(chimera_common SHARED
    logging.c snprintf.c
)

target_link_libraries(chimera_common unwind pthread dl)

add_subdirectory(tests)

install(TARGETS chimera_common DESTINATION lib)