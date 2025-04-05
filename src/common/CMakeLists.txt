add_library(chimera_common SHARED
    logging.c snprintf.c
)

add_subdirectory(tests)