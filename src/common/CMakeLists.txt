add_library(chimera_common STATIC
    logging.c snprintf.c
)

add_subdirectory(tests)