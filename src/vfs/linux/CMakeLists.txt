add_library(chimera_vfs_linux SHARED
    linux.c
)

install(TARGETS chimera_vfs_linux DESTINATION lib)