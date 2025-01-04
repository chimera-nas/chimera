add_library(chimera_vfs_root SHARED
    vfs_root.c
)

install(TARGETS chimera_vfs_root DESTINATION lib)