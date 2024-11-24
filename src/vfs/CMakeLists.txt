
add_library(chimera_vfs vfs.c vfs_root.c vfs_procs.c)

add_subdirectory(memfs)
add_subdirectory(linux)

target_link_libraries(chimera_vfs PRIVATE chimera_vfs_memfs chimera_vfs_linux)
