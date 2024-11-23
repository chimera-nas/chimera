
add_library(chimera_vfs vfs.c)

add_subdirectory(memfs)

target_link_libraries(chimera_vfs PRIVATE chimera_vfs_memfs)
