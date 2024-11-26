
add_library(chimera_vfs vfs.c vfs_root.c
            vfs_proc_mkdir.c vfs_proc_close.c vfs_proc_open.c
            vfs_proc_readdir.c vfs_proc_getattr.c vfs_proc_lookup.c
            vfs_proc_lookup_path.c vfs_proc_getrootfh.c)

add_subdirectory(memfs)
add_subdirectory(linux)

target_link_libraries(chimera_vfs PRIVATE chimera_vfs_memfs chimera_vfs_linux)
