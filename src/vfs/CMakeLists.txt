
add_library(chimera_vfs SHARED vfs.c 
            vfs_proc_mkdir.c vfs_proc_close.c vfs_proc_open.c
            vfs_proc_readdir.c vfs_proc_getattr.c vfs_proc_lookup.c
            vfs_proc_lookup_path.c vfs_proc_getrootfh.c vfs_proc_read.c
            vfs_proc_remove.c vfs_proc_write.c vfs_proc_commit.c
            vfs_proc_access.c vfs_proc_open_at.c vfs_proc_symlink.c
            vfs_proc_readlink.c vfs_proc_rename.c vfs_proc_setattr.c
            vfs_proc_link.c vfs_dump.c)

install(TARGETS chimera_vfs DESTINATION lib)

add_subdirectory(root)
add_subdirectory(memfs)
add_subdirectory(linux)

target_link_libraries(chimera_vfs PRIVATE chimera_vfs_root chimera_vfs_memfs chimera_vfs_linux)
