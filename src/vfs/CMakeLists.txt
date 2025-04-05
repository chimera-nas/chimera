
add_library(chimera_vfs SHARED vfs.c 
            vfs_proc_mkdir.c vfs_proc_close.c vfs_proc_open.c
            vfs_proc_readdir.c vfs_proc_getattr.c vfs_proc_lookup.c
            vfs_proc_lookup_path.c vfs_proc_getrootfh.c vfs_proc_read.c
            vfs_proc_remove.c vfs_proc_write.c vfs_proc_commit.c
            vfs_proc_open_at.c vfs_proc_symlink.c vfs_proc_link.c
            vfs_proc_readlink.c vfs_proc_rename.c vfs_proc_setattr.c
            vfs_dump.c)

target_compile_definitions(chimera_vfs PRIVATE
    -DXXH_INLINE_ALL
    $<$<STREQUAL:${CMAKE_SYSTEM_PROCESSOR},aarch64>:-DXXH_VECTOR=XXH_NEON>
    $<$<NOT:$<STREQUAL:${CMAKE_SYSTEM_PROCESSOR},aarch64>>:-DXXH_VECTOR=XXH_AVX2>
)

install(TARGETS chimera_vfs DESTINATION lib)

add_subdirectory(root)
add_subdirectory(memfs)
add_subdirectory(linux)
add_subdirectory(io_uring)
add_subdirectory(demofs)
add_subdirectory(cairn)

target_link_libraries(chimera_vfs  
    chimera_vfs_root chimera_vfs_memfs chimera_vfs_linux chimera_vfs_io_uring chimera_vfs_demofs chimera_vfs_cairn xxhash dl)
