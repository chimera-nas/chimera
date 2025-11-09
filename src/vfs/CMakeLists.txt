# SPDX-FileCopyrightText: 2025 Ben Jarvis
#
# SPDX-License-Identifier: LGPL-2.1-only

add_library(chimera_vfs SHARED vfs.c 
            vfs_proc_mount.c vfs_proc_umount.c
            vfs_proc_mkdir.c vfs_proc_close.c vfs_proc_open.c
            vfs_proc_readdir.c vfs_proc_getattr.c vfs_proc_lookup.c
            vfs_proc_lookup_path.c vfs_proc_read.c
            vfs_proc_remove.c vfs_proc_write.c vfs_proc_commit.c
            vfs_proc_open_at.c vfs_proc_symlink.c vfs_proc_link.c
            vfs_proc_readlink.c vfs_proc_rename.c vfs_proc_setattr.c
            vfs_proc_create_path.c vfs_proc_create_unlinked.c
            vfs_proc_find.c vfs_dump.c)

target_compile_definitions(chimera_vfs PRIVATE
    XXH_INLINE_ALL
    $<$<STREQUAL:${CMAKE_SYSTEM_PROCESSOR},aarch64>:XXH_VECTOR=XXH_NEON>
    $<$<NOT:$<STREQUAL:${CMAKE_SYSTEM_PROCESSOR},aarch64>>:XXH_VECTOR=XXH_AVX2>
)

install(TARGETS chimera_vfs DESTINATION lib)

add_subdirectory(root)
add_subdirectory(memfs)
add_subdirectory(linux)
add_subdirectory(nfs)

if (IO_URING_ENABLED)
    add_subdirectory(io_uring)
endif()

add_subdirectory(demofs)

if (CAIRN_ENABLED)
    add_subdirectory(cairn)
endif()

target_link_libraries(chimera_vfs  
    chimera_vfs_root chimera_vfs_memfs chimera_vfs_linux chimera_vfs_demofs 
    chimera_vfs_nfs
    chimera_common prometheus-c xxhash dl urcu)

if (IO_URING_ENABLED)
    target_link_libraries(chimera_vfs chimera_vfs_io_uring)
endif()

if (DEMFS_ENABLED)
    target_link_libraries(chimera_vfs chimera_vfs_demofs)
endif()

if (CAIRN_ENABLED)
    target_link_libraries(chimera_vfs chimera_vfs_cairn)
endif()
