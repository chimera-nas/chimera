# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_library(chimera_fuse ${CHIMERA_LIBRARY_TYPE}
    fuse.c
    fuse_mount.c
    fuse_node_table.c
    fuse_dispatch.c
    fuse_coherence.c
    fuse_proc_lookup.c
    fuse_proc_getattr.c
    fuse_proc_dir.c
    fuse_proc_io.c
    fuse_proc_lock.c
    fuse_proc_namespace.c
    fuse_proc_xattr.c
    fuse_uring.c)

target_link_libraries(chimera_fuse chimera_vfs chimera_common evpl)

# FUSE-over-io_uring (fuse_uring.c); without liburing the mount negotiates
# plain /dev/fuse only.
if (IO_URING_ENABLED)
    target_compile_definitions(chimera_fuse PRIVATE CHIMERA_FUSE_URING=1)
    target_link_libraries(chimera_fuse ${URING_LIB})
endif()

install(TARGETS chimera_fuse DESTINATION lib)

if (NOT DISABLE_TESTS)
    add_subdirectory(tests)
endif()
