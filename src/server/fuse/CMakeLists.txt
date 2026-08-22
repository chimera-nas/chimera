# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_library(chimera_fuse SHARED
    fuse.c
    fuse_mount.c
    fuse_node_table.c
    fuse_dispatch.c
    fuse_proc_lookup.c
    fuse_proc_getattr.c
    fuse_proc_dir.c
    fuse_proc_io.c
    fuse_proc_namespace.c
    fuse_proc_xattr.c)

target_link_libraries(chimera_fuse chimera_vfs chimera_common evpl)

install(TARGETS chimera_fuse DESTINATION lib)

if (NOT DISABLE_TESTS)
    add_subdirectory(tests)
endif()
