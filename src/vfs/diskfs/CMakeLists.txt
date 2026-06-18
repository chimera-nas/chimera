# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_library(chimera_vfs_diskfs SHARED
    diskfs.c
    diskfs_attr.c
    diskfs_block.c
    diskfs_btree.c
    diskfs_clone.c
    diskfs_inode.c
    diskfs_io.c
    diskfs_log.c
    diskfs_mount.c
    diskfs_namespace.c
    diskfs_reclaim.c
    space_map.c
)

target_link_libraries(chimera_vfs_diskfs jansson)

target_compile_definitions(chimera_vfs_diskfs PRIVATE
    XXH_INLINE_ALL
    XXH_VECTOR=${CHIMERA_XXH_VECTOR}
)

install(TARGETS chimera_vfs_diskfs DESTINATION lib)
