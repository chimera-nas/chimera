# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# memfs is compiled once as an object library: the loadable module is built
# from it, and a test that instruments the backend in-process embeds it (the
# VFS then uses the embedded vfs_memfs rather than loading the module).
add_library(chimera_vfs_memfs_objs OBJECT
    memfs.c
)

set_target_properties(chimera_vfs_memfs_objs PROPERTIES POSITION_INDEPENDENT_CODE ON)

target_compile_definitions(chimera_vfs_memfs_objs PRIVATE
    XXH_INLINE_ALL
    XXH_VECTOR=${CHIMERA_XXH_VECTOR}
)

target_link_libraries(chimera_vfs_memfs_objs PUBLIC evpl jansson)

add_library(chimera_vfs_memfs ${CHIMERA_VFS_MODULE_TYPE}
    $<TARGET_OBJECTS:chimera_vfs_memfs_objs>
)

target_link_libraries(chimera_vfs_memfs evpl jansson)

chimera_vfs_module(chimera_vfs_memfs)
