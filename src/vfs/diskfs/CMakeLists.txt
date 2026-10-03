# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# Compiled once as an object library: the loadable module is built from it,
# and the white-box tests below embed it so they can reach the diskfs_test.h
# SDK (the VFS then uses the embedded vfs_diskfs rather than loading the
# module).
add_library(chimera_vfs_diskfs_objs OBJECT
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
    diskfs_stream.c
    space_map.c
)

# White-box test SDK (diskfs_test.h): introspection + invariant checks +
# crash/reclaim hooks the diskfs MBT harness embeds.  It needs the
# private internal structures, so it lives with the module rather than in a
# test directory; its symbols are test-only side doors, prefixed diskfs_test_.
# It exists only for the harness, so keep it (and the tests) out of a
# DISABLE_TESTS=ON build -- the production/Docker image ships no test code.
if (NOT DISABLE_TESTS)
    target_sources(chimera_vfs_diskfs_objs PRIVATE diskfs_test.c)
endif()

set_target_properties(chimera_vfs_diskfs_objs PROPERTIES POSITION_INDEPENDENT_CODE ON)

target_link_libraries(chimera_vfs_diskfs_objs PUBLIC evpl jansson)

target_compile_definitions(chimera_vfs_diskfs_objs PRIVATE
    XXH_INLINE_ALL
    XXH_VECTOR=${CHIMERA_XXH_VECTOR}
)

if(CHIMERA_LINUX AND VFIO_INCLUDE_DIR)
    target_compile_definitions(chimera_vfs_diskfs_objs PRIVATE CHIMERA_HAVE_VFIO=1)
endif()

add_library(chimera_vfs_diskfs ${CHIMERA_VFS_MODULE_TYPE}
    $<TARGET_OBJECTS:chimera_vfs_diskfs_objs>
)

target_link_libraries(chimera_vfs_diskfs evpl jansson)

chimera_vfs_module(chimera_vfs_diskfs)

if (NOT DISABLE_TESTS)
    add_subdirectory(tests)
endif()
