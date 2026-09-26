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

if (NOT DISABLE_TESTS AND CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_executable(memfs_rename_lock_test memfs_rename_lock_test.c)
    target_compile_definitions(memfs_rename_lock_test PRIVATE
        XXH_INLINE_ALL XXH_VECTOR=${CHIMERA_XXH_VECTOR})
    target_compile_options(memfs_rename_lock_test PRIVATE -ffunction-sections -fdata-sections)
    target_link_options(memfs_rename_lock_test PRIVATE -Wl,--gc-sections)
    target_link_libraries(memfs_rename_lock_test chimera_vfs chimera_common evpl jansson pthread)
    add_test(NAME chimera/vfs/memfs_rename_lock COMMAND memfs_rename_lock_test)
    set_tests_properties(chimera/vfs/memfs_rename_lock PROPERTIES TIMEOUT 10)
endif()
