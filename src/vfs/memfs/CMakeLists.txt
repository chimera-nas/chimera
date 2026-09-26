# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_library(chimera_vfs_memfs SHARED
    memfs.c
)

target_compile_definitions(chimera_vfs_memfs PRIVATE
    XXH_INLINE_ALL
    XXH_VECTOR=${CHIMERA_XXH_VECTOR}
)

target_link_libraries(chimera_vfs_memfs jansson)

install(TARGETS chimera_vfs_memfs DESTINATION lib)

if (NOT DISABLE_TESTS)
    add_executable(memfs_rename_lock_test memfs_rename_lock_test.c)
    target_compile_definitions(memfs_rename_lock_test PRIVATE
        XXH_INLINE_ALL XXH_VECTOR=${CHIMERA_XXH_VECTOR})
    target_compile_options(memfs_rename_lock_test PRIVATE -ffunction-sections -fdata-sections)
    target_link_options(memfs_rename_lock_test PRIVATE -Wl,--gc-sections)
    target_link_libraries(memfs_rename_lock_test chimera_vfs chimera_common evpl jansson pthread)
    add_test(NAME chimera/vfs/memfs_rename_lock COMMAND memfs_rename_lock_test)
    set_tests_properties(chimera/vfs/memfs_rename_lock PROPERTIES TIMEOUT 10)
endif()
