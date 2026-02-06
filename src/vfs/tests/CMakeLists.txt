# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_executable(vfs_user_cache_test vfs_user_cache_test.c)
target_link_libraries(vfs_user_cache_test chimera_vfs urcu)
add_test(chimera/vfs/user_cache_test vfs_user_cache_test)

add_executable(vfs_kv_test vfs_kv_test.c)
target_link_libraries(vfs_kv_test chimera_vfs chimera_vfs_memfs evpl)
add_test(chimera/vfs/kv_test vfs_kv_test)
