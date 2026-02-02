# SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_executable(vfs_user_cache_test vfs_user_cache_test.c)
target_link_libraries(vfs_user_cache_test chimera_vfs urcu)
add_test(chimera/vfs/user_cache_test vfs_user_cache_test)
