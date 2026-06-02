# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_executable(vfs_user_cache_test vfs_user_cache_test.c)
target_link_libraries(vfs_user_cache_test chimera_vfs urcu-qsbr)
add_test(chimera/vfs/user_cache_test vfs_user_cache_test)

add_executable(vfs_kv_test vfs_kv_test.c)
target_link_libraries(vfs_kv_test chimera_vfs chimera_vfs_memfs evpl)
add_test(chimera/vfs/kv_test vfs_kv_test)

add_executable(vfs_async_delegation_test vfs_async_delegation_test.c)
target_link_libraries(vfs_async_delegation_test chimera_vfs chimera_vfs_memfs evpl)
add_test(chimera/vfs/async_delegation_test vfs_async_delegation_test)

if(CAIRN_ENABLED)
	add_executable(vfs_sync_delegation_test vfs_sync_delegation_test.c)
	target_link_libraries(vfs_sync_delegation_test chimera_vfs chimera_vfs_cairn evpl)
	add_test(chimera/vfs/sync_delegation_test vfs_sync_delegation_test)
endif()

add_executable(vfs_notify_test vfs_notify_test.c)
target_link_libraries(vfs_notify_test chimera_vfs urcu-qsbr)
add_test(chimera/vfs/notify_test vfs_notify_test)

add_executable(vfs_state_test vfs_state_test.c)
target_link_libraries(vfs_state_test chimera_vfs)
add_test(chimera/vfs/state_test vfs_state_test)

add_executable(vfs_acl_test vfs_acl_test.c)
target_link_libraries(vfs_acl_test chimera_vfs)
add_test(chimera/vfs/acl_test vfs_acl_test)

add_executable(vfs_enforce_test vfs_enforce_test.c)
target_link_libraries(vfs_enforce_test chimera_vfs chimera_vfs_memfs evpl)
add_test(chimera/vfs/enforce_test vfs_enforce_test)

add_executable(vfs_idmap_test vfs_idmap_test.c)
target_link_libraries(vfs_idmap_test chimera_vfs)
add_test(chimera/vfs/idmap_test vfs_idmap_test)

add_executable(vfs_identity_test vfs_identity_test.c)
target_link_libraries(vfs_identity_test chimera_vfs chimera_vfs_memfs evpl urcu-qsbr)
add_test(chimera/vfs/identity_test vfs_identity_test)
