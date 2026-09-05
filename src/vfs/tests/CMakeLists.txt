# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_executable(vfs_user_cache_test vfs_user_cache_test.c)
target_link_libraries(vfs_user_cache_test chimera_vfs)
add_test(chimera/vfs/user_cache_test vfs_user_cache_test)

add_executable(vfs_kv_test vfs_kv_test.c)
target_link_libraries(vfs_kv_test chimera_vfs chimera_vfs_memkv evpl)
if (SQLITE_ENABLED)
    target_link_libraries(vfs_kv_test chimera_vfs_sqlite)
    target_compile_definitions(vfs_kv_test PRIVATE CHIMERA_KV_TEST_SQLITE=1)
endif()
if (CAIRN_ENABLED)
    target_link_libraries(vfs_kv_test chimera_vfs_cairn)
    target_compile_definitions(vfs_kv_test PRIVATE CHIMERA_KV_TEST_CAIRN=1)
endif()
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
target_link_libraries(vfs_notify_test chimera_vfs)
add_test(chimera/vfs/notify_test vfs_notify_test)

add_executable(vfs_claim_test vfs_claim_test.c)
target_link_libraries(vfs_claim_test chimera_vfs)
add_test(chimera/vfs/claim_test vfs_claim_test)

add_executable(vfs_acl_test vfs_acl_test.c)
target_link_libraries(vfs_acl_test chimera_vfs)
add_test(chimera/vfs/acl_test vfs_acl_test)

add_executable(vfs_enforce_test vfs_enforce_test.c)
target_link_libraries(vfs_enforce_test chimera_vfs chimera_vfs_memfs evpl)
add_test(chimera/vfs/enforce_test vfs_enforce_test)

add_executable(vfs_clone_range_test vfs_clone_range_test.c)
target_link_libraries(vfs_clone_range_test chimera_vfs chimera_vfs_memfs evpl)
add_test(chimera/vfs/clone_range_test vfs_clone_range_test)

add_executable(vfs_inode_scoped_remove_test vfs_inode_scoped_remove_test.c)
target_link_libraries(vfs_inode_scoped_remove_test chimera_vfs chimera_vfs_memfs evpl)
add_test(chimera/vfs/inode_scoped_remove_test vfs_inode_scoped_remove_test)

add_executable(vfs_idmap_test vfs_idmap_test.c)
target_link_libraries(vfs_idmap_test chimera_vfs)
add_test(chimera/vfs/idmap_test vfs_idmap_test)

add_executable(vfs_sid_test vfs_sid_test.c)
target_link_libraries(vfs_sid_test chimera_vfs)
add_test(chimera/vfs/sid_test vfs_sid_test)

add_executable(vfs_identity_test vfs_identity_test.c)
target_link_libraries(vfs_identity_test chimera_vfs chimera_vfs_memfs evpl)
add_test(chimera/vfs/identity_test vfs_identity_test)

add_executable(vfs_statfs_mask_test vfs_statfs_mask_test.c)
target_link_libraries(vfs_statfs_mask_test chimera_vfs chimera_vfs_memfs chimera_vfs_memkv evpl)
add_test(chimera/vfs/statfs_mask_test vfs_statfs_mask_test)

add_executable(vfs_umount_wait_test vfs_umount_wait_test.c)
target_link_libraries(vfs_umount_wait_test chimera_vfs chimera_vfs_memfs chimera_vfs_memkv evpl)
add_test(chimera/vfs/umount_wait_test vfs_umount_wait_test)

# zero_range reproducer / regression: VFS zero_range (DEALLOCATE+ALLOCATE) data
# integrity, parameterized by backend.  memfs/linux confirm the invariant holds
# (and that the model checker is sound); the diskfs cases reproduce the bug.
add_executable(vfs_zerorange_test vfs_zerorange_test.c)
target_link_libraries(vfs_zerorange_test chimera_vfs chimera_vfs_memfs
    chimera_vfs_diskfs chimera_vfs_memkv evpl)
if(CHIMERA_LINUX)
    target_link_libraries(vfs_zerorange_test chimera_vfs_linux)
endif()
# Engine-managed backends only: deterministic and host-fs-independent.  memfs
# and cairn confirm the invariant holds; the diskfs cases reproduce the bug.
# The linux / io_uring passthrough backends are also valid CLI args for manual
# runs (their libs are linked), but they are not registered as tests -- their
# fallocate behavior is the host kernel's and is unreliable under the
# container / CI filesystems.
add_test(NAME chimera/vfs/zerorange_memfs COMMAND vfs_zerorange_test memfs)
# The diskfs_aio case drives libevpl's libaio block backend, which only exists
# where libaio does (Linux); elsewhere opening the device fails.  Gate it the
# same way the posix suite gates its diskfs_aio variants.
if(HAVE_LIBAIO)
    add_test(NAME chimera/vfs/zerorange_diskfs_aio COMMAND vfs_zerorange_test diskfs_aio)
endif()
if(CHIMERA_VFS_IO_URING)
    target_link_libraries(vfs_zerorange_test chimera_vfs_io_uring)
    add_test(NAME chimera/vfs/zerorange_diskfs_io_uring COMMAND vfs_zerorange_test diskfs_io_uring)
endif()
if(CAIRN_ENABLED)
    target_link_libraries(vfs_zerorange_test chimera_vfs_cairn)
    add_test(NAME chimera/vfs/zerorange_cairn COMMAND vfs_zerorange_test cairn)
endif()

# xattr DAC regression, parameterized by passthrough backend: the linux and
# io_uring modules must issue f*xattr(2) under the caller's credential, since
# the kernel evaluates user.* xattr permission against the calling thread's
# fsuid and their cached open_by_handle_at descriptor was opened privileged.
# Root only (setfsuid), and the backend must be able to serve the scratch
# directory (name_to_handle_at fails on overlayfs); the test exits 77 otherwise.
add_executable(vfs_xattr_dac_test vfs_xattr_dac_test.c)
target_link_libraries(vfs_xattr_dac_test chimera_vfs chimera_vfs_memkv evpl)
if(CHIMERA_LINUX)
    target_link_libraries(vfs_xattr_dac_test chimera_vfs_linux)
    add_test(NAME chimera/vfs/xattr_dac_linux COMMAND vfs_xattr_dac_test linux)
    set_tests_properties(chimera/vfs/xattr_dac_linux PROPERTIES SKIP_RETURN_CODE 77)
endif()
if(CHIMERA_VFS_IO_URING)
    target_link_libraries(vfs_xattr_dac_test chimera_vfs_io_uring)
    add_test(NAME chimera/vfs/xattr_dac_io_uring COMMAND vfs_xattr_dac_test io_uring)
    set_tests_properties(chimera/vfs/xattr_dac_io_uring PROPERTIES SKIP_RETURN_CODE 77)
endif()

# Native Windows SID round trip through the VFS, parameterized by
# backend: only the engine-managed backends that store the canonical ACL
# natively (memfs, cairn, diskfs) can persist a SID-bearing principal and the
# owner/group SID companions.  The passthrough backends are mode-only.
add_executable(vfs_acl_sid_test vfs_acl_sid_test.c)
target_link_libraries(vfs_acl_sid_test chimera_vfs chimera_vfs_memfs
    chimera_vfs_diskfs chimera_vfs_memkv evpl)
add_test(NAME chimera/vfs/acl_sid_memfs COMMAND vfs_acl_sid_test memfs)
if(HAVE_LIBAIO)
    add_test(NAME chimera/vfs/acl_sid_diskfs_aio COMMAND vfs_acl_sid_test diskfs_aio)
endif()
if(CHIMERA_VFS_IO_URING)
    target_link_libraries(vfs_acl_sid_test chimera_vfs_io_uring)
    add_test(NAME chimera/vfs/acl_sid_diskfs_io_uring COMMAND vfs_acl_sid_test diskfs_io_uring)
endif()
if(CAIRN_ENABLED)
    target_link_libraries(vfs_acl_sid_test chimera_vfs_cairn)
    add_test(NAME chimera/vfs/acl_sid_cairn COMMAND vfs_acl_sid_test cairn)
endif()

# Tag every test registered above with the 'vfs' label so the suite can be run
# independently via `ctest -L vfs`.
get_property(_vfs_tests DIRECTORY PROPERTY TESTS)
if(_vfs_tests)
    set_property(TEST ${_vfs_tests} APPEND PROPERTY LABELS vfs)
endif()
