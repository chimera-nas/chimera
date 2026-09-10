# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_executable(vfs_user_cache_test vfs_user_cache_test.c)
target_link_libraries(vfs_user_cache_test chimera_vfs)
add_test(chimera/vfs/user_cache_test vfs_user_cache_test)

add_executable(vfs_clock_test vfs_clock_test.c)
target_link_libraries(vfs_clock_test chimera_vfs)
add_test(chimera/vfs/clock_test vfs_clock_test)

if(CHIMERA_VFS_IO_URING AND CHIMERA_LIMITS_TESTING)
    add_executable(vfs_io_uring_sizing_test vfs_io_uring_sizing_test.c)
    target_link_libraries(vfs_io_uring_sizing_test
        chimera_vfs_io_uring_objs chimera_vfs evpl uring ${CMAKE_DL_LIBS})
    # Resource sizing is outside the protocol models. Keep this initialization
    # probe in the quick tier so an ignored size override is caught early.
    set(_sizing_saved_tier "${CHIMERA_TEST_TIER}")
    set(CHIMERA_TEST_TIER quick)
    add_test(NAME chimera/vfs/io_uring_sizing
        COMMAND ${CMAKE_SOURCE_DIR}/scripts/test_limits_wrapper.sh
                $<TARGET_FILE:vfs_io_uring_sizing_test>)
    add_executable(vfs_io_uring_diagnostics_test vfs_io_uring_diagnostics_test.c)
    target_link_libraries(vfs_io_uring_diagnostics_test
        chimera_vfs_io_uring_objs chimera_vfs evpl uring ${CMAKE_DL_LIBS})
    add_test(NAME chimera/vfs/io_uring_diagnostics
        COMMAND ${CMAKE_SOURCE_DIR}/scripts/test_limits_wrapper.sh
                $<TARGET_FILE:vfs_io_uring_diagnostics_test>)
    set(CHIMERA_TEST_TIER "${_sizing_saved_tier}")
endif()

add_executable(vfs_kv_test vfs_kv_test.c)
target_link_libraries(vfs_kv_test chimera_vfs evpl)
if (SQLITE_ENABLED)
    target_compile_definitions(vfs_kv_test PRIVATE CHIMERA_KV_TEST_SQLITE=1)
endif()
if (CAIRN_ENABLED)
    target_compile_definitions(vfs_kv_test PRIVATE CHIMERA_KV_TEST_CAIRN=1)
endif()
add_test(chimera/vfs/kv_test vfs_kv_test)

add_executable(vfs_async_delegation_test vfs_async_delegation_test.c)
target_link_libraries(vfs_async_delegation_test chimera_vfs evpl)
add_test(chimera/vfs/async_delegation_test vfs_async_delegation_test)

if(CAIRN_ENABLED)
	add_executable(vfs_sync_delegation_test vfs_sync_delegation_test.c)
	target_link_libraries(vfs_sync_delegation_test chimera_vfs evpl)
	add_test(chimera/vfs/sync_delegation_test vfs_sync_delegation_test)

    # The sequential protocol models cannot force a stale inode read to race
    # with a rename. Keep this deterministic backend interleaving in quick CI,
    # including the non-pinned compatibility path used by older RocksDB.
    set(_cairn_saved_tier "${CHIMERA_TEST_TIER}")
    set(CHIMERA_TEST_TIER quick)
    foreach(variant native compat)
        add_executable(vfs_cairn_conflict_${variant}_test vfs_cairn_conflict_test.c)
        target_link_libraries(vfs_cairn_conflict_${variant}_test
            chimera_vfs evpl jansson rocksdb)
        target_compile_definitions(vfs_cairn_conflict_${variant}_test PRIVATE
            $<TARGET_PROPERTY:chimera_vfs_cairn,COMPILE_DEFINITIONS>)
        if(variant STREQUAL "compat")
            target_compile_definitions(vfs_cairn_conflict_${variant}_test PRIVATE
                ROCKSDB_LEGACY_COMPAT=1)
        endif()
        add_test(NAME chimera/vfs/cairn_conflict_${variant}
            COMMAND vfs_cairn_conflict_${variant}_test)
        set_tests_properties(chimera/vfs/cairn_conflict_${variant} PROPERTIES TIMEOUT 30)
    endforeach()
    add_executable(vfs_cairn_reply_attrs_test vfs_cairn_reply_attrs_test.c)
    target_link_libraries(vfs_cairn_reply_attrs_test chimera_vfs evpl jansson rocksdb)
    target_compile_definitions(vfs_cairn_reply_attrs_test PRIVATE
        $<TARGET_PROPERTY:chimera_vfs_cairn,COMPILE_DEFINITIONS>)
    add_test(NAME chimera/vfs/cairn_reply_attrs COMMAND vfs_cairn_reply_attrs_test)
    set_tests_properties(chimera/vfs/cairn_reply_attrs PROPERTIES TIMEOUT 30)
    set(CHIMERA_TEST_TIER "${_cairn_saved_tier}")
endif()

add_executable(vfs_notify_test vfs_notify_test.c)
target_link_libraries(vfs_notify_test chimera_vfs)
add_test(chimera/vfs/notify_test vfs_notify_test)

add_executable(vfs_claim_test vfs_claim_test.c)
target_link_libraries(vfs_claim_test chimera_vfs)
# Claim waiter races are not represented by sequential protocol traces.
set(_claim_saved_tier "${CHIMERA_TEST_TIER}")
set(CHIMERA_TEST_TIER quick)
add_test(chimera/vfs/claim_test vfs_claim_test)
set_tests_properties(chimera/vfs/claim_test PROPERTIES TIMEOUT 30)
set(CHIMERA_TEST_TIER "${_claim_saved_tier}")

add_executable(vfs_acl_test vfs_acl_test.c)
target_link_libraries(vfs_acl_test chimera_vfs)
add_test(chimera/vfs/acl_test vfs_acl_test)

add_executable(vfs_enforce_test vfs_enforce_test.c)
target_link_libraries(vfs_enforce_test chimera_vfs evpl)
add_test(chimera/vfs/enforce_test vfs_enforce_test)

# The attrstrip shim module lives in the test binary (wrapping an embedded
# memfs) and is found by chimera_vfs_init in the executable's own exports
# (dlsym(RTLD_DEFAULT), or GetProcAddress on the executable on Windows),
# hence ENABLE_EXPORTS.
add_executable(vfs_attr_gate_test vfs_attr_gate_test.c)
target_link_libraries(vfs_attr_gate_test chimera_vfs chimera_vfs_memfs_objs evpl)
set_target_properties(vfs_attr_gate_test PROPERTIES ENABLE_EXPORTS ON)
add_test(chimera/vfs/attr_gate_test vfs_attr_gate_test)

add_executable(vfs_clone_range_test vfs_clone_range_test.c)
target_link_libraries(vfs_clone_range_test chimera_vfs evpl)
add_test(chimera/vfs/clone_range_test vfs_clone_range_test)

add_executable(vfs_inode_scoped_remove_test vfs_inode_scoped_remove_test.c)
target_link_libraries(vfs_inode_scoped_remove_test chimera_vfs evpl)
add_test(chimera/vfs/inode_scoped_remove_test vfs_inode_scoped_remove_test)

add_executable(vfs_idmap_test vfs_idmap_test.c)
target_link_libraries(vfs_idmap_test chimera_vfs)
add_test(chimera/vfs/idmap_test vfs_idmap_test)

add_executable(vfs_sid_test vfs_sid_test.c)
target_link_libraries(vfs_sid_test chimera_vfs)
add_test(chimera/vfs/sid_test vfs_sid_test)

add_executable(vfs_identity_test vfs_identity_test.c)
target_link_libraries(vfs_identity_test chimera_vfs evpl)
add_test(chimera/vfs/identity_test vfs_identity_test)

add_executable(vfs_statfs_mask_test vfs_statfs_mask_test.c)
target_link_libraries(vfs_statfs_mask_test chimera_vfs evpl)
add_test(chimera/vfs/statfs_mask_test vfs_statfs_mask_test)

# Mount points (nested paths through the built-in rootfs, a filesystem at
# "/", mounts inside mounts) are not modelled by the protocol traces, so keep
# this in the quick tier.
add_executable(vfs_mount_tree_test vfs_mount_tree_test.c)
target_link_libraries(vfs_mount_tree_test chimera_vfs evpl)
set(_mount_tree_saved_tier "${CHIMERA_TEST_TIER}")
set(CHIMERA_TEST_TIER quick)
add_test(chimera/vfs/mount_tree_test vfs_mount_tree_test)
set_tests_properties(chimera/vfs/mount_tree_test PROPERTIES TIMEOUT 60)
set(CHIMERA_TEST_TIER "${_mount_tree_saved_tier}")

add_executable(vfs_umount_wait_test vfs_umount_wait_test.c)
target_link_libraries(vfs_umount_wait_test chimera_vfs evpl)
add_test(chimera/vfs/umount_wait_test vfs_umount_wait_test)

# zero_range reproducer / regression: VFS zero_range (DEALLOCATE+ALLOCATE) data
# integrity, parameterized by backend.  memfs/linux confirm the invariant holds
# (and that the model checker is sound); the diskfs cases reproduce the bug.
# The VFS compound executor: sequence chaining, one callback, and the
# stop-at-first-failure rule.  memfs only -- what is under test is the
# executor, not any backend.
add_executable(vfs_compound_test vfs_compound_test.c)
target_link_libraries(vfs_compound_test chimera_vfs chimera_vfs_memfs
    chimera_vfs_memkv evpl)
add_test(NAME chimera/vfs/compound COMMAND vfs_compound_test)
set_tests_properties(chimera/vfs/compound PROPERTIES LABELS "vfs" TIMEOUT 60)

add_executable(vfs_zerorange_test vfs_zerorange_test.c)
target_link_libraries(vfs_zerorange_test chimera_vfs evpl)
# Engine-managed backends only: deterministic and host-fs-independent.  memfs
# and cairn confirm the invariant holds; the diskfs cases reproduce the bug.
# The linux / io_uring passthrough backends are also valid CLI args for manual
# runs (their modules are built), but they are not registered as tests -- their
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
    add_test(NAME chimera/vfs/zerorange_diskfs_io_uring COMMAND vfs_zerorange_test diskfs_io_uring)
endif()
if(CAIRN_ENABLED)
    add_test(NAME chimera/vfs/zerorange_cairn COMMAND vfs_zerorange_test cairn)
endif()

# xattr DAC regression, parameterized by passthrough backend: the linux and
# io_uring modules must issue f*xattr(2) under the caller's credential, since
# the kernel evaluates user.* xattr permission against the calling thread's
# fsuid and their cached open_by_handle_at descriptor was opened privileged.
# Root only (setfsuid), and the backend must be able to serve the scratch
# directory (name_to_handle_at fails on overlayfs); the test exits 77 otherwise.
if(CHIMERA_LINUX)
    add_executable(vfs_xattr_dac_test vfs_xattr_dac_test.c)
    target_link_libraries(vfs_xattr_dac_test chimera_vfs evpl)
    add_test(NAME chimera/vfs/xattr_dac_linux COMMAND vfs_xattr_dac_test linux)
    set_tests_properties(chimera/vfs/xattr_dac_linux PROPERTIES SKIP_RETURN_CODE 77)
    if(CHIMERA_VFS_IO_URING)
        add_test(NAME chimera/vfs/xattr_dac_io_uring COMMAND vfs_xattr_dac_test io_uring)
        set_tests_properties(chimera/vfs/xattr_dac_io_uring PROPERTIES SKIP_RETURN_CODE 77)
    endif()
endif()

# Native Windows SID round trip through the VFS, parameterized by
# backend: only the engine-managed backends that store the canonical ACL
# natively (memfs, cairn, diskfs) can persist a SID-bearing principal and the
# owner/group SID companions.  The passthrough backends are mode-only.
add_executable(vfs_acl_sid_test vfs_acl_sid_test.c)
target_link_libraries(vfs_acl_sid_test chimera_vfs evpl)
add_test(NAME chimera/vfs/acl_sid_memfs COMMAND vfs_acl_sid_test memfs)
if(HAVE_LIBAIO)
    add_test(NAME chimera/vfs/acl_sid_diskfs_aio COMMAND vfs_acl_sid_test diskfs_aio)
endif()
if(CHIMERA_VFS_IO_URING)
    add_test(NAME chimera/vfs/acl_sid_diskfs_io_uring COMMAND vfs_acl_sid_test diskfs_io_uring)
endif()
if(CAIRN_ENABLED)
    add_test(NAME chimera/vfs/acl_sid_cairn COMMAND vfs_acl_sid_test cairn)
endif()

# The stored ACL, not the mode, decides a non-root open on every open route,
# parameterized by the backends that store the canonical ACL natively.
add_executable(vfs_open_acl_test vfs_open_acl_test.c)
target_link_libraries(vfs_open_acl_test chimera_vfs evpl)
add_test(NAME chimera/vfs/open_acl_memfs COMMAND vfs_open_acl_test memfs)
if(HAVE_LIBAIO)
    add_test(NAME chimera/vfs/open_acl_diskfs_aio COMMAND vfs_open_acl_test diskfs_aio)
endif()
if(CHIMERA_VFS_IO_URING)
    add_test(NAME chimera/vfs/open_acl_diskfs_io_uring COMMAND vfs_open_acl_test diskfs_io_uring)
endif()
if(CAIRN_ENABLED)
    add_test(NAME chimera/vfs/open_acl_cairn COMMAND vfs_open_acl_test cairn)
endif()

# Tag every test registered above with the 'vfs' label so the suite can be run
# independently via `ctest -L vfs`.
get_property(_vfs_tests DIRECTORY PROPERTY TESTS)
if(_vfs_tests)
    set_property(TEST ${_vfs_tests} APPEND PROPERTY LABELS vfs)
endif()
