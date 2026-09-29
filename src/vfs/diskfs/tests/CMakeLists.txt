# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# Model-based / quint-tier tests: the quick tier a plain `ctest` runs.
set(CHIMERA_TEST_TIER quick)

# White-box diskfs tests.  They embed diskfs (for the diskfs_test.h SDK
# compiled into it) and drive it directly through the VFS on a pread-backed
# device file; the memkv pool KV backend the mount needs is loaded at runtime.
# ENABLE_EXPORTS lets chimera_vfs_init find the embedded vfs_diskfs, so the
# VFS and the SDK share one copy of diskfs rather than also loading the module.
#
# The include of the parent directory is for diskfs_test.h (which lives with the
# module because it needs the private internal structures).

# Ground-truth smoke test: exercises the SDK, the space-allocator invariants,
# directory-tree growth, fragmenting churn + reclaim, and crash recovery.  Needs
# no trace corpus, so it always runs.
add_executable(diskfs_smoke_test diskfs_smoke_test.c)
target_include_directories(diskfs_smoke_test PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_link_libraries(diskfs_smoke_test
    chimera_vfs chimera_vfs_diskfs_objs evpl)
set_target_properties(diskfs_smoke_test PROPERTIES ENABLE_EXPORTS ON)
add_test(NAME chimera/vfs/diskfs/smoke_test COMMAND diskfs_smoke_test)
set_tests_properties(chimera/vfs/diskfs/smoke_test PROPERTIES
    LABELS "quint;diskfs_mbt"
    TIMEOUT 300)

# ENOSPC-boundary test: mounts the bounded pool the kvm nfstest_alloc wrapper
# uses and runs that suite's alloc06 case in-process, so the allocator's
# out-of-space failure path is covered by the quick tier.
add_executable(diskfs_enospc_test diskfs_enospc_test.c)
target_include_directories(diskfs_enospc_test PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_link_libraries(diskfs_enospc_test
    chimera_vfs chimera_vfs_diskfs_objs evpl)
set_target_properties(diskfs_enospc_test PROPERTIES ENABLE_EXPORTS ON)
add_test(NAME chimera/vfs/diskfs/enospc_test COMMAND diskfs_enospc_test)
set_tests_properties(chimera/vfs/diskfs/enospc_test PROPERTIES
    LABELS "quint;diskfs_mbt"
    TIMEOUT 300)

# The quint diskfs stress model + replay harness (generated corpus).
add_subdirectory(quint)

# Drive the production allocator without a journal worker, so pending-space
# accounting and concurrent admission are deterministic rather than timing-dependent.
add_executable(space_map_budget_test space_map_budget_test.c ../space_map.c)
target_include_directories(space_map_budget_test PRIVATE
    ${CMAKE_SOURCE_DIR}/src ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_link_libraries(space_map_budget_test chimera_common)
add_test(NAME chimera/vfs/diskfs/space_budget_test COMMAND space_map_budget_test)
set_tests_properties(chimera/vfs/diskfs/space_budget_test PROPERTIES
    LABELS "quint;diskfs_mbt" TIMEOUT 30)
