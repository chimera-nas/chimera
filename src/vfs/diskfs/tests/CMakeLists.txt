# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# Model-based / quint-tier tests: the quick tier a plain `ctest` runs.
set(CHIMERA_TEST_TIER quick)

# White-box diskfs tests.  They link the diskfs module (for the diskfs_test.h
# SDK exported from it) plus memkv (the pool KV backend the mount needs) and
# drive diskfs directly through the VFS on a pread-backed device file.
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
    chimera_vfs chimera_vfs_diskfs chimera_vfs_memkv evpl)
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
    chimera_vfs chimera_vfs_diskfs chimera_vfs_memkv evpl)
add_test(NAME chimera/vfs/diskfs/enospc_test COMMAND diskfs_enospc_test)
set_tests_properties(chimera/vfs/diskfs/enospc_test PROPERTIES
    LABELS "quint;diskfs_mbt"
    TIMEOUT 300)

# The quint diskfs stress model + replay harness (generated corpus).
add_subdirectory(quint)
