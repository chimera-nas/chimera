# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# Bounds test for the proxy's file-handle re-encoders.  Both helpers are static
# inline, so nothing needs linking, but the generated NFS XDR headers have to
# exist first.
add_executable(nfs_fh_bounds_test nfs_fh_bounds_test.c)
target_include_directories(nfs_fh_bounds_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_SOURCE_DIR}/src)
add_dependencies(nfs_fh_bounds_test chimera_nfs_common)
add_test(chimera/vfs/nfs/fh_bounds_test nfs_fh_bounds_test)
