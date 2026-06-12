# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_executable(nfs3_fh_test nfs3_fh_test.c)
# Header-only logic (chimera_nfs3_unmarshall_fh + chimera_vfs_encode_fh_parent
# are static inline), but it needs the generated nfs3_xdr.h for struct nfs_fh3.
target_include_directories(nfs3_fh_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_BINARY_DIR}/..)
add_dependencies(nfs3_fh_test chimera_nfs_common)
add_test(chimera/nfs_common/fh_test nfs3_fh_test)
