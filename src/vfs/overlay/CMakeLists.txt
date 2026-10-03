# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_library(chimera_vfs_overlay ${CHIMERA_VFS_MODULE_TYPE}
    overlay.c
)

target_link_libraries(chimera_vfs_overlay evpl xxhash)

chimera_vfs_module(chimera_vfs_overlay)
