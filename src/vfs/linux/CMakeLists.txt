# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_library(chimera_vfs_linux ${CHIMERA_VFS_MODULE_TYPE}
    linux.c
)

target_link_libraries(chimera_vfs_linux evpl jansson)

chimera_vfs_module(chimera_vfs_linux)