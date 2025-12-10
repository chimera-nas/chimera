# SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_library(chimera_vfs_root SHARED
    vfs_root.c
)

install(TARGETS chimera_vfs_root DESTINATION lib)