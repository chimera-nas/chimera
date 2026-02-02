# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_library(chimera_vfs_linux SHARED
    linux.c
)

target_link_libraries(chimera_vfs_linux jansson)

install(TARGETS chimera_vfs_linux DESTINATION lib)