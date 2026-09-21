# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_library(chimera_vfs_root ${CHIMERA_LIBRARY_TYPE}
    vfs_root.c
)

target_link_libraries(chimera_vfs_root evpl)

install(TARGETS chimera_vfs_root DESTINATION lib)