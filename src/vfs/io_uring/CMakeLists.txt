# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_library(chimera_vfs_io_uring SHARED
    io_uring.c
)

target_link_libraries(chimera_vfs_io_uring uring jansson)

install(TARGETS chimera_vfs_io_uring DESTINATION lib)