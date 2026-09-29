# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# Compiled once as an object library: the loadable module is built from it,
# and the white-box sizing/diagnostics tests embed it to drive vfs_io_uring
# directly.
add_library(chimera_vfs_io_uring_objs OBJECT
    io_uring.c
)

set_target_properties(chimera_vfs_io_uring_objs PROPERTIES POSITION_INDEPENDENT_CODE ON)

target_link_libraries(chimera_vfs_io_uring_objs PUBLIC evpl uring jansson)

add_library(chimera_vfs_io_uring ${CHIMERA_VFS_MODULE_TYPE}
    $<TARGET_OBJECTS:chimera_vfs_io_uring_objs>
)

target_link_libraries(chimera_vfs_io_uring evpl uring jansson)

chimera_vfs_module(chimera_vfs_io_uring)
