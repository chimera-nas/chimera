# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_library(chimera_vfs_sqlite SHARED
    sqlite.c
)

target_include_directories(chimera_vfs_sqlite PRIVATE ${SQLITE3_INCLUDE_DIR})

target_link_libraries(chimera_vfs_sqlite jansson ${SQLITE3_LIBRARY})

install(TARGETS chimera_vfs_sqlite DESTINATION lib)
