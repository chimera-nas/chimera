# SPDX-FileCopyrightText: 2025 Ben Jarvis
#
# SPDX-License-Identifier: LGPL-2.1-only

add_library(chimera_vfs_linux SHARED
    linux.c
)

install(TARGETS chimera_vfs_linux DESTINATION lib)