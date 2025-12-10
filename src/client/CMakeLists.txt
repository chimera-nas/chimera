# SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_library(chimera_client SHARED client.c 
            client_mount.c client_umount.c
            client_open.c client_mkdir.c client_close.c client_read.c client_write.c
            client_symlink.c client_link.c client_remove.c client_rename.c
            client_readlink.c client_stat.c)
target_link_libraries(chimera_client chimera_vfs evpl prometheus-c)

install(TARGETS chimera_client DESTINATION lib)

if (NOT DISABLE_TESTS)
    add_subdirectory(tests)
endif()