# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_library(chimera_vfs_smb SHARED
    smb.c
    smb_mount.c
    smb_umount.c
    smb_ops.c
    smb_io.c
    smb_namespace.c
    smb_ntlm.c
)

target_link_libraries(chimera_vfs_smb chimera_common evpl crypto)

install(TARGETS chimera_vfs_smb DESTINATION lib)

if (NOT DISABLE_TESTS)
    add_subdirectory(tests)
endif()
