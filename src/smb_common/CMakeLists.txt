# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# Shared SMB2 foundation, in the same position src/nfs_common holds for NFS:
# the wire definitions plus the cryptography and codecs whose output the server
# (src/server/smb) and the client VFS module (src/vfs/smb) must agree on byte
# for byte.  Both link this; neither links the other.
add_library(chimera_smb_common SHARED
    smb_signing.c
    smb_encrypt.c
    smb_compress.c
)

target_include_directories(chimera_smb_common PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(chimera_smb_common chimera_common evpl crypto)

install(TARGETS chimera_smb_common DESTINATION lib)
