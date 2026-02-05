# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_library(chimera_server SHARED server.c)
target_link_libraries(chimera_server chimera_nfs chimera_s3 chimera_smb chimera_rest chimera_vfs evpl unwind)

install(TARGETS chimera_server DESTINATION lib)

add_subdirectory(nfs)

add_subdirectory(s3)

add_subdirectory(smb)

add_subdirectory(rest)
