# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_library(chimera_identity_nss ${CHIMERA_LIBRARY_TYPE}
    nss.c
)

target_link_libraries(chimera_identity_nss evpl)

install(TARGETS chimera_identity_nss DESTINATION lib)
