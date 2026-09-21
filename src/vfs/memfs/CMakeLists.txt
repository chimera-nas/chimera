# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_library(chimera_vfs_memfs ${CHIMERA_LIBRARY_TYPE}
    memfs.c
)

target_compile_definitions(chimera_vfs_memfs PRIVATE
    XXH_INLINE_ALL
    XXH_VECTOR=${CHIMERA_XXH_VECTOR}
)

target_link_libraries(chimera_vfs_memfs evpl jansson)

install(TARGETS chimera_vfs_memfs DESTINATION lib)
