# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_library(chimera_vfs_memkv SHARED
    memkv.c
)

target_compile_definitions(chimera_vfs_memkv PRIVATE
    XXH_INLINE_ALL
    XXH_VECTOR=${CHIMERA_XXH_VECTOR}
)

target_link_libraries(chimera_vfs_memkv jansson)

install(TARGETS chimera_vfs_memkv DESTINATION lib)
