# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_library(chimera_vfs_daos ${CHIMERA_VFS_MODULE_TYPE}
    vfs_daos.c
    vfs_daos_fh.c
    vfs_daos_ops.c
    vfs_daos_registry.c
    vfs_daos_config.c
    vfs_daos_errmap.c
)

target_include_directories(chimera_vfs_daos PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/ext/libevpl/include
    ${CMAKE_SOURCE_DIR}/ext/libevpl/ext/prometheus-c
    ${CMAKE_SOURCE_DIR}/ext/libevpl/ext/prometheus-c/ext/stopwatch
    ${CMAKE_SOURCE_DIR}/ext/libevpl/ext/oteltracing-c
)
# DAOS headers warn under -Werror.  Treat them as system headers so those
# diagnostics stay out of this module's build.
target_include_directories(chimera_vfs_daos SYSTEM PRIVATE
    ${DAOS_FS_INCLUDE_DIR}
)

target_compile_definitions(chimera_vfs_daos PRIVATE
    XXH_INLINE_ALL
    XXH_VECTOR=${CHIMERA_XXH_VECTOR}
)

target_link_libraries(chimera_vfs_daos
    ${DAOS_DFS_LIBRARY}
    ${DAOS_CLIENT_LIBRARY}
    xxhash
    jansson
    ${CHIMERA_UUID_LIB}
)

chimera_vfs_module(chimera_vfs_daos)

if (NOT DISABLE_TESTS)
    add_subdirectory(tests)
endif()
