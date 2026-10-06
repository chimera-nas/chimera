# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

set(VFS_DAOS_TEST_INCLUDES
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/ext/libevpl/include
    ${CMAKE_SOURCE_DIR}/ext/libevpl/ext/prometheus-c
    ${CMAKE_SOURCE_DIR}/ext/libevpl/ext/prometheus-c/ext/stopwatch
    ${CMAKE_SOURCE_DIR}/ext/libevpl/ext/oteltracing-c
)

find_package(Threads REQUIRED)

add_executable(vfs_daos_fh_test test_fh.c ../vfs_daos_fh.c)
target_include_directories(vfs_daos_fh_test PRIVATE ${VFS_DAOS_TEST_INCLUDES})
target_link_libraries(vfs_daos_fh_test PRIVATE xxhash)
target_compile_definitions(vfs_daos_fh_test PRIVATE
    XXH_INLINE_ALL
    XXH_VECTOR=${CHIMERA_XXH_VECTOR}
)
target_compile_options(vfs_daos_fh_test PRIVATE -Wall -Wextra -Werror)
add_test(NAME chimera/vfs/daos/fh COMMAND vfs_daos_fh_test)

add_executable(vfs_daos_registry_test
    test_registry.c dfs_mock.c ../vfs_daos_registry.c ../vfs_daos_fh.c)
target_include_directories(vfs_daos_registry_test PRIVATE ${VFS_DAOS_TEST_INCLUDES})
target_link_libraries(vfs_daos_registry_test PRIVATE xxhash)
target_compile_definitions(vfs_daos_registry_test PRIVATE
    XXH_INLINE_ALL
    XXH_VECTOR=${CHIMERA_XXH_VECTOR}
)
target_compile_options(vfs_daos_registry_test PRIVATE
    -Wall -Wextra -Werror=implicit-function-declaration)
add_test(NAME chimera/vfs/daos/registry COMMAND vfs_daos_registry_test)

add_executable(vfs_daos_ops_test
    test_ops.c dfs_mock.c sdk_mock.c
    ../vfs_daos_registry.c ../vfs_daos_fh.c
    ../vfs_daos_config.c ../vfs_daos_errmap.c)
target_include_directories(vfs_daos_ops_test PRIVATE ${VFS_DAOS_TEST_INCLUDES})
target_link_libraries(vfs_daos_ops_test PRIVATE
    xxhash jansson ${CHIMERA_UUID_LIB} Threads::Threads)
target_compile_definitions(vfs_daos_ops_test PRIVATE
    XXH_INLINE_ALL
    XXH_VECTOR=${CHIMERA_XXH_VECTOR}
)
target_compile_options(vfs_daos_ops_test PRIVATE -Wall -Wextra)
add_test(NAME chimera/vfs/daos/ops COMMAND vfs_daos_ops_test)
set_tests_properties(chimera/vfs/daos/ops PROPERTIES TIMEOUT 30)

add_executable(vfs_daos_config_test test_config.c ../vfs_daos_config.c)
target_include_directories(vfs_daos_config_test PRIVATE ${VFS_DAOS_TEST_INCLUDES})
target_link_libraries(vfs_daos_config_test PRIVATE jansson ${CHIMERA_UUID_LIB})
target_compile_options(vfs_daos_config_test PRIVATE
    -Wall -Wextra -Werror=implicit-function-declaration)
add_test(NAME chimera/vfs/daos/config COMMAND vfs_daos_config_test)

add_executable(vfs_daos_load_test test_load.c)
target_include_directories(vfs_daos_load_test PRIVATE ${VFS_DAOS_TEST_INCLUDES})
target_link_libraries(vfs_daos_load_test PRIVATE ${CHIMERA_DL_LIB})
add_test(NAME chimera/vfs/daos/load
    COMMAND vfs_daos_load_test
            $<TARGET_FILE:chimera_vfs>
            $<TARGET_FILE:chimera_vfs_daos>)

foreach(vfs_daos_test
        vfs_daos_fh_test
        vfs_daos_registry_test
        vfs_daos_ops_test
        vfs_daos_config_test
        vfs_daos_load_test)
    target_include_directories(${vfs_daos_test} SYSTEM PRIVATE
        ${DAOS_FS_INCLUDE_DIR})
endforeach()
