# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# REST API authentication test
add_executable(rest_auth_test rest_auth_test.c)
target_link_libraries(rest_auth_test chimera_server chimera_metrics)
target_include_directories(rest_auth_test PRIVATE ${CMAKE_SOURCE_DIR}/src)

chimera_add_netns_test(chimera/server/rest/auth_test ${CMAKE_CURRENT_BINARY_DIR}/rest_auth_test)
set_tests_properties(chimera/server/rest/auth_test PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/src/server/smb/tests/suppressions.txt")

# REST API VFS mounts test (mount options round-trip and validation)
add_executable(rest_mounts_test rest_mounts_test.c)
target_link_libraries(rest_mounts_test chimera_server chimera_metrics)
target_include_directories(rest_mounts_test PRIVATE ${CMAKE_SOURCE_DIR}/src)

chimera_add_netns_test(chimera/server/rest/mounts_test ${CMAKE_CURRENT_BINARY_DIR}/rest_mounts_test)
set_tests_properties(chimera/server/rest/mounts_test PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/src/server/smb/tests/suppressions.txt")

# REST API NFS exports test (export_id round-trip and validation)
add_executable(rest_exports_test rest_exports_test.c)
target_link_libraries(rest_exports_test chimera_server chimera_metrics)
target_include_directories(rest_exports_test PRIVATE ${CMAKE_SOURCE_DIR}/src)

chimera_add_netns_test(chimera/server/rest/exports_test ${CMAKE_CURRENT_BINARY_DIR}/rest_exports_test)
set_tests_properties(chimera/server/rest/exports_test PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/src/server/smb/tests/suppressions.txt")
