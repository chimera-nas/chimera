# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# REST API authentication test
add_executable(rest_auth_test rest_auth_test.c)
target_link_libraries(rest_auth_test chimera_server chimera_metrics jansson)
target_include_directories(rest_auth_test PRIVATE ${CMAKE_SOURCE_DIR}/src)

add_test(NAME chimera/server/rest/auth_test
    COMMAND ${CMAKE_SOURCE_DIR}/scripts/netns_test_wrapper.sh
            ${CMAKE_CURRENT_BINARY_DIR}/rest_auth_test)
set_tests_properties(chimera/server/rest/auth_test PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/src/server/smb/tests/suppressions.txt")
