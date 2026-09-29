# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# Daemon configuration validation test: export settings that must fail
# startup, two-pass export_id assignment, and sec config round-trip.
chimera_add_netns_test(chimera/daemon/config_test ${Python3_EXECUTABLE}
            ${CMAKE_CURRENT_SOURCE_DIR}/daemon_config_test.py
            $<TARGET_FILE:chimera>)
set_tests_properties(chimera/daemon/config_test PROPERTIES
    TIMEOUT 600
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/src/server/smb/tests/suppressions.txt")

# The configs the project ships must start: the installed default config
# everywhere, and the Docker image's config on Linux, where its linux
# passthrough mount exists.
chimera_add_netns_test(chimera/daemon/shipped_config/default ${Python3_EXECUTABLE}
            ${CMAKE_CURRENT_SOURCE_DIR}/shipped_config_test.py
            $<TARGET_FILE:chimera> ${CMAKE_SOURCE_DIR}/src/daemon/chimera.json
            ${CMAKE_CURRENT_BINARY_DIR})
set_tests_properties(chimera/daemon/shipped_config/default PROPERTIES TIMEOUT 120)
if(CHIMERA_LINUX)
    chimera_add_netns_test(chimera/daemon/shipped_config/docker ${Python3_EXECUTABLE}
                ${CMAKE_CURRENT_SOURCE_DIR}/shipped_config_test.py
                $<TARGET_FILE:chimera> ${CMAKE_SOURCE_DIR}/chimera-docker.json
                ${CMAKE_CURRENT_BINARY_DIR})
    set_tests_properties(chimera/daemon/shipped_config/docker PROPERTIES TIMEOUT 120)
endif()

add_test(NAME chimera/daemon/native_lifecycle
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/daemon_native_test.py
            $<TARGET_FILE:chimera>)
set_tests_properties(chimera/daemon/native_lifecycle PROPERTIES TIMEOUT 120)
