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

add_test(NAME chimera/daemon/native_lifecycle
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/daemon_native_test.py
            $<TARGET_FILE:chimera>)
set_tests_properties(chimera/daemon/native_lifecycle PROPERTIES TIMEOUT 120)
