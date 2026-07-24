# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# Daemon configuration validation test: export settings that must fail
# startup, two-pass export_id assignment, and sec config round-trip.
add_test(NAME chimera/daemon/config_test
    COMMAND ${CMAKE_SOURCE_DIR}/scripts/netns_test_wrapper.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/daemon_config_test.sh
            $<TARGET_FILE:chimera>)
set_tests_properties(chimera/daemon/config_test PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/src/server/smb/tests/suppressions.txt")
