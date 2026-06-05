# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# Daemon-level mount tests run a real chimera daemon in a network namespace.
if(CHIMERA_NETNS_TESTING)
    add_test(
        NAME chimera/server/mount_create
        COMMAND ${CMAKE_SOURCE_DIR}/scripts/netns_test_wrapper.sh
                bash ${CMAKE_CURRENT_SOURCE_DIR}/mount_create_test.sh
                ${CMAKE_BINARY_DIR}/src/daemon/chimera
    )
    set_tests_properties(chimera/server/mount_create PROPERTIES TIMEOUT 60)
endif()
