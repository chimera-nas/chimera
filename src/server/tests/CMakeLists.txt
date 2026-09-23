# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# Daemon-level mount tests run a real chimera daemon in a network namespace.
chimera_add_netns_test(chimera/server/mount_create ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/mount_create_test.py
            $<TARGET_FILE:chimera>
)
set_tests_properties(chimera/server/mount_create PROPERTIES TIMEOUT 60)
