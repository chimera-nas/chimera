# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# SMB2 client mount/umount smoke test.  Stands up an in-process SMB server over
# a memfs share and drives the vfs_smb client through a full session
# establish + teardown over loopback TCP.  Like the smbclient auth test it
# reaches the server over the in-process transport, so it binds no port and
# needs no namespace; the wrapper is only there for the resource limits.
add_executable(smb_mount_test smb_mount_test.c)
target_link_libraries(smb_mount_test chimera_server chimera_metrics)
target_include_directories(smb_mount_test PRIVATE ${CMAKE_SOURCE_DIR}/src)

if(CHIMERA_LIMITS_TESTING)
    add_test(NAME chimera/vfs/smb/mount
        COMMAND ${CMAKE_SOURCE_DIR}/scripts/test_limits_wrapper.sh
                ${CMAKE_CURRENT_BINARY_DIR}/smb_mount_test)
endif()

# The engine's create gate on the path-only backend: non-root creating opens
# through vfs_smb over a loopback SMB server, decided by the engine from the
# modefromsid mode/uid/gid the proxy reports.
add_executable(smb_create_gate_test smb_create_gate_test.c)
target_link_libraries(smb_create_gate_test chimera_server chimera_metrics)
target_include_directories(smb_create_gate_test PRIVATE ${CMAKE_SOURCE_DIR}/src)

if(CHIMERA_LIMITS_TESTING)
    add_test(NAME chimera/vfs/smb/create_gate
        COMMAND ${CMAKE_SOURCE_DIR}/scripts/test_limits_wrapper.sh
                ${CMAKE_CURRENT_BINARY_DIR}/smb_create_gate_test)
endif()

# Path-only parent resolution from a non-root start: the path procs issued from
# the vfs_smb mount's own root handle, which takes their whole-path arms.  The
# test embeds memfs (and wraps it as the "nochk" module), found by dlsym, so
# the binary exports its symbols.
add_executable(smb_pathop_guard_test smb_pathop_guard_test.c)
target_link_libraries(smb_pathop_guard_test chimera_server chimera_metrics
                      chimera_vfs_memfs_objs)
target_include_directories(smb_pathop_guard_test PRIVATE ${CMAKE_SOURCE_DIR}/src)
set_target_properties(smb_pathop_guard_test PROPERTIES ENABLE_EXPORTS ON)

if(CHIMERA_LIMITS_TESTING)
    add_test(NAME chimera/vfs/smb/pathop_guard
        COMMAND ${CMAKE_SOURCE_DIR}/scripts/test_limits_wrapper.sh
                ${CMAKE_CURRENT_BINARY_DIR}/smb_pathop_guard_test)
endif()
