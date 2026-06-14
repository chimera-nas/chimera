# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# SMB2 client mount/umount smoke test.  Stands up an in-process SMB server over
# a memfs share and drives the vfs_smb client through a full session
# establish + teardown over loopback TCP.  Like the smbclient auth test it
# binds the privileged SMB port, so it runs under the netns wrapper.
add_executable(smb_mount_test smb_mount_test.c)
target_link_libraries(smb_mount_test chimera_server chimera_metrics jansson)
target_include_directories(smb_mount_test PRIVATE ${CMAKE_SOURCE_DIR}/src)

if(CHIMERA_NETNS_TESTING)
    add_test(NAME chimera/vfs/smb/mount
        COMMAND ${CMAKE_SOURCE_DIR}/scripts/netns_test_wrapper.sh
                ${CMAKE_CURRENT_BINARY_DIR}/smb_mount_test)
endif()
