# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# Bounds test for the proxy's file-handle re-encoders.  Both helpers are static
# inline, so nothing needs linking, but the generated NFS XDR headers have to
# exist first.
add_executable(nfs_fh_bounds_test nfs_fh_bounds_test.c)
target_include_directories(nfs_fh_bounds_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_SOURCE_DIR}/src)
add_dependencies(nfs_fh_bounds_test chimera_nfs_common)
add_test(chimera/vfs/nfs/fh_bounds_test nfs_fh_bounds_test)

# Deterministically deliver a fresh OPEN before the preceding CLOSE reply.
add_executable(nfs_open_state_test nfs_open_state_test.c ../nfs4_open_state.c)
target_include_directories(nfs_open_state_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_SOURCE_DIR}/src)
add_dependencies(nfs_open_state_test chimera_nfs_common)
find_package(Threads REQUIRED)
target_link_libraries(nfs_open_state_test Threads::Threads)
add_test(chimera/vfs/nfs/open_state_test nfs_open_state_test)
add_executable(nfs_write_payload_test nfs_write_payload_test.c)
target_include_directories(nfs_write_payload_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_link_libraries(nfs_write_payload_test chimera_nfs_common evpl evpl_rpc2)
add_test(chimera/vfs/nfs/write_payload_test nfs_write_payload_test)

add_executable(nfs_attr_change_test nfs_attr_change_test.c)
target_include_directories(nfs_attr_change_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)
add_dependencies(nfs_attr_change_test chimera_nfs_common)
add_test(chimera/vfs/nfs/attr_change_test nfs_attr_change_test)

add_executable(nfs_delay_retry_test nfs_delay_retry_test.c)
target_include_directories(nfs_delay_retry_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_compile_options(nfs_delay_retry_test PRIVATE -ffunction-sections -fdata-sections)
target_link_options(nfs_delay_retry_test PRIVATE -Wl,--gc-sections)
target_link_libraries(nfs_delay_retry_test chimera_nfs_common chimera_common evpl evpl_rpc2 pthread)
add_test(chimera/vfs/nfs/delay_retry_test nfs_delay_retry_test)
