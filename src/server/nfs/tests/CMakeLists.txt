# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# Unit test for the unified NFSv4 state model (Phases 1-4).
add_executable(test_state_table test_state_table.c)
target_link_libraries(test_state_table chimera_nfs chimera_server chimera_vfs chimera_nfs_common chimera_common evpl evpl_rpc2 pthread uuid urcu urcu-common)
target_include_directories(test_state_table PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/src/server/nfs
    ${NFS_COMMON_INCLUDE_DIR})

add_test(NAME chimera/server/nfs/state_table
    COMMAND ${CMAKE_CURRENT_BINARY_DIR}/test_state_table)

# Unit test for the NFS4.1 SEQUENCE replay slot state machine.  Drives
# nfs4_replay_slot_acquire / _finalize directly.  We compile the
# session source file straight into the test executable (rather than
# linking libchimera_nfs.so) because the replay-slot functions are not
# SYMBOL_EXPORT'd -- they are only called from inside the chimera_nfs
# library at runtime.
add_executable(test_replay_slot
    test_replay_slot.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../nfs4_session.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../nfs4_state.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../nfs4_lease.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../nfs4_recovery.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../nfs4_callback.c)
target_include_directories(test_replay_slot PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_SOURCE_DIR}/src
    ${NFS_COMMON_INCLUDE_DIR})
target_link_libraries(test_replay_slot chimera_vfs chimera_common evpl evpl_rpc2 pthread uuid urcu urcu-common)
add_test(NAME chimera/server/nfs/replay_slot COMMAND test_replay_slot)
