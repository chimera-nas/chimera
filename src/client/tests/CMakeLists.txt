# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

macro(add_client_testprog name)
    add_executable(${name} ${name}.c)
    target_link_libraries(${name} chimera_client chimera_server jansson)
    set_target_properties(${name} PROPERTIES
        TEST_FILE "${CMAKE_CURRENT_SOURCE_DIR}/${name}.c")
endmacro()

# These run unwrapped, but their fixture chowns the per-test session dir to
# the target test uid/gid (CAP_CHOWN) and brings up the io_uring VFS module
# (unlimited RLIMIT_MEMLOCK) -- the same privileges the netns environment
# provides. Gate them on it so unprivileged `make` skips rather than fails.
macro(add_client_test testname prog backend user)
    if(CHIMERA_NETNS_TESTING)
        add_test(NAME chimera/client/${testname} COMMAND ${CMAKE_CURRENT_BINARY_DIR}/${prog} -b ${backend} -U ${user})
        get_target_property(test_file ${prog} TEST_FILE)
        set_tests_properties(chimera/client/${testname} PROPERTIES
            ENVIRONMENT "TEST_FILE=${test_file}")
    endif()
endmacro()

macro(add_client_nfs_test testname prog backend user)
    if(CHIMERA_NETNS_TESTING)
        add_test(NAME chimera/client/${testname} COMMAND ${CMAKE_SOURCE_DIR}/scripts/netns_test_wrapper.sh ${CMAKE_CURRENT_BINARY_DIR}/${prog} -v 3 -b ${backend} -U ${user})
        get_target_property(test_file ${prog} TEST_FILE)
        set_tests_properties(chimera/client/${testname} PROPERTIES
            ENVIRONMENT "TEST_FILE=${test_file}")
    endif()
endmacro()

add_executable(client_basic basic.c)
target_link_libraries(client_basic chimera_client)
# Runs unwrapped (no loopback networking needed), but initializing the VFS
# brings up the io_uring module, whose queue allocation needs an unlimited
# RLIMIT_MEMLOCK. The netns wrapper raises that limit for every other test;
# gate this one on the same privilege so unprivileged `make` doesn't abort.
if(CHIMERA_NETNS_TESTING)
    add_test(NAME chimera/client/basic COMMAND client_basic)
endif()

add_executable(client_read_into read_into.c)
target_link_libraries(client_read_into chimera_client evpl prometheus-c)
if(CHIMERA_NETNS_TESTING)
    add_test(NAME chimera/client/read_into COMMAND client_read_into)
endif()

# List of test programs
set(CLIENT_TEST_PROGRAMS
    test_symlink
    test_link
    test_remove
    test_rename
    test_readlink
    test_stat
    test_fstat
    test_ftruncate
    test_io
)

# Create test programs
foreach(prog ${CLIENT_TEST_PROGRAMS})
    add_client_testprog(${prog})
endforeach()

# Helper macro to generate tests for a backend and all test users
macro(_generate_tests_for_backend backend)
    foreach(user root myuser)
        # Generate direct backend tests
        foreach(prog ${CLIENT_TEST_PROGRAMS})
            string(REGEX REPLACE "^test_" "" test_name ${prog})
            add_client_test(${test_name}_${backend}_${user} ${prog} ${backend} ${user})
        endforeach()

        # Generate NFS3 tests
        foreach(prog ${CLIENT_TEST_PROGRAMS})
            string(REGEX REPLACE "^test_" "" test_name ${prog})
            add_client_nfs_test(${test_name}_nfs3_${backend}_${user} ${prog} ${backend} ${user})
        endforeach()
    endforeach()
endmacro()

# Macro to generate all tests for a backend
macro(generate_backend_tests backend condition_var)
    if("${condition_var}" STREQUAL "")
        _generate_tests_for_backend(${backend})
    elseif(${condition_var})
        _generate_tests_for_backend(${backend})
    endif()
endmacro()

# Generate tests for each backend
generate_backend_tests(memfs "")
generate_backend_tests(diskfs_io_uring IO_URING_ENABLED)
if(HAVE_LIBAIO)
    generate_backend_tests(diskfs_aio "")
endif()
generate_backend_tests(cairn CAIRN_ENABLED)
generate_backend_tests(linux "")
generate_backend_tests(io_uring CHIMERA_VFS_IO_URING)

# SMB client tests: drive the vfs_smb client against an in-process SMB server
# (backed by memfs) over loopback.  Limited to the file operations implemented
# so far, and to the root user -- the memfs share root's ACL denies an
# unprivileged user's create, so myuser variants fail server-side authorization
# (not a client defect).  The "-s" flag selects SMB mode in client_test_common.h.
macro(add_client_smb_test prog)
    string(REGEX REPLACE "^test_" "" _smb_name ${prog})
    if(CHIMERA_NETNS_TESTING)
        add_test(NAME chimera/client/${_smb_name}_smb_memfs_root
            COMMAND ${CMAKE_SOURCE_DIR}/scripts/netns_test_wrapper.sh
                    ${CMAKE_CURRENT_BINARY_DIR}/${prog} -s -b memfs -U root)
        get_target_property(_smb_test_file ${prog} TEST_FILE)
        set_tests_properties(chimera/client/${_smb_name}_smb_memfs_root PROPERTIES
            ENVIRONMENT "TEST_FILE=${_smb_test_file}")
    endif()
endmacro()

# NOTE: test_symlink / test_readlink are intentionally NOT in the SMB set yet.
# Symlink *creation* works over SMB, but reading a symlink back (open with
# FILE_OPEN_REPARSE_POINT, then FSCTL_GET_REPARSE_POINT) currently returns
# ObjectNameNotFound against the server's reparse-open path -- an alpha gap to
# fix in the SMB client's own iteration.  They still run on the FH backends.
foreach(prog test_stat test_fstat test_ftruncate test_remove test_rename test_io)
    add_client_smb_test(${prog})
endforeach()

# Tag every test registered above with the 'client' label so the suite can be
# run independently via `ctest -L client`, and with the VFS backend encoded in
# its name (longest-name-first so diskfs_io_uring beats the io_uring substring)
# so an entire backend can be run across every suite via e.g. `ctest -L linux`.
# Tests with no backend in their name keep only the suite label.
get_property(_client_tests DIRECTORY PROPERTY TESTS)
if(_client_tests)
    set_property(TEST ${_client_tests} APPEND PROPERTY LABELS client)
    foreach(_t IN LISTS _client_tests)
        foreach(_be diskfs_io_uring diskfs_aio io_uring memfs linux cairn)
            if(_t MATCHES "_${_be}($|_)")
                set_property(TEST ${_t} APPEND PROPERTY LABELS ${_be})
                break()
            endif()
        endforeach()
    endforeach()
endif()
