# SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

macro(add_client_testprog name)
    add_executable(${name} ${name}.c)
    target_link_libraries(${name} chimera_client chimera_server jansson)
    set_target_properties(${name} PROPERTIES
        TEST_FILE "${CMAKE_CURRENT_SOURCE_DIR}/${name}.c")
endmacro()

macro(add_client_test testname prog backend)
    add_test(NAME chimera/client/${testname} COMMAND ${CMAKE_CURRENT_BINARY_DIR}/${prog} -b ${backend})
    get_target_property(test_file ${prog} TEST_FILE)
    set_tests_properties(chimera/client/${testname} PROPERTIES 
        ENVIRONMENT "TEST_FILE=${test_file}")
endmacro()

macro(add_client_nfs_test testname prog backend)
    add_test(NAME chimera/client/${testname} COMMAND ${CMAKE_SOURCE_DIR}/scripts/netns_test_wrapper.sh ${CMAKE_CURRENT_BINARY_DIR}/${prog} -v 3 -b ${backend})
    get_target_property(test_file ${prog} TEST_FILE)
    set_tests_properties(chimera/client/${testname} PROPERTIES 
        ENVIRONMENT "TEST_FILE=${test_file}")
endmacro()

# Keep basic test as-is for now
add_executable(client_basic basic.c)
target_link_libraries(client_basic chimera_client)
add_test(NAME chimera/client/basic COMMAND client_basic)

# List of test programs
set(CLIENT_TEST_PROGRAMS
    test_symlink
    test_link
    test_remove
    test_rename
    test_readlink
    test_stat
)

# Create test programs
foreach(prog ${CLIENT_TEST_PROGRAMS})
    add_client_testprog(${prog})
endforeach()

# Helper macro to generate tests for a backend
macro(_generate_tests_for_backend backend)
    # Generate direct backend tests
    foreach(prog ${CLIENT_TEST_PROGRAMS})
        string(REGEX REPLACE "^test_" "" test_name ${prog})
        add_client_test(${test_name}_${backend} ${prog} ${backend})
    endforeach()

    # Generate NFS3 tests
    foreach(prog ${CLIENT_TEST_PROGRAMS})
        string(REGEX REPLACE "^test_" "" test_name ${prog})
        add_client_nfs_test(${test_name}_nfs3_${backend} ${prog} ${backend})
    endforeach()
endmacro()

# Macro to generate all tests for a backend
macro(generate_backend_tests backend condition_var)
    if(condition_var)
        if(${condition_var})
            _generate_tests_for_backend(${backend})
        endif()
    else()
        _generate_tests_for_backend(${backend})
    endif()
endmacro()

# Generate tests for each backend
generate_backend_tests(memfs "")
generate_backend_tests(demofs "")
generate_backend_tests(cairn CAIRN_ENABLED)
generate_backend_tests(linux "")
generate_backend_tests(io_uring IO_URING_ENABLED)
