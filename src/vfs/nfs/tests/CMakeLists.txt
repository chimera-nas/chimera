# SPDX-FileCopyrightText: 2025 Ben Jarvis
#
# SPDX-License-Identifier: LGPL-2.1-only

macro(add_nfs_testprog name)
    add_executable(${name} ${name}.c)
    target_link_libraries(${name} chimera_client chimera_server jansson)
    set_target_properties(${name} PROPERTIES
    TEST_FILE "${CMAKE_CURRENT_SOURCE_DIR}/${name}.c")
endmacro()

macro(add_nfs_test testname prog)
    add_test(NAME chimera/client/nfs/${testname} COMMAND ${CMAKE_SOURCE_DIR}/scripts/netns_test_wrapper.sh  ${CMAKE_CURRENT_BINARY_DIR}/${prog} ${ARGN})
    get_target_property(test_file ${prog} TEST_FILE)
    set_tests_properties(chimera/client/nfs/${testname} PROPERTIES 
        ENVIRONMENT "TEST_FILE=${test_file}")
endmacro()

add_nfs_testprog(nfs_mount)
add_nfs_testprog(nfs_mkdir)

add_nfs_test(nfs3_memfs_mount nfs_mount -v 3 -b memfs)
add_nfs_test(nfs3_memfs_mkdir nfs_mkdir -v 3 -b memfs)