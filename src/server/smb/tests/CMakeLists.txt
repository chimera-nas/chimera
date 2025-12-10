# SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

macro(add_libsmb2_testprog name)
    add_executable(${name} ${name}.c)
    target_link_libraries(${name} smb2 chimera_server jansson gssapi_krb5)
    set_target_properties(${name} PROPERTIES
    TEST_FILE "${CMAKE_CURRENT_SOURCE_DIR}/${name}.c")
endmacro()

macro(add_libsmb2_test testname prog)
    add_test(NAME chimera/server/smb/${testname} COMMAND ${CMAKE_SOURCE_DIR}/scripts/netns_test_wrapper.sh  ${CMAKE_CURRENT_BINARY_DIR}/${prog} ${ARGN})
    get_target_property(test_file ${prog} TEST_FILE)
    set_tests_properties(chimera/server/smb/${testname} PROPERTIES 
        ENVIRONMENT "TEST_FILE=${test_file};LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/suppressions.txt")
endmacro()

add_libsmb2_testprog(libsmb2_basic)
add_libsmb2_testprog(libsmb2_bigdir)
add_libsmb2_testprog(libsmb2_echo)

add_libsmb2_test(libsmb2_memfs_basic libsmb2_basic -b memfs)
add_libsmb2_test(libsmb2_memfs_bigdir libsmb2_bigdir -b memfs)
add_libsmb2_test(libsmb2_memfs_echo libsmb2_echo -b memfs)


add_libsmb2_test(libsmb2_linux_basic libsmb2_basic -b linux)
add_libsmb2_test(libsmb2_linux_bigdir libsmb2_bigdir -b linux)
add_libsmb2_test(libsmb2_linux_echo libsmb2_echo -b linux)


add_libsmb2_test(libsmb2_demofs_basic libsmb2_basic -b demofs)
add_libsmb2_test(libsmb2_demofs_bigdir libsmb2_bigdir -b demofs)
add_libsmb2_test(libsmb2_demofs_echo libsmb2_echo -b demofs)
