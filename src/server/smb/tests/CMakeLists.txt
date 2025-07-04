macro(add_libsmb2_testprog name)
    add_executable(${name} ${name}.c)
    target_link_libraries(${name} smb2 chimera_server jansson)
    set_target_properties(${name} PROPERTIES
    TEST_FILE "${CMAKE_CURRENT_SOURCE_DIR}/${name}.c")
endmacro()

macro(add_libsmb2_test testname prog)
    add_test(NAME chimera/server/smb/${testname} COMMAND ${CMAKE_SOURCE_DIR}/scripts/netns_test_wrapper.sh  ${CMAKE_CURRENT_BINARY_DIR}/${prog} ${ARGN})
    get_target_property(test_file ${prog} TEST_FILE)
    set_tests_properties(chimera/server/smb/${testname} PROPERTIES 
        ENVIRONMENT "TEST_FILE=${test_file}")
endmacro()

add_libsmb2_testprog(libsmb2_basic)
add_libsmb2_testprog(libsmb2_bigdir)

add_libsmb2_test(libsmb2_basic libsmb2_basic -b memfs)
add_libsmb2_test(libsmb2_bigdir libsmb2_bigdir -b memfs)
