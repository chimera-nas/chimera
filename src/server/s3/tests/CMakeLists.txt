macro(add_libs3_testprog name)
    add_executable(${name} ${name}.c)
    target_link_libraries(${name} s3 chimera_server jansson)
    set_target_properties(${name} PROPERTIES
    TEST_FILE "${CMAKE_CURRENT_SOURCE_DIR}/${name}.c")
endmacro()

macro(add_libs3_test testname prog)
    add_test(NAME chimera/server/s3/${testname} COMMAND ${CMAKE_SOURCE_DIR}/scripts/netns_test_wrapper.sh  ${CMAKE_CURRENT_BINARY_DIR}/${prog} ${ARGN})
    get_target_property(test_file ${prog} TEST_FILE)
    set_tests_properties(chimera/server/s3/${testname} PROPERTIES 
        ENVIRONMENT "TEST_FILE=${test_file}")
endmacro()

add_libs3_testprog(libs3_put)
add_libs3_testprog(libs3_get)
add_libs3_testprog(libs3_head)
add_libs3_testprog(libs3_delete)
add_libs3_testprog(libs3_list)

add_libs3_test(libs3_memfs_put libs3_put -b memfs)
add_libs3_test(libs3_memfs_get libs3_get -b memfs)
add_libs3_test(libs3_memfs_head libs3_head -b memfs)
add_libs3_test(libs3_memfs_delete libs3_delete -b memfs)
add_libs3_test(libs3_memfs_list libs3_list -b memfs)

add_libs3_test(libs3_memfs_path_put libs3_put -p -b memfs)
add_libs3_test(libs3_memfs_path_get libs3_get -p -b memfs)
add_libs3_test(libs3_memfs_path_head libs3_head -p -b memfs)
add_libs3_test(libs3_memfs_path_delete libs3_delete -p -b memfs)
add_libs3_test(libs3_memfs_path_list libs3_list -p -b memfs)

add_libs3_test(libs3_demofs_put libs3_put -b demofs)
add_libs3_test(libs3_demofs_get libs3_get -b demofs)
add_libs3_test(libs3_demofs_head libs3_head -b demofs)
add_libs3_test(libs3_demofs_delete libs3_delete -b demofs)
add_libs3_test(libs3_demofs_list libs3_list -b demofs)

add_libs3_test(libs3_cairn_put libs3_put -b cairn)
add_libs3_test(libs3_cairn_get libs3_get -b cairn)
add_libs3_test(libs3_cairn_head libs3_head -b cairn)
add_libs3_test(libs3_cairn_delete libs3_delete -b cairn)
add_libs3_test(libs3_cairn_list libs3_list -b cairn)

add_libs3_test(libs3_linux_put libs3_put -b linux)
add_libs3_test(libs3_linux_get libs3_get -b linux)
add_libs3_test(libs3_linux_head libs3_head -b linux)
add_libs3_test(libs3_linux_delete libs3_delete -b linux)
add_libs3_test(libs3_linux_list libs3_list -b linux)

add_libs3_test(libs3_io_uring_put libs3_put -b io_uring)
add_libs3_test(libs3_io_uring_get libs3_get -b io_uring)
add_libs3_test(libs3_io_uring_head libs3_head -b io_uring)
add_libs3_test(libs3_io_uring_delete libs3_delete -b io_uring)
add_libs3_test(libs3_io_uring_list libs3_list -b io_uring)




