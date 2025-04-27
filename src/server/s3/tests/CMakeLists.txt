add_executable(libs3_put libs3_put.c)
add_executable(libs3_get libs3_get.c)
add_executable(libs3_head libs3_head.c)
add_executable(libs3_delete libs3_delete.c)
add_executable(libs3_list libs3_list.c)

target_link_libraries(libs3_put chimera_server s3 pthread jansson)
target_link_libraries(libs3_get chimera_server s3 pthread jansson)
target_link_libraries(libs3_head chimera_server s3 pthread jansson)
target_link_libraries(libs3_delete chimera_server s3 pthread jansson)
target_link_libraries(libs3_list chimera_server s3 pthread jansson)

add_test(NAME chimera/server/s3/libs3_memfs_put COMMAND libs3_put -b memfs)
add_test(NAME chimera/server/s3/libs3_memfs_get COMMAND libs3_get -b memfs)
add_test(NAME chimera/server/s3/libs3_memfs_head COMMAND libs3_head -b memfs)
add_test(NAME chimera/server/s3/libs3_memfs_delete COMMAND libs3_delete -b memfs)
add_test(NAME chimera/server/s3/libs3_memfs_list COMMAND libs3_list -b memfs)

add_test(NAME chimera/server/s3/libs3_memfs_path_put COMMAND libs3_put -p -b memfs)
add_test(NAME chimera/server/s3/libs3_memfs_path_get COMMAND libs3_get -p -b memfs)
add_test(NAME chimera/server/s3/libs3_memfs_path_head COMMAND libs3_head -p -b memfs)
add_test(NAME chimera/server/s3/libs3_memfs_path_delete COMMAND libs3_delete -p -b memfs)
add_test(NAME chimera/server/s3/libs3_memfs_path_list COMMAND libs3_list -p -b memfs)

add_test(NAME chimera/server/s3/libs3_demofs_put COMMAND libs3_put -b demofs)
add_test(NAME chimera/server/s3/libs3_demofs_get COMMAND libs3_get -b demofs)
add_test(NAME chimera/server/s3/libs3_demofs_head COMMAND libs3_head -b demofs)
add_test(NAME chimera/server/s3/libs3_demofs_delete COMMAND libs3_delete -b demofs)
add_test(NAME chimera/server/s3/libs3_demofs_list COMMAND libs3_list -b demofs)

add_test(NAME chimera/server/s3/libs3_cairn_put COMMAND libs3_put -b cairn)
add_test(NAME chimera/server/s3/libs3_cairn_get COMMAND libs3_get -b cairn)
add_test(NAME chimera/server/s3/libs3_cairn_head COMMAND libs3_head -b cairn)
add_test(NAME chimera/server/s3/libs3_cairn_delete COMMAND libs3_delete -b cairn)
add_test(NAME chimera/server/s3/libs3_cairn_list COMMAND libs3_list -b cairn)

add_test(NAME chimera/server/s3/libs3_linux_put COMMAND libs3_put -b linux)
add_test(NAME chimera/server/s3/libs3_linux_get COMMAND libs3_get -b linux)
add_test(NAME chimera/server/s3/libs3_linux_head COMMAND libs3_head -b linux)
add_test(NAME chimera/server/s3/libs3_linux_delete COMMAND libs3_delete -b linux)
add_test(NAME chimera/server/s3/libs3_linux_list COMMAND libs3_list -b linux)

add_test(NAME chimera/server/s3/libs3_io_uring_put COMMAND libs3_put -b io_uring)
add_test(NAME chimera/server/s3/libs3_io_uring_get COMMAND libs3_get -b io_uring)
add_test(NAME chimera/server/s3/libs3_io_uring_head COMMAND libs3_head -b io_uring)
add_test(NAME chimera/server/s3/libs3_io_uring_delete COMMAND libs3_delete -b io_uring)
add_test(NAME chimera/server/s3/libs3_io_uring_list COMMAND libs3_list -b io_uring)




