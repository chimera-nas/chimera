
add_executable(libsmb2_basic libsmb2_basic.c)
target_link_libraries(libsmb2_basic chimera_server smb2)

add_test(NAME chimera/server/smb/libsmb2_basic COMMAND libsmb2_basic -b memfs)

add_executable(libsmb2_bigdir libsmb2_bigdir.c)
target_link_libraries(libsmb2_bigdir chimera_server smb2)

add_test(NAME chimera/server/smb/libsmb2_bigdir COMMAND libsmb2_bigdir -b memfs)
