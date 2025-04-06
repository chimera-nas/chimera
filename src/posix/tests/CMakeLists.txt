add_executable(posix_basic basic.c)
target_link_libraries(posix_basic chimera_client)

add_test(NAME chimera/posix/basic COMMAND posix_basic)