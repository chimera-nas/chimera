# SPDX-FileCopyrightText: 2025 Ben Jarvis
#
# SPDX-License-Identifier: LGPL-2.1-only

add_executable(client_basic basic.c)
target_link_libraries(client_basic chimera_client)

add_executable(client_test_symlink test_symlink.c)
target_link_libraries(client_test_symlink chimera_client)

add_executable(client_test_link test_link.c)
target_link_libraries(client_test_link chimera_client)

add_executable(client_test_remove test_remove.c)
target_link_libraries(client_test_remove chimera_client)

add_executable(client_test_rename test_rename.c)
target_link_libraries(client_test_rename chimera_client)

add_executable(client_test_readlink test_readlink.c)
target_link_libraries(client_test_readlink chimera_client)

add_executable(client_test_stat test_stat.c)
target_link_libraries(client_test_stat chimera_client)

add_test(NAME chimera/client/basic COMMAND client_basic)
add_test(NAME chimera/client/symlink COMMAND client_test_symlink)
add_test(NAME chimera/client/link COMMAND client_test_link)
add_test(NAME chimera/client/remove COMMAND client_test_remove)
add_test(NAME chimera/client/rename COMMAND client_test_rename)
add_test(NAME chimera/client/readlink COMMAND client_test_readlink)
add_test(NAME chimera/client/stat COMMAND client_test_stat)
