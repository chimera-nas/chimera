# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only


# SMB authentication test program
add_executable(smb_auth_test smb_auth_test.c)
target_link_libraries(smb_auth_test chimera_vfs urcu urcu-cds)
target_include_directories(smb_auth_test PRIVATE ${CMAKE_SOURCE_DIR}/src)

add_test(NAME chimera/server/smb/auth_test
    COMMAND ${CMAKE_CURRENT_BINARY_DIR}/smb_auth_test --mode=local)

# SMB smbclient authentication test
add_executable(smbclient_auth_test smbclient_auth_test.c)
target_link_libraries(smbclient_auth_test chimera_server chimera_metrics jansson)
target_include_directories(smbclient_auth_test PRIVATE ${CMAKE_SOURCE_DIR}/src)

add_test(NAME chimera/server/smb/smbclient_ntlm
    COMMAND ${CMAKE_SOURCE_DIR}/scripts/netns_test_wrapper.sh
            ${CMAKE_CURRENT_BINARY_DIR}/smbclient_auth_test --mode=ntlm)

add_test(NAME chimera/server/smb/smbclient_kerberos
    COMMAND ${CMAKE_SOURCE_DIR}/scripts/kerberos_test_wrapper.sh
            ${CMAKE_CURRENT_BINARY_DIR}/smbclient_auth_test --mode=kerberos)
set_tests_properties(chimera/server/smb/smbclient_kerberos PROPERTIES
    SKIP_RETURN_CODE 77)

find_program(SAMBA_TOOL samba-tool)
if(SAMBA_TOOL)
    add_test(NAME chimera/server/smb/smbclient_winbind
        COMMAND ${CMAKE_SOURCE_DIR}/scripts/ad_test_wrapper.sh
                ${CMAKE_CURRENT_BINARY_DIR}/smbclient_auth_test --mode=winbind)
endif()

# REST API user/share manipulation test
add_executable(rest_user_manip_test rest_user_manip_test.c)
target_link_libraries(rest_user_manip_test chimera_server chimera_metrics jansson)
target_include_directories(rest_user_manip_test PRIVATE ${CMAKE_SOURCE_DIR}/src)

add_test(NAME chimera/server/smb/rest_user_manip
    COMMAND ${CMAKE_SOURCE_DIR}/scripts/netns_test_wrapper.sh
            ${CMAKE_CURRENT_BINARY_DIR}/rest_user_manip_test)
