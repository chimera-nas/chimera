# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only


# SMB authentication test program
add_executable(smb_auth_test smb_auth_test.c)
target_link_libraries(smb_auth_test chimera_vfs urcu urcu-cds)
target_include_directories(smb_auth_test PRIVATE ${CMAKE_SOURCE_DIR}/src)

add_test(NAME chimera/server/smb/auth_test
    COMMAND ${CMAKE_CURRENT_BINARY_DIR}/smb_auth_test --mode=local)
set_tests_properties(chimera/server/smb/auth_test PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/suppressions.txt")

# SMB smbclient authentication test
add_executable(smbclient_auth_test smbclient_auth_test.c)
target_link_libraries(smbclient_auth_test chimera_server chimera_metrics jansson)
target_include_directories(smbclient_auth_test PRIVATE ${CMAKE_SOURCE_DIR}/src)

add_test(NAME chimera/server/smb/smbclient_ntlm
    COMMAND ${CMAKE_SOURCE_DIR}/scripts/netns_test_wrapper.sh
            ${CMAKE_CURRENT_BINARY_DIR}/smbclient_auth_test --mode=ntlm)
set_tests_properties(chimera/server/smb/smbclient_ntlm PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/suppressions.txt")

add_test(NAME chimera/server/smb/smbclient_kerberos
    COMMAND ${CMAKE_SOURCE_DIR}/scripts/kerberos_test_wrapper.sh
            ${CMAKE_CURRENT_BINARY_DIR}/smbclient_auth_test --mode=kerberos)
set_tests_properties(chimera/server/smb/smbclient_kerberos PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/suppressions.txt")

add_test(NAME chimera/server/smb/smbclient_winbind
    COMMAND ${CMAKE_SOURCE_DIR}/scripts/ad_test_wrapper.sh
            ${CMAKE_CURRENT_BINARY_DIR}/smbclient_auth_test --mode=winbind)
set_tests_properties(chimera/server/smb/smbclient_winbind PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/suppressions.txt")

# REST API user/share manipulation test
add_executable(rest_user_manip_test rest_user_manip_test.c)
target_link_libraries(rest_user_manip_test chimera_server chimera_metrics jansson)
target_include_directories(rest_user_manip_test PRIVATE ${CMAKE_SOURCE_DIR}/src)

add_test(NAME chimera/server/smb/rest_user_manip
    COMMAND ${CMAKE_SOURCE_DIR}/scripts/netns_test_wrapper.sh
            ${CMAKE_CURRENT_BINARY_DIR}/rest_user_manip_test)
set_tests_properties(chimera/server/smb/rest_user_manip PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/suppressions.txt")

# SMB smbtorture integration tests
add_executable(smbtorture_test smbtorture_test.c)
target_link_libraries(smbtorture_test chimera_server chimera_metrics jansson)
target_include_directories(smbtorture_test PRIVATE ${CMAKE_SOURCE_DIR}/src)

set(SMBTORTURE_LSAN "LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/suppressions.txt")
set(SMBTORTURE_CMD ${CMAKE_CURRENT_BINARY_DIR}/smbtorture_test)

set(SMBTORTURE_SUITES
    smb2.acls
    smb2.acls_non_canonical
    smb2.async_dosmode
    smb2.change_notify_disabled
    smb2.charset
    smb2.check-sharemode
    smb2.compound
    smb2.compound_async
    smb2.compound_find
    smb2.connect
    smb2.create
    smb2.create_no_streams
    smb2.credits
    smb2.delete-on-close-perms
    smb2.deny
    smb2.dir
    smb2.dosmode
    smb2.durable-open
    smb2.durable-open-disconnect
    smb2.durable-v2-delay
    smb2.durable-v2-open
    smb2.ea
    smb2.fileid
    smb2.getinfo
    smb2.ioctl
    smb2.ioctl-on-stream
    smb2.kernel-oplocks
    smb2.lease
    smb2.lock
    smb2.maximum_allowed
    smb2.mkdir
    smb2.name-mangling
    smb2.notify
    smb2.notify-inotify
    smb2.openattr
    smb2.oplock
    smb2.read
    smb2.rename
    smb2.replay
    smb2.rw
    smb2.samba3misc
    smb2.sdread
    smb2.secleak
    smb2.session
    smb2.session-id
    smb2.session-require-signing
    smb2.set-sparse-ioctl
    smb2.setinfo
    smb2.sharemode
    smb2.streams
    smb2.tcon
    smb2.timestamp_resolution
    smb2.timestamps
    smb2.twrp
    smb2.winattr
    smb2.winattr2
    smb2.zero-data-ioctl
)

macro(add_smbtorture_backend_tests backend)
    foreach(SUITE ${SMBTORTURE_SUITES})
        string(REPLACE "." "_" SUITE_ID ${SUITE})
        add_test(NAME chimera/server/smb/smbtorture_${SUITE_ID}_${backend}
            COMMAND ${CMAKE_SOURCE_DIR}/scripts/netns_test_wrapper.sh
                    ${SMBTORTURE_CMD} -b ${backend} ${SUITE})
        set_tests_properties(chimera/server/smb/smbtorture_${SUITE_ID}_${backend} PROPERTIES
            ENVIRONMENT "${SMBTORTURE_LSAN}")
    endforeach()
endmacro()

add_smbtorture_backend_tests(memfs)
add_smbtorture_backend_tests(linux)
add_smbtorture_backend_tests(demofs_io_uring)
if(HAVE_LIBAIO)
    add_smbtorture_backend_tests(demofs_aio)
endif()
if(CAIRN_ENABLED)
    add_smbtorture_backend_tests(cairn)
endif()
if(IO_URING_ENABLED)
    add_smbtorture_backend_tests(io_uring)
endif()
