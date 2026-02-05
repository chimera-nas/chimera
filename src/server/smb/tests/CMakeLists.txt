# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
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
add_libsmb2_testprog(libsmb2_rename)

# SMB authentication test program
add_executable(smb_auth_test smb_auth_test.c)
target_link_libraries(smb_auth_test chimera_vfs urcu urcu-cds)
target_include_directories(smb_auth_test PRIVATE ${CMAKE_SOURCE_DIR}/src)

add_test(NAME chimera/server/smb/auth_test
    COMMAND ${CMAKE_CURRENT_BINARY_DIR}/smb_auth_test --mode=local)
set_tests_properties(chimera/server/smb/auth_test PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/suppressions.txt")

add_libsmb2_test(libsmb2_memfs_basic libsmb2_basic -b memfs)
add_libsmb2_test(libsmb2_memfs_bigdir libsmb2_bigdir -b memfs)
add_libsmb2_test(libsmb2_memfs_echo libsmb2_echo -b memfs)
add_libsmb2_test(libsmb2_memfs_rename libsmb2_rename -b memfs)


add_libsmb2_test(libsmb2_linux_basic libsmb2_basic -b linux)
add_libsmb2_test(libsmb2_linux_bigdir libsmb2_bigdir -b linux)
add_libsmb2_test(libsmb2_linux_echo libsmb2_echo -b linux)
add_libsmb2_test(libsmb2_linux_rename libsmb2_rename -b linux)


add_libsmb2_test(libsmb2_demofs_basic libsmb2_basic -b demofs)
add_libsmb2_test(libsmb2_demofs_bigdir libsmb2_bigdir -b demofs)
add_libsmb2_test(libsmb2_demofs_echo libsmb2_echo -b demofs)
add_libsmb2_test(libsmb2_demofs_rename libsmb2_rename -b demofs)

# =============================================================================
# AD Integration Tests (optional, require samba/krb5 packages)
# =============================================================================
# These tests are labeled "ad" and "slow" - excluded from normal test runs.
# Run with: ctest -L ad
# Or: ctest --label-exclude ad  (to exclude them)

option(ENABLE_AD_TESTS "Enable AD integration tests (requires samba, krb5-kdc)" OFF)

if(ENABLE_AD_TESTS)
    # Check for required tools
    find_program(SAMBA_TOOL samba-tool)
    find_program(KRB5KDC krb5kdc)

    # Build AD integration test programs
    add_libsmb2_testprog(libsmb2_kerberos)
    add_libsmb2_testprog(libsmb2_winbind)

    if(KRB5KDC)
        # Kerberos-only test with MIT KDC (lighter weight than full AD)
        # This tests GSSAPI/SPNEGO authentication path
        add_test(NAME chimera/server/smb/kerberos_auth
            COMMAND ${CMAKE_SOURCE_DIR}/scripts/kerberos_test_wrapper.sh
                    ${CMAKE_CURRENT_BINARY_DIR}/libsmb2_kerberos)
        set_tests_properties(chimera/server/smb/kerberos_auth PROPERTIES
            LABELS "ad;kerberos"
            TIMEOUT 120
            ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/suppressions.txt")
    endif()

    if(SAMBA_TOOL)
        # Full AD integration test with Samba AD DC
        # This tests both Kerberos and NTLM via winbind
        add_test(NAME chimera/server/smb/ad_kerberos_auth
            COMMAND ${CMAKE_SOURCE_DIR}/scripts/ad_test_wrapper.sh
                    ${CMAKE_CURRENT_BINARY_DIR}/libsmb2_kerberos)
        set_tests_properties(chimera/server/smb/ad_kerberos_auth PROPERTIES
            LABELS "ad;slow"
            TIMEOUT 180
            ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/suppressions.txt")

        add_test(NAME chimera/server/smb/ad_winbind_auth
            COMMAND ${CMAKE_SOURCE_DIR}/scripts/ad_test_wrapper.sh
                    ${CMAKE_CURRENT_BINARY_DIR}/libsmb2_winbind)
        set_tests_properties(chimera/server/smb/ad_winbind_auth PROPERTIES
            LABELS "ad;slow"
            TIMEOUT 180
            ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/suppressions.txt")

        # Run the auth_test with full AD environment
        add_test(NAME chimera/server/smb/auth_ad_full
            COMMAND ${CMAKE_SOURCE_DIR}/scripts/ad_test_wrapper.sh
                    ${CMAKE_CURRENT_BINARY_DIR}/smb_auth_test --mode=all)
        set_tests_properties(chimera/server/smb/auth_ad_full PROPERTIES
            LABELS "ad;slow"
            TIMEOUT 180
            ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/suppressions.txt")
    endif()

endif()
