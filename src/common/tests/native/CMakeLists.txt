# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: LGPL-2.1-only
cmake_minimum_required(VERSION 3.22)
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    project(chimera_native_runtime C)
    set(CMAKE_C_STANDARD 11)
    set(CMAKE_C_STANDARD_REQUIRED ON)
    enable_testing()
endif()
if(NOT MSVC)
    find_package(Threads REQUIRED)
endif()
get_filename_component(CHIMERA_SOURCE_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../../.." ABSOLUTE)
# The full build already registers this test against chimera_common.
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    add_executable(native_rcu_test ../rcu_test.c ../../chimera_rcu.c)
    target_include_directories(native_rcu_test PRIVATE
        "${CHIMERA_SOURCE_ROOT}/src" "${CHIMERA_SOURCE_ROOT}/ext/libevpl/include")
    if(MSVC)
        target_compile_options(native_rcu_test PRIVATE /W4 /WX /experimental:c11atomics /UNDEBUG)
    else()
        target_compile_options(native_rcu_test PRIVATE -Wall -Wextra -Werror -UNDEBUG)
        find_package(Threads REQUIRED)
        target_link_libraries(native_rcu_test PRIVATE Threads::Threads)
    endif()
    add_test(NAME chimera/native/rcu COMMAND native_rcu_test)
    set_tests_properties(chimera/native/rcu PROPERTIES TIMEOUT 60)

endif()

add_executable(native_password_test ../password_test.c
    ${CHIMERA_SOURCE_ROOT}/3rdparty/crypt/crypt_sha512.c)
if(MSVC)
    target_compile_definitions(native_password_test PRIVATE _CRT_SECURE_NO_WARNINGS)
    target_compile_options(native_password_test PRIVATE /W3 /WX /wd4244 /wd4267)
else()
    # Preserve the vendored crypt implementation's aggregate initializer style.
    target_compile_options(native_password_test PRIVATE -Wno-missing-braces)
endif()
add_test(NAME chimera/native/password_hash COMMAND native_password_test)
set_tests_properties(chimera/native/password_hash PROPERTIES TIMEOUT 60)

add_executable(native_unicode_test ../unicode_test.c)
target_include_directories(native_unicode_test PRIVATE
    "${CHIMERA_SOURCE_ROOT}/src" "${CHIMERA_SOURCE_ROOT}/ext/libevpl/include")
if(MSVC)
    target_compile_options(native_unicode_test PRIVATE /experimental:c11atomics)
else()
    find_package(Iconv REQUIRED)
    target_link_libraries(native_unicode_test PRIVATE Iconv::Iconv)
endif()
add_test(NAME chimera/native/unicode COMMAND native_unicode_test)

add_executable(native_atomic_test ../atomic_test.c)
target_include_directories(native_atomic_test PRIVATE
    "${CHIMERA_SOURCE_ROOT}/src" "${CHIMERA_SOURCE_ROOT}/ext/libevpl/include")
if(MSVC)
    target_compile_options(native_atomic_test PRIVATE /W3 /WX /experimental:c11atomics)
else()
    target_link_libraries(native_atomic_test PRIVATE Threads::Threads)
endif()
add_test(NAME chimera/native/atomic COMMAND native_atomic_test)
set_tests_properties(chimera/native/atomic PROPERTIES TIMEOUT 60)

if(WIN32)
    add_executable(native_file_acl_test ../file_acl_test.c)
    target_include_directories(native_file_acl_test PRIVATE
        "${CHIMERA_SOURCE_ROOT}/src" "${CHIMERA_SOURCE_ROOT}/ext/libevpl/include")
    target_compile_definitions(native_file_acl_test PRIVATE _CRT_SECURE_NO_WARNINGS)
    target_compile_options(native_file_acl_test PRIVATE /experimental:c11atomics)
    target_link_libraries(native_file_acl_test PRIVATE advapi32)
    add_test(NAME chimera/native/file_acl COMMAND native_file_acl_test)
endif()

add_executable(native_getopt_test ../getopt_test.c)
target_include_directories(native_getopt_test PRIVATE "${CHIMERA_SOURCE_ROOT}/src")
add_test(NAME chimera/native/getopt COMMAND native_getopt_test)

add_executable(native_host_fixture_test ../host_fixture_test.c)
target_include_directories(native_host_fixture_test PRIVATE
    "${CHIMERA_SOURCE_ROOT}/src" "${CHIMERA_SOURCE_ROOT}/ext/libevpl/include")
if(MSVC)
    target_compile_definitions(native_host_fixture_test PRIVATE _CRT_SECURE_NO_WARNINGS)
    target_compile_options(native_host_fixture_test PRIVATE /experimental:c11atomics)
    target_link_libraries(native_host_fixture_test PRIVATE bcrypt)
endif()
add_test(NAME chimera/native/host_fixture COMMAND native_host_fixture_test)

if(WIN32)
    add_executable(native_watchdog_test ../watchdog_test.c)
    target_include_directories(native_watchdog_test PRIVATE
        "${CHIMERA_SOURCE_ROOT}/src" "${CHIMERA_SOURCE_ROOT}/ext/libevpl/include")
    target_compile_options(native_watchdog_test PRIVATE /experimental:c11atomics)
    add_test(NAME chimera/native/watchdog_cancel COMMAND native_watchdog_test)
    add_test(NAME chimera/native/watchdog_expire COMMAND native_watchdog_test expire)
    set_tests_properties(chimera/native/watchdog_expire PROPERTIES WILL_FAIL TRUE TIMEOUT 5)
endif()

add_executable(native_range_test range_test.c)
target_include_directories(native_range_test PRIVATE "${CHIMERA_SOURCE_ROOT}/src")
add_test(NAME chimera/native/range COMMAND native_range_test)
