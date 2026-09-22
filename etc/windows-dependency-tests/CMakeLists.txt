# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Unlicense
cmake_minimum_required(VERSION 3.22)
project(chimera_dependency_tests LANGUAGES C)
find_package(OpenSSL 3 REQUIRED)
add_executable(openssl_tls openssl_tls.c)
set_target_properties(openssl_tls PROPERTIES C_STANDARD 11 C_STANDARD_REQUIRED ON)
target_link_libraries(openssl_tls PRIVATE OpenSSL::SSL)
enable_testing()
add_test(NAME openssl_tls12 COMMAND openssl_tls 1.2)
add_test(NAME openssl_tls13 COMMAND openssl_tls 1.3)
set_tests_properties(openssl_tls12 openssl_tls13 PROPERTIES TIMEOUT 30)
