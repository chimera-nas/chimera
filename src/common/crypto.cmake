# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: LGPL-2.1-only
# Kept independently buildable so backend vectors run without storage or model dependencies.
add_library(chimera_crypto ${CHIMERA_LIBRARY_TYPE}
    ${CMAKE_CURRENT_LIST_DIR}/crypto.c)
set_target_properties(chimera_crypto PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(chimera_crypto PUBLIC ${CMAKE_CURRENT_LIST_DIR}/..)
if(WIN32)
    target_sources(chimera_crypto PRIVATE ${CMAKE_CURRENT_LIST_DIR}/crypto_windows.c)
    target_link_libraries(chimera_crypto PUBLIC bcrypt crypt32)
else()
    find_package(OpenSSL 3 REQUIRED)
    target_sources(chimera_crypto PRIVATE ${CMAKE_CURRENT_LIST_DIR}/crypto_openssl.c)
    target_link_libraries(chimera_crypto PUBLIC OpenSSL::Crypto)
endif()
