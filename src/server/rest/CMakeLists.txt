# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

find_package(PkgConfig REQUIRED)
pkg_check_modules(JANSSON REQUIRED jansson)

add_library(chimera_rest SHARED
    rest.c
    rest_users.c
    rest_shares.c
    rest_swagger.c
)
target_include_directories(chimera_rest PRIVATE ${JANSSON_INCLUDE_DIRS})
target_link_libraries(chimera_rest evpl_http evpl ${JANSSON_LIBRARIES})

# Embed Swagger UI files at build time
set(SWAGGER_DIR ${CMAKE_CURRENT_SOURCE_DIR}/swagger)
set(SWAGGER_EMBEDDED_C ${CMAKE_CURRENT_BINARY_DIR}/swagger_embedded.c)
set(OPENAPI_JSON ${CMAKE_SOURCE_DIR}/docs/openapi.json)

add_custom_command(
    OUTPUT ${SWAGGER_EMBEDDED_C}
    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/embed_files.py
            ${SWAGGER_DIR} ${OPENAPI_JSON} ${SWAGGER_EMBEDDED_C}
    DEPENDS
        ${CMAKE_CURRENT_SOURCE_DIR}/embed_files.py
        ${SWAGGER_DIR}/index.html
        ${SWAGGER_DIR}/swagger-ui-bundle.min.js
        ${SWAGGER_DIR}/swagger-ui-standalone-preset.min.js
        ${SWAGGER_DIR}/swagger-ui.min.css
        ${OPENAPI_JSON}
    COMMENT "Embedding Swagger UI files"
)

target_sources(chimera_rest PRIVATE ${SWAGGER_EMBEDDED_C})

install(TARGETS chimera_rest DESTINATION lib)
