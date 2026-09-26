# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

if(NOT WIN32)
    find_package(PkgConfig REQUIRED)
endif()
if(NOT WIN32)
    pkg_check_modules(JANSSON REQUIRED jansson)
endif()

add_library(chimera_rest ${CHIMERA_LIBRARY_TYPE}
    rest.c rest_request.c rest_module.c rest_auth.c rest_services.c)
target_compile_definitions(chimera_rest PRIVATE
    CHIMERA_REST_MODULE_SUFFIX="${CMAKE_SHARED_MODULE_SUFFIX}")

add_library(chimera_rest_core MODULE rest_core.c rest_users.c rest_exports.c
    rest_shares.c rest_buckets.c rest_mounts.c rest_filesystems.c rest_config.c)
add_library(chimera_rest_docs MODULE rest_swagger.c)
# Bundled modules call back into one host runtime through private services.
# In particular, Windows DLLs must never link copies of the static VFS/server.
function(configure_rest_module module)
    target_sources(${module} PRIVATE rest_module_bind.c)
    set_target_properties(${module} PROPERTIES PREFIX "")
    target_compile_definitions(${module} PRIVATE CHIMERA_REST_BUNDLED_MODULE=1)
    target_include_directories(${module} PRIVATE ${JANSSON_INCLUDE_DIRS})
    target_link_libraries(${module} PRIVATE jansson)
    if(CHIMERA_LINUX)
        # Clang's ASan runtime is supplied by the executable, so its Debug
        # modules intentionally contain unresolved sanitizer imports.
        target_link_options(${module} PRIVATE
            "$<$<NOT:$<AND:$<C_COMPILER_ID:Clang>,$<CONFIG:Debug>>>:-Wl,-z,defs>")
    endif()
endfunction()
foreach(module chimera_rest_core chimera_rest_docs)
    configure_rest_module(${module})
    install(TARGETS ${module} LIBRARY DESTINATION lib RUNTIME DESTINATION bin)
endforeach()

if(NOT DISABLE_TESTS)
    add_library(chimera_rest_debug MODULE rest_debug.c)
    configure_rest_module(chimera_rest_debug)
endif()

add_library(chimera_rest_sdk INTERFACE)
set_target_properties(chimera_rest_sdk PROPERTIES EXPORT_NAME RestSDK)
target_include_directories(chimera_rest_sdk INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/sdk>
    $<INSTALL_INTERFACE:include/chimera/rest/sdk>)
install(TARGETS chimera_rest_sdk EXPORT ChimeraRestSDK COMPONENT rest-sdk)
install(EXPORT ChimeraRestSDK FILE ChimeraRestSDKConfig.cmake NAMESPACE Chimera::
    DESTINATION lib/cmake/ChimeraRestSDK COMPONENT rest-sdk)
install(DIRECTORY sdk/ DESTINATION include/chimera/rest/sdk COMPONENT rest-sdk
    FILES_MATCHING PATTERN "*.h")
install(FILES ${CMAKE_SOURCE_DIR}/LICENSES/LGPL-2.1-only.txt
    DESTINATION share/licenses/chimera-rest-sdk COMPONENT rest-sdk)

target_include_directories(chimera_rest PRIVATE ${JANSSON_INCLUDE_DIRS})
# crypt(3) is a separate library only on Linux (libcrypt/libxcrypt); on Darwin
# it lives in libSystem but supports only DES, so users.json password hashes
# in the crypt(5) $-schemes ($6$ SHA-512 etc.) cannot verify with it.  Use
# libxcrypt when present (brew install libxcrypt; keg-only, hence the explicit
# paths) for full scheme support plus the reentrant crypt_r().
if(WIN32)
    target_sources(chimera_rest PRIVATE ${CMAKE_SOURCE_DIR}/3rdparty/crypt/crypt_sha512.c)
    set(CHIMERA_CRYPT_LIB "")
elseif (CHIMERA_LINUX)
    set(CHIMERA_CRYPT_LIB crypt)
else()
    find_library(CHIMERA_XCRYPT_LIB crypt
                 PATHS /opt/homebrew/opt/libxcrypt/lib /usr/local/opt/libxcrypt/lib
                 NO_DEFAULT_PATH)
    find_path(CHIMERA_XCRYPT_INCLUDE crypt.h
              PATHS /opt/homebrew/opt/libxcrypt/include /usr/local/opt/libxcrypt/include
              NO_DEFAULT_PATH)
    if (CHIMERA_XCRYPT_LIB AND CHIMERA_XCRYPT_INCLUDE)
        message(STATUS "libxcrypt found (${CHIMERA_XCRYPT_LIB}); full crypt(5) scheme support")
        set(CHIMERA_CRYPT_LIB ${CHIMERA_XCRYPT_LIB})
        target_include_directories(chimera_rest PRIVATE ${CHIMERA_XCRYPT_INCLUDE})
        target_compile_definitions(chimera_rest PRIVATE CHIMERA_HAVE_XCRYPT=1)
    else()
        message(STATUS "libxcrypt not found; REST auth limited to DES password hashes")
        set(CHIMERA_CRYPT_LIB "")
    endif()
endif()

target_link_libraries(chimera_rest chimera_vfs evpl_http evpl
                      chimera_crypto ${CHIMERA_CRYPT_LIB} ${CHIMERA_DL_LIB} ${JANSSON_LIBRARIES})

# Embed Swagger UI files at build time
set(SWAGGER_DIR ${CMAKE_CURRENT_SOURCE_DIR}/swagger)
set(SWAGGER_EMBEDDED_C ${CMAKE_CURRENT_BINARY_DIR}/swagger_embedded.c)
set(OPENAPI_EMBEDDED_C ${CMAKE_CURRENT_BINARY_DIR}/openapi_embedded.c)
set(OPENAPI_JSON ${CMAKE_SOURCE_DIR}/docs/openapi.json)

add_custom_command(
    OUTPUT ${SWAGGER_EMBEDDED_C} ${OPENAPI_EMBEDDED_C}
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/embed_files.py
            ${SWAGGER_DIR} ${OPENAPI_JSON} ${SWAGGER_EMBEDDED_C} ${OPENAPI_EMBEDDED_C}
    DEPENDS
        ${CMAKE_CURRENT_SOURCE_DIR}/embed_files.py
        ${SWAGGER_DIR}/index.html
        ${SWAGGER_DIR}/swagger-ui-bundle.min.js
        ${SWAGGER_DIR}/swagger-ui-standalone-preset.min.js
        ${SWAGGER_DIR}/swagger-ui.min.css
        ${OPENAPI_JSON}
    COMMENT "Embedding Swagger UI files"
)

target_sources(chimera_rest_docs PRIVATE ${SWAGGER_EMBEDDED_C})
target_sources(chimera_rest PRIVATE ${OPENAPI_EMBEDDED_C})

if(WBCLIENT_FOUND)
    target_include_directories(chimera_rest PRIVATE ${WBCLIENT_INCLUDE_DIRS})
    target_link_libraries(chimera_rest ${WBCLIENT_LIBRARIES})
endif()

install(TARGETS chimera_rest DESTINATION lib)
add_dependencies(chimera_server chimera_rest_core chimera_rest_docs)
if(NOT DISABLE_TESTS)
    add_dependencies(chimera_server chimera_rest_debug)
endif()

if(NOT DISABLE_TESTS)
    add_subdirectory(tests)
endif()
