# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: LGPL-2.1-only

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
find_package(Python3 REQUIRED COMPONENTS Interpreter)

# Every chimera library is a shared object, a DLL on Windows, as on Linux, so
# each holds one copy of its state (logging, RCU, the VFS) whoever loads it.
# A DLL must link every library it calls, so the libraries form a strict
# hierarchy with no cycles; common/export.h covers what crosses a boundary.
set(CHIMERA_LIBRARY_TYPE SHARED)

if(WIN32)
    # DLLs are found beside the executable that loads them, so every runtime
    # artifact -- executables, DLLs and the runtime-loaded VFS modules -- goes
    # to one directory.
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
    set(CHIMERA_GSS_LIB "")
    find_path(CHIMERA_UTHASH_INCLUDE uthash.h REQUIRED)
    find_path(CHIMERA_XXHASH_INCLUDE xxhash.h REQUIRED)
    include_directories(SYSTEM ${CHIMERA_UTHASH_INCLUDE} ${CHIMERA_XXHASH_INCLUDE})
    find_package(jansson CONFIG REQUIRED)
    if(NOT TARGET jansson)
        add_library(jansson ALIAS jansson::jansson)
    endif()
    find_package(xxHash CONFIG REQUIRED)
    if(NOT TARGET xxhash)
        add_library(xxhash ALIAS xxHash::xxhash)
    endif()
    if(CAIRN_ENABLED)
        find_package(RocksDB CONFIG REQUIRED)
        if(NOT TARGET rocksdb)
            add_library(rocksdb ALIAS RocksDB::rocksdb)
        endif()
    endif()
else()
    set(CHIMERA_GSS_LIB gssapi_krb5)
endif()

if(MSVC)
    add_compile_options(/W3 /WX $<$<COMPILE_LANGUAGE:C>:/experimental:c11atomics> /wd4244 /wd4267 /wd4018)
    add_compile_definitions(_CRT_SECURE_NO_WARNINGS _CRT_NONSTDC_NO_WARNINGS
                            WIN32_LEAN_AND_MEAN NOMINMAX)
    set(CHIMERA_GENERATED_C_OPTIONS /wd4101 /wd4189)
else()
    set(CHIMERA_GENERATED_C_OPTIONS -Wno-unused)
    if(CMAKE_C_COMPILER_ID STREQUAL "GNU")
        list(APPEND CHIMERA_GENERATED_C_OPTIONS -Wno-format-truncation)
    endif()
endif()

if(WIN32)
    set(CHIMERA_GSS_DEFAULT OFF)
else()
    set(CHIMERA_GSS_DEFAULT ON)
endif()
option(CHIMERA_GSSAPI "Build the MIT Kerberos authentication provider" ${CHIMERA_GSS_DEFAULT})
if(WIN32 AND CHIMERA_GSSAPI)
    message(FATAL_ERROR "The MIT GSSAPI provider is not ported to native Windows; use CHIMERA_GSSAPI=OFF")
endif()
if(CHIMERA_GSSAPI)
    add_compile_definitions(CHIMERA_HAVE_GSSAPI=1)
    set(CHIMERA_NFS_GSS_SOURCE nfs_gss.c)
    set(CHIMERA_SMB_GSS_SOURCE smb_gssapi.c)
else()
    set(CHIMERA_GSS_LIB "")
    set(CHIMERA_NFS_GSS_SOURCE nfs_gss_none.c)
    set(CHIMERA_SMB_GSS_SOURCE smb_gssapi_none.c)
endif()
