# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# Inline the liburcu read-side primitives for the server TUs that touch the RCU
# caches (nfs/s3/smb/rest + server.c).  See src/vfs/CMakeLists.txt; deliberately
# not set for the GPL fio plugin.  chimera is LGPL-2.1.  (URCU_API_MAP, selecting
# the unprefixed QSBR API, is set project-wide in the top-level CMakeLists.txt.)
add_compile_definitions(_LGPL_SOURCE)

add_library(chimera_server SHARED server.c)
target_link_libraries(chimera_server chimera_nfs chimera_s3 chimera_smb chimera_rest chimera_vfs chimera_common evpl ${CHIMERA_UNWIND_LIB} ${CHIMERA_OTEL_LIBS})

install(TARGETS chimera_server DESTINATION lib)

# RPCSEC_GSS for the client half.  krb5_local.c is libevpl's self-contained
# test realm -- real MIT krb5, no KDC, no keytab file, no network -- compiled
# straight out of the submodule rather than duplicated here.  Where MIT krb5 is
# absent its _none twin stands in and the krb5 cells self-skip at run time.
#
# macOS ships Heimdal as the system gssapi, which is not what this wants, and
# homebrew's krb5 is keg-only; point pkg-config at it as libevpl's own tests do.
if(APPLE)
    set(ENV{PKG_CONFIG_PATH}
        "${HOMEBREW_PREFIX}/opt/krb5/lib/pkgconfig:$ENV{PKG_CONFIG_PATH}")
endif()

find_package(PkgConfig QUIET)

if(PkgConfig_FOUND)
    pkg_check_modules(KRB5_GSSAPI QUIET krb5-gssapi krb5)
endif()

set(MBT_KRB5_DIR ${CMAKE_SOURCE_DIR}/ext/libevpl/src/rpc2/tests)

if(KRB5_GSSAPI_FOUND)
    message(STATUS "NFS MBT krb5 flavors enabled (krb5 ${KRB5_GSSAPI_VERSION})")
else()
    message(STATUS "NFS MBT krb5 flavors disabled (no MIT krb5)")
endif()

# Every MBT target gets the realm, because every one of them includes
# nfs3_mbt_common.h and that header is what establishes the context.
macro(mbt_add_krb5 tgt)
    target_include_directories(${tgt} PRIVATE ${MBT_KRB5_DIR})

    if(KRB5_GSSAPI_FOUND)
        target_sources(${tgt} PRIVATE ${MBT_KRB5_DIR}/krb5_local.c)
        target_include_directories(${tgt} PRIVATE ${KRB5_GSSAPI_INCLUDE_DIRS})
        target_link_directories(${tgt} PRIVATE ${KRB5_GSSAPI_LIBRARY_DIRS})
        target_link_libraries(${tgt} ${KRB5_GSSAPI_LIBRARIES})
    else()
        target_sources(${tgt} PRIVATE ${MBT_KRB5_DIR}/krb5_local_none.c)
    endif()
endmacro()

add_subdirectory(nfs)

add_subdirectory(s3)

add_subdirectory(smb)

add_subdirectory(rest)

# The FUSE protocol server drives the raw /dev/fuse kernel ABI
# (<linux/fuse.h>), which exists only on Linux.  server.c guards its
# registration with #ifdef __linux__.
if (CHIMERA_LINUX)
    add_subdirectory(fuse)
    target_link_libraries(chimera_server chimera_fuse)
endif()

if (NOT DISABLE_TESTS)
    add_subdirectory(tests)
endif()
