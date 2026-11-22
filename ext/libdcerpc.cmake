# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# libdcerpc, from the libsmb2 submodule (ext/libsmb2/libdcerpc), built the
# way libdcerpc builds itself with -DWITH_LIBSMB2=OFF: the NDR coders and the
# server runtime (<dcerpc/server.h>) only, no libsmb2 transport and no client
# side.  The SMB server feeds it the bytes clients write to the srvsvc,
# wkssvc, lsarpc and samr named pipes and returns the fragments it produces.
#
# libdcerpc's own CMakeLists.txt is not used: it insists on Linux/FreeBSD,
# installs a shared library and headers, and builds its tests and utilities.
# Here it is a static, position-independent library linked into chimera_smb.
#
# The coders and their public headers <dcerpc/<iface>.h> are generated from
# the IDL at build time by idlcomp.py, so python3 is needed.  Every interface
# is generated, also those chimera does not serve, because
# dcerpc_create_context() registers all of them.

set(LIBDCERPC_SRC ${CMAKE_CURRENT_SOURCE_DIR}/libsmb2/libdcerpc)
set(LIBDCERPC_GEN ${CMAKE_CURRENT_BINARY_DIR}/libdcerpc)

if(NOT EXISTS ${LIBDCERPC_SRC}/dcerpc-server.c)
    message(FATAL_ERROR
        "ext/libsmb2 is missing; run: git submodule update --init ext/libsmb2")
endif()

find_package(Python3 REQUIRED COMPONENTS Interpreter)

include(CheckIncludeFile)
foreach(_hdr arpa/inet.h fcntl.h inttypes.h netinet/in.h poll.h stdint.h
             stdlib.h string.h sys/poll.h sys/socket.h sys/stat.h
             sys/types.h sys/unistd.h unistd.h)
    string(TOUPPER "HAVE_${_hdr}" _var)
    string(REGEX REPLACE "[/.]" "_" _var "${_var}")
    check_include_file(${_hdr} ${_var})
endforeach()
# HAVE_LIBSMB2 stays undefined: coders and server runtime only.
configure_file(${LIBDCERPC_SRC}/config.h.cmake ${LIBDCERPC_GEN}/config.h)

set(LIBDCERPC_IFACES srvsvc wkssvc winreg lsa samr dssetup netlogon epm)

set(LIBDCERPC_SOURCES
    ${LIBDCERPC_SRC}/dcerpc.c
    ${LIBDCERPC_SRC}/dcerpc-dtyp.c
    ${LIBDCERPC_SRC}/dcerpc-epm.c
    ${LIBDCERPC_SRC}/dcerpc-server.c
    ${LIBDCERPC_SRC}/dcerpc-unicode.c)
set(LIBDCERPC_HEADERS)

foreach(_iface ${LIBDCERPC_IFACES})
    set(_coder ${LIBDCERPC_GEN}/${_iface}_coder.c)
    set(_header ${LIBDCERPC_GEN}/include/dcerpc/${_iface}.h)
    add_custom_command(
        OUTPUT  ${_coder} ${_header}
        COMMAND ${CMAKE_COMMAND} -E make_directory ${LIBDCERPC_GEN}/include/dcerpc
        COMMAND ${Python3_EXECUTABLE} ${LIBDCERPC_SRC}/idlcomp.py
                ${LIBDCERPC_SRC}/${_iface}/${_iface}.idl --libdcerpc
                -o ${_coder} --header ${_header}
        DEPENDS ${LIBDCERPC_SRC}/idlcomp.py ${LIBDCERPC_SRC}/${_iface}/${_iface}.idl
        COMMENT "Generating libdcerpc ${_iface} coders"
        VERBATIM)
    list(APPEND LIBDCERPC_SOURCES ${_coder})
    list(APPEND LIBDCERPC_HEADERS ${_header})
endforeach()

add_library(dcerpc STATIC ${LIBDCERPC_SOURCES} ${LIBDCERPC_HEADERS})
set_target_properties(dcerpc PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_compile_definitions(dcerpc PRIVATE HAVE_CONFIG_H _FILE_OFFSET_BITS=64)
if(MSVC)
    target_compile_definitions(dcerpc PRIVATE "_U_=")
else()
    target_compile_definitions(dcerpc PRIVATE "_U_=__attribute__((unused))")
    # Third-party code: keep its warnings visible but out of chimera's -Werror.
    target_compile_options(dcerpc PRIVATE -Wno-error -w)
endif()
target_include_directories(dcerpc
    PUBLIC  ${LIBDCERPC_SRC}/include ${LIBDCERPC_GEN}/include
    PRIVATE ${LIBDCERPC_GEN} ${LIBDCERPC_SRC})
