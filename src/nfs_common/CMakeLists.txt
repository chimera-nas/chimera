# SPDX-FileCopyrightText: 2025 Ben Jarvis
#
# SPDX-License-Identifier: LGPL-2.1-only

# Set the paths to the XDR specification files and the output C and header files
set(NFS3_XDR_X ${CMAKE_CURRENT_SOURCE_DIR}/nfs3.x)
set(NFS3_XDR_C ${CMAKE_CURRENT_BINARY_DIR}/nfs3_xdr.c)
set(NFS3_XDR_H ${CMAKE_CURRENT_BINARY_DIR}/nfs3_xdr.h)

set(NFS4_XDR_X ${CMAKE_CURRENT_SOURCE_DIR}/nfs4.x)
set(NFS4_XDR_C ${CMAKE_CURRENT_BINARY_DIR}/nfs4_xdr.c)
set(NFS4_XDR_H ${CMAKE_CURRENT_BINARY_DIR}/nfs4_xdr.h)

set(PORTMAP_XDR_X ${CMAKE_CURRENT_SOURCE_DIR}/portmap.x)
set(PORTMAP_XDR_C ${CMAKE_CURRENT_BINARY_DIR}/portmap_xdr.c)
set(PORTMAP_XDR_H ${CMAKE_CURRENT_BINARY_DIR}/portmap_xdr.h)

set(NFS_MOUNT_XDR_X ${CMAKE_CURRENT_SOURCE_DIR}/mount.x)
set(NFS_MOUNT_XDR_C ${CMAKE_CURRENT_BINARY_DIR}/nfs_mount_xdr.c)
set(NFS_MOUNT_XDR_H ${CMAKE_CURRENT_BINARY_DIR}/nfs_mount_xdr.h)

add_custom_command(
    OUTPUT ${NFS3_XDR_C} ${NFS3_XDR_H}
    COMMAND ${XDRZCC} -r ${NFS3_XDR_X} ${NFS3_XDR_C} ${NFS3_XDR_H}
    DEPENDS ${NFS3_XDR_X} ${XDRZCC}
    COMMENT "Compiling ${NFS3_XDR_X}"
)

add_custom_command(
    OUTPUT ${NFS4_XDR_C} ${NFS4_XDR_H}
    COMMAND ${XDRZCC} -r ${NFS4_XDR_X} ${NFS4_XDR_C} ${NFS4_XDR_H}
    DEPENDS ${NFS4_XDR_X} ${XDRZCC}
    COMMENT "Compiling ${NFS4_XDR_X}"
)

add_custom_command(
    OUTPUT ${NFS_MOUNT_XDR_C} ${NFS_MOUNT_XDR_H}
    COMMAND ${XDRZCC} -r ${NFS_MOUNT_XDR_X} ${NFS_MOUNT_XDR_C} ${NFS_MOUNT_XDR_H}
    DEPENDS ${NFS_MOUNT_XDR_X} ${XDRZCC}
    COMMENT "Compiling ${NFS_MOUNT_XDR_X}"
)

add_custom_command(
    OUTPUT ${PORTMAP_XDR_C} ${PORTMAP_XDR_H}
    COMMAND ${XDRZCC} -r ${PORTMAP_XDR_X} ${PORTMAP_XDR_C} ${PORTMAP_XDR_H}
    DEPENDS ${PORTMAP_XDR_X} ${XDRZCC}
    COMMENT "Compiling ${PORTMAP_XDR_X}"
)

set(NFS_COMMON_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR} PARENT_SCOPE)

include_directories(${CMAKE_CURRENT_BINARY_DIR})

# Suppress warnings for the generated source files
set_source_files_properties(
    ${NFS3_XDR_C} ${NFS4_XDR_C} ${NFS_MOUNT_XDR_C} ${PORTMAP_XDR_C} PROPERTIES COMPILE_OPTIONS "-Wno-unused;-Wno-switch;-Wno-format-truncation"
)

add_definitions(-fvisibility=default)


# Create libraries for NFSv3 and NFSv4
add_library(chimera_nfs_common  SHARED 
            ${NFS3_XDR_C} ${NFS3_XDR_H}
            ${NFS4_XDR_C} ${NFS4_XDR_H}
            ${NFS_MOUNT_XDR_C} ${NFS_MOUNT_XDR_H}
            ${PORTMAP_XDR_C} ${PORTMAP_XDR_H})

target_link_libraries(chimera_nfs_common chimera_common evpl evpl_rpc2)

install(TARGETS chimera_nfs_common DESTINATION lib)

# Ensure xdrzcc is built before these libraries
add_dependencies(chimera_nfs_common xdrzcc)