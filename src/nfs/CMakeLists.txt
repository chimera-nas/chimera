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

include_directories(${CMAKE_CURRENT_BINARY_DIR})

# Suppress warnings for the generated source files
set_source_files_properties(
    ${NFS3_XDR_C} ${NFS4_XDR_C} ${NFS_MOUNT_XDR_C} ${PORTMAP_XDR_C} PROPERTIES COMPILE_OPTIONS "-Wno-unused;-Wno-switch"
)

add_definitions(-DXDR_CUSTOM_IOVEC=${CMAKE_SOURCE_DIR}/ext/libevpl/src/rpc2/xdr_iovec.h)
add_definitions(-DXDR_CUSTOM_DUMP=1)

# Create libraries for NFSv3 and NFSv4
add_library(chimera_nfs SHARED nfs.c nfs4_session.c nfs_mount.c nfs_portmap.c nfs3_dump.c nfs4_dump.c

            nfs3_proc_null.c nfs3_proc_getattr.c nfs3_proc_setattr.c nfs3_proc_lookup.c 
            nfs3_proc_access.c nfs3_proc_readlink.c nfs3_proc_read.c nfs3_proc_write.c
            nfs3_proc_create.c nfs3_proc_mkdir.c nfs3_proc_remove.c nfs3_proc_mknod.c nfs3_proc_symlink.c
            nfs3_proc_rmdir.c nfs3_proc_rename.c nfs3_proc_readdir.c nfs3_proc_readdirplus.c
            nfs3_proc_link.c nfs3_proc_commit.c nfs3_proc_fsinfo.c nfs3_proc_pathconf.c nfs3_proc_fsstat.c


            nfs4_proc_getfh.c nfs4_proc_access.c nfs4_proc_putrootfh.c nfs4_proc_putfh.c
            nfs4_proc_getattr.c nfs4_proc_lookup.c nfs4_proc_compound.c nfs4_proc_null.c
            nfs4_proc_readdir.c nfs4_proc_close.c nfs4_proc_create.c  nfs4_proc_open.c
            nfs4_proc_setclientid.c nfs4_proc_setclientid_confirm.c nfs4_proc_remove.c
            nfs4_proc_read.c nfs4_proc_write.c nfs4_proc_commit.c nfs4_proc_readlink.c
            nfs4_proc_setattr.c nfs4_proc_link.c nfs4_proc_rename.c nfs4_proc_savefh.c
            nfs4_proc_exchange_id.c nfs4_proc_create_session.c nfs4_proc_destroy_session.c
            nfs4_proc_destroy_clientid.c nfs4_proc_sequence.c nfs4_proc_reclaim_complete.c
            nfs4_proc_secinfo_no_name.c
            ${NFS3_XDR_C} ${NFS3_XDR_H} ${NFS4_XDR_C} ${NFS4_XDR_H} ${NFS_MOUNT_XDR_C} ${NFS_MOUNT_XDR_H} ${PORTMAP_XDR_C} ${PORTMAP_XDR_H})

target_link_libraries(chimera_nfs chimera_common evpl evpl_rpc2 pthread uuid)

install(TARGETS chimera_nfs DESTINATION lib)

# Ensure xdrzcc is built before these libraries
add_dependencies(chimera_nfs xdrzcc)

