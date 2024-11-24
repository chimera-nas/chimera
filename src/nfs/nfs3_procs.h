#pragma once

#include "nfs_common.h"

void chimera_nfs3_null(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_getattr(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    GETATTR3args          *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_setattr(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    SETATTR3args          *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_lookup(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    LOOKUP3args           *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_access(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    ACCESS3args           *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_readlink(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    READLINK3args         *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_read(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    READ3args             *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_write(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    WRITE3args            *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_create(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    CREATE3args           *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_mkdir(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    MKDIR3args            *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_symlink(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    SYMLINK3args          *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_mknod(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    MKNOD3args            *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_remove(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    REMOVE3args           *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_rmdir(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    RMDIR3args            *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_rename(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    RENAME3args           *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_link(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    LINK3args             *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_readdir(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    READDIR3args          *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_readdirplus(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    READDIRPLUS3args      *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_fsstat(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    FSSTAT3args           *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_fsinfo(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    FSINFO3args           *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_pathconf(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    PATHCONF3args         *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_commit(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    COMMIT3args           *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

