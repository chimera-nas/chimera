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
    struct GETATTR3args   *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_setattr(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct SETATTR3args   *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_lookup(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct LOOKUP3args    *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_access(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct ACCESS3args    *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_readlink(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct READLINK3args  *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_read(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct READ3args      *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_write(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct WRITE3args     *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_create(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct CREATE3args    *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_mkdir(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct MKDIR3args     *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_symlink(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct SYMLINK3args   *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_mknod(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct MKNOD3args     *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_remove(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct REMOVE3args    *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_rmdir(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct RMDIR3args     *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_rename(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct RENAME3args    *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_link(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct LINK3args      *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_readdir(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct READDIR3args   *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_readdirplus(
    struct evpl             *evpl,
    struct evpl_rpc2_conn   *conn,
    struct READDIRPLUS3args *args,
    struct evpl_rpc2_msg    *msg,
    void                    *private_data);

void chimera_nfs3_fsstat(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct FSSTAT3args    *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_fsinfo(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct FSINFO3args    *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_pathconf(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct PATHCONF3args  *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

void chimera_nfs3_commit(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct COMMIT3args    *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data);

