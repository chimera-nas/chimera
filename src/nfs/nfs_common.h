#pragma once

#include "portmap_xdr.h"
#include "nfs_mount_xdr.h"
#include "nfs3_xdr.h"
#include "nfs4_xdr.h"
#include "nfs4_session.h"
#include "uthash/utlist.h"
struct chimera_server_nfs_thread;

struct nfs_request {
    struct chimera_server_nfs_thread *thread;
    struct nfs4_session              *session;
    uint8_t                           fh[NFS4_FHSIZE];
    int                               fhlen;
    int                               index;
    struct evpl_rpc2_conn            *conn;
    struct evpl_rpc2_msg             *msg;
    struct nfs_request               *next;
    union {
        struct mountargs3       *args_mount;
        struct LOOKUP3args      *args_lookup;
        struct GETATTR3args     *args_getattr;
        struct READDIR3args     *args_readdir;
        struct READDIRPLUS3args *args_readdirplus;
        struct FSINFO3args      *args_fsinfo;
        struct COMPOUND4args    *args_compound;

    };
    union {
        struct READDIR3res     res_readdir;
        struct READDIRPLUS3res res_readdirplus;
        struct COMPOUND4res    res_compound;
    };
};

struct chimera_server_nfs_shared {

    struct chimera_vfs      *vfs;

    struct NFS_PORTMAP_V2    portmap_v2;
    struct NFS_MOUNT_V3      mount_v3;
    struct NFS_V3            nfs_v3;
    struct NFS_V4            nfs_v4;
    struct NFS_V4_CB         nfs_v4_cb;

    struct nfs4_client_table nfs4_shared_clients;

};

struct chimera_server_nfs_thread {
    struct chimera_server_nfs_shared *shared;
    struct chimera_vfs_thread        *vfs;
    struct evpl                      *evpl;
    struct evpl_rpc2_agent           *rpc2_agent;
    struct evpl_rpc2_server          *nfs_server;
    struct evpl_rpc2_server          *mount_server;
    struct evpl_rpc2_server          *portmap_server;
    struct evpl_endpoint             *nfs_endpoint;
    struct evpl_endpoint             *mount_endpoint;
    struct evpl_endpoint             *portmap_endpoint;
    int                               active;
    int                               again;
    struct nfs_request               *free_requests;
};

static inline struct nfs_request *
nfs_request_alloc(
    struct chimera_server_nfs_thread *thread,
    struct evpl_rpc2_conn            *conn,
    struct evpl_rpc2_msg             *msg)
{
    struct nfs_request *req;

    if (thread->free_requests) {
        req = thread->free_requests;
        LL_DELETE(thread->free_requests, req);
    } else {
        req         = calloc(1, sizeof(*req));
        req->thread = thread;
    }


    req->conn = conn;
    req->msg  = msg;

    return req;
} /* nfs_request_alloc */

static inline void
nfs_request_free(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req)
{
    LL_PREPEND(thread->free_requests, req);
} /* nfs_request_free */
