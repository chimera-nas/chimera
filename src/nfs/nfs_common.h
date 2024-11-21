#pragma once

#include "nfs3_xdr.h"
#include "nfs4_xdr.h"
#include "nfs4_session.h"

struct chimera_server_nfs_thread;
struct nfs4_request {
    struct chimera_server_nfs_thread *thread;
    struct nfs4_session              *session;
    uint8_t                           fh[NFS4_FHSIZE];
    int                               fhlen;
    int                               index;
    COMPOUND4args                    *args;
    COMPOUND4res                      res;
    struct evpl_rpc2_conn            *conn;
    struct evpl_rpc2_msg             *msg;
    struct nfs4_request              *next;
};

struct chimera_server_nfs_shared {

    struct chimera_vfs      *vfs;

    struct NFS_V3            nfs_v3;
    struct NFS_V4            nfs_v4;
    struct NFS_V4_CB         nfs_v4_cb;

    struct nfs4_client_table nfs4_shared_clients;

};

struct chimera_server_nfs_thread {
    struct chimera_server_nfs_shared *shared;
    struct evpl                      *evpl;
    struct evpl_rpc2_agent           *rpc2_agent;
    struct evpl_rpc2_server          *server;
    struct evpl_endpoint             *endpoint;
    int                               active;
    int                               again;
    struct nfs4_request              *free_requests;
};