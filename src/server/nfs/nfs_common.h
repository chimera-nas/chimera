// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <utlist.h>

#include "evpl/evpl_rpc2.h"
#include "portmap_xdr.h"
#include "vfs/vfs_cred.h"
#include "nfs_mount_xdr.h"
#include "nfs3_xdr.h"
#include "nfs4_xdr.h"
#include "nfs4_session.h"

struct chimera_server_nfs_thread;

struct nfs_nfs3_readdir_cursor {
    uint64_t       count;
    struct entry3 *entries;
    struct entry3 *last;
};

struct nfs_nfs3_readdirplus_cursor {
    uint64_t           count;
    struct entryplus3 *entries;
    struct entryplus3 *last;
};

struct nfs_nfs4_readdir_cursor {
    uint64_t       count;
    struct entry4 *entries;
    struct entry4 *last;
};

struct nfs_request {
    struct chimera_server_nfs_thread *thread;
    struct nfs4_session              *session;
    struct chimera_vfs_cred           cred;
    uint8_t                           fh[NFS4_FHSIZE];
    int                               fhlen;
    uint8_t                           saved_fh[NFS4_FHSIZE];
    int                               saved_fhlen;
    struct chimera_vfs_open_handle   *handle;
    int                               index;
    struct evpl_rpc2_conn            *conn;
    struct evpl_rpc2_encoding        *encoding;
    struct nfs_request               *next;
    union {
        struct mountargs3       *args_mount;
        struct ACCESS3args      *args_access;
        struct LOOKUP3args      *args_lookup;
        struct CREATE3args      *args_create;
        struct GETATTR3args     *args_getattr;
        struct FSSTAT3args      *args_fsstat;
        struct READ3args        *args_read;
        struct READDIR3args     *args_readdir;
        struct READDIRPLUS3args *args_readdirplus;
        struct FSINFO3args      *args_fsinfo;
        struct WRITE3args       *args_write;
        struct COMMIT3args      *args_commit;
        struct COMPOUND4args    *args_compound;
        struct RMDIR3args       *args_rmdir;
        struct REMOVE3args      *args_remove;
        struct MKDIR3args       *args_mkdir;
        struct SYMLINK3args     *args_symlink;
        struct SETATTR3args     *args_setattr;
        struct READLINK3args    *args_readlink;
        struct MKNOD3args       *args_mknod;
        struct WRITE4args       *args_write4;
    };
    union {
        struct READLINK3res    res_readlink;
        struct READDIR3res     res_readdir;
        struct READDIRPLUS3res res_readdirplus;
        struct COMPOUND4res    res_compound;
    };
    union {
        struct nfs_nfs3_readdir_cursor     readdir3_cursor;
        struct nfs_nfs3_readdirplus_cursor readdirplus3_cursor;
        struct nfs_nfs4_readdir_cursor     readdir4_cursor;
    };

};

struct chimera_nfs_export {
    char                       name[CHIMERA_VFS_NAME_MAX];
    char                       path[CHIMERA_VFS_PATH_MAX];
    struct chimera_nfs_export *prev;
    struct chimera_nfs_export *next;
};

struct chimera_server_nfs_shared {

    const struct chimera_server_config *config;
    struct chimera_vfs                 *vfs;

    struct PORTMAP_V2                   portmap_v2;
    struct PORTMAP_V3                   portmap_v3;
    struct PORTMAP_V4                   portmap_v4;
    struct NFS_MOUNT_V3                 mount_v3;
    struct NFS_V3                       nfs_v3;
    struct NFS_V4                       nfs_v4;
    struct NFS_V4_CB                    nfs_v4_cb;

    struct chimera_nfs_export          *exports;
    pthread_mutex_t                     exports_lock;
    struct evpl_endpoint               *nfs_endpoint;
    struct evpl_endpoint               *mount_endpoint;
    struct evpl_endpoint               *portmap_endpoint;
    struct evpl_endpoint               *nfs_rdma_endpoint;

    struct evpl_rpc2_server            *portmap_server;
    struct evpl_rpc2_server            *mount_server;
    struct evpl_rpc2_server            *nfs_server;
    struct evpl_rpc2_server            *nfs_rdma_server;

    uint64_t                            nfs_verifier;

    struct nfs4_client_table            nfs4_shared_clients;

    struct prometheus_histogram        *op_histogram;
    struct prometheus_metrics          *metrics;
};

struct chimera_server_nfs_thread {
    struct evpl_rpc2_thread          *rpc2_thread;
    struct chimera_server_nfs_shared *shared;
    struct chimera_vfs_thread        *vfs_thread;
    struct chimera_vfs               *vfs;
    struct evpl                      *evpl;
    struct evpl_rpc2_thread          *nfs_server_thread;
    struct evpl_rpc2_thread          *mount_server_thread;
    struct evpl_rpc2_thread          *portmap_server_thread;
    int                               active;
    int                               again;
    struct nfs_request               *free_requests;
};

static inline struct nfs_request *
nfs_request_alloc(
    struct chimera_server_nfs_thread *thread,
    struct evpl_rpc2_conn            *conn,
    struct evpl_rpc2_encoding        *encoding)
{
    struct nfs_request *req;

    if (thread->free_requests) {
        req = thread->free_requests;
        LL_DELETE(thread->free_requests, req);
    } else {
        req         = calloc(1, sizeof(*req));
        req->thread = thread;
    }


    req->conn     = conn;
    req->encoding = encoding;

    return req;
} /* nfs_request_alloc */

static inline void
nfs_request_free(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req)
{
    LL_PREPEND(thread->free_requests, req);
} /* nfs_request_free */

/*
 * Map RPC2 credentials to VFS credentials.
 *
 * This converts the protocol-specific evpl_rpc2_cred to the
 * protocol-independent chimera_vfs_cred used by the VFS layer.
 *
 * For AUTH_SYS, UNIX credentials are extracted directly.
 * For AUTH_NONE or unknown, anonymous credentials are used.
 *
 * @param vfs_cred    Output VFS credentials
 * @param rpc_cred    Input RPC credentials
 */
static inline void
chimera_nfs_map_cred(
    struct chimera_vfs_cred     *vfs_cred,
    const struct evpl_rpc2_cred *rpc_cred)
{
    if (!rpc_cred || rpc_cred->flavor == EVPL_RPC2_AUTH_NONE) {
        /* Anonymous credentials */
        chimera_vfs_cred_init_anonymous(vfs_cred,
                                        CHIMERA_VFS_ANON_UID,
                                        CHIMERA_VFS_ANON_GID);
    } else if (rpc_cred->flavor == EVPL_RPC2_AUTH_SYS) {
        /* UNIX credentials */
        chimera_vfs_cred_init_unix(vfs_cred,
                                   rpc_cred->authsys.uid,
                                   rpc_cred->authsys.gid,
                                   rpc_cred->authsys.num_gids,
                                   rpc_cred->authsys.gids);
    } else {
        /* Unknown auth flavor - use anonymous */
        chimera_vfs_cred_init_anonymous(vfs_cred,
                                        CHIMERA_VFS_ANON_UID,
                                        CHIMERA_VFS_ANON_GID);
    }
} /* chimera_nfs_map_cred */
