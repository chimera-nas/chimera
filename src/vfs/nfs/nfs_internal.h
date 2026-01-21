// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <string.h>
#include <pthread.h>
#include <utlist.h>
#include "vfs/vfs.h"
#include "evpl/evpl.h"
#include "portmap_xdr.h"
#include "nfs_mount_xdr.h"
#include "nfs3_xdr.h"
#include "nfs4_xdr.h"
#include "uthash.h"
#include "common/misc.h"
#include "vfs/vfs_fh.h"
#include "evpl/evpl_rpc2_cred.h"

#define chimera_nfsclient_debug(...) chimera_debug("nfsclient", \
                                                   __FILE__, \
                                                   __LINE__, \
                                                   __VA_ARGS__)
#define chimera_nfsclient_info(...)  chimera_info("nfsclient", \
                                                  __FILE__, \
                                                  __LINE__, \
                                                  __VA_ARGS__)
#define chimera_nfsclient_error(...) chimera_error("nfsclient", \
                                                   __FILE__, \
                                                   __LINE__, \
                                                   __VA_ARGS__)
#define chimera_nfsclient_fatal(...) chimera_fatal("nfsclient", \
                                                   __FILE__, \
                                                   __LINE__, \
                                                   __VA_ARGS__)
#define chimera_nfsclient_abort(...) chimera_abort("nfsclient", \
                                                   __FILE__, \
                                                   __LINE__, \
                                                   __VA_ARGS__)

#define chimera_nfsclient_fatal_if(cond, ...) \
        chimera_fatal_if(cond, "nfsclient", __FILE__, __LINE__, __VA_ARGS__)

#define chimera_nfsclient_abort_if(cond, ...) \
        chimera_abort_if(cond, "nfsclient", __FILE__, __LINE__, __VA_ARGS__)

enum chimera_nfs_client_server_state {
    CHIMERA_NFS_CLIENT_SERVER_STATE_DISCOVERING,
    CHIMERA_NFS_CLIENT_SERVER_STATE_DISCOVERED,
};

enum chimera_nfs_client_mount_state {
    CHIMERA_NFS_CLIENT_MOUNT_STATE_MOUNTING,
    CHIMERA_NFS_CLIENT_MOUNT_STATE_MOUNTED,
};

struct chimera_nfs_client_mount;

struct chimera_nfs_client_server_thread {
    struct chimera_nfs_thread        *thread;
    struct chimera_nfs_shared        *shared;
    struct chimera_nfs_client_server *server;

    struct evpl_rpc2_conn            *portmap_conn;
    struct evpl_rpc2_conn            *mount_conn;
    struct evpl_rpc2_conn            *nfs_conn;
};

struct chimera_nfs_client_server {
    struct chimera_nfs_shared  *shared;
    int                         state;
    int                         refcnt;
    int                         nfsvers;
    int                         index;
    int                         use_rdma;
    enum evpl_protocol_id       rdma_protocol;

    struct evpl_endpoint       *portmap_endpoint;
    struct evpl_endpoint       *mount_endpoint;
    struct evpl_endpoint       *nfs_endpoint;

    uint16_t                    mount_port;
    uint16_t                    nfs_port;

    struct chimera_vfs_request *pending_mounts;

    char                        hostname[256];

};

struct chimera_nfs_client_mount {
    int                               status;
    int                               nfsvers;
    struct chimera_nfs_client_server *server;
    struct chimera_nfs_client_mount  *prev;
    struct chimera_nfs_client_mount  *next;
    struct chimera_vfs_request       *mount_request;
    char                              path[CHIMERA_VFS_PATH_MAX];
};

struct chimera_nfs_client_open_handle {
    int                                    dirty;
    struct chimera_nfs_client_open_handle *next;
};

struct chimera_nfs_shared {
    struct chimera_nfs_client_mount   *mounts;

    struct chimera_nfs_client_server **servers;
    struct chimera_nfs_client_server  *servers_map;
    int                                max_servers;
    pthread_mutex_t                    lock;

    struct PORTMAP_V2                  portmap_v2;
    struct NFS_MOUNT_V3                mount_v3;
    struct NFS_V3                      nfs_v3;
    struct NFS_V4                      nfs_v4;
    struct NFS_V4_CB                   nfs_v4_cb;

    struct prometheus_histogram       *op_histogram;
    struct prometheus_metrics         *metrics;
};

struct chimera_nfs_thread {
    struct evpl                              *evpl;
    struct chimera_nfs_shared                *shared;
    struct evpl_rpc2_thread                  *rpc2_thread;
    struct chimera_nfs_client_server_thread **server_threads;
    struct chimera_nfs_client_open_handle    *free_open_handles;
    int                                       max_server_threads;
};

static inline struct chimera_nfs_client_open_handle *
chimera_nfs_thread_open_handle_alloc(struct chimera_nfs_thread *thread)
{
    struct chimera_nfs_client_open_handle *open_handle = thread->free_open_handles;

    if (open_handle) {
        LL_DELETE(thread->free_open_handles, open_handle);
    } else {
        open_handle = calloc(1, sizeof(*open_handle));
    }

    return open_handle;
} // chimera_nfs_thread_open_handle_alloc

static inline void
chimera_nfs_thread_open_handle_free(
    struct chimera_nfs_thread             *thread,
    struct chimera_nfs_client_open_handle *open_handle)
{
    LL_PREPEND(thread->free_open_handles, open_handle);
} // chimera_nfs_thread_open_handle_free

static inline struct chimera_nfs_client_server_thread *
chimera_nfs_thread_get_server_thread(
    struct chimera_nfs_thread *thread,
    const uint8_t             *fh,
    int                        fhlen)
{
    struct chimera_nfs_client_server_thread *server_thread = NULL;
    int                                      index;

    if (unlikely(fhlen < CHIMERA_VFS_MOUNT_ID_SIZE + 1)) {
        return NULL;
    }

    /* Server index is at position CHIMERA_VFS_MOUNT_ID_SIZE (first byte of fh_fragment) */
    index = fh[CHIMERA_VFS_MOUNT_ID_SIZE];

    if (unlikely(index > thread->shared->max_servers)) {
        return NULL;
    }

    if (unlikely(thread->max_server_threads != thread->shared->max_servers && index >= thread->max_server_threads)) {
        thread->max_server_threads = thread->shared->max_servers;
        thread->server_threads     = realloc(thread->server_threads,
                                             thread->max_server_threads * sizeof(*thread->server_threads));
    }

    if (unlikely(!thread->server_threads[index])) {
        thread->server_threads[index]         = calloc(1, sizeof(*thread->server_threads[index]));
        thread->server_threads[index]->thread = thread;
        thread->server_threads[index]->shared = thread->shared;
        thread->server_threads[index]->server = thread->shared->servers[index];
    }

    server_thread = thread->server_threads[index];

    if (unlikely(!server_thread->nfs_conn)) {
        enum evpl_protocol_id proto = server_thread->server->use_rdma
                                      ? server_thread->server->rdma_protocol
                                      : EVPL_STREAM_SOCKET_TCP;
        server_thread->nfs_conn = evpl_rpc2_client_connect(thread->rpc2_thread,
                                                           proto,
                                                           server_thread->server->nfs_endpoint,
                                                           NULL, 0, NULL);
    }

    return server_thread;
} // chimera_nfs_thread_get_server_thread

static inline void
chimera_nfs3_map_fh(
    const uint8_t *fh,
    int            fhlen,
    uint8_t      **mapped_fh,
    int           *mapped_fhlen)
{
    /* Skip mount_id (16 bytes) + server_index (1 byte) to get remote NFS fh */
    *mapped_fh    = (uint8_t *) fh + CHIMERA_VFS_MOUNT_ID_SIZE + 1;
    *mapped_fhlen = fhlen - CHIMERA_VFS_MOUNT_ID_SIZE - 1;
} // chimera_nfs3_map_fh

/*
 * Initialize an RPC2 credential for AUTH_SYS from a VFS credential.
 * The RPC2 cred is stack-allocated by the caller.
 *
 * @param rpc2_cred     Pointer to stack-allocated evpl_rpc2_cred
 * @param vfs_cred      VFS credential with uid/gid/groups
 * @param machine_name  Machine name string (from chimera_vfs)
 * @param machine_name_len Length of machine name
 */
static inline void
chimera_nfs_init_rpc2_cred(
    struct evpl_rpc2_cred         *rpc2_cred,
    const struct chimera_vfs_cred *vfs_cred,
    const char                    *machine_name,
    int                            machine_name_len)
{
    uint32_t ngids;

    rpc2_cred->flavor = EVPL_RPC2_AUTH_SYS;
    rpc2_cred->key    = 0;
    rpc2_cred->next   = NULL;

    /* Handle NULL credential - use root (uid=0, gid=0) */
    if (!vfs_cred) {
        rpc2_cred->authsys.uid      = 0;
        rpc2_cred->authsys.gid      = 0;
        rpc2_cred->authsys.num_gids = 0;
        rpc2_cred->authsys.gids     = NULL;
    } else {
        rpc2_cred->authsys.uid = vfs_cred->uid;
        rpc2_cred->authsys.gid = vfs_cred->gid;

        /* Assign pointer to gids array (valid for duration of call) */
        ngids = vfs_cred->ngids;
        if (ngids > EVPL_RPC2_AUTH_SYS_MAX_GIDS) {
            ngids = EVPL_RPC2_AUTH_SYS_MAX_GIDS;
        }
        rpc2_cred->authsys.num_gids = ngids;
        rpc2_cred->authsys.gids     = (uint32_t *) vfs_cred->gids;
    }

    /* Assign pointer to machine name (valid for duration of call) */
    rpc2_cred->authsys.machinename     = machine_name;
    rpc2_cred->authsys.machinename_len = machine_name_len;
} /* chimera_nfs_init_rpc2_cred */

void chimera_nfs3_dispatch(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_dispatch(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);

void chimera_nfs3_mount(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);

void chimera_nfs3_umount(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);

void chimera_nfs3_lookup(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_getattr(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_setattr(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_mkdir(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_remove(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_readdir(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_open(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_open_at(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_close(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_read(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_write(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_commit(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_symlink(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_readlink(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_rename(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs3_link(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);

void chimera_nfs4_mount(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_lookup(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_getattr(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_setattr(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_mkdir(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_remove(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_readdir(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_open(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_open_at(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_close(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_read(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_write(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_commit(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_symlink(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_readlink(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_rename(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void chimera_nfs4_link(
    struct chimera_nfs_thread *,
    struct chimera_nfs_shared *,
    struct chimera_vfs_request *,
    void *);
