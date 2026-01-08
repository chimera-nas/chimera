// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
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

    if (unlikely(fhlen < 2)) {
        return NULL;
    }

    if (unlikely(fh[0] != CHIMERA_VFS_FH_MAGIC_NFS)) {
        return NULL;
    }

    index = fh[1];

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
        server_thread->nfs_conn = evpl_rpc2_client_connect(thread->rpc2_thread,
                                                           EVPL_STREAM_SOCKET_TCP,
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
    *mapped_fh    = (uint8_t *) fh + 2;
    *mapped_fhlen = fhlen - 2;
} // chimera_nfs3_map_fh

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
