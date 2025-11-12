// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <pthread.h>
#include "vfs/vfs.h"
#include "evpl/evpl.h"
#include "portmap_xdr.h"
#include "nfs_mount_xdr.h"
#include "nfs3_xdr.h"
#include "nfs4_xdr.h"
#include "uthash.h"

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

enum nfs_client_server_state {
    NFS_CLIENT_SERVER_STATE_DISCOVERING,
    NFS_CLIENT_SERVER_STATE_DISCOVERED,
};

enum nfs_client_mount_state {
    NFS_CLIENT_MOUNT_STATE_MOUNTING,
    NFS_CLIENT_MOUNT_STATE_MOUNTED,
};

struct nfs_client_mount;

struct nfs_client_server_thread {
    struct nfs_thread        *thread;
    struct nfs_shared        *shared;
    struct nfs_client_server *server;

    struct evpl_rpc2_conn    *portmap_conn;
    struct evpl_rpc2_conn    *mount_conn;
    struct evpl_rpc2_conn    *nfs_conn;
};

struct nfs_client_server {
    struct nfs_shared          *shared;
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

struct nfs_client_mount {
    int                         status;
    int                         fhlen;
    struct nfs_client_server   *server;
    struct nfs_client_mount    *prev;
    struct nfs_client_mount    *next;
    struct chimera_vfs_request *mount_request;
    uint8_t                     fh[CHIMERA_VFS_FH_SIZE];
    char                        path[CHIMERA_VFS_PATH_MAX];
};

struct nfs_shared {
    struct nfs_client_mount     *mounts;

    struct nfs_client_server   **servers;
    struct nfs_client_server    *servers_map;
    int                          max_servers;
    pthread_mutex_t              lock;

    uint32_t                     protocol_version;

    struct NFS_PORTMAP_V2        portmap_v2;
    struct NFS_MOUNT_V3          mount_v3;
    struct NFS_V3                nfs_v3;
    struct NFS_V4                nfs_v4;
    struct NFS_V4_CB             nfs_v4_cb;

    struct prometheus_histogram *op_histogram;
    struct prometheus_metrics   *metrics;
};

struct nfs_thread {
    struct evpl                      *evpl;
    struct nfs_shared                *shared;
    struct evpl_rpc2_thread          *rpc2_thread;
    struct nfs_client_server_thread **server_threads;
    int                               max_server_threads;
};

static inline struct nfs_client_server_thread *
nfs_thread_get_server_thread(
    struct nfs_thread *thread,
    const uint8_t     *fh,
    int                fhlen)
{
    if (fhlen < 2) {
        return NULL;
    }

    if (fh[0] != CHIMERA_VFS_FH_MAGIC_NFS) {
        return NULL;
    }

    int index = fh[1];

    if (index > thread->shared->max_servers) {
        return NULL;
    }

    if (thread->max_server_threads != thread->shared->max_servers && index >= thread->max_server_threads) {
        thread->max_server_threads = thread->shared->max_servers;
        thread->server_threads     = realloc(thread->server_threads,
                                             thread->max_server_threads * sizeof(*thread->server_threads));
    }

    if (!thread->server_threads[index]) {
        thread->server_threads[index]         = calloc(1, sizeof(*thread->server_threads[index]));
        thread->server_threads[index]->thread = thread;
        thread->server_threads[index]->shared = thread->shared;
        thread->server_threads[index]->server = thread->shared->servers[index];
    }

    return thread->server_threads[index];
} // nfs_thread_get_server_thread

static inline void
nfs3_map_fh(
    const uint8_t *fh,
    int            fhlen,
    uint8_t      **mapped_fh,
    int           *mapped_fhlen)
{
    *mapped_fh    = (uint8_t *) fh + 2;
    *mapped_fhlen = fhlen - 2;
} // nfs3_map_fh

void nfs3_dispatch(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs4_dispatch(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);

void nfs3_mount(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);

void nfs3_umount(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);

void nfs3_lookup(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs3_getattr(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs3_setattr(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs3_mkdir(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs3_remove(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs3_readdir(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs3_open(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs3_open_at(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs3_create_unlinked(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs3_close(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs3_read(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs3_write(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs3_commit(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs3_symlink(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs3_readlink(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs3_rename(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs3_link(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);

void nfs4_mount(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs4_lookup(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs4_getattr(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs4_setattr(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs4_mkdir(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs4_remove(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs4_readdir(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs4_open(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs4_open_at(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs4_create_unlinked(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs4_close(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs4_read(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs4_write(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs4_commit(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs4_symlink(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs4_readlink(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs4_rename(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
void nfs4_link(
    struct nfs_thread *,
    struct nfs_shared *,
    struct chimera_vfs_request *,
    void *);
