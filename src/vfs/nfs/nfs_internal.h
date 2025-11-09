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

enum nfs_client_mount_state {
    NFS_CLIENT_MOUNT_STATE_MOUNTING,
    NFS_CLIENT_MOUNT_STATE_MOUNTED,
};

struct nfs_client_mount {
    int                         keylen;
    int                         index;
    int                         nfsvers;
    int                         refcnt;
    int                         status;

    uint8_t                     fh[CHIMERA_VFS_FH_SIZE];
    int                         fhlen;

    struct evpl_endpoint       *portmap_endpoint;
    struct evpl_endpoint       *mount_endpoint;
    struct evpl_endpoint       *nfs_endpoint;
    struct evpl_endpoint       *nfs_rdma_endpoint;

    struct evpl_rpc2_conn      *portmap_conn;
    struct evpl_rpc2_conn      *mount_conn;
    struct evpl_rpc2_conn      *nfs_conn;

    struct chimera_vfs_request *mount_requests;

    struct UT_hash_handle       hh;
    char                        key[1024];
    char                        hostname[256];
    char                        path[CHIMERA_VFS_PATH_MAX];
};

struct nfs_shared {
    struct nfs_client_mount    **mounts;
    struct nfs_client_mount     *mounts_map;
    int                          max_mounts;
    pthread_rwlock_t             mounts_lock;

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
    struct evpl             *evpl;
    struct nfs_shared       *shared;
    struct evpl_rpc2_thread *rpc2_thread;
};

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
