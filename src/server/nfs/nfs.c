// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only


#include <pthread.h>
#include <utlist.h>

#include "nfs.h"
#include "server/protocol.h"
#include "server/server.h"
#include "vfs/vfs.h"
#include "evpl/evpl_rpc2.h"
#include "nfs3_procs.h"
#include "nfs4_procs.h"
#include "nfs_mount.h"
#include "nfs_portmap.h"
#include "nfs_common.h"
#include "nfs_internal.h"
#include "prometheus-c.h"

#include "common/macros.h"

#define NFS_PROGIDX_PORTMAP_V2 0
#define NFS_PROGIDX_MOUNT_V3   1
#define NFS_PROGIDX_V3         2
#define NFS_PROGIDX_V4         3
#define NFS_PROGIDX_V4_CB      4
#define NFS_PROGIDX_MAX        5

static void
chimera_nfs_init_metrics(
    struct chimera_server_nfs_shared *shared,
    struct evpl_rpc2_program         *program)
{
    program->metrics = calloc(program->maxproc + 1, sizeof(struct prometheus_histogram_series *));

    for (int i = 0; i <= program->maxproc; i++) {
        program->metrics[i] = prometheus_histogram_create_series(shared->op_histogram,
                                                                 (const char *[]) { "name" },
                                                                 (const char *[]) { program->procs[i] },
                                                                 1);
    }
} /* chimera_nfs_init_metrics */

static void *
nfs_server_init(
    const struct chimera_server_config *config,
    struct chimera_vfs                 *vfs,
    struct prometheus_metrics          *metrics)
{
    struct chimera_server_nfs_shared *shared;
    struct timespec                   now;
    struct evpl_rpc2_program         *programs[3];
    int                               nfs_rdma;
    const char                       *nfs_rdma_hostname;
    int                               nfs_rdma_port;


    nfs_rdma          = chimera_server_config_get_nfs_rdma(config);
    nfs_rdma_hostname = chimera_server_config_get_nfs_rdma_hostname(config);
    nfs_rdma_port     = chimera_server_config_get_nfs_rdma_port(config);

    clock_gettime(CLOCK_REALTIME, &now);

    shared = calloc(1, sizeof(*shared));

    shared->config = config;

    shared->vfs = vfs;

    shared->nfs_verifier = now.tv_sec * 1000000000ULL + now.tv_nsec;

    chimera_nfs_abort_if(sizeof(shared->nfs_verifier) != NFS3_WRITEVERFSIZE,
                         "nfs_verifier size mismatch");

    NFS_PORTMAP_V2_init(&shared->portmap_v2);
    NFS_MOUNT_V3_init(&shared->mount_v3);
    NFS_V3_init(&shared->nfs_v3);
    NFS_V4_init(&shared->nfs_v4);
    NFS_V4_CB_init(&shared->nfs_v4_cb);

    shared->metrics      = metrics;
    shared->op_histogram = prometheus_metrics_create_histogram_exponential(metrics, "chimera_nfs_op_latency",
                                                                           "The latency of NFS operations", 24);


    chimera_nfs_init_metrics(shared, &shared->portmap_v2.rpc2);
    chimera_nfs_init_metrics(shared, &shared->mount_v3.rpc2);
    chimera_nfs_init_metrics(shared, &shared->nfs_v3.rpc2);
    chimera_nfs_init_metrics(shared, &shared->nfs_v4.rpc2);
    chimera_nfs_init_metrics(shared, &shared->nfs_v4_cb.rpc2);

    shared->mount_v3.recv_call_MOUNTPROC3_NULL    = chimera_nfs_mount_null;
    shared->mount_v3.recv_call_MOUNTPROC3_MNT     = chimera_nfs_mount_mnt;
    shared->mount_v3.recv_call_MOUNTPROC3_DUMP    = chimera_nfs_mount_dump;
    shared->mount_v3.recv_call_MOUNTPROC3_UMNT    = chimera_nfs_mount_umnt;
    shared->mount_v3.recv_call_MOUNTPROC3_UMNTALL = chimera_nfs_mount_umntall;
    shared->mount_v3.recv_call_MOUNTPROC3_EXPORT  = chimera_nfs_mount_export;

    shared->portmap_v2.recv_call_PMAPPROC_NULL    = chimera_portmap_null;
    shared->portmap_v2.recv_call_PMAPPROC_GETPORT = chimera_portmap_getport;

    shared->nfs_v3.recv_call_NFSPROC3_NULL        = chimera_nfs3_null;
    shared->nfs_v3.recv_call_NFSPROC3_GETATTR     = chimera_nfs3_getattr;
    shared->nfs_v3.recv_call_NFSPROC3_SETATTR     = chimera_nfs3_setattr;
    shared->nfs_v3.recv_call_NFSPROC3_LOOKUP      = chimera_nfs3_lookup;
    shared->nfs_v3.recv_call_NFSPROC3_ACCESS      = chimera_nfs3_access;
    shared->nfs_v3.recv_call_NFSPROC3_READLINK    = chimera_nfs3_readlink;
    shared->nfs_v3.recv_call_NFSPROC3_READ        = chimera_nfs3_read;
    shared->nfs_v3.recv_call_NFSPROC3_WRITE       = chimera_nfs3_write;
    shared->nfs_v3.recv_call_NFSPROC3_MKDIR       = chimera_nfs3_mkdir;
    shared->nfs_v3.recv_call_NFSPROC3_MKNOD       = chimera_nfs3_mknod;
    shared->nfs_v3.recv_call_NFSPROC3_CREATE      = chimera_nfs3_create;
    shared->nfs_v3.recv_call_NFSPROC3_REMOVE      = chimera_nfs3_remove;
    shared->nfs_v3.recv_call_NFSPROC3_RMDIR       = chimera_nfs3_rmdir;
    shared->nfs_v3.recv_call_NFSPROC3_RENAME      = chimera_nfs3_rename;
    shared->nfs_v3.recv_call_NFSPROC3_LINK        = chimera_nfs3_link;
    shared->nfs_v3.recv_call_NFSPROC3_SYMLINK     = chimera_nfs3_symlink;
    shared->nfs_v3.recv_call_NFSPROC3_READDIR     = chimera_nfs3_readdir;
    shared->nfs_v3.recv_call_NFSPROC3_READDIRPLUS = chimera_nfs3_readdirplus;
    shared->nfs_v3.recv_call_NFSPROC3_FSSTAT      = chimera_nfs3_fsstat;
    shared->nfs_v3.recv_call_NFSPROC3_FSINFO      = chimera_nfs3_fsinfo;
    shared->nfs_v3.recv_call_NFSPROC3_PATHCONF    = chimera_nfs3_pathconf;
    shared->nfs_v3.recv_call_NFSPROC3_COMMIT      = chimera_nfs3_commit;

    shared->nfs_v4.recv_call_NFSPROC4_NULL     = chimera_nfs4_null;
    shared->nfs_v4.recv_call_NFSPROC4_COMPOUND = chimera_nfs4_compound;

    nfs4_client_table_init(&shared->nfs4_shared_clients);

    shared->mount_endpoint   = evpl_endpoint_create("0.0.0.0", 20048);
    shared->portmap_endpoint = evpl_endpoint_create("0.0.0.0", 111);
    shared->nfs_endpoint     = evpl_endpoint_create("0.0.0.0", 2049);

    if (nfs_rdma) {
        shared->nfs_rdma_endpoint = evpl_endpoint_create(nfs_rdma_hostname, nfs_rdma_port);
    }

    programs[0] = &shared->mount_v3.rpc2;

    shared->mount_server = evpl_rpc2_server_init(programs, 1);

    programs[0] = &shared->portmap_v2.rpc2;

    shared->portmap_server = evpl_rpc2_server_init(programs, 1);

    programs[0] = &shared->nfs_v3.rpc2;
    programs[1] = &shared->nfs_v4.rpc2;
    programs[2] = &shared->nfs_v4_cb.rpc2;

    shared->nfs_server = evpl_rpc2_server_init(programs, 3);

    return shared;
} /* nfs_server_init */

void
nfs_server_start(void *arg)
{
    struct chimera_server_nfs_shared *shared = arg;

    evpl_rpc2_server_start(shared->nfs_server, EVPL_STREAM_SOCKET_TCP, shared->nfs_endpoint);

    if (shared->nfs_rdma_endpoint) {
        evpl_rpc2_server_start(shared->nfs_server, EVPL_DATAGRAM_RDMACM_RC, shared->nfs_rdma_endpoint);
    }

    evpl_rpc2_server_start(shared->mount_server, EVPL_STREAM_SOCKET_TCP, shared->mount_endpoint);

    evpl_rpc2_server_start(shared->portmap_server, EVPL_STREAM_SOCKET_TCP, shared->portmap_endpoint);

} /* nfs_server_start */

void
nfs_server_stop(void *arg)
{
    struct chimera_server_nfs_shared *shared = arg;

    evpl_rpc2_server_stop(shared->mount_server);
    evpl_rpc2_server_stop(shared->portmap_server);
    evpl_rpc2_server_stop(shared->nfs_server);

} /* nfs_server_stop */

static void
nfs_server_destroy(void *data)
{
    struct chimera_server_nfs_shared *shared = data;

    /* Close out all the nfs4 session state */
    nfs4_client_table_free(&shared->nfs4_shared_clients);

    if (shared->op_histogram) {
        prometheus_histogram_destroy(shared->metrics, shared->op_histogram);
    }

    evpl_rpc2_server_destroy(shared->mount_server);
    evpl_rpc2_server_destroy(shared->portmap_server);
    evpl_rpc2_server_destroy(shared->nfs_server);

    free(shared->portmap_v2.rpc2.metrics);
    free(shared->mount_v3.rpc2.metrics);
    free(shared->nfs_v3.rpc2.metrics);
    free(shared->nfs_v4.rpc2.metrics);
    free(shared->nfs_v4_cb.rpc2.metrics);


    free(shared);
} /* nfs_server_destroy */

static void *
nfs_server_thread_init(
    struct evpl               *evpl,
    struct chimera_vfs_thread *vfs_thread,
    void                      *data)
{
    struct chimera_server_nfs_shared *shared = data;
    struct chimera_server_nfs_thread *thread;

    thread         = calloc(1, sizeof(*thread));
    thread->evpl   = evpl;
    thread->shared = data;

    thread->vfs        = shared->vfs;
    thread->vfs_thread = vfs_thread;

    thread->nfs_server_thread = evpl_rpc2_thread_init(evpl, NULL, 0, NULL, thread);

    evpl_rpc2_server_attach(thread->nfs_server_thread, shared->mount_server, thread);
    evpl_rpc2_server_attach(thread->nfs_server_thread, shared->portmap_server, thread);
    evpl_rpc2_server_attach(thread->nfs_server_thread, shared->nfs_server, thread);

    return thread;
} /* nfs_server_thread_init */

static void
nfs_server_thread_destroy(void *data)
{
    struct chimera_server_nfs_thread *thread = data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_request               *req;

    evpl_rpc2_server_detach(thread->nfs_server_thread, shared->mount_server);
    evpl_rpc2_server_detach(thread->nfs_server_thread, shared->portmap_server);
    evpl_rpc2_server_detach(thread->nfs_server_thread, shared->nfs_server);

    evpl_rpc2_thread_destroy(thread->nfs_server_thread);

    while (thread->free_requests) {
        req = thread->free_requests;
        LL_DELETE(thread->free_requests, req);
        free(req);
    }

    free(thread);
} /* nfs_server_thread_destroy */

SYMBOL_EXPORT struct chimera_server_protocol nfs_protocol = {
    .init           = nfs_server_init,
    .destroy        = nfs_server_destroy,
    .start          = nfs_server_start,
    .stop           = nfs_server_stop,
    .thread_init    = nfs_server_thread_init,
    .thread_destroy = nfs_server_thread_destroy,
};
