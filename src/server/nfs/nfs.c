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
#include "nfs_external_portmap.h"

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
    int                               nfs_tcp_rdma_port;
    int                               external_portmap;

    nfs_rdma          = chimera_server_config_get_nfs_rdma(config);
    nfs_rdma_hostname = chimera_server_config_get_nfs_rdma_hostname(config);
    nfs_rdma_port     = chimera_server_config_get_nfs_rdma_port(config);
    nfs_tcp_rdma_port = chimera_server_config_get_nfs_tcp_rdma_port(config);
    external_portmap  = chimera_server_config_get_external_portmap(config);
    chimera_nfs_debug("NFS RDMA: %s", nfs_rdma ? "enabled" : "disabled");
    chimera_nfs_debug("NFS TCP-RDMA: %s (port %d)", nfs_tcp_rdma_port > 0 ? "enabled" : "disabled", nfs_tcp_rdma_port);
    chimera_nfs_debug("External Portmap: %s", external_portmap ? "enabled" : "disabled");

    clock_gettime(CLOCK_REALTIME, &now);

    shared = calloc(1, sizeof(*shared));

    shared->config = config;

    shared->vfs = vfs;

    shared->nfs_verifier = now.tv_sec * 1000000000ULL + now.tv_nsec;

    chimera_nfs_abort_if(sizeof(shared->nfs_verifier) != NFS3_WRITEVERFSIZE,
                         "nfs_verifier size mismatch");

    NFS_MOUNT_V3_init(&shared->mount_v3);
    NFS_V3_init(&shared->nfs_v3);
    NFS_V4_init(&shared->nfs_v4);
    NFS_V4_CB_init(&shared->nfs_v4_cb);

    shared->metrics      = metrics;
    shared->op_histogram = prometheus_metrics_create_histogram_exponential(metrics, "chimera_nfs_op_latency",
                                                                           "The latency of NFS operations", 24);

    if (!external_portmap) {
        /* PORTMAP V2 */
        PORTMAP_V2_init(&shared->portmap_v2);
        chimera_nfs_init_metrics(shared, &shared->portmap_v2.rpc2);
        shared->portmap_v2.recv_call_PMAPPROC_NULL    = chimera_portmap_null_v2;
        shared->portmap_v2.recv_call_PMAPPROC_GETPORT = chimera_portmap_getport_v2;
        shared->portmap_v2.recv_call_PMAPPROC_DUMP    = chimera_portmap_dump_v2;

        /* PORTMAP V3 (rpcbind) */
        PORTMAP_V3_init(&shared->portmap_v3);
        chimera_nfs_init_metrics(shared, &shared->portmap_v3.rpc2);
        shared->portmap_v3.recv_call_rpcbproc_getaddr = chimera_portmap_getaddr_v3;
        shared->portmap_v3.recv_call_rpcbproc_dump    = chimera_portmap_dump_v3;

        /* PORTMAP V4 (rpcbind) */
        PORTMAP_V4_init(&shared->portmap_v4);
        chimera_nfs_init_metrics(shared, &shared->portmap_v4.rpc2);
        shared->portmap_v4.recv_call_RPCBPROC_GETADDR = chimera_portmap_getaddr_v4;
        shared->portmap_v4.recv_call_RPCBPROC_DUMP    = chimera_portmap_dump_v4;
    }

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

    shared->mount_endpoint = evpl_endpoint_create("0.0.0.0", NFS_MOUNT_PORT);
    shared->nfs_endpoint   = evpl_endpoint_create("0.0.0.0", NFS_PORT);

    if (nfs_tcp_rdma_port > 0) {
        /* TCP-RDMA enabled - use TCP-RDMA port (hostname falls back to 0.0.0.0 if not set) */
        const char *rdma_host = nfs_rdma_hostname ? nfs_rdma_hostname : "0.0.0.0";
        shared->nfs_rdma_endpoint = evpl_endpoint_create(rdma_host, nfs_tcp_rdma_port);
    } else if (nfs_rdma) {
        /* Native RDMA enabled */
        shared->nfs_rdma_endpoint = evpl_endpoint_create(nfs_rdma_hostname, nfs_rdma_port);
    }
    if (external_portmap) {
        chimera_nfs_debug("Using external portmap/rpcbind services");
        shared->portmap_server   = NULL;
        shared->portmap_endpoint = NULL;
    } else {
        chimera_nfs_debug("Initializing internal portmap support");
        shared->portmap_endpoint = evpl_endpoint_create("0.0.0.0", 111);
        programs[0]              = &shared->portmap_v2.rpc2;
        programs[1]              = &shared->portmap_v3.rpc2;
        programs[2]              = &shared->portmap_v4.rpc2;
        shared->portmap_server   = evpl_rpc2_server_init(programs, 3);
    }

    chimera_nfs_debug("Initializing NFS mountd server");
    programs[0] = &shared->mount_v3.rpc2;

    shared->mount_server = evpl_rpc2_server_init(programs, 1);

    chimera_nfs_debug("Initializing NFS server");
    programs[0] = &shared->nfs_v3.rpc2;
    programs[1] = &shared->nfs_v4.rpc2;
    programs[2] = &shared->nfs_v4_cb.rpc2;

    shared->nfs_server = evpl_rpc2_server_init(programs, 3);

    pthread_mutex_init(&shared->exports_lock, NULL);
    return shared;
} /* nfs_server_init */


void
nfs_server_start(void *arg)
{
    struct chimera_server_nfs_shared *shared = arg;
    enum evpl_protocol_id             rdma_protocol;

    evpl_rpc2_server_start(shared->nfs_server, EVPL_STREAM_SOCKET_TCP, shared->nfs_endpoint);

    if (shared->nfs_rdma_endpoint) {
        /* Use TCP-RDMA emulation when nfs_tcp_rdma_port > 0, otherwise use native RDMA */
        rdma_protocol = chimera_server_config_get_nfs_tcp_rdma_port(shared->config) > 0
                        ? EVPL_DATAGRAM_TCP_RDMA : EVPL_DATAGRAM_RDMACM_RC;
        evpl_rpc2_server_start(shared->nfs_server, rdma_protocol, shared->nfs_rdma_endpoint);
    }

    evpl_rpc2_server_start(shared->mount_server, EVPL_STREAM_SOCKET_TCP, shared->mount_endpoint);

    if (shared->portmap_server) {
        evpl_rpc2_server_start(shared->portmap_server, EVPL_STREAM_SOCKET_TCP, shared->portmap_endpoint);
    } else {
        register_nfs_rpc_services();
    }

} /* nfs_server_start */

void
nfs_server_stop(void *arg)
{
    struct chimera_server_nfs_shared *shared = arg;

    evpl_rpc2_server_stop(shared->mount_server);
    evpl_rpc2_server_stop(shared->nfs_server);
    if (shared->portmap_server) {
        evpl_rpc2_server_stop(shared->portmap_server);
    } else {
        unregister_nfs_rpc_services();
    }

} /* nfs_server_stop */

static void
nfs_server_destroy(void *data)
{
    struct chimera_server_nfs_shared *shared = data;
    struct chimera_nfs_export        *export;

    /* Close out all the nfs4 session state */
    nfs4_client_table_free(&shared->nfs4_shared_clients);

    if (shared->op_histogram) {
        prometheus_histogram_destroy(shared->metrics, shared->op_histogram);
    }

    evpl_rpc2_server_destroy(shared->mount_server);
    evpl_rpc2_server_destroy(shared->nfs_server);

    if (shared->portmap_server) {
        evpl_rpc2_server_destroy(shared->portmap_server);
        free(shared->portmap_v2.rpc2.metrics);
        free(shared->portmap_v3.rpc2.metrics);
        free(shared->portmap_v4.rpc2.metrics);
    }
    free(shared->mount_v3.rpc2.metrics);
    free(shared->nfs_v3.rpc2.metrics);
    free(shared->nfs_v4.rpc2.metrics);
    free(shared->nfs_v4_cb.rpc2.metrics);

    while (shared->exports) {
        export = shared->exports;
        LL_DELETE(shared->exports, export);
        free(export);
    }

    free(shared);
} /* nfs_server_destroy */

static void
chimera_nfs_server_notify(
    struct evpl_rpc2_thread *thread,
    struct evpl_rpc2_conn   *conn,
    struct evpl_rpc2_notify *notify,
    void                    *private_data)
{
    char local_addr[80], remote_addr[80];

    switch (notify->notify_type) {
        case EVPL_RPC2_NOTIFY_CONNECTED:
            evpl_rpc2_conn_get_local_address(conn, local_addr, sizeof(local_addr));
            evpl_rpc2_conn_get_remote_address(conn, remote_addr, sizeof(remote_addr));
            chimera_nfs_info("Client connected from %s to %s", remote_addr, local_addr);
            break;
        case EVPL_RPC2_NOTIFY_DISCONNECTED:
            evpl_rpc2_conn_get_local_address(conn, local_addr, sizeof(local_addr));
            evpl_rpc2_conn_get_remote_address(conn, remote_addr, sizeof(remote_addr));
            chimera_nfs_info("Client disconnected from %s to %s", remote_addr, local_addr);
            break;
    } /* switch */
} /* chimera_nfs_server_notify */

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

    thread->rpc2_thread = evpl_rpc2_thread_init(evpl, NULL, 0, chimera_nfs_server_notify, thread);

    evpl_rpc2_server_attach(thread->rpc2_thread, shared->mount_server, thread);
    evpl_rpc2_server_attach(thread->rpc2_thread, shared->nfs_server, thread);
    if (shared->portmap_server) {
        evpl_rpc2_server_attach(thread->rpc2_thread, shared->portmap_server, thread);
    }

    return thread;
} /* nfs_server_thread_init */

static void
nfs_server_thread_destroy(void *data)
{
    struct chimera_server_nfs_thread *thread = data;
    struct nfs_request               *req;

    evpl_rpc2_server_detach(thread->rpc2_thread, thread->shared->mount_server);
    evpl_rpc2_server_detach(thread->rpc2_thread, thread->shared->nfs_server);
    if (thread->shared->portmap_server) {
        evpl_rpc2_server_detach(thread->rpc2_thread, thread->shared->portmap_server);
    }
    evpl_rpc2_thread_destroy(thread->rpc2_thread);

    while (thread->free_requests) {
        req = thread->free_requests;
        LL_DELETE(thread->free_requests, req);
        free(req);
    }

    free(thread);
} /* nfs_server_thread_destroy */

SYMBOL_EXPORT void
chimera_nfs_add_export(
    void       *nfs_shared,
    const char *name,
    const char *path)
{
    struct chimera_server_nfs_shared *shared = nfs_shared;
    struct chimera_nfs_export        *export = calloc(1, sizeof(*export));

    snprintf(export->name, sizeof(export->name), "%s", name);
    snprintf(export->path, sizeof(export->path), "%s", path);

    pthread_mutex_lock(&shared->exports_lock);
    LL_PREPEND(shared->exports, export);
    pthread_mutex_unlock(&shared->exports_lock);

} /* chimera_nfs_add_export */


SYMBOL_EXPORT int
chimera_nfs_remove_export(
    void       *nfs_shared,
    const char *name)
{
    struct chimera_server_nfs_shared *shared = nfs_shared;
    struct chimera_nfs_export        *export;
    int                               found = 0;

    pthread_mutex_lock(&shared->exports_lock);
    LL_FOREACH(shared->exports, export)
    {
        if (strcmp(export->name, name) == 0) {
            LL_DELETE(shared->exports, export);
            free(export);
            found = 1;
            break;
        }
    }
    pthread_mutex_unlock(&shared->exports_lock);

    return found ? 0 : -1;
} /* chimera_nfs_remove_export */

SYMBOL_EXPORT const struct chimera_nfs_export *
chimera_nfs_get_export(
    void       *nfs_shared,
    const char *name)
{
    struct chimera_server_nfs_shared *shared = nfs_shared;
    struct chimera_nfs_export        *export;

    pthread_mutex_lock(&shared->exports_lock);
    LL_FOREACH(shared->exports, export)
    {
        if (strcmp(export->name, name) == 0) {
            pthread_mutex_unlock(&shared->exports_lock);
            return export;
        }
    }
    pthread_mutex_unlock(&shared->exports_lock);

    return NULL;
} /* chimera_nfs_get_export */

SYMBOL_EXPORT void
chimera_nfs_iterate_exports(
    void                         *nfs_shared,
    chimera_nfs_export_iterate_cb callback,
    void                         *data)
{
    struct chimera_server_nfs_shared *shared = nfs_shared;
    struct chimera_nfs_export        *export;

    pthread_mutex_lock(&shared->exports_lock);
    LL_FOREACH(shared->exports, export)
    {
        if (callback(export, data) != 0) {
            break;
        }
    }
    pthread_mutex_unlock(&shared->exports_lock);
} /* chimera_nfs_iterate_exports */

SYMBOL_EXPORT const char *
chimera_nfs_export_get_name(const struct chimera_nfs_export *export)
{
    return export->name;
} /* chimera_nfs_export_get_name */

SYMBOL_EXPORT const char *
chimera_nfs_export_get_path(const struct chimera_nfs_export *export)
{
    return export->path;
} /* chimera_nfs_export_get_path */

SYMBOL_EXPORT struct chimera_server_protocol nfs_protocol = {
    .init           = nfs_server_init,
    .destroy        = nfs_server_destroy,
    .start          = nfs_server_start,
    .stop           = nfs_server_stop,
    .thread_init    = nfs_server_thread_init,
    .thread_destroy = nfs_server_thread_destroy,
};
