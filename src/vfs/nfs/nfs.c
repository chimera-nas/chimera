// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "vfs/vfs.h"
#include "nfs.h"
#include "nfs_internal.h"
#include "nfs3_open_state.h"
#include "common/logging.h"
#include "common/macros.h"
#include "evpl/evpl.h"
#include "evpl/evpl_rpc2.h"

#define CHIMERA_NFS_DEFAULT_VERSION 3
#define CHIMERA_NFS_RDMA_PORT       20049

static int
chimera_nfs_get_mount_version(const struct chimera_vfs_mount_options *options)
{
    int i;
    int vers;

    for (i = 0; i < options->num_options; i++) {
        if (strcmp(options->options[i].key, "vers") == 0) {
            if (!options->options[i].value) {
                return -1;                 /* vers requires a value */
            }
            vers = atoi(options->options[i].value);
            if (vers != 3 && vers != 4) {
                return -1;                 /* only v3 and v4 supported */
            }
            return vers;
        }
    }

    return CHIMERA_NFS_DEFAULT_VERSION;
} /* chimera_nfs_get_mount_version */

/* Get the proto mount option - returns 1 if rdma, 0 otherwise */
static int
chimera_nfs_get_mount_rdma(const struct chimera_vfs_mount_options *options)
{
    int i;

    for (i = 0; i < options->num_options; i++) {
        if (strcmp(options->options[i].key, "proto") == 0) {
            if (options->options[i].value &&
                strcmp(options->options[i].value, "rdma") == 0) {
                return 1;
            }
        }
    }

    return 0;
} /* chimera_nfs_get_mount_rdma */

/* Get the port mount option - returns the port, or default if not specified */
static int
chimera_nfs_get_mount_port(
    const struct chimera_vfs_mount_options *options,
    int                                     default_port)
{
    int i;

    for (i = 0; i < options->num_options; i++) {
        if (strcmp(options->options[i].key, "port") == 0) {
            if (options->options[i].value) {
                return atoi(options->options[i].value);
            }
        }
    }

    return default_port;
} /* chimera_nfs_get_mount_port */

static void *
chimera_nfs_init(
    const char                *cfgdata,
    struct prometheus_metrics *metrics)
{
    (void) metrics;
    struct chimera_nfs_shared *shared = calloc(1, sizeof(*shared));

    pthread_mutex_init(&shared->lock, NULL);
    pthread_mutex_init(&shared->cb_lock, NULL);

    shared->max_servers = 64;
    shared->servers     = calloc(shared->max_servers, sizeof(*shared->servers));
    shared->servers_map = NULL;

    /* Default until the common tcp_flavor is observed at mount time. */
    shared->tcp_protocol = EVPL_STREAM_SOCKET_TCP;

    PORTMAP_V2_init(&shared->portmap_v2);
    NFS_MOUNT_V3_init(&shared->mount_v3);
    NFS_V3_init(&shared->nfs_v3);
    NFS_V4_init(&shared->nfs_v4);
    NFS_V4_CB_init(&shared->nfs_v4_cb);
    /* Serve incoming CB_COMPOUND (back-channel callbacks) on connections that
     * register the NFS_V4_CB program; the back channel is activated per session
     * by the control connection (nfs4_cb.c). */
    shared->nfs_v4_cb.recv_call_CB_COMPOUND = chimera_nfs4_cb_compound;
    NLM_V4_init(&shared->nlm_v4);

    return shared;
} /* chimera_nfs_init */

static void
chimera_nfs_destroy(void *private_data)
{
    struct chimera_nfs_shared *shared = private_data;
    int                        i;

    /* Stop the back-channel control thread first: tearing down its rpc2 thread
    * disconnects every cb_conn, which fires chimera_nfs4_cb_control_notify and
    * walks shared->servers.  It must finish before those servers are freed. */
    chimera_nfs4_cb_control_stop(shared);
    pthread_mutex_destroy(&shared->cb_lock);

    for (i = 0; i < shared->max_servers; i++) {
        if (shared->servers[i]) {
            /* Free NFS4 session if present */
            if (shared->servers[i]->nfs4_session) {
                pthread_mutex_destroy(&shared->servers[i]->nfs4_session->lock);
                free(shared->servers[i]->nfs4_session);
            }
            free(shared->servers[i]);
        }
    }

    free(shared->mounts);
    free(shared->servers);

    free(shared);
} /* chimera_nfs_destroy */

const char *
chimera_nfs_protocol_to_string(enum evpl_protocol_id protocol)
{
    switch (protocol) {
        case EVPL_DATAGRAM_RDMACM_RC:
            return "RDMA";
        case EVPL_DATAGRAM_TCP_RDMA:
            return "TCP-RDMA";
        case EVPL_DATAGRAM_SOCKET_UDP:
            return "UDP";
        case EVPL_DATAGRAM_RDMACM_UD:
            return "RDMACM-UD";
        case EVPL_STREAM_SOCKET_TCP:
            return "TCP";
        case EVPL_STREAM_XLIO_TCP:
            return "XLIO-TCP";
        case EVPL_STREAM_IO_URING_TCP:
            return "IO-URING-TCP";
        case EVPL_STREAM_RDMACM_RC:
            return "RDMA-RC";
        case EVPL_STREAM_SOCKET_TLS:
            return "TLS";
        case EVPL_NUM_PROTO:
            return "UNKNOWN";
    } /* switch */
    return "UNKNOWN";
} /* chimera_nfs_protocol_to_string */

static void
chimera_nfs_notify(
    struct evpl_rpc2_thread *thread,
    struct evpl_rpc2_conn   *conn,
    struct evpl_rpc2_notify *notify,
    void                    *private_data)
{
    struct chimera_nfs_thread               *nfs_thread = private_data;
    struct chimera_nfs_client_server_thread *server_thread;
    char                                     local_addr[80], remote_addr[80];
    int                                      i;

    switch (notify->notify_type) {
        case EVPL_RPC2_NOTIFY_CONNECTED:
            evpl_rpc2_conn_get_local_address(conn, local_addr, sizeof(local_addr));
            evpl_rpc2_conn_get_remote_address(conn, remote_addr, sizeof(remote_addr));
            chimera_nfsclient_info("Connected from %s to %s", local_addr, remote_addr);
            break;
        case EVPL_RPC2_NOTIFY_DISCONNECTED:
            evpl_rpc2_conn_get_local_address(conn, local_addr, sizeof(local_addr));
            evpl_rpc2_conn_get_remote_address(conn, remote_addr, sizeof(remote_addr));
            chimera_nfsclient_info("Disconnected from %s to %s", local_addr, remote_addr);

            /* rpc2 drops in-flight calls on disconnect without firing their
             * callbacks.  Error-complete the NFS4.1 requests stranded on this
             * connection's slot table and reset it so the next op re-establishes
             * cleanly (rather than hanging and leaking slots). */
            for (i = 0; nfs_thread && i < nfs_thread->max_server_threads; i++) {
                server_thread = nfs_thread->server_threads[i];
                if (server_thread && server_thread->nfs_conn == conn) {
                    chimera_nfs4_slot_table_reset(nfs_thread->evpl, &server_thread->slots);
                    break;
                }
            }
            break;
    } /* switch */
} /* chimera_nfs_notify */

static void *
chimera_nfs_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_nfs_shared *shared = private_data;
    struct chimera_nfs_thread *thread = calloc(1, sizeof(*thread));
    struct evpl_rpc2_program  *programs[6];

    thread->shared = shared;
    thread->evpl   = evpl;

    /* Count client threads so the NFS4.1 fore-channel slot pool can be
     * partitioned evenly (max_slots / nfs_thread_count) per thread. */
    atomic_fetch_add(&shared->nfs_thread_count, 1);

    programs[0] = &shared->mount_v3.rpc2;
    programs[1] = &shared->portmap_v2.rpc2;
    programs[2] = &shared->nfs_v3.rpc2;
    programs[3] = &shared->nfs_v4.rpc2;
    programs[4] = &shared->nfs_v4_cb.rpc2;
    programs[5] = &shared->nlm_v4.rpc2;

    thread->rpc2_thread = evpl_rpc2_thread_init(evpl, programs, 6, chimera_nfs_notify, thread);

    thread->max_server_threads = shared->max_servers;
    thread->server_threads     = calloc(thread->max_server_threads, sizeof(*thread->server_threads));

    /* Persistent doorbell the back-channel control thread rings to resume mounts
     * established on its behalf. */
    chimera_nfs4_cb_thread_init(thread);

    return thread;
} /* chimera_nfs_thread_init */

static void
chimera_nfs_thread_destroy(void *private_data)
{
    struct chimera_nfs_thread             *thread = private_data;
    struct chimera_nfs_client_open_handle *open_handle;
    int                                    i;

    while (thread->free_open_handles) {
        open_handle = thread->free_open_handles;
        LL_DELETE(thread->free_open_handles, open_handle);
        free(open_handle);
    }

    /* Remove the back-channel resume doorbell while this thread's evpl is still
     * valid (no establishment can be in flight: mounts complete before their
     * thread is torn down). */
    chimera_nfs4_cb_thread_destroy(thread);

    /* Tear down the rpc2 thread first: it disconnects every connection, which
     * fires chimera_nfs_notify(DISCONNECTED) -> chimera_nfs4_slot_table_reset,
     * and that walks thread->server_threads.  Freeing the server_threads before
     * this would be a use-after-free. */
    evpl_rpc2_thread_destroy(thread->rpc2_thread);

    for (i = 0; i < thread->max_server_threads; i++) {
        if (thread->server_threads[i]) {
            chimera_nfs4_slot_table_destroy(&thread->server_threads[i]->slots);
            free(thread->server_threads[i]);
        }
    }

    free(thread->server_threads);

    free(thread);
} /* chimera_nfs_thread_destroy */

static void
chimera_nfs_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_thread        *thread = private_data;
    struct chimera_nfs_shared        *shared = thread->shared;
    struct chimera_nfs_client_server *server;
    struct chimera_nfs_client_mount  *mount;
    const uint8_t                    *fh;
    int                               nfsvers;

    if (request->opcode == CHIMERA_VFS_OP_MOUNT) {
        /* Resolve the common TCP flavor for outbound connections from the
         * VFS once we have a thread/vfs in hand. Constant per process. */
        shared->tcp_protocol = chimera_tcp_flavor_to_protocol(request->thread->vfs->tcp_flavor);

        nfsvers = chimera_nfs_get_mount_version(&request->mount.options);
        if (nfsvers < 0) {
            chimera_nfsclient_error("Invalid NFS version in mount options");
            request->status = CHIMERA_VFS_EINVAL;
            request->complete(request);
            return;
        }
    } else if (request->opcode == CHIMERA_VFS_OP_UMOUNT) {
        mount   = request->umount.mount_private;
        nfsvers = mount->nfsvers;
    } else if (request->opcode == CHIMERA_VFS_OP_CLOSE) {
        /* Close requests get server routing from the open state stored in
         * vfs_private, since the request may not carry a valid fh. */
        struct chimera_nfs3_open_state *open_state =
            (struct chimera_nfs3_open_state *) request->close.vfs_private;

        if (!open_state) {
            request->status = CHIMERA_VFS_OK;
            request->complete(request);
            return;
        }

        server  = shared->servers[open_state->server_index];
        nfsvers = server->nfsvers;
    } else {
        fh = request->fh;

        if (unlikely(request->fh_len < CHIMERA_VFS_MOUNT_ID_SIZE + 1)) {
            chimera_nfsclient_error("fhlen %d < %d", request->fh_len, CHIMERA_VFS_MOUNT_ID_SIZE + 1);
            request->status = CHIMERA_VFS_EINVAL;
            request->complete(request);
            return;
        }

        /* Server index is at position CHIMERA_VFS_MOUNT_ID_SIZE (first byte of fh_fragment) */
        server = shared->servers[fh[CHIMERA_VFS_MOUNT_ID_SIZE]];

        if (unlikely(!server)) {
            chimera_nfsclient_error("server not found for fh %p", fh);
            request->status = CHIMERA_VFS_EINVAL;
            request->complete(request);
            return;
        }

        nfsvers = server->nfsvers;
    }

    switch (nfsvers) {
        case 3:
            chimera_nfs3_dispatch(thread, shared, request, private_data);
            break;
        case 4:
            chimera_nfs4_dispatch(thread, shared, request, private_data);
            break;
        default:
            request->status = CHIMERA_VFS_EFAULT;
            request->complete(request);
            break;
    } /* switch */
} /* chimera_nfs_dispatch */

SYMBOL_EXPORT struct chimera_vfs_module vfs_nfs = {
    .name     = "nfs",
    .fh_magic = CHIMERA_VFS_FH_MAGIC_NFS,
    /* CAP_READ_PROVIDES_BUFFERS: the proxy returns the data buffers handed up
     * by its upstream RPC reply (it reassigns request->read.iov), so the VFS
     * core must not pre-allocate buffers for it. */
    .capabilities   = CHIMERA_VFS_CAP_OPEN_FILE_REQUIRED | CHIMERA_VFS_CAP_FS | CHIMERA_VFS_CAP_FS_RELATIVE_OP |
        CHIMERA_VFS_CAP_FS_LOCK | CHIMERA_VFS_CAP_READ_PROVIDES_BUFFERS,
    .init           = chimera_nfs_init,
    .destroy        = chimera_nfs_destroy,
    .thread_init    = chimera_nfs_thread_init,
    .thread_destroy = chimera_nfs_thread_destroy,
    .dispatch       = chimera_nfs_dispatch,
};
