// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdint.h>
#include <stdlib.h>
#include <pthread.h>

#include "vfs/vfs.h"
#include "nfs.h"
#include "nfs_internal.h"
#include "common/logging.h"
#include "common/macros.h"
#include "evpl/evpl.h"
#include "evpl/evpl_rpc2.h"

static void *
chimera_nfs_init(const char *cfgfile)
{
    struct chimera_nfs_shared *shared = calloc(1, sizeof(*shared));

    pthread_mutex_init(&shared->lock, NULL);

    shared->max_servers = 64;
    shared->servers     = calloc(shared->max_servers, sizeof(*shared->servers));
    shared->servers_map = NULL;

    NFS_PORTMAP_V2_init(&shared->portmap_v2);
    NFS_MOUNT_V3_init(&shared->mount_v3);
    NFS_V3_init(&shared->nfs_v3);
    NFS_V4_init(&shared->nfs_v4);
    NFS_V4_CB_init(&shared->nfs_v4_cb);

    return shared;
} /* chimera_nfs_init */

static void
chimera_nfs_destroy(void *private_data)
{
    struct chimera_nfs_shared *shared = private_data;
    int                        i;

    for (i = 0; i < shared->max_servers; i++) {
        if (shared->servers[i]) {
            free(shared->servers[i]);
        }
    }

    free(shared->mounts);
    free(shared->servers);

    free(shared);
} /* chimera_nfs_destroy */

static void
chimera_nfs_notify(
    struct evpl_rpc2_thread *thread,
    struct evpl_rpc2_conn   *conn,
    struct evpl_rpc2_notify *notify,
    void                    *private_data)
{
    char local_addr[80], remote_addr[80];

    switch (notify->notify_type) {
        case EVPL_RPC2_NOTIFY_CONNECTED:
            evpl_bind_get_local_address(conn->bind, local_addr, sizeof(local_addr));
            evpl_bind_get_remote_address(conn->bind, remote_addr, sizeof(remote_addr));
            chimera_nfsclient_info("Connected from %s to %s", local_addr, remote_addr);
            break;
        case EVPL_RPC2_NOTIFY_DISCONNECTED:
            evpl_bind_get_local_address(conn->bind, local_addr, sizeof(local_addr));
            evpl_bind_get_remote_address(conn->bind, remote_addr, sizeof(remote_addr));
            chimera_nfsclient_info("Disconnected from %s to %s", local_addr, remote_addr);
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
    struct evpl_rpc2_program  *programs[5];

    thread->shared = shared;
    thread->evpl   = evpl;

    programs[0] = &shared->mount_v3.rpc2;
    programs[1] = &shared->portmap_v2.rpc2;
    programs[2] = &shared->nfs_v3.rpc2;
    programs[3] = &shared->nfs_v4.rpc2;
    programs[4] = &shared->nfs_v4_cb.rpc2;

    thread->rpc2_thread = evpl_rpc2_thread_init(evpl, programs, 5, chimera_nfs_notify, thread);

    thread->max_server_threads = shared->max_servers;
    thread->server_threads     = calloc(thread->max_server_threads, sizeof(*thread->server_threads));

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

    for (i = 0; i < thread->max_server_threads; i++) {
        if (thread->server_threads[i]) {
            free(thread->server_threads[i]);
        }
    }

    free(thread->server_threads);

    evpl_rpc2_thread_destroy(thread->rpc2_thread);

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
    const uint8_t                    *fh;
    int                               nfsvers;

    if (request->opcode == CHIMERA_VFS_OP_MOUNT || request->opcode == CHIMERA_VFS_OP_UMOUNT) {
        nfsvers = 3;
    } else {
        fh = request->fh;

        if (unlikely(request->fh_len < 2)) {
            chimera_nfsclient_error("fhlen %d < 2", request->fh_len);
            request->status = CHIMERA_VFS_EINVAL;
            request->complete(request);
            return;
        }

        server = shared->servers[fh[1]];

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
    .name           = "nfs",
    .fh_magic       = CHIMERA_VFS_FH_MAGIC_NFS,
    .capabilities   = 0,
    .init           = chimera_nfs_init,
    .destroy        = chimera_nfs_destroy,
    .thread_init    = chimera_nfs_thread_init,
    .thread_destroy = chimera_nfs_thread_destroy,
    .dispatch       = chimera_nfs_dispatch,
};
