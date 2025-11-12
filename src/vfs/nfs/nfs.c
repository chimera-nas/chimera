// SPDX-FileCopyrightText: 2025 Ben Jarvis
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
nfs_init(const char *cfgfile)
{
    struct nfs_shared *shared = calloc(1, sizeof(*shared));

    shared->protocol_version = 3;

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
} /* nfs_init */

static void
nfs_destroy(void *private_data)
{
    struct nfs_shared *shared = private_data;

    free(shared->mounts);
    free(shared->servers);

    free(shared);
} /* nfs_destroy */

static void *
nfs_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct nfs_shared        *shared = private_data;
    struct nfs_thread        *thread = calloc(1, sizeof(*thread));
    struct evpl_rpc2_program *programs[5];

    thread->shared = shared;
    thread->evpl   = evpl;

    programs[0] = &shared->mount_v3.rpc2;
    programs[1] = &shared->portmap_v2.rpc2;
    programs[2] = &shared->nfs_v3.rpc2;
    programs[3] = &shared->nfs_v4.rpc2;
    programs[4] = &shared->nfs_v4_cb.rpc2;

    thread->rpc2_thread = evpl_rpc2_thread_init(evpl, programs, 5);

    thread->max_server_threads = shared->max_servers;
    thread->server_threads     = calloc(thread->max_server_threads, sizeof(*thread->server_threads));

    return thread;
} /* nfs_thread_init */

static void
nfs_thread_destroy(void *private_data)
{
    struct nfs_thread *thread = private_data;
    int                i;

    for (i = 0; i < thread->max_server_threads; i++) {
        if (thread->server_threads[i]) {
            free(thread->server_threads[i]);
        }
    }

    free(thread->server_threads);

    evpl_rpc2_thread_destroy(thread->rpc2_thread);

    free(thread);
} /* nfs_thread_destroy */

static void
nfs_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct nfs_thread *thread = private_data;
    struct nfs_shared *shared = thread->shared;

    switch (shared->protocol_version) {
        case 3:
            nfs3_dispatch(thread, shared, request, private_data);
            break;
        case 4:
            nfs4_dispatch(thread, shared, request, private_data);
            break;
        default:
            chimera_error("nfs", __FILE__, __LINE__,
                          "nfs_dispatch: unknown protocol version %u",
                          shared->protocol_version);
            request->status = CHIMERA_VFS_ENOTSUP;
            request->complete(request);
            break;
    } /* switch */
} /* nfs_dispatch */

SYMBOL_EXPORT struct chimera_vfs_module vfs_nfs = {
    .name           = "nfs",
    .fh_magic       = CHIMERA_VFS_FH_MAGIC_NFS,
    .capabilities   = 0,
    .init           = nfs_init,
    .destroy        = nfs_destroy,
    .thread_init    = nfs_thread_init,
    .thread_destroy = nfs_thread_destroy,
    .dispatch       = nfs_dispatch,
};
