// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <utlist.h>
#include <pthread.h>

#include "nfs_internal.h"
#include "evpl/evpl_rpc2.h"
#include "vfs/vfs_internal.h"

struct nfs_client_server_thread_ctx {
    struct nfs_client_server_thread *server_thread;
};

static void
mount_mountd_mnt_callback(
    struct evpl      *evpl,
    struct mountres3 *reply,
    int               status,
    void             *private_data)
{
    struct nfs_client_mount             *mount         = private_data;
    struct chimera_vfs_request          *request       = mount->mount_request;
    struct nfs_shared                   *shared        = mount->server->shared;
    struct nfs_client_server_thread_ctx *server_ctx    = request->plugin_data;
    struct nfs_client_server_thread     *server_thread = server_ctx->server_thread;

    if (status != 0) {
        chimera_nfsclient_error("NFS3 GetRootFH mount mnt callback failed %s", mount->path);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    evpl_rpc2_client_disconnect(server_thread->thread->rpc2_thread, server_thread->mount_conn);
    server_thread->mount_conn = NULL;

    mount->fh[0] = CHIMERA_VFS_FH_MAGIC_NFS;
    mount->fh[1] = mount->server->index;
    memcpy(mount->fh + 2, reply->mountinfo.fhandle.data, reply->mountinfo.fhandle.len);
    mount->fhlen = 2 + reply->mountinfo.fhandle.len;

    request->mount.r_attr.va_set_mask = CHIMERA_VFS_ATTR_FH;
    memcpy(request->mount.r_attr.va_fh, mount->fh, mount->fhlen);
    request->mount.r_attr.va_fh_len = mount->fhlen;

    pthread_mutex_unlock(&shared->lock);
    mount->status = NFS_CLIENT_MOUNT_STATE_MOUNTED;
    pthread_mutex_unlock(&shared->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* nfs3_mount_mountd_mnt_callback */

static void
nfs3_mount_process_mount(
    struct nfs_client_server_thread *server_thread,
    struct chimera_vfs_request      *request)
{
    struct nfs_client_server *server = server_thread->server;
    struct nfs_shared        *shared = server_thread->shared;
    struct nfs_client_mount  *mount;
    struct mountarg3          mount_arg;
    int                       i;
    const char               *path = NULL;

    mount = calloc(1, sizeof(*mount));

    mount->server        = server;
    mount->status        = NFS_CLIENT_MOUNT_STATE_MOUNTING;
    mount->mount_request = request;

    for (i = 0; i < request->mount.pathlen; i++) {
        if (request->mount.path[i] == ':') {
            path = request->mount.path + i + 1;
            break;
        }
    }

    if (!path) {
        chimera_nfsclient_error("NFS3 GetRootFH mount process mount failed %s", request->mount.path);
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    memcpy(mount->path, path, strlen(path) + 1);

    pthread_mutex_lock(&shared->lock);
    DL_APPEND(shared->mounts, mount);
    pthread_mutex_unlock(&shared->lock);

    mount_arg.path.str = mount->path;
    mount_arg.path.len = strlen(mount->path);

    shared->mount_v3.send_call_MOUNTPROC3_MNT(&shared->mount_v3.rpc2,
                                              server_thread->thread->evpl,
                                              server_thread->mount_conn,
                                              &mount_arg,
                                              mount_mountd_mnt_callback, mount);
}     /* nfs3_mount_process_mount */

static void
nfs3_mount_discover_callback(
    struct nfs_client_server_thread *server_thread,
    int                              status)
{
    struct nfs_client_server *server = server_thread->server;
    struct nfs_shared        *shared = server_thread->shared;

    evpl_rpc2_client_disconnect(server_thread->thread->rpc2_thread, server_thread->portmap_conn);
    server_thread->portmap_conn = NULL;

    pthread_mutex_lock(&shared->lock);
    server->state = NFS_CLIENT_SERVER_STATE_DISCOVERED;
    pthread_mutex_unlock(&shared->lock);

    nfs3_mount_process_mount(server_thread, server->pending_mounts);

} /* nfs3_mount_discover_callback */

static void
nfs3_mount_nfs_null_callback(
    struct evpl *evpl,
    int          status,
    void        *private_data)
{
    struct nfs_client_server_thread *server_thread = private_data;

    nfs3_mount_discover_callback(server_thread, status);
} /* nfs3_mount_nfs_null_callback */

static void
portmap_getport_nfs_callback(
    struct evpl *evpl,
    struct port *reply,
    int          status,
    void        *private_data)
{
    struct nfs_client_server_thread *server_thread = private_data;
    struct nfs_client_server        *server        = server_thread->server;
    struct nfs_shared               *shared        = server_thread->shared;

    if (status != 0) {
        chimera_nfsclient_error("NFS3 portmap getport NFS failed %s", server->hostname);
        nfs3_mount_discover_callback(server_thread, status);
        return;
    }

    server->nfs_port     = reply->port;
    server->nfs_endpoint = evpl_endpoint_create(server->hostname, reply->port);

    server_thread->nfs_conn = evpl_rpc2_client_connect(server_thread->thread->rpc2_thread,
                                                       EVPL_STREAM_SOCKET_TCP,
                                                       server->nfs_endpoint);

    shared->nfs_v3.send_call_NFSPROC3_NULL(&shared->nfs_v3.rpc2,
                                           server_thread->thread->evpl,
                                           server_thread->nfs_conn,
                                           nfs3_mount_nfs_null_callback, server_thread);

} /* portmap_getport_nfs_callback */


static void
mount_mountd_null_callback(
    struct evpl *evpl,
    int          status,
    void        *private_data)
{
    struct nfs_client_server_thread *server_thread = private_data;
    struct nfs_client_server        *server        = server_thread->server;
    struct nfs_shared               *shared        = server_thread->shared;
    struct mapping                   mapping;

    if (status != 0) {
        chimera_nfsclient_error("NFS3 mountd null failed %s", server->hostname);
        nfs3_mount_discover_callback(server_thread, status);
        return;
    }

    mapping.prog = 100003;
    mapping.vers = 3;
    mapping.prot = 6;
    mapping.port = 0;

    shared->portmap_v2.send_call_PMAPPROC_GETPORT(&shared->portmap_v2.rpc2,
                                                  server_thread->thread->evpl,
                                                  server_thread->portmap_conn,
                                                  &mapping,
                                                  portmap_getport_nfs_callback, server_thread);
} /* mount_null_callback */

static void
portmap_getport_mountd_callback(
    struct evpl *evpl,
    struct port *reply,
    int          status,
    void        *private_data)
{
    struct nfs_client_server_thread *server_thread = private_data;
    struct nfs_client_server        *server        = server_thread->server;
    struct nfs_shared               *shared        = server_thread->shared;

    if (status != 0) {
        chimera_nfsclient_error("NFS3 portmap getport mountd failed %s", server->hostname);
        nfs3_mount_discover_callback(server_thread, status);
        return;
    }

    server->mount_port     = reply->port;
    server->mount_endpoint = evpl_endpoint_create(server->hostname, reply->port);

    server_thread->mount_conn = evpl_rpc2_client_connect(server_thread->thread->rpc2_thread,
                                                         EVPL_STREAM_SOCKET_TCP,
                                                         server->mount_endpoint);

    shared->mount_v3.send_call_MOUNTPROC3_NULL(&shared->mount_v3.rpc2,
                                               server_thread->thread->evpl,
                                               server_thread->mount_conn,
                                               mount_mountd_null_callback, server_thread);
} /* portmap_getport_mountd_callback */

static void
portmap_null_callback(
    struct evpl *evpl,
    int          status,
    void        *private_data)
{
    struct nfs_client_server_thread *server_thread = private_data;
    struct nfs_client_server        *server        = server_thread->server;
    struct nfs_shared               *shared        = server_thread->shared;
    struct mapping                   mapping;

    if (status != 0) {
        chimera_nfsclient_error("NFS3 portmap null failed %s", server->hostname);
        nfs3_mount_discover_callback(server_thread, status);
        return;
    }

    mapping.prog = 100005;
    mapping.vers = 3;
    mapping.prot = 6;
    mapping.port = 0;

    shared->portmap_v2.send_call_PMAPPROC_GETPORT(&shared->portmap_v2.rpc2,
                                                  server_thread->thread->evpl,
                                                  server_thread->portmap_conn,
                                                  &mapping,
                                                  portmap_getport_mountd_callback, server_thread);
} /* portmap_null_callback */

void
nfs3_mount(
    struct nfs_thread          *thread,
    struct nfs_shared          *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    const char                          *path     = request->mount.path;
    const char                          *hostname = NULL;
    struct nfs_client_server           **new_servers, *server   = NULL;
    struct nfs_client_server_thread     *server_thread;
    struct nfs_client_server_thread_ctx *server_thread_ctx;
    int                                  hostnamelen = 0;
    int                                  i, idx = -1;
    int                                  need_discover = 0;

    for (int i = 0; i < request->mount.pathlen; i++) {
        if (path[i] == ':') {
            hostname    = path;
            hostnamelen = i;
            break;
        }
    }

    if (hostnamelen == 0) {
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    pthread_mutex_lock(&shared->lock);

    for (i = 0; i < shared->max_servers; i++) {
        if (shared->servers[i] &&
            strcmp(shared->servers[i]->hostname, hostname) == 0 &&
            shared->servers[i]->nfsvers == 3) {
            server = shared->servers[i];
            break;
        }
    }

    if (server) {

        server->refcnt++;

        if (server->state == NFS_CLIENT_SERVER_STATE_DISCOVERING) {
            /* Someone else is discovering this server, so we need to wait for them to complete */
            DL_APPEND(server->pending_mounts, request);
        }

    } else {
        need_discover = 1;

        for (i = 0; i < shared->max_servers; i++) {
            if (shared->servers[i] == NULL) {
                idx = i;
                break;
            }
        }

        if (idx < 0 || idx >= shared->max_servers) {
            shared->max_servers *= 2;
            new_servers          = calloc(shared->max_servers, sizeof(*new_servers));
            memcpy(new_servers, shared->servers, (shared->max_servers / 2) * sizeof(*new_servers));
            free(shared->servers);
            shared->servers = new_servers;

            idx = i;
        }

        server          = calloc(1, sizeof(*server));
        server->state   = NFS_CLIENT_SERVER_STATE_DISCOVERING;
        server->refcnt  = 1;
        server->nfsvers = 3;
        server->shared  = shared;

        strncpy(server->hostname, hostname, hostnamelen);

        shared->servers[idx] = server;

        server->index = idx;

        need_discover = 1;

        DL_APPEND(server->pending_mounts, request);
    }

    pthread_mutex_unlock(&shared->lock);

    server_thread         = calloc(1, sizeof(*server_thread));
    server_thread->thread = thread;
    server_thread->shared = shared;
    server_thread->server = server;

    server_thread_ctx                = request->plugin_data;
    server_thread_ctx->server_thread = server_thread;

    if (thread->max_server_threads != shared->max_servers) {
        thread->max_server_threads = shared->max_servers;
        thread->server_threads     = realloc(thread->server_threads,
                                             thread->max_server_threads * sizeof(*thread->server_threads));
    }

    thread->server_threads[idx] = server_thread;

    if (need_discover) {

        server->portmap_endpoint = evpl_endpoint_create(server->hostname, 111);

        server_thread->portmap_conn = evpl_rpc2_client_connect(thread->rpc2_thread,
                                                               EVPL_STREAM_SOCKET_TCP,
                                                               server->portmap_endpoint);

        if (!server_thread->portmap_conn) {
            nfs3_mount_discover_callback(server_thread, CHIMERA_VFS_EINVAL);
            return;
        }

        shared->portmap_v2.send_call_PMAPPROC_NULL(&shared->portmap_v2.rpc2,
                                                   thread->evpl,
                                                   server_thread->portmap_conn,
                                                   portmap_null_callback, server_thread);
    }
} /* nfs3_mount */
