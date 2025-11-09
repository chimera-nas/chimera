// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <utlist.h>

#include "nfs_internal.h"
#include "evpl/evpl_rpc2.h"

struct nfs_mount_ctx {
    struct nfs_thread *thread;
    struct nfs_shared *shared;
};

static void
mountd_mnt_callback(
    struct evpl      *evpl,
    struct mountres3 *reply,
    int               status,
    void             *private_data)
{
    struct nfs_client_mount    *mount   = private_data;
    struct chimera_vfs_request *request = mount->mount_requests;
    struct nfs_mount_ctx       *ctx     = request->plugin_data;
    struct nfs_shared          *shared  = ctx->shared;

    chimera_nfsclient_info("NFS3 GetRootFH mount mnt callback %s status %d res %d", mount->key, status, reply->
                           fhs_status);

    if (status != 0) {
        chimera_nfsclient_error("NFS3 GetRootFH mount mnt callback failed %s", mount->key);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    pthread_rwlock_wrlock(&shared->mounts_lock);
    mount->fh[0] = CHIMERA_VFS_FH_MAGIC_NFS;
    memcpy(mount->fh + 1, reply->mountinfo.fhandle.data, reply->mountinfo.fhandle.len);
    mount->fhlen  = 1 + reply->mountinfo.fhandle.len;
    mount->status = NFS_CLIENT_MOUNT_STATE_MOUNTED;

    request->mount.r_attr.va_set_mask = CHIMERA_VFS_ATTR_FH;
    request->mount.r_attr.va_fh_len   = mount->fhlen;
    memcpy(request->mount.r_attr.va_fh, mount->fh, mount->fhlen);

    pthread_rwlock_unlock(&shared->mounts_lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);


} /* mount_mnt_callback */

static void
mount_null_callback(
    struct evpl *evpl,
    int          status,
    void        *private_data)
{
    struct nfs_client_mount    *mount   = private_data;
    struct chimera_vfs_request *request = mount->mount_requests;
    struct nfs_mount_ctx       *ctx     = request->plugin_data;
    struct nfs_shared          *shared  = ctx->shared;
    struct mountarg3            mount_arg;

    mount_arg.path.str = mount->path;
    mount_arg.path.len = strlen(mount->path);

    chimera_nfsclient_info("NFS3 GetRootFH mount null callback %s", mount->key);

    shared->mount_v3.send_call_MOUNTPROC3_MNT(&shared->mount_v3.rpc2, evpl, mount->mount_conn, &mount_arg,
                                              mountd_mnt_callback, mount);
} /* mount_null_callback */

static void
portmap_getport_callback(
    struct evpl *evpl,
    struct port *reply,
    int          status,
    void        *private_data)
{
    struct nfs_client_mount    *mount   = private_data;
    struct chimera_vfs_request *request = mount->mount_requests;
    struct nfs_mount_ctx       *ctx     = request->plugin_data;
    struct nfs_shared          *shared  = ctx->shared;

    chimera_nfsclient_info("NFS3 GetRootFH portmap getport callback %s port %u", mount->key, reply->port);


    mount->mount_endpoint = evpl_endpoint_create(mount->hostname, reply->port);

    mount->mount_conn = evpl_rpc2_client_connect(ctx->thread->rpc2_thread,
                                                 EVPL_STREAM_SOCKET_TCP,
                                                 mount->mount_endpoint);

    if (!mount->mount_conn) {
        chimera_nfsclient_error("NFS3 GetRootFH mount mount connect failed %s", mount->hostname);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    shared->mount_v3.send_call_MOUNTPROC3_NULL(&shared->mount_v3.rpc2,
                                               evpl,
                                               mount->mount_conn,
                                               mount_null_callback,
                                               mount);
} /* portmap_getport_callback */


static void
portmap_null_callback(
    struct evpl *evpl,
    int          status,
    void        *private_data)
{
    struct nfs_client_mount    *mount   = private_data;
    struct chimera_vfs_request *request = mount->mount_requests;
    struct nfs_mount_ctx       *ctx     = request->plugin_data;
    struct nfs_shared          *shared  = ctx->shared;
    struct mapping              mapping;

    chimera_nfsclient_info("NFS3 GetRootFH portmap null callback %s", mount->key);

    mapping.prog = 100005;
    mapping.vers = 3;
    mapping.prot = 6;
    mapping.port = 0;

    shared->portmap_v2.send_call_PMAPPROC_GETPORT(&shared->portmap_v2.rpc2,
                                                  evpl, mount->portmap_conn,
                                                  &mapping,
                                                  portmap_getport_callback, mount);
} /* portmap_null_callback */

void
nfs3_mount(
    struct nfs_thread          *thread,
    struct nfs_shared          *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    const char              *path     = request->mount.path;
    const char              *hostname = NULL;
    struct nfs_client_mount *mount    = NULL, **new_mounts;
    struct nfs_mount_ctx    *ctx = private_data;
    int                      hostnamelen = 0, mount_keylen = 0;
    int                      index;
    char                     mount_key[256];

    for (int i = 0; i < request->mount.pathlen; i++) {
        if (path[i] == ':') {
            hostname    = path;
            hostnamelen = i;
            break;
        }
    }

    if (hostnamelen == 0) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
    }

    mount_keylen = snprintf(mount_key, sizeof(mount_key), "%.*s?v3", hostnamelen, hostname);

    chimera_nfsclient_info("NFS3 GetRootFH mount key %.*s", mount_keylen, mount_key);

    pthread_rwlock_rdlock(&shared->mounts_lock);

    HASH_FIND(hh, shared->mounts_map, mount_key, mount_keylen, mount);

    if (mount) {
        if (mount->status == NFS_CLIENT_MOUNT_STATE_MOUNTING) {
            DL_APPEND(mount->mount_requests, request);
        }

        mount->refcnt++;
    }

    pthread_rwlock_unlock(&shared->mounts_lock);

    if (mount && mount->status == NFS_CLIENT_MOUNT_STATE_MOUNTED) {
        chimera_nfsclient_info("NFS3 GetRootFH mount found %s", mount->key);
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    pthread_rwlock_wrlock(&shared->mounts_lock);

    do {

        index = -1;

        for (index = 0; index < shared->max_mounts; index++) {
            if (shared->mounts[index] == NULL) {
                break;
            }
        }

        if (index < 0) {
            shared->max_mounts *= 2;
            new_mounts          = calloc(shared->max_mounts, sizeof(*new_mounts));
            memcpy(new_mounts, shared->mounts, shared->max_mounts * sizeof(*new_mounts));
            free(shared->mounts);
            shared->mounts = new_mounts;
        }

    } while (index < 0);

    mount = calloc(1, sizeof(*mount));

    shared->mounts[index] = mount;

    mount->keylen  = mount_keylen;
    mount->index   = index;
    mount->nfsvers = 3;
    mount->refcnt  = 1;
    mount->status  = NFS_CLIENT_MOUNT_STATE_MOUNTING;

    strncpy(mount->key, mount_key, mount_keylen);
    strncpy(mount->hostname, hostname, hostnamelen);
    strncpy(mount->path, request->mount.path + hostnamelen + 1, request->mount.pathlen - hostnamelen + 1);

    HASH_ADD(hh, shared->mounts_map, key, mount_keylen, mount);

    DL_APPEND(mount->mount_requests, request);

    ctx         = request->plugin_data;
    ctx->thread = thread;
    ctx->shared = shared;

    pthread_rwlock_unlock(&shared->mounts_lock);

    chimera_nfsclient_info("NFS3 GetRootFH mount added %s hostname '%s' path '%s'", mount->key, mount->hostname, mount->
                           path);

    mount->portmap_endpoint = evpl_endpoint_create(mount->hostname, 111);

    mount->portmap_conn = evpl_rpc2_client_connect(thread->rpc2_thread,
                                                   EVPL_STREAM_SOCKET_TCP,
                                                   mount->portmap_endpoint);

    if (!mount->portmap_conn) {
        chimera_nfsclient_error("NFS3 GetRootFH mount portmap connect failed %s", mount->hostname);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }


    shared->portmap_v2.send_call_PMAPPROC_NULL(&shared->portmap_v2.rpc2, thread->evpl, mount->portmap_conn,
                                               portmap_null_callback, mount);



} /* nfs3_getrootfh */
