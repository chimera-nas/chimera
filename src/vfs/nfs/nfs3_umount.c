// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "vfs/vfs_internal.h"

void
chimera_nfs3_umount(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_mount  *mount  = request->umount.mount_private;
    struct chimera_nfs_client_server *server = mount->server;

    pthread_mutex_lock(&shared->lock);

    DL_DELETE(shared->mounts, mount);

    free(mount);

    server->refcnt--;

    pthread_mutex_unlock(&shared->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);

} /* chimera_nfs3_umount */