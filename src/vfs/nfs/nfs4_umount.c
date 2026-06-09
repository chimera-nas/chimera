// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"

void
chimera_nfs4_umount(
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

    /* Free session when last mount is removed.  Per-thread slot tables are
     * freed in chimera_nfs_thread_destroy (they live on the server_threads). */
    if (server->refcnt == 0 && server->nfs4_session) {
        chimera_nfs4_session_pool_destroy(server->nfs4_session);
        pthread_mutex_destroy(&server->nfs4_session->lock);
        free(server->nfs4_session);
        server->nfs4_session = NULL;
    }

    pthread_mutex_unlock(&shared->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);

} /* chimera_nfs4_umount */
