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

    /*
     * The server (and its persistent back-channel control connection) outlives
     * an individual mount, so the NFSv4.1 session is kept alive here even when
     * the last mount is removed: a subsequent mount of the same server reuses
     * it (see chimera_nfs4_cb_control_doorbell).  Tearing it down on refcnt==0
     * would force a CREATE_SESSION on remount while per-thread slot tables still
     * reference the old session -- corrupting the slot pool.  The session and
     * its slot pool are freed once, with the server, in chimera_nfs_destroy.
     */

    pthread_mutex_unlock(&shared->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);

} /* chimera_nfs4_umount */
