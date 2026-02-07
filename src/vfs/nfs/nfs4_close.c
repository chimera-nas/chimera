// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs4_open_state.h"

void
chimera_nfs4_close(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs4_open_state *open_state;

    open_state = (struct chimera_nfs4_open_state *) request->close.vfs_private;

    /* Handle case where no open state was tracked (e.g., inferred opens) */
    if (!open_state) {
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    /* TODO: Send NFS4 CLOSE compound to release the stateid on server.
     * For now, just free the local state. */

    /* Free the open state */
    chimera_nfs4_open_state_free(open_state);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs4_close */
