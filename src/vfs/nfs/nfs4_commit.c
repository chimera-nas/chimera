// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs4_open_state.h"
#include "nfs4_pnfs.h"

void
chimera_nfs4_commit(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs4_open_state *open_state =
        request->commit.handle ?
        (struct chimera_nfs4_open_state *) request->commit.handle->vfs_private : NULL;

    /* If a pNFS layout collected dirty writes (sent to the data server), turn
     * this COMMIT into a LAYOUTCOMMIT so the MDS learns the new file size before
     * we acknowledge -- otherwise a re-open would race the deferred close-time
     * LAYOUTCOMMIT and read back a stale size. */
    if (open_state &&
        chimera_nfs4_pnfs_commit(thread, shared, request, open_state)) {
        return;
    }

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs4_commit */
