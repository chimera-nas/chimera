// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs4_open_state.h"
#include "nfs4_pnfs.h"

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

    /* Drop this file's layout from the recall registry before it is freed.
     * Idempotent: a no-op unless the layout reached VALID (or was fenced). */
    chimera_nfs4_pnfs_layout_unregister(shared, &open_state->layout);

    /* If a pNFS layout is held, report the new size to the MDS (LAYOUTCOMMIT)
     * and return the layout (LAYOUTRETURN) before freeing.  When it takes the
     * request it frees the open state and completes asynchronously. */
    if (chimera_nfs4_pnfs_close(thread, shared, request, open_state)) {
        return;
    }

    /* TODO: Send NFS4 CLOSE compound to release the stateid on server.
     * For now, just free the local state. */

    /* Free the open state */
    chimera_nfs4_open_state_free(open_state);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs4_close */
