// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs4_open_state.h"

void
chimera_nfs4_open(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs4_open_state *state = NULL;

    /*
     * For inferred opens (internal opens used for path traversal, like opening
     * a parent directory before open_at), we don't need to do an actual NFS4
     * OPEN operation. Just allocate minimal state and return OK.
     *
     * Similarly, directories don't need NFS4 OPEN - they're accessed via
     * READDIR, LOOKUP etc.
     */
    if ((request->open.flags & CHIMERA_VFS_OPEN_INFERRED) ||
        (request->open.flags & CHIMERA_VFS_OPEN_DIRECTORY)) {
        request->open.r_vfs_private = 0;
        request->status             = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    /*
     * For regular file opens (not creates - those go through open_at),
     * we need to allocate state for tracking dirty writes and silly renames.
     * However, a true NFS4 OPEN by file handle would require CLAIM_FH which
     * is only supported in NFSv4.1+. For now, we allocate state and return OK,
     * relying on the fact that reads/writes will use an anonymous stateid.
     */
    state = chimera_nfs4_open_state_alloc();

    if (!state) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    request->open.r_vfs_private = (uint64_t) state;
    request->status             = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs4_open */

