// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs3_open_state.h"

void
chimera_nfs3_open(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs3_open_state *state;

    /* Allocate open state for dirty tracking and silly rename support */
    state = chimera_nfs3_open_state_alloc();

    if (!state) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    /* Store open state pointer for direct access on write/close */
    request->open.r_vfs_private = (uint64_t) state;

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs3_open */

