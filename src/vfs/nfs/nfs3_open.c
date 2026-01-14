// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"

void
chimera_nfs3_open(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    /* NFS3 is stateless - no per-open handle needed */
    request->open.r_vfs_private = 0;

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs3_open */

