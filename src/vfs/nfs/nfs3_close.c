// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"

void
chimera_nfs3_close(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    /* NFS3 is stateless - nothing to do on close.
     * Applications should call fsync/fdatasync for data persistence.
     */
    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs3_close */

