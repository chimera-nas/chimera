// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"

void
nfs3_readdir(
    struct nfs_thread          *thread,
    struct nfs_shared          *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    request->status = CHIMERA_VFS_ENOTSUP;
    request->complete(request);
}

