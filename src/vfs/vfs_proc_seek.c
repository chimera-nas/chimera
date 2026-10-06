// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include "vfs/vfs_internal_procs.h"
#include "vfs/vfs_pnfs.h"
#include "vfs/vfs_claim.h"
#include "vfs_internal.h"
#include "vfs_release.h"
#include "common/macros.h"
static void
chimera_vfs_seek_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_seek_callback_t callback = request->proto_callback;


    chimera_vfs_io_claim_release(request);
    chimera_vfs_complete(request);

    /* Drop the pNFS backing-file reference the redirect took (no-op when the
     * op was not redirected). */
    if (request->io_pnfs_backing) {
        chimera_vfs_release(request->thread, request->io_pnfs_backing);
        request->io_pnfs_backing = NULL;
    }

    callback(request->status,
             request->seek.r_eof,
             request->seek.r_offset,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_seek_complete */

static void
chimera_vfs_seek_dispatch(struct chimera_vfs_request *request)
{
    chimera_vfs_pnfs_dispatch(request, 0, 0);
} /* chimera_vfs_seek_dispatch */

SYMBOL_EXPORT void
chimera_vfs_seek_view(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    uint64_t                          offset,
    uint32_t                          what,
    const struct chimera_vfs_io_view *view,
    chimera_vfs_seek_callback_t       callback,
    void                             *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc_by_handle(thread, cred, handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), 0, 0, private_data);
        return;
    }

    request->opcode             = CHIMERA_VFS_OP_SEEK;
    request->complete           = chimera_vfs_seek_complete;
    request->seek.handle        = handle;
    request->seek.offset        = offset;
    request->seek.what          = what;
    request->seek.r_eof         = 0;
    request->seek.r_offset      = 0;
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    request->io_handle = handle;
    chimera_vfs_io_view_copy(&request->io_view, &request->io_owner, view);
    request->io_owner_valid = request->io_view.owner != NULL;
    chimera_vfs_io_claim_acquire(request, request->io_view.owner,
                                 chimera_vfs_seek_dispatch);

} /* chimera_vfs_seek_view */
