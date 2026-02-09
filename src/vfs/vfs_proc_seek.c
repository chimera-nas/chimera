// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs/vfs_procs.h"
#include "vfs_internal.h"
#include "common/macros.h"
static void
chimera_vfs_seek_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_seek_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status,
             request->seek.r_eof,
             request->seek.r_offset,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_seek_complete */

SYMBOL_EXPORT void
chimera_vfs_seek(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint32_t                        what,
    chimera_vfs_seek_callback_t     callback,
    void                           *private_data)
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

    chimera_vfs_dispatch(request);

} /* chimera_vfs_seek */
