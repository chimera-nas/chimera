// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs/vfs_procs.h"
#include "vfs_internal.h"
#include "common/macros.h"

static void
chimera_vfs_read_plus_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_read_plus_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status,
             request->read_plus.r_is_data,
             request->read_plus.r_length,
             request->read_plus.r_eof,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_read_plus_complete */

SYMBOL_EXPORT void
chimera_vfs_read_plus(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    struct chimera_vfs_open_handle  *handle,
    uint64_t                         offset,
    uint64_t                         length,
    chimera_vfs_read_plus_callback_t callback,
    void                            *private_data)
{
    struct chimera_vfs_request *request;

    if (!(handle->vfs_module->capabilities & CHIMERA_VFS_CAP_READ_PLUS)) {
        callback(CHIMERA_VFS_ENOTSUP, 0, 0, 0, private_data);
        return;
    }

    request = chimera_vfs_request_alloc_by_handle(thread, cred, handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), 0, 0, 0, private_data);
        return;
    }

    request->opcode              = CHIMERA_VFS_OP_READ_PLUS;
    request->complete            = chimera_vfs_read_plus_complete;
    request->read_plus.handle    = handle;
    request->read_plus.offset    = offset;
    request->read_plus.length    = length;
    request->read_plus.r_is_data = 0;
    request->read_plus.r_length  = 0;
    request->read_plus.r_eof     = 0;
    request->proto_callback      = callback;
    request->proto_private_data  = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_read_plus */
