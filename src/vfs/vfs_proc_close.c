// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"
#include "common/macros.h"
#include "vfs_open_cache.h"

static void
chimera_vfs_close_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_close_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    if (callback) {
        callback(request->status, request->proto_private_data);
    }

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_close_complete */

SYMBOL_EXPORT void
chimera_vfs_close(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_open_handle *handle,
    chimera_vfs_close_callback_t    callback,
    void                           *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc_with_module(thread,
                                                    NULL,
                                                    handle->fh,
                                                    handle->fh_len,
                                                    handle->fh_hash,
                                                    handle->vfs_module);

    if (CHIMERA_VFS_IS_ERR(request)) {
        if (callback) {
            callback(CHIMERA_VFS_PTR_ERR(request), private_data);
        }
        return;
    }

    request->opcode             = CHIMERA_VFS_OP_CLOSE;
    request->complete           = chimera_vfs_close_complete;
    request->close.vfs_private  = handle->vfs_private;
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    chimera_vfs_dispatch(request);

} /* chimera_vfs_close */
