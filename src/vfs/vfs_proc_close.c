// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
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
    struct chimera_vfs_thread   *thread,
    const void                  *fh,
    uint32_t                     fhlen,
    uint64_t                     vfs_private,
    chimera_vfs_close_callback_t callback,
    void                        *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc(thread, fh, fhlen);

    request->opcode             = CHIMERA_VFS_OP_CLOSE;
    request->complete           = chimera_vfs_close_complete;
    request->close.vfs_private  = vfs_private;
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    chimera_vfs_dispatch(request);

} /* chimera_vfs_close */
