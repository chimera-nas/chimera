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

    /* A failed close is never routine: whatever the handle pinned in the
     * backend (a descriptor, an inode reference, a deferred unlink) stays
     * pinned, so the effect is a silent leak rather than a returned error.
     * Most closes originate in the open-handle cache -- idle eviction, the
     * shutdown sweep, a detached-handle drop -- and pass no callback at all,
     * and the close thread's callback only decrements its pending count, so
     * without this the status has no reader anywhere.  Report it here, where
     * every caller funnels through. */
    if (unlikely(request->status != CHIMERA_VFS_OK)) {
        chimera_vfs_error("close failed on %s handle %lx: error %d",
                          request->module ? request->module->name : "(none)",
                          request->close.vfs_private,
                          request->status);
    }

    if (callback) {
        callback(request->status, request->proto_private_data);
    }

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_close_complete */

SYMBOL_EXPORT void
chimera_vfs_close(
    struct chimera_vfs_thread   *thread,
    struct chimera_vfs_module   *vfs_module,
    const void                  *fh,
    int                          fhlen,
    uint64_t                     vfs_private,
    uint64_t                     fh_hash,
    chimera_vfs_close_callback_t callback,
    void                        *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc_with_module(thread,
                                                    NULL,
                                                    fh,
                                                    fhlen,
                                                    fh_hash,
                                                    vfs_module);

    if (CHIMERA_VFS_IS_ERR(request)) {
        if (callback) {
            callback(CHIMERA_VFS_PTR_ERR(request), private_data);
        }
        return;
    }

    request->opcode             = CHIMERA_VFS_OP_CLOSE;
    request->complete           = chimera_vfs_close_complete;
    request->close.vfs_private  = vfs_private;
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    chimera_vfs_dispatch(request);

} /* chimera_vfs_close */
