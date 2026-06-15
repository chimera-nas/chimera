// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>

#include "vfs/vfs_procs.h"
#include "vfs/vfs_state.h"
#include "vfs/vfs_internal.h"
#include "common/macros.h"

/* Completion for a stand-alone caching-lease recall (no backend op): the recall
 * has drained (every conflicting caching holder acked or was revoked), so report
 * success and free the request. */
static void
chimera_vfs_recall_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_recall_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);
    callback(CHIMERA_VFS_OK, request->proto_private_data);
    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_recall_complete */

/* Recall every OTHER caching lease on the file backing `handle` (the operating
 * open's own lease is spared via io_handle) and PARK until the recall drains,
 * then invoke `callback`.  This is the namespace-mutation recall (an RqLs lease's
 * handle cache is broken) WITHOUT any backend mutation -- used by the SMB
 * delete-on-close path so the peer's lease break is acknowledged before the
 * SetInfo reply is sent (smb2.lease.unlink: the client checks the break count
 * synchronously right after the SetInfo completes). */
SYMBOL_EXPORT void
chimera_vfs_recall_handle_lease(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    chimera_vfs_recall_callback_t   callback,
    void                           *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc_by_handle(thread, cred, handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), private_data);
        return;
    }

    request->opcode             = CHIMERA_VFS_OP_GETATTR; /* inert: no dispatch */
    request->complete           = chimera_vfs_recall_complete;
    request->io_handle          = handle;
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    /* Single-step recall: break each OTHER holder's handle cache exactly once
     * (RH -> R), sparing the operating open (io_handle), and park until the
     * client's break ack arrives. */
    chimera_vfs_io_recall_single(request, handle->fh, handle->fh_len,
                                 handle->fh_hash, CHIMERA_VFS_LEASE_MODE_R,
                                 chimera_vfs_recall_complete);
} /* chimera_vfs_recall_handle_lease */
