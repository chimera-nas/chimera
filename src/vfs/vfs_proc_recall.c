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

/* True if `fh` currently has a live (non-implicit) share holder. */
SYMBOL_EXPORT int
chimera_vfs_fh_has_share_holder(
    struct chimera_vfs_thread *thread,
    const uint8_t             *fh,
    uint32_t                   fh_len)
{
    struct chimera_vfs_state      *state = thread->vfs->vfs_state;
    struct chimera_vfs_file_state *fs;
    int                            open = 0;

    fs = chimera_vfs_state_get(state, fh, fh_len,
                               chimera_vfs_hash(fh, fh_len), false);
    if (fs) {
        open = chimera_vfs_state_has_other_share_holder(fs, NULL);
        chimera_vfs_state_put(state, fs);
    }
    return open;
} /* chimera_vfs_fh_has_share_holder */

/* Completion for a by-FH caching recall: the recall has drained, so report
 * whether the file still has a live share holder (a holder that did NOT close
 * on the handle-lease break) and free the request. */
static void
chimera_vfs_recall_caching_fh_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_recall_fh_callback_t callback = request->proto_callback;
    int                              still_open;

    still_open = chimera_vfs_fh_has_share_holder(request->thread, request->fh,
                                                 request->fh_len);

    chimera_vfs_complete(request);
    callback(CHIMERA_VFS_OK, still_open, request->proto_private_data);
    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_recall_caching_fh_complete */

/* Single-step recall of EVERY caching lease on the file named by a bare FH
 * (no open handle, so nothing is spared), breaking each holder's HANDLE cache
 * once -- and ONLY the handle cache (RWH -> RW, RH -> R) -- then PARKing until
 * the recall drains and reporting whether a holder kept the file open.  A
 * directory rename invalidates only the cached path/handle of a contained open,
 * not its data: per MS-SMB2 3.3.4.7 / [MS-FSA] 2.1.5.1.2 the handle-caching bit
 * is revoked while read+write caching is retained, so the retain floor is R|W
 * (a single RWH->RW break, NOT a RWH->RH->R cascade that would strip the write
 * cache first).  A well-behaved holder closes on the handle-lease break (freeing
 * its share reservation); a holder that only acks keeps the file open.  Used by
 * the SMB directory-rename path to break the handle leases of files open inside
 * a directory being renamed. */
SYMBOL_EXPORT void
chimera_vfs_recall_caching_fh(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    const uint8_t                   *fh,
    uint32_t                         fh_len,
    chimera_vfs_recall_fh_callback_t callback,
    void                            *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc(thread, cred, fh, fh_len);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), 0, private_data);
        return;
    }

    request->opcode             = CHIMERA_VFS_OP_GETATTR; /* inert: no dispatch */
    request->complete           = chimera_vfs_recall_caching_fh_complete;
    request->io_handle          = NULL; /* spare nothing: break all holders */
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    chimera_vfs_io_recall_single(request, fh, fh_len, request->fh_hash,
                                 CHIMERA_VFS_LEASE_MODE_R | CHIMERA_VFS_LEASE_MODE_W,
                                 chimera_vfs_recall_caching_fh_complete);
} /* chimera_vfs_recall_caching_fh */
