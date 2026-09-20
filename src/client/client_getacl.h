// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <string.h>

#include "client_internal.h"
#include "vfs/sdk/vfs_acl.h"

/*
 * GETACL dispatch: OPEN_PATH the object (O_PATH, following a final symlink),
 * GETATTR the canonical NFSv4/Windows ACL (CHIMERA_VFS_ATTR_ACL) through that
 * handle, and copy the returned ACL into the caller's buffer.  The sequence
 * reports the ACL BY VALUE -- a copy the op owns, valid until the compound is
 * freed -- so it is read out in the completion before the free.
 */

static void
chimera_getacl_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_client_request        *request        = private_data;
    struct chimera_client_thread         *thread         = request->thread;
    chimera_setattr_callback_t            callback       = request->getacl.callback;
    void                                 *callback_arg   = request->getacl.private_data;
    int                                   heap_allocated = request->heap_allocated;
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                status;

    status = chimera_vfs_compound_status(compound);

    if (status == CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(compound,
                                     chimera_vfs_compound_num_ops(compound) - 1);

        if (!(op->attr.va_set_mask & CHIMERA_VFS_ATTR_ACL) || !op->attr.va_acl) {
            /* Backend returned no ACL (mode-only object / backend without ACL
             * storage): report an empty ACL rather than failing. */
            request->getacl.r_acl_aces = 0;
            if (request->getacl.acl_bufsize >= chimera_acl_size(0)) {
                request->getacl.acl_buf->num_aces   = 0;
                request->getacl.acl_buf->ctrl_flags = 0;
            } else {
                status = CHIMERA_VFS_ERANGE;
            }
        } else {
            const struct chimera_acl *acl  = op->attr.va_acl;
            size_t                    need = chimera_acl_size(acl->num_aces);

            request->getacl.r_acl_aces = acl->num_aces;

            if (request->getacl.acl_bufsize >= need) {
                memcpy(request->getacl.acl_buf, acl, need);
            } else {
                status = CHIMERA_VFS_ERANGE;
            }
        }
    }

    /* The ACL copy and the OPEN's handle are the sequence's; the free below
     * releases both, after the copy-out above. */
    if (heap_allocated) {
        chimera_client_request_free(thread, request);
    }

    chimera_vfs_compound_free(compound);

    callback(thread, status, callback_arg);
} /* chimera_getacl_sequence_complete */

static inline void
chimera_dispatch_getacl(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_vfs_compound *compound;
    int                          open_idx, get_idx;

    compound = chimera_client_compound_at_root(thread, request);

    /* An O_PATH open resolves on every backend, path-only ones included, and
     * is what the per-op chain opened; the GETATTR addresses its handle. */
    open_idx = chimera_vfs_compound_add_open_path(
        compound, request->getacl.path, request->getacl.path_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED, NULL, 0);

    get_idx = chimera_vfs_compound_add_getattr(compound, CHIMERA_VFS_ATTR_ACL);

    if (open_idx >= 0 && get_idx >= 0) {
        chimera_vfs_compound_op_use_handle(compound, (uint32_t) get_idx,
                                           (uint32_t) open_idx);
    }

    chimera_vfs_compound_submit(compound, chimera_getacl_sequence_complete,
                                request);
} /* chimera_dispatch_getacl */
