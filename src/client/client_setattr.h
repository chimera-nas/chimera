// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

/*
 * setattr resolves the path through chimera_vfs_open, which internally picks
 * the path-op (path-only backends, e.g. SMB) or FH-relative (memfs/nfs)
 * strategy.  This avoids relying on a re-openable child fh from lookup, which
 * path-only mounts do not return.
 */
static void
chimera_setattr_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_client_request *request        = private_data;
    struct chimera_client_thread  *thread         = request->thread;
    chimera_setattr_callback_t     callback       = request->setattr.callback;
    void                          *callback_arg   = request->setattr.private_data;
    int                            heap_allocated = request->heap_allocated;
    enum chimera_vfs_error         status;

    status = chimera_vfs_compound_status(compound);

    /* The tail is written out rather than routed through
     * chimera_setattr_vfs_complete, which releases the handle the per-op path
     * opened: here the sequence owns that handle and frees it below, and
     * chimera_vfs_release does not take a NULL. */
    if (heap_allocated) {
        chimera_client_request_free(thread, request);
    }

    chimera_vfs_compound_free(compound);

    callback(thread, status, callback_arg);
} /* chimera_setattr_sequence_complete */

static inline void
chimera_dispatch_setattr(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_vfs_compound *compound;
    unsigned int                 open_flags;
    int                          open_idx, set_idx;

    /*
     * Use a real file handle instead of OPEN_PATH when setting size, because
     * ftruncate() requires a real file descriptor, not an O_PATH handle --
     * and open it with WRITE intent: truncate(2) by path requires write
     * permission on the file (unlike ftruncate, whose rights were bound at
     * its own open), and the open gate is where every backend -- engine-
     * gated and remote-DAC proxy alike -- enforces exactly that.  The wire
     * SETATTR cannot make the distinction (which is why servers owner-
     * override size changes, as nfsd does); the client must.
     */
    if (request->setattr.set_attr.va_set_mask & CHIMERA_VFS_ATTR_SIZE) {
        open_flags = CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_WRITE_ONLY;
    } else {
        open_flags = CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED;
    }

    compound = chimera_client_compound_at_root(thread, request);

    open_idx = chimera_vfs_compound_add_open_path(
        compound, request->setattr.path, request->setattr.path_len,
        open_flags, NULL, 0);

    /* Addressing the open's handle rather than handing it in keeps this a
     * setattr rather than an fsetattr -- the object's own mode is rechecked,
     * which is what truncate(2) by path means, and what the per-operation
     * path did. */
    set_idx = chimera_vfs_compound_add_setattr(compound, NULL,
                                               &request->setattr.set_attr, 0);

    if (open_idx >= 0 && set_idx >= 0) {
        chimera_vfs_compound_op_use_handle(compound, (uint32_t) set_idx,
                                           (uint32_t) open_idx);
    }

    chimera_frontend_compound_submit(compound, chimera_setattr_sequence_complete,
                                     request);
} /* chimera_dispatch_setattr */

/* Same compound as path SETATTR, with final-symlink traversal disabled. */
static inline void
chimera_dispatch_lsetattr(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_vfs_compound *compound = chimera_client_compound_at_root(thread, request);
    unsigned int                 flags    = CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_NOFOLLOW |
        ((request->setattr.set_attr.va_set_mask & CHIMERA_VFS_ATTR_SIZE) ?
         CHIMERA_VFS_OPEN_WRITE_ONLY : CHIMERA_VFS_OPEN_PATH);
    int                          open_idx = chimera_vfs_compound_add_open_path(compound,
                                                                               request->setattr.path, request->setattr.
                                                                               path_len, flags, NULL, 0);
    int                          set_idx = chimera_vfs_compound_add_setattr(compound, NULL,
                                                                            &request->setattr.set_attr, 0);

    if (open_idx >= 0 && set_idx >= 0) {
        chimera_vfs_compound_op_use_handle(compound, set_idx, open_idx);
    }
    chimera_frontend_compound_submit(compound, chimera_setattr_sequence_complete, request);
} /* chimera_dispatch_lsetattr */
