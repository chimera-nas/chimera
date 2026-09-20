// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

/*
 * setattr resolves the path through an OPEN, which internally picks the
 * path-op (path-only backends, e.g. SMB) or FH-relative (memfs/nfs) strategy.
 * This avoids relying on a re-openable child fh from lookup, which path-only
 * mounts do not return.
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

    /* The sequence owns the handle its OPEN produced and releases it in the
     * free below; nothing here releases anything. */
    if (heap_allocated) {
        chimera_client_request_free(thread, request);
    }

    chimera_vfs_compound_free(compound);

    callback(thread, status, callback_arg);
} /* chimera_setattr_sequence_complete */

/*
 * The open every setattr resolves through.
 *
 * Use a real file handle instead of OPEN_PATH when setting size, because
 * ftruncate() requires a real file descriptor, not an O_PATH handle -- and
 * open it with WRITE intent: truncate(2) by path requires write permission
 * on the file (unlike ftruncate, whose rights were bound at its own open),
 * and the open gate is where every backend -- engine-gated and remote-DAC
 * proxy alike -- enforces exactly that.  The wire SETATTR cannot make the
 * distinction (which is why servers owner-override size changes, as nfsd
 * does); the client must.
 */
static inline unsigned int
chimera_setattr_open_flags(
    const struct chimera_client_request *request,
    int                                  nofollow)
{
    unsigned int open_flags;

    if (request->setattr.set_attr.va_set_mask & CHIMERA_VFS_ATTR_SIZE) {
        open_flags = CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_WRITE_ONLY;
    } else {
        open_flags = CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED;
    }

    /* The attributes go on a final-component symlink itself rather than on
     * its target: lchown(2), and the *at() forms under AT_SYMLINK_NOFOLLOW. */
    if (nofollow) {
        open_flags |= CHIMERA_VFS_OPEN_NOFOLLOW;
    }

    return open_flags;
} /* chimera_setattr_open_flags */

/* OPEN_PATH from the export root, then the SETATTR through the open's
 * handle.  Addressing the open's handle rather than handing it in keeps this
 * a setattr rather than an fsetattr -- the object's own mode is rechecked,
 * which is what truncate(2) by path means, and what the per-operation path
 * did. */
static inline void
chimera_dispatch_setattr_path(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request,
    int                            nofollow)
{
    struct chimera_vfs_compound *compound;
    int                          open_idx, set_idx;

    compound = chimera_client_compound_at_root(thread, request);

    open_idx = chimera_vfs_compound_add_open_path(
        compound, request->setattr.path, request->setattr.path_len,
        chimera_setattr_open_flags(request, nofollow), NULL, 0);

    set_idx = chimera_vfs_compound_add_setattr(compound, NULL,
                                               &request->setattr.set_attr, 0, 0);

    if (open_idx >= 0 && set_idx >= 0) {
        chimera_vfs_compound_op_use_handle(compound, (uint32_t) set_idx,
                                           (uint32_t) open_idx);
    }

    chimera_vfs_compound_submit(compound, chimera_setattr_sequence_complete,
                                request);
} /* chimera_dispatch_setattr_path */

static inline void
chimera_dispatch_setattr(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_setattr_path(thread, request, 0);
} /* chimera_dispatch_setattr */

/* Variant that does NOT follow a final symlink (for lchown(2) and friends):
 * the attributes are applied to the symlink itself, not its target. */
static inline void
chimera_dispatch_lsetattr(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_setattr_path(thread, request, 1);
} /* chimera_dispatch_lsetattr */

/*
 * The *at() form with a real directory descriptor (fchownat, fchmodat,
 * utimensat): the descriptor is lent and checked to be a directory, then the
 * path form's own OPEN_PATH and SETATTR run from it -- honouring nofollow,
 * which the per-op chain this replaced ignored.  See
 * chimera_client_compound_at_dir for why a path op and not a named OPEN.
 */
static inline void
chimera_dispatch_setattr_at(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_vfs_compound *compound;
    int                          open_idx, set_idx;

    compound = chimera_client_compound_at_dir(thread, request,
                                              request->setattr.parent_handle,
                                              request->setattr.dir_open_flags);

    open_idx = chimera_vfs_compound_add_open_path(
        compound, request->setattr.path, request->setattr.path_len,
        chimera_setattr_open_flags(request, request->setattr.nofollow),
        NULL, 0);

    set_idx = chimera_vfs_compound_add_setattr(compound, NULL,
                                               &request->setattr.set_attr, 0, 0);

    if (open_idx >= 0 && set_idx >= 0) {
        chimera_vfs_compound_op_use_handle(compound, (uint32_t) set_idx,
                                           (uint32_t) open_idx);
    }

    chimera_vfs_compound_submit(compound, chimera_setattr_sequence_complete,
                                request);
} /* chimera_dispatch_setattr_at */
