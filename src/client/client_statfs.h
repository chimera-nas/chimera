// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

static inline void
chimera_attrs_to_statvfs(
    const struct chimera_vfs_attrs *attrs,
    struct chimera_statvfs         *st)
{
    // Use a default block size of 4096 (common filesystem block size)
    st->f_bsize   = 4096;
    st->f_frsize  = 4096;
    st->f_blocks  = attrs->va_fs_space_total / st->f_frsize;
    st->f_bfree   = attrs->va_fs_space_free / st->f_frsize;
    st->f_bavail  = attrs->va_fs_space_avail / st->f_frsize;
    st->f_files   = attrs->va_fs_files_total;
    st->f_ffree   = attrs->va_fs_files_free;
    st->f_favail  = attrs->va_fs_files_avail;
    st->f_fsid    = attrs->va_fsid;
    st->f_flag    = 0;
    st->f_namemax = 255;
} /* chimera_attrs_to_statvfs */

/*
 * statfs is filesystem-wide: resolve the path through an OPEN_PATH op (which
 * picks the path-op vs FH-relative strategy internally, so it works on
 * path-only mounts that return no re-openable child fh from lookup), then read
 * the statfs attributes from the resulting handle.
 */
static void
chimera_statfs_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_client_request        *request = private_data;
    struct chimera_client_thread         *thread  = request->thread;
    const struct chimera_vfs_compound_op *op;
    chimera_statfs_callback_t             callback       = request->statfs.callback;
    void                                 *callback_arg   = request->statfs.private_data;
    int                                   heap_allocated = request->heap_allocated;
    struct chimera_statvfs                st;
    enum chimera_vfs_error                status;

    status = chimera_vfs_compound_status(compound);

    if (status == CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(compound,
                                     chimera_vfs_compound_num_ops(compound) - 1);
        chimera_attrs_to_statvfs((struct chimera_vfs_attrs *) &op->attr, &st);
    }

    /* The sequence owns the handle its OPEN produced and releases it in the
     * free below; nothing here releases anything. */
    if (heap_allocated) {
        chimera_client_request_free(thread, request);
    }

    chimera_vfs_compound_free(compound);

    callback(thread, status, status == CHIMERA_VFS_OK ? &st : NULL,
             callback_arg);
} /* chimera_statfs_sequence_complete */

static inline void
chimera_dispatch_statfs(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_vfs_compound *compound;
    int                          open_idx, stat_idx;

    compound = chimera_client_compound_at_root(thread, request);

    open_idx = chimera_vfs_compound_add_open_path(
        compound, request->statfs.path, request->statfs.path_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED, NULL, 0);

    stat_idx = chimera_vfs_compound_add_getattr(compound,
                                                CHIMERA_VFS_ATTR_MASK_STATFS);

    /* Address the handle the open produced rather than the current object:
     * on a path-only mount that handle is the only usable reference to what
     * the path resolved. */
    if (open_idx >= 0 && stat_idx >= 0) {
        chimera_vfs_compound_op_use_handle(compound, (uint32_t) stat_idx,
                                           (uint32_t) open_idx);
    }

    chimera_vfs_compound_submit(compound, chimera_statfs_sequence_complete,
                                request);
} /* chimera_dispatch_statfs */
