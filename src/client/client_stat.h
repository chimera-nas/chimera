// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "common/platform.h"

#include "client_internal.h"

static inline void
chimera_attrs_to_stat(
    const struct chimera_vfs_attrs *attrs,
    struct chimera_stat            *st)
{
    st->st_dev   = attrs->va_dev;
    st->st_ino   = attrs->va_ino;
    st->st_mode  = attrs->va_mode;
    st->st_nlink = attrs->va_nlink;
    st->st_uid   = attrs->va_uid;
    st->st_gid   = attrs->va_gid;
    /* va_rdev is the canonical VFS encoding (major << 32 | minor); decode it
     * back to a host dev_t for the caller's struct stat. */
    st->st_rdev = makedev(attrs->va_rdev >> 32, attrs->va_rdev & 0xFFFFFFFF);
    st->st_size = attrs->va_size;
    st->st_atim = attrs->va_atime;
    st->st_mtim = attrs->va_mtime;
    st->st_ctim = attrs->va_ctime;
} /* chimera_attrs_to_stat */

/*
 * stat resolves the path with a single lookup that returns the full stat
 * attributes directly -- no separate open + getattr.  This works for every
 * backend (the VFS lookup returns whatever attrs are requested) and is required
 * for path-only mounts, where lookup returns no re-openable child fh.
 */
static void
chimera_stat_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_client_request        *request = private_data;
    struct chimera_client_thread         *thread  = request->thread;
    const struct chimera_vfs_compound_op *op;
    chimera_stat_callback_t               callback     = request->stat.callback;
    void                                 *callback_arg = request->stat.private_data;
    struct chimera_stat                   st;
    enum chimera_vfs_error                status;

    status = chimera_vfs_compound_status(compound);

    if (status == CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(compound,
                                     chimera_vfs_compound_num_ops(compound) - 1);
        chimera_attrs_to_stat(&op->attr, &st);
    }

    chimera_vfs_compound_free(compound);
    chimera_client_request_free(thread, request);

    callback(thread, status, status == CHIMERA_VFS_OK ? &st : NULL,
             callback_arg);
} /* chimera_stat_sequence_complete */

static inline void
chimera_dispatch_stat(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_vfs_compound *compound;

    compound = chimera_client_compound_at_root(thread, request);

    /* One op for the whole path: the lookup returns the attributes with it, so
    * there is no open and no getattr -- which is also what makes this work on
    * a path-only mount, where the resolved child has no re-openable handle. */
    chimera_vfs_compound_add_lookup_path(compound,
                                         request->stat.path,
                                         request->stat.path_len,
                                         CHIMERA_VFS_ATTR_MASK_STAT,
                                         request->stat.flags);

    chimera_frontend_compound_submit(compound, chimera_stat_sequence_complete,
                                     request);
} /* chimera_dispatch_stat */
