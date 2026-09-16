// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <sys/stat.h>
#ifdef _WIN32
#include "common/platform.h"
#endif // ifdef _WIN32

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
chimera_stat_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request      = private_data;
    struct chimera_client_thread  *thread       = request->thread;
    chimera_stat_callback_t        callback     = request->stat.callback;
    void                          *callback_arg = request->stat.private_data;
    struct chimera_stat            st;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_client_request_free(thread, request);
        callback(thread, error_code, NULL, callback_arg);
        return;
    }

    chimera_attrs_to_stat(attr, &st);

    chimera_client_request_free(thread, request);

    callback(thread, CHIMERA_VFS_OK, &st, callback_arg);
} /* chimera_stat_lookup_complete */

static void
chimera_stat_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_attrs              attr;
    enum chimera_vfs_error                status;

    status = chimera_vfs_compound_status(compound);

    memset(&attr, 0, sizeof(attr));

    if (status == CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(compound,
                                     chimera_vfs_compound_num_ops(compound) - 1);
        attr = op->attr;
    }

    /* Taken out before the free: a freed sequence is recycled and reset. */
    chimera_vfs_compound_free(compound);

    chimera_stat_lookup_complete(status, &attr, private_data);
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
     * a path-only mount, where the resolved child has no re-openable handle.
     *
     * Resolution starts at the export root, not at request->stat.handle.  That
     * field is vestigial here: chimera_stat() is the only entry point and sets
     * it NULL, so the dirfd branch this replaced -- and the ENOTDIR dircheck
     * that guarded it, which asked the descriptor's live inode whether it was
     * still a directory -- could not be reached.  Wiring an *at() entry point
     * means restoring both, against a sequence that starts at the handle. */
    chimera_vfs_compound_add_lookup_path(compound,
                                         request->stat.path,
                                         request->stat.path_len,
                                         CHIMERA_VFS_ATTR_MASK_STAT,
                                         request->stat.flags);

    chimera_vfs_compound_submit(compound, chimera_stat_sequence_complete,
                                request);
} /* chimera_dispatch_stat */
