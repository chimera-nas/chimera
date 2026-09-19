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

/*
 * A directory descriptor has to BE a directory, and that has to be answered
 * from the descriptor itself rather than from how the walk behind it failed:
 * resolving a path through a regular file surfaces as ENOTDIR on some backends
 * and ENOENT on others (the SMB proxy), and POSIX owes fstatat() ENOTDIR
 * either way.  The gate asks the question of the GETATTR the sequence already
 * ran, so it costs no extra round trip.
 */
static void
chimera_stat_dircheck_gate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct chimera_client_request        *request = private_data;
    const struct chimera_vfs_compound_op *op;

    if (*status != CHIMERA_VFS_OK || (int) index != request->gate_index) {
        return;
    }

    op = chimera_vfs_compound_op(compound, index);

    if ((op->attr.va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
        !S_ISDIR(op->attr.va_mode)) {
        *status = CHIMERA_VFS_ENOTDIR;
    }
} /* chimera_stat_dircheck_gate */

static inline void
chimera_dispatch_stat(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_vfs_compound *compound;

    /* `handle`, when set, is the *at() family's directory descriptor: the walk
     * starts from that open directory and the path is relative to it.  NULL is
     * AT_FDCWD (and any absolute path), which starts at the export root.
     *
     * chimera_fstatat() is the entry point that sets it -- an earlier version
     * of this function assumed chimera_stat() was the only caller, resolved
     * every path from the root, and so answered fstatat(dfd, "b") for the
     * WRONG directory whenever dfd was not the root. */
    if (request->stat.handle) {
        compound = chimera_vfs_compound_alloc(thread->vfs_thread,
                                              chimera_client_req_cred(request));
        request->compound = compound;

        /* PUTHANDLE, not PUTFH: the descriptor is a handle we already hold,
         * and the GETATTR below has to ask the LIVE inode.  Naming it by
         * filehandle instead would make the executor re-open it, which a
         * path-only backend cannot do at all -- only the mount root is
         * re-openable over the SMB proxy -- and which would answer from a
         * re-resolved name even where it works, so a directory unlinked while
         * the fd stayed open would look like ENOENT instead of the directory
         * it still is.  The flags are what the descriptor was really opened
         * with -- see open_flags on the request. */
        chimera_vfs_compound_add_puthandle(compound, request->stat.handle,
                                           request->stat.open_flags);

        request->gate_index =
            chimera_vfs_compound_add_getattr(compound, CHIMERA_VFS_ATTR_MODE);
        chimera_vfs_compound_set_gate(compound, chimera_stat_dircheck_gate,
                                      request);
    } else {
        compound = chimera_client_compound_at_root(thread, request);
    }

    /* One op for the whole path: the lookup returns the attributes with it, so
    * there is no open and no getattr -- which is also what makes this work on
    * a path-only mount, where the resolved child has no re-openable handle. */
    chimera_vfs_compound_add_lookup_path(compound,
                                         request->stat.path,
                                         request->stat.path_len,
                                         CHIMERA_VFS_ATTR_MASK_STAT,
                                         request->stat.flags);

    chimera_vfs_compound_submit(compound, chimera_stat_sequence_complete,
                                request);
} /* chimera_dispatch_stat */
