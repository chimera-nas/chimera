// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <sys/stat.h>

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

static inline void
chimera_stat_walk(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_vfs_open_handle *parent = request->stat.handle;

    chimera_vfs_lookup(
        thread->vfs_thread,
        chimera_client_req_cred(request),
        parent ? parent->fh : thread->client->root_fh,
        parent ? parent->fh_len : thread->client->root_fh_len,
        request->stat.path,
        request->stat.path_len,
        CHIMERA_VFS_ATTR_MASK_STAT,
        request->stat.flags,
        chimera_stat_lookup_complete,
        request);
} /* chimera_stat_walk */

/* Resolving a relative path under a non-directory dirfd is ENOTDIR, and the
 * answer comes from the descriptor's LIVE inode -- a directory whose name was
 * unlinked while the fd stayed open is still a directory, where re-resolving
 * its stale path would give a misleading ENOENT, and a path-only backend
 * (the SMB proxy) can answer no other way.  Same shape as utimensat's
 * dircheck. */
static void
chimera_stat_dircheck_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;
    struct chimera_client_thread  *thread  = request->thread;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_stat_callback_t callback     = request->stat.callback;
        void                   *callback_arg = request->stat.private_data;

        chimera_client_request_free(thread, request);
        callback(thread, error_code, NULL, callback_arg);
        return;
    }

    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) && !S_ISDIR(attr->va_mode)) {
        chimera_stat_callback_t callback     = request->stat.callback;
        void                   *callback_arg = request->stat.private_data;

        chimera_client_request_free(thread, request);
        callback(thread, CHIMERA_VFS_ENOTDIR, NULL, callback_arg);
        return;
    }

    chimera_stat_walk(thread, request);
} /* chimera_stat_dircheck_complete */

/*
 * `handle`, when set, is the *at() family's directory descriptor: the walk
 * starts from that open directory instead of the export root, and the path is
 * relative to it.  NULL is AT_FDCWD (and any absolute path), which starts at
 * the root.
 */
static inline void
chimera_dispatch_stat(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    if (request->stat.handle) {
        chimera_vfs_getattr(
            thread->vfs_thread,
            chimera_client_req_cred(request),
            request->stat.handle,
            CHIMERA_VFS_ATTR_MASK_STAT,
            chimera_stat_dircheck_complete,
            request);
        return;
    }

    chimera_stat_walk(thread, request);
} /* chimera_dispatch_stat */
