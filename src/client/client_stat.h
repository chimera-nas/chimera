// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <sys/sysmacros.h>

#include "client_internal.h"
#include "client_txn.h"

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
    struct chimera_client_request *request = private_data;

    if (error_code == CHIMERA_VFS_OK) {
        chimera_attrs_to_stat(attr, &request->sync_stat);
    }

    chimera_client_txn_finish(request->thread, request, error_code);
} /* chimera_stat_lookup_complete */

static void
chimera_stat_start(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_lookup(
        thread->vfs_thread,
        chimera_client_req_cred(request), request->txn,
        thread->client->root_fh,
        thread->client->root_fh_len,
        request->stat.path,
        request->stat.path_len,
        CHIMERA_VFS_ATTR_MASK_STAT,
        request->stat.flags,
        chimera_stat_lookup_complete,
        request);
} /* chimera_stat_start */

static void
chimera_stat_reply(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_stat_callback_t callback     = request->stat.callback;
    void                   *callback_arg = request->stat.private_data;
    enum chimera_vfs_error  status       = request->txn_op_status;
    struct chimera_stat     st           = request->sync_stat;

    chimera_client_request_free(thread, request);

    if (status != CHIMERA_VFS_OK) {
        callback(thread, status, NULL, callback_arg);
        return;
    }

    callback(thread, CHIMERA_VFS_OK, &st, callback_arg);
} /* chimera_stat_reply */

static inline void
chimera_dispatch_stat(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_client_txn_run(thread, request,
                           thread->client->root_fh,
                           thread->client->root_fh_len,
                           CHIMERA_VFS_TXN_READ,
                           chimera_stat_start, chimera_stat_reply);
} /* chimera_dispatch_stat */
