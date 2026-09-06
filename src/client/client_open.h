// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_compound.h"

/* An open that creates or truncates mutates the namespace and needs a write
 * compound; a plain open is a read-mode snapshot. */
static inline enum chimera_vfs_compound_mode
chimera_open_compound_mode(unsigned int flags)
{
    return (flags & (CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_TRUNCATE)) ?
           CHIMERA_VFS_COMPOUND_WRITE : CHIMERA_VFS_COMPOUND_READ;
} /* chimera_open_compound_mode */

/* Shared reply for both the path and _at open compounds.  The returned open
 * handle is stashed in request->sync_open_handle by the op completion. */
static void
chimera_open_reply(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_open_callback_t         callback     = request->open.callback;
    void                           *callback_arg = request->open.private_data;
    enum chimera_vfs_error          status       = request->compound_op_status;
    struct chimera_vfs_open_handle *oh           = request->sync_open_handle;

    /* A failed compound can still be carrying the op's open handle: the open
     * itself succeeded and the failure came from the commit (a hard commit
     * error, or ECOMPOUND_EXHAUSTED once the replay budget is spent).  The
     * caller only consumes the handle on success, so release the reference
     * here and report the failure with no handle. */
    if (status != CHIMERA_VFS_OK && oh) {
        chimera_vfs_release(thread->vfs_thread, oh);
        oh = NULL;
    }

    chimera_client_request_free(thread, request);

    callback(thread, status, oh, callback_arg);
} /* chimera_open_reply */

static void
chimera_open_vfs_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data)
{
    struct chimera_client_request *request = private_data;

    request->sync_open_handle = oh;

    chimera_client_compound_finish(request->thread, request, error_code);
} /* chimera_open_vfs_complete */

/* Unlike the other path ops, an open cannot release its per-attempt handle
 * before commit -- the handle IS the result.  So each replay leg must instead
 * release the PREVIOUS attempt's reference before re-acquiring: the cache
 * hands the same (fh, access_mode, cred) handle back with another opencnt,
 * and overwriting sync_open_handle would strand the first reference forever
 * (seen as an opencnt that umount can never drain). */
static inline void
chimera_open_release_prev_attempt(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    if (request->sync_open_handle) {
        chimera_vfs_release(thread->vfs_thread, request->sync_open_handle);
        request->sync_open_handle = NULL;
    }
} /* chimera_open_release_prev_attempt */

static void
chimera_open_start(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_open_release_prev_attempt(thread, request);

    /* set_attr (creation mode) is initialized by the caller. */
    chimera_vfs_open(
        thread->vfs_thread,
        chimera_client_req_cred(request), request->compound,
        thread->client->root_fh,
        thread->client->root_fh_len,
        request->open.path,
        request->open.path_len,
        request->open.flags,
        &request->open.set_attr,
        CHIMERA_VFS_ATTR_FH,
        chimera_open_vfs_complete,
        request);
} /* chimera_open_start */

static inline void
chimera_dispatch_open(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    /* Requests are pooled: anchor the "non-NULL means a live reference from a
     * previous attempt" invariant the replay release above relies on. */
    request->sync_open_handle = NULL;

    chimera_client_compound_run(thread, request,
                                thread->client->root_fh,
                                thread->client->root_fh_len,
                                chimera_open_compound_mode(request->open.flags),
                                chimera_open_start, chimera_open_reply);
} /* chimera_dispatch_open */

static void
chimera_open_at_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data)
{
    struct chimera_client_request *request = private_data;

    request->sync_open_handle = oh;

    chimera_client_compound_finish(request->thread, request, error_code);
} /* chimera_open_at_complete */

static void
chimera_open_at_start(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_open_release_prev_attempt(thread, request);

    /* set_attr (creation mode) is initialized by the caller. */
    chimera_vfs_open_at(
        thread->vfs_thread,
        chimera_client_req_cred(request), request->compound,
        request->open.parent_handle,
        request->open.path,
        request->open.path_len,
        request->open.flags,
        &request->open.set_attr,
        CHIMERA_VFS_ATTR_FH,
        0,
        0,
        chimera_open_at_complete,
        request);
} /* chimera_open_at_start */

static inline void
chimera_dispatch_open_at(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *parent_handle,
    struct chimera_client_request  *request)
{
    request->open.parent_handle = parent_handle;

    /* See chimera_dispatch_open: pooled requests, anchor the invariant. */
    request->sync_open_handle = NULL;

    chimera_client_compound_run(thread, request,
                                parent_handle->fh, parent_handle->fh_len,
                                chimera_open_compound_mode(request->open.flags),
                                chimera_open_at_start, chimera_open_reply);
} /* chimera_dispatch_open_at */
