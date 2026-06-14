// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

static void chimera_readlink_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data);

static void
chimera_readlink_complete(
    enum chimera_vfs_error    error_code,
    int                       targetlen,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request  *request        = private_data;
    struct chimera_client_thread   *thread         = request->thread;
    struct chimera_vfs_open_handle *handle         = request->readlink.handle;
    chimera_readlink_callback_t     callback       = request->readlink.callback;
    void                           *callback_arg   = request->readlink.private_data;
    char                           *target         = request->readlink.target;
    int                             heap_allocated = request->heap_allocated;

    if (heap_allocated) {
        chimera_client_request_free(thread, request);
    }

    chimera_vfs_release(thread->vfs_thread, handle);

    callback(thread, error_code, target, targetlen, callback_arg);

} /* chimera_readlink_complete */

static void
chimera_readlink_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data)
{
    struct chimera_client_request *request = private_data;

    (void) attr;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_client_thread *thread       = request->thread;
        chimera_readlink_callback_t   callback     = request->readlink.callback;
        void                         *callback_arg = request->readlink.private_data;

        chimera_client_request_free(thread, request);
        callback(thread, error_code, NULL, 0, callback_arg);
        return;
    }

    request->readlink.handle = oh;

    chimera_vfs_readlink(
        request->thread->vfs_thread,
        chimera_client_req_cred(request),
        oh,
        request->readlink.target,
        request->readlink.target_maxlength,
        0,
        chimera_readlink_complete,
        request);

} /* chimera_readlink_open_complete */

static inline void
chimera_dispatch_readlink(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    /*
     * Resolve the path through chimera_vfs_open so this works for both
     * path-only backends (which return no re-openable child fh from lookup)
     * and FH-relative backends.  NOFOLLOW keeps the final symlink itself
     * (its target is what we want to read), rather than following it.
     */
    chimera_vfs_open(
        thread->vfs_thread,
        chimera_client_req_cred(request),
        thread->client->root_fh,
        thread->client->root_fh_len,
        request->readlink.path,
        request->readlink.path_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_NOFOLLOW,
        NULL, /* set_attr */
        0,    /* attr_mask */
        chimera_readlink_open_complete,
        request);
} /* chimera_dispatch_readlink */
