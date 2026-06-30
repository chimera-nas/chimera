// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_dispatch.h"
#include "client_txn.h"

/* Shared reply for both the path and _at remove transactions. */
static void
chimera_remove_reply(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_remove_callback_t callback     = request->remove.callback;
    void                     *callback_arg = request->remove.private_data;
    enum chimera_vfs_error    status       = request->txn_op_status;

    /* Note: parent handle (for the _at variant) is NOT released - caller owns it */
    chimera_client_request_free(thread, request);

    callback(thread, status, callback_arg);
} /* chimera_remove_reply */

static void
chimera_remove_vfs_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_client_request *request = private_data;

    chimera_client_txn_finish(request->thread, request, error_code);
} /* chimera_remove_vfs_complete */

static void
chimera_remove_start(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_remove(
        thread->vfs_thread,
        chimera_client_req_cred(request), request->txn,
        thread->client->root_fh,
        thread->client->root_fh_len,
        request->remove.path,
        request->remove.path_len,
        chimera_remove_vfs_complete,
        request);
} /* chimera_remove_start */

static inline void
chimera_dispatch_remove(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{

    if (unlikely(request->remove.name_offset == -1)) {
        chimera_dispatch_error_remove(thread, request, CHIMERA_VFS_EINVAL);
        return;
    }

    chimera_client_txn_run(thread, request,
                           thread->client->root_fh,
                           thread->client->root_fh_len,
                           CHIMERA_VFS_TXN_WRITE,
                           chimera_remove_start, chimera_remove_reply);
} /* chimera_dispatch_remove */

static void
chimera_remove_dispatch_at_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    chimera_client_txn_finish(request->thread, request, error_code);
} /* chimera_remove_dispatch_at_complete */

static void
chimera_remove_at_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;
    struct chimera_client_thread  *thread  = request->thread;

    if (error_code != CHIMERA_VFS_OK) {
        /* Child doesn't exist or other error - fail the transaction. */
        chimera_client_txn_finish(thread, request, error_code);
        return;
    }

    /* Save the child FH for the remove call */
    request->remove.child_fh_len = attr->va_fh_len;
    memcpy(request->remove.child_fh, attr->va_fh, attr->va_fh_len);

    /* Now call remove with the child FH */
    chimera_vfs_remove_at(
        thread->vfs_thread,
        chimera_client_req_cred(request), request->txn,
        request->remove.parent_handle,
        request->remove.path,
        request->remove.path_len,
        request->remove.child_fh,
        request->remove.child_fh_len,
        0,
        0,
        NULL,
        chimera_remove_dispatch_at_complete,
        request);
} /* chimera_remove_at_lookup_complete */

static void
chimera_remove_at_start(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    /* First lookup the child to get its FH for silly rename optimization.
     * Use NOFOLLOW (0) because we want the FH of the symlink itself,
     * not the target it points to. */
    chimera_vfs_lookup_at(
        thread->vfs_thread,
        chimera_client_req_cred(request), request->txn,
        request->remove.parent_handle,
        request->remove.path,
        request->remove.path_len,
        CHIMERA_VFS_ATTR_FH,
        0,
        chimera_remove_at_lookup_complete,
        request);
} /* chimera_remove_at_start */

static inline void
chimera_dispatch_remove_at(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *parent_handle,
    struct chimera_client_request  *request)
{
    /* Save parent handle for use in the start/lookup callbacks */
    request->remove.parent_handle = parent_handle;

    chimera_client_txn_run(thread, request,
                           parent_handle->fh, parent_handle->fh_len,
                           CHIMERA_VFS_TXN_WRITE,
                           chimera_remove_at_start, chimera_remove_reply);
} /* chimera_dispatch_remove_at */
