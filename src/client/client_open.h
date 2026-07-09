// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_txn.h"

/* An open that creates or truncates mutates the namespace and needs a write
 * transaction; a plain open is a read-mode snapshot. */
static inline enum chimera_vfs_txn_mode
chimera_open_txn_mode(unsigned int flags)
{
    return (flags & (CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_TRUNCATE)) ?
           CHIMERA_VFS_TXN_WRITE : CHIMERA_VFS_TXN_READ;
} /* chimera_open_txn_mode */

/* Shared reply for both the path and _at open transactions.  The returned open
 * handle is stashed in request->sync_open_handle by the op completion. */
static void
chimera_open_reply(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_open_callback_t         callback     = request->open.callback;
    void                           *callback_arg = request->open.private_data;
    enum chimera_vfs_error          status       = request->txn_op_status;
    struct chimera_vfs_open_handle *oh           = request->sync_open_handle;

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

    chimera_client_txn_finish(request->thread, request, error_code);
} /* chimera_open_vfs_complete */

static void
chimera_open_start(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    /* set_attr (creation mode) is initialized by the caller. */
    chimera_vfs_open(
        thread->vfs_thread,
        chimera_client_req_cred(request), request->txn,
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
    chimera_client_txn_run(thread, request,
                           thread->client->root_fh,
                           thread->client->root_fh_len,
                           chimera_open_txn_mode(request->open.flags),
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

    chimera_client_txn_finish(request->thread, request, error_code);
} /* chimera_open_at_complete */

static void
chimera_open_at_start(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    /* set_attr (creation mode) is initialized by the caller. */
    chimera_vfs_open_at(
        thread->vfs_thread,
        chimera_client_req_cred(request), request->txn,
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

    chimera_client_txn_run(thread, request,
                           parent_handle->fh, parent_handle->fh_len,
                           chimera_open_txn_mode(request->open.flags),
                           chimera_open_at_start, chimera_open_reply);
} /* chimera_dispatch_open_at */
