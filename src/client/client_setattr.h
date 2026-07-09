// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_txn.h"

/* Shared reply for every setattr transaction (path, lpath, and _at).  The
 * internally-opened handle (path variants) is released in the op completion;
 * the _at variant's parent handle is caller-owned and never released here. */
static void
chimera_setattr_reply(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_setattr_callback_t callback     = request->setattr.callback;
    void                      *callback_arg = request->setattr.private_data;
    enum chimera_vfs_error     status       = request->txn_op_status;

    chimera_client_request_free(thread, request);

    callback(thread, status, callback_arg);
} /* chimera_setattr_reply */

/* Resolve the open flags for a path-based setattr.  A real (INFERRED) handle is
* needed when setting size, because ftruncate requires a real fd, not O_PATH. */
static inline unsigned int
chimera_setattr_open_flags(struct chimera_client_request *request)
{
    if (request->setattr.set_attr.va_set_mask & CHIMERA_VFS_ATTR_SIZE) {
        return CHIMERA_VFS_OPEN_INFERRED;
    }
    return CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED;
} /* chimera_setattr_open_flags */

static void
chimera_setattr_vfs_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_client_request  *request = private_data;
    struct chimera_vfs_open_handle *handle  = request->setattr.parent_handle;

    /* Release the handle opened during this attempt before commit/replay. */
    chimera_vfs_release(request->thread->vfs_thread, handle);
    request->setattr.parent_handle = NULL;

    chimera_client_txn_finish(request->thread, request, error_code);
} /* chimera_setattr_vfs_complete */

static void
chimera_setattr_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data)
{
    struct chimera_client_request *request = private_data;

    (void) attr;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_client_txn_finish(request->thread, request, error_code);
        return;
    }

    request->setattr.parent_handle = oh;

    chimera_vfs_setattr(
        request->thread->vfs_thread,
        chimera_client_req_cred(request), request->txn,
        oh,
        &request->setattr.set_attr,
        0,  /* pre_attr_mask */
        0,  /* post_attr_mask */
        chimera_setattr_vfs_complete,
        request);
} /* chimera_setattr_open_complete */

/*
 * setattr resolves the path through chimera_vfs_open, which internally picks
 * the path-op (path-only backends, e.g. SMB) or FH-relative (memfs/nfs)
 * strategy.  This avoids relying on a re-openable child fh from lookup, which
 * path-only mounts do not return.
 */
static void
chimera_setattr_start(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_open(
        thread->vfs_thread,
        chimera_client_req_cred(request), request->txn,
        thread->client->root_fh,
        thread->client->root_fh_len,
        request->setattr.path,
        request->setattr.path_len,
        chimera_setattr_open_flags(request),
        NULL, /* set_attr */
        0,    /* attr_mask */
        chimera_setattr_open_complete,
        request);
} /* chimera_setattr_start */

static inline void
chimera_dispatch_setattr(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    request->setattr.parent_handle = NULL;

    chimera_client_txn_run(thread, request,
                           thread->client->root_fh,
                           thread->client->root_fh_len,
                           CHIMERA_VFS_TXN_WRITE,
                           chimera_setattr_start, chimera_setattr_reply);
} /* chimera_dispatch_setattr */

/* Variant that does NOT follow a final symlink (for lchown(2) and friends): the
 * attributes are applied to the symlink itself, not its target. */
static void
chimera_lsetattr_start(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_open(
        thread->vfs_thread,
        chimera_client_req_cred(request), request->txn,
        thread->client->root_fh,
        thread->client->root_fh_len,
        request->setattr.path,
        request->setattr.path_len,
        chimera_setattr_open_flags(request) | CHIMERA_VFS_OPEN_NOFOLLOW,
        NULL, /* set_attr */
        0,    /* attr_mask */
        chimera_setattr_open_complete,
        request);
} /* chimera_lsetattr_start */

static inline void
chimera_dispatch_lsetattr(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    request->setattr.parent_handle = NULL;

    chimera_client_txn_run(thread, request,
                           thread->client->root_fh,
                           thread->client->root_fh_len,
                           CHIMERA_VFS_TXN_WRITE,
                           chimera_lsetattr_start, chimera_setattr_reply);
} /* chimera_dispatch_lsetattr */

/* Completion callback for setattr_at operations - doesn't release parent handle */
static void
chimera_setattr_dispatch_at_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    /* Note: parent handle is NOT released - caller owns it */
    chimera_client_txn_finish(request->thread, request, error_code);
} /* chimera_setattr_dispatch_at_complete */

static void
chimera_setattr_at_start(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_setattr(
        thread->vfs_thread,
        chimera_client_req_cred(request), request->txn,
        request->setattr.parent_handle,
        &request->setattr.set_attr,
        0,  /* pre_attr_mask */
        0,  /* post_attr_mask */
        chimera_setattr_dispatch_at_complete,
        request);
} /* chimera_setattr_at_start */

static inline void
chimera_dispatch_setattr_at(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *parent_handle,
    struct chimera_client_request  *request)
{
    request->setattr.parent_handle = parent_handle;

    chimera_client_txn_run(thread, request,
                           parent_handle->fh, parent_handle->fh_len,
                           CHIMERA_VFS_TXN_WRITE,
                           chimera_setattr_at_start, chimera_setattr_reply);
} /* chimera_dispatch_setattr_at */
