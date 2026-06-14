// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

static void chimera_setattr_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data);

static void
chimera_setattr_vfs_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_client_request  *request        = private_data;
    struct chimera_client_thread   *thread         = request->thread;
    struct chimera_vfs_open_handle *handle         = request->setattr.parent_handle;
    chimera_setattr_callback_t      callback       = request->setattr.callback;
    void                           *callback_arg   = request->setattr.private_data;
    int                             heap_allocated = request->heap_allocated;

    if (heap_allocated) {
        chimera_client_request_free(thread, request);
    }

    chimera_vfs_release(thread->vfs_thread, handle);

    callback(thread, error_code, callback_arg);
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
        struct chimera_client_thread *thread       = request->thread;
        chimera_setattr_callback_t    callback     = request->setattr.callback;
        void                         *callback_arg = request->setattr.private_data;

        chimera_client_request_free(thread, request);
        callback(thread, error_code, callback_arg);
        return;
    }

    request->setattr.parent_handle = oh;

    chimera_vfs_setattr(
        request->thread->vfs_thread,
        chimera_client_req_cred(request),
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
static inline void
chimera_dispatch_setattr(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    unsigned int open_flags;

    /*
     * Use a real file handle instead of OPEN_PATH when setting size, because
     * ftruncate() requires a real file descriptor, not an O_PATH handle.
     */
    if (request->setattr.set_attr.va_set_mask & CHIMERA_VFS_ATTR_SIZE) {
        open_flags = CHIMERA_VFS_OPEN_INFERRED;
    } else {
        open_flags = CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED;
    }

    chimera_vfs_open(
        thread->vfs_thread,
        chimera_client_req_cred(request),
        thread->client->root_fh,
        thread->client->root_fh_len,
        request->setattr.path,
        request->setattr.path_len,
        open_flags,
        NULL, /* set_attr */
        0,    /* attr_mask */
        chimera_setattr_open_complete,
        request);
} /* chimera_dispatch_setattr */

/* Variant that does NOT follow a final symlink (for lchown(2) and friends): the
 * attributes are applied to the symlink itself, not its target.  Resolves through
 * chimera_vfs_open with OPEN_NOFOLLOW (capability-aware, like chimera_dispatch_
 * setattr) rather than a lookup that returns no re-openable child fh on a
 * path-only mount. */
static inline void
chimera_dispatch_lsetattr(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    unsigned int open_flags;

    if (request->setattr.set_attr.va_set_mask & CHIMERA_VFS_ATTR_SIZE) {
        open_flags = CHIMERA_VFS_OPEN_INFERRED;
    } else {
        open_flags = CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED;
    }
    open_flags |= CHIMERA_VFS_OPEN_NOFOLLOW;

    chimera_vfs_open(
        thread->vfs_thread,
        chimera_client_req_cred(request),
        thread->client->root_fh,
        thread->client->root_fh_len,
        request->setattr.path,
        request->setattr.path_len,
        open_flags,
        NULL, /* set_attr */
        0,    /* attr_mask */
        chimera_setattr_open_complete,
        request);
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
    struct chimera_client_request *request        = private_data;
    struct chimera_client_thread  *thread         = request->thread;
    chimera_setattr_callback_t     callback       = request->setattr.callback;
    void                          *callback_arg   = request->setattr.private_data;
    int                            heap_allocated = request->heap_allocated;

    if (heap_allocated) {
        chimera_client_request_free(thread, request);
    }

    /* Note: parent handle is NOT released - caller owns it */
    callback(thread, error_code, callback_arg);
} /* chimera_setattr_dispatch_at_complete */

static inline void
chimera_dispatch_setattr_at(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *parent_handle,
    struct chimera_client_request  *request)
{
    chimera_vfs_setattr(
        thread->vfs_thread,
        chimera_client_req_cred(request),
        parent_handle,
        &request->setattr.set_attr,
        0,  /* pre_attr_mask */
        0,  /* post_attr_mask */
        chimera_setattr_dispatch_at_complete,
        request);
} /* chimera_dispatch_setattr_at */
