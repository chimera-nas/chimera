// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

static void chimera_setattr_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data);

static void
chimera_setattr_vfs_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_client_request  *request       = private_data;
    struct chimera_client_thread   *thread        = request->thread;
    struct chimera_vfs_open_handle *handle        = request->setattr.parent_handle;
    chimera_setattr_callback_t      callback      = request->setattr.callback;
    void                           *callback_arg  = request->setattr.private_data;
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
    void                           *private_data)
{
    struct chimera_client_request *request = private_data;

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
        oh,
        &request->setattr.set_attr,
        0,  /* pre_attr_mask */
        0,  /* post_attr_mask */
        chimera_setattr_vfs_complete,
        request);
} /* chimera_setattr_open_complete */

static void
chimera_setattr_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;
    unsigned int                   open_flags;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_client_thread *thread       = request->thread;
        chimera_setattr_callback_t    callback     = request->setattr.callback;
        void                         *callback_arg = request->setattr.private_data;

        chimera_client_request_free(thread, request);
        callback(thread, error_code, callback_arg);
        return;
    }

    memcpy(request->fh, attr->va_fh, attr->va_fh_len);
    request->fh_len = attr->va_fh_len;

    /*
     * Use a real file handle instead of OPEN_PATH when setting size, because
     * ftruncate() requires a real file descriptor, not an O_PATH handle.
     */
    if (request->setattr.set_attr.va_req_mask & CHIMERA_VFS_ATTR_SIZE) {
        open_flags = CHIMERA_VFS_OPEN_INFERRED;
    } else {
        open_flags = CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED;
    }

    chimera_vfs_open(
        request->thread->vfs_thread,
        request->fh,
        request->fh_len,
        open_flags,
        chimera_setattr_open_complete,
        request);
} /* chimera_setattr_lookup_complete */

static inline void
chimera_dispatch_setattr(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_lookup_path(
        thread->vfs_thread,
        root_fh,
        sizeof(root_fh),
        request->setattr.path,
        request->setattr.path_len,
        CHIMERA_VFS_ATTR_FH,
        chimera_setattr_lookup_complete,
        request);
} /* chimera_dispatch_setattr */

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
        parent_handle,
        &request->setattr.set_attr,
        0,  /* pre_attr_mask */
        0,  /* post_attr_mask */
        chimera_setattr_dispatch_at_complete,
        request);
} /* chimera_dispatch_setattr_at */
