// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_dispatch.h"

/*
 * Remove flow (optimized for silly rename):
 * 1. Lookup the child to get child FH (if ENOENT, return immediately)
 * 2. Lookup the parent to get parent FH
 * 3. Open the parent to get parent handle
 * 4. Call remove with parent handle + child name + child FH
 *
 * This allows the backend (e.g., NFS3) to skip its own lookup since
 * we provide the child FH directly. The backend can use the child FH
 * to check if the file is open and needs silly rename handling.
 */

static void chimera_remove_parent_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data);

static void
chimera_remove_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_client_request  *request        = private_data;
    struct chimera_client_thread   *thread         = request->thread;
    struct chimera_vfs_open_handle *parent_handle  = request->remove.parent_handle;
    chimera_remove_callback_t       callback       = request->remove.callback;
    void                           *callback_arg   = request->remove.private_data;
    int                             heap_allocated = request->heap_allocated;

    if (heap_allocated) {
        chimera_client_request_free(thread, request);
    }

    chimera_vfs_release(thread->vfs_thread, parent_handle);

    callback(thread, error_code, callback_arg);

} /* chimera_remove_complete */

static void
chimera_remove_parent_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_client_thread *thread       = request->thread;
        chimera_remove_callback_t     callback     = request->remove.callback;
        void                         *callback_arg = request->remove.private_data;

        chimera_client_request_free(thread, request);
        callback(thread, error_code, callback_arg);
        return;
    }

    request->remove.parent_handle = oh;

    chimera_vfs_remove(
        request->thread->vfs_thread,
        oh,
        request->remove.path + request->remove.name_offset,
        request->remove.path_len - request->remove.name_offset,
        request->remove.child_fh,
        request->remove.child_fh_len,
        0,
        0,
        chimera_remove_complete,
        request);

} /* chimera_remove_parent_open_complete */

static void
chimera_remove_parent_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_client_thread *thread       = request->thread;
        chimera_remove_callback_t     callback     = request->remove.callback;
        void                         *callback_arg = request->remove.private_data;

        chimera_client_request_free(thread, request);
        callback(thread, error_code, callback_arg);
        return;
    }

    chimera_vfs_open(
        request->thread->vfs_thread,
        attr->va_fh,
        attr->va_fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
        chimera_remove_parent_open_complete,
        request);

} /* chimera_remove_parent_lookup_complete */

static void
chimera_remove_child_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;
    struct chimera_client_thread  *thread  = request->thread;

    if (error_code != CHIMERA_VFS_OK) {
        /* Child doesn't exist or other error - return immediately */
        chimera_remove_callback_t callback     = request->remove.callback;
        void                     *callback_arg = request->remove.private_data;

        chimera_client_request_free(thread, request);
        callback(thread, error_code, callback_arg);
        return;
    }

    /* Save the child FH for the remove call */
    request->remove.child_fh_len = attr->va_fh_len;
    memcpy(request->remove.child_fh, attr->va_fh, attr->va_fh_len);

    /* Now lookup the parent directory */
    chimera_vfs_lookup_path(
        thread->vfs_thread,
        thread->client->root_fh,
        thread->client->root_fh_len,
        request->remove.path,
        request->remove.parent_len,
        CHIMERA_VFS_ATTR_FH,
        CHIMERA_VFS_LOOKUP_FOLLOW,
        chimera_remove_parent_lookup_complete,
        request);

} /* chimera_remove_child_lookup_complete */

static inline void
chimera_dispatch_remove(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{

    if (unlikely(request->remove.name_offset == -1)) {
        chimera_dispatch_error_remove(thread, request, CHIMERA_VFS_EINVAL);
        return;
    }

    /* First lookup the child to get its FH.
     * Use NOFOLLOW (0) because we want the FH of the symlink itself,
     * not the target it points to. */
    chimera_vfs_lookup_path(
        thread->vfs_thread,
        thread->client->root_fh,
        thread->client->root_fh_len,
        request->remove.path,
        request->remove.path_len,  /* Full path to child */
        CHIMERA_VFS_ATTR_FH,
        0,  /* Don't follow symlinks - we want to remove the symlink, not target */
        chimera_remove_child_lookup_complete,
        request);
} /* chimera_dispatch_remove */

static void
chimera_remove_dispatch_at_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_client_request *request        = private_data;
    struct chimera_client_thread  *thread         = request->thread;
    chimera_remove_callback_t      callback       = request->remove.callback;
    void                          *callback_arg   = request->remove.private_data;
    int                            heap_allocated = request->heap_allocated;

    if (heap_allocated) {
        chimera_client_request_free(thread, request);
    }

    /* Note: parent handle is NOT released - caller owns it */
    callback(thread, error_code, callback_arg);
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
        /* Child doesn't exist or other error - return immediately */
        chimera_remove_callback_t callback       = request->remove.callback;
        void                     *callback_arg   = request->remove.private_data;
        int                       heap_allocated = request->heap_allocated;

        if (heap_allocated) {
            chimera_client_request_free(thread, request);
        }
        callback(thread, error_code, callback_arg);
        return;
    }

    /* Save the child FH for the remove call */
    request->remove.child_fh_len = attr->va_fh_len;
    memcpy(request->remove.child_fh, attr->va_fh, attr->va_fh_len);

    /* Now call remove with the child FH */
    chimera_vfs_remove(
        thread->vfs_thread,
        request->remove.parent_handle,
        request->remove.path,
        request->remove.path_len,
        request->remove.child_fh,
        request->remove.child_fh_len,
        0,
        0,
        chimera_remove_dispatch_at_complete,
        request);
} /* chimera_remove_at_lookup_complete */

static inline void
chimera_dispatch_remove_at(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *parent_handle,
    struct chimera_client_request  *request)
{
    /* Save parent handle for use in callback */
    request->remove.parent_handle = parent_handle;

    /* First lookup the child to get its FH for silly rename optimization.
     * Use NOFOLLOW (0) because we want the FH of the symlink itself,
     * not the target it points to. */
    chimera_vfs_lookup(
        thread->vfs_thread,
        parent_handle,
        request->remove.path,
        request->remove.path_len,
        CHIMERA_VFS_ATTR_FH,
        0,
        chimera_remove_at_lookup_complete,
        request);
} /* chimera_dispatch_remove_at */
