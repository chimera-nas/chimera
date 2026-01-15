// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

static inline void
chimera_attrs_to_statvfs(
    const struct chimera_vfs_attrs *attrs,
    struct chimera_statvfs         *st)
{
    // Use a default block size of 4096 (common filesystem block size)
    st->f_bsize   = 4096;
    st->f_frsize  = 4096;
    st->f_blocks  = attrs->va_fs_space_total / st->f_frsize;
    st->f_bfree   = attrs->va_fs_space_free / st->f_frsize;
    st->f_bavail  = attrs->va_fs_space_avail / st->f_frsize;
    st->f_files   = attrs->va_fs_files_total;
    st->f_ffree   = attrs->va_fs_files_free;
    st->f_favail  = attrs->va_fs_files_avail;
    st->f_fsid    = attrs->va_fsid;
    st->f_flag    = 0;
    st->f_namemax = 255;
} /* chimera_attrs_to_statvfs */

static void chimera_statfs_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data);

static void
chimera_statfs_getattr_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request  *request        = private_data;
    struct chimera_client_thread   *thread         = request->thread;
    struct chimera_vfs_open_handle *handle         = request->statfs.handle;
    chimera_statfs_callback_t       callback       = request->statfs.callback;
    void                           *callback_arg   = request->statfs.private_data;
    int                             heap_allocated = request->heap_allocated;
    struct chimera_statvfs          st;

    if (error_code != CHIMERA_VFS_OK) {
        if (heap_allocated) {
            chimera_client_request_free(thread, request);
        }
        chimera_vfs_release(thread->vfs_thread, handle);
        callback(thread, error_code, NULL, callback_arg);
        return;
    }

    chimera_attrs_to_statvfs(attr, &st);

    if (heap_allocated) {
        chimera_client_request_free(thread, request);
    }

    chimera_vfs_release(thread->vfs_thread, handle);

    callback(thread, CHIMERA_VFS_OK, &st, callback_arg);

} /* chimera_statfs_getattr_complete */

static void
chimera_statfs_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_client_thread *thread       = request->thread;
        chimera_statfs_callback_t     callback     = request->statfs.callback;
        void                         *callback_arg = request->statfs.private_data;

        chimera_client_request_free(thread, request);
        callback(thread, error_code, NULL, callback_arg);
        return;
    }

    request->statfs.handle = oh;

    chimera_vfs_getattr(
        request->thread->vfs_thread,
        oh,
        CHIMERA_VFS_ATTR_MASK_STATFS,
        chimera_statfs_getattr_complete,
        request);

} /* chimera_statfs_open_complete */

static void
chimera_statfs_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_client_thread *thread       = request->thread;
        chimera_statfs_callback_t     callback     = request->statfs.callback;
        void                         *callback_arg = request->statfs.private_data;

        chimera_client_request_free(thread, request);
        callback(thread, error_code, NULL, callback_arg);
        return;
    }

    memcpy(request->fh, attr->va_fh, attr->va_fh_len);
    request->fh_len = attr->va_fh_len;

    chimera_vfs_open(
        request->thread->vfs_thread,
        request->fh,
        request->fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED,
        chimera_statfs_open_complete,
        request);

} /* chimera_statfs_lookup_complete */

static inline void
chimera_dispatch_statfs(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_lookup_path(
        thread->vfs_thread,
        thread->client->root_fh,
        thread->client->root_fh_len,
        request->statfs.path,
        request->statfs.path_len,
        CHIMERA_VFS_ATTR_FH,
        CHIMERA_VFS_LOOKUP_FOLLOW,
        chimera_statfs_lookup_complete,
        request);
} /* chimera_dispatch_statfs */
