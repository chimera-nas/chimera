// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

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
    st->st_rdev  = attrs->va_rdev;
    st->st_size  = attrs->va_size;
    st->st_atim  = attrs->va_atime;
    st->st_mtim  = attrs->va_mtime;
    st->st_ctim  = attrs->va_ctime;
} /* chimera_attrs_to_stat */

static void chimera_stat_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data);

static void
chimera_stat_getattr_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request  *request = private_data;
    struct chimera_client_thread   *thread  = request->thread;
    struct chimera_vfs_open_handle *handle  = request->stat.handle;
    chimera_stat_callback_t         callback     = request->stat.callback;
    void                           *callback_arg = request->stat.private_data;
    int                             heap_allocated = request->heap_allocated;
    struct chimera_stat             st;

    if (error_code != CHIMERA_VFS_OK) {
        if (heap_allocated) {
            chimera_client_request_free(thread, request);
        }
        chimera_vfs_release(thread->vfs_thread, handle);
        callback(thread, error_code, NULL, callback_arg);
        return;
    }

    chimera_attrs_to_stat(attr, &st);

    if (heap_allocated) {
        chimera_client_request_free(thread, request);
    }

    chimera_vfs_release(thread->vfs_thread, handle);

    callback(thread, CHIMERA_VFS_OK, &st, callback_arg);

} /* chimera_stat_getattr_complete */

static void
chimera_stat_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_client_thread *thread       = request->thread;
        chimera_stat_callback_t       callback     = request->stat.callback;
        void                         *callback_arg = request->stat.private_data;

        chimera_client_request_free(thread, request);
        callback(thread, error_code, NULL, callback_arg);
        return;
    }

    request->stat.handle = oh;

    chimera_vfs_getattr(
        request->thread->vfs_thread,
        oh,
        CHIMERA_VFS_ATTR_MASK_STAT,
        chimera_stat_getattr_complete,
        request);

} /* chimera_stat_open_complete */

static void
chimera_stat_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_client_thread *thread       = request->thread;
        chimera_stat_callback_t       callback     = request->stat.callback;
        void                         *callback_arg = request->stat.private_data;

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
        chimera_stat_open_complete,
        request);

} /* chimera_stat_lookup_complete */

static inline void
chimera_dispatch_stat(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_lookup_path(
        thread->vfs_thread,
        root_fh,
        sizeof(root_fh),
        request->stat.path,
        request->stat.path_len,
        CHIMERA_VFS_ATTR_FH,
        chimera_stat_lookup_complete,
        request);
} /* chimera_dispatch_stat */
