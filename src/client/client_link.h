// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_dispatch.h"

static void chimera_link_dest_parent_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data);

static void
chimera_link_at_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_client_request  *request            = private_data;
    struct chimera_client_thread   *thread             = request->thread;
    struct chimera_vfs_open_handle *dest_parent_handle = request->link.dest_parent_handle;
    chimera_link_callback_t         callback           = request->link.callback;
    void                           *callback_arg       = request->link.private_data;
    int                             heap_allocated     = request->heap_allocated;

    if (heap_allocated) {
        chimera_client_request_free(thread, request);
    }

    chimera_vfs_release(thread->vfs_thread, dest_parent_handle);

    callback(thread, error_code, callback_arg);

} /* chimera_link_at_complete */

static void
chimera_link_dest_parent_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_client_thread *thread       = request->thread;
        chimera_link_callback_t       callback     = request->link.callback;
        void                         *callback_arg = request->link.private_data;

        chimera_client_request_free(thread, request);
        callback(thread, error_code, callback_arg);
        return;
    }

    request->link.dest_parent_handle = oh;
    memcpy(request->link.dest_fh, oh->fh, oh->fh_len);
    request->link.dest_fh_len = oh->fh_len;

    chimera_vfs_link(
        request->thread->vfs_thread,
        request->link.source_fh,
        request->link.source_fh_len,
        request->link.dest_fh,
        request->link.dest_fh_len,
        request->link.dest_path + request->link.dest_name_offset,
        request->link.dest_path_len - request->link.dest_name_offset,
        0,
        CHIMERA_VFS_ATTR_FH,
        0,
        0,
        chimera_link_at_complete,
        request);

} /* chimera_link_dest_parent_complete */

static void
chimera_link_dest_parent_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_client_thread *thread       = request->thread;
        chimera_link_callback_t       callback     = request->link.callback;
        void                         *callback_arg = request->link.private_data;

        chimera_client_request_free(thread, request);
        callback(thread, error_code, callback_arg);
        return;
    }

    chimera_vfs_open(
        request->thread->vfs_thread,
        attr->va_fh,
        attr->va_fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
        chimera_link_dest_parent_complete,
        request);

} /* chimera_link_dest_parent_lookup_complete */

static void
chimera_link_source_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_client_thread *thread       = request->thread;
        chimera_link_callback_t       callback     = request->link.callback;
        void                         *callback_arg = request->link.private_data;

        chimera_client_request_free(thread, request);
        callback(thread, error_code, callback_arg);
        return;
    }

    memcpy(request->link.source_fh, attr->va_fh, attr->va_fh_len);
    request->link.source_fh_len = attr->va_fh_len;

    chimera_vfs_lookup_path(
        request->thread->vfs_thread,
        root_fh,
        sizeof(root_fh),
        request->link.dest_path,
        request->link.dest_parent_len,
        CHIMERA_VFS_ATTR_FH,
        chimera_link_dest_parent_lookup_complete,
        request);

} /* chimera_link_source_complete */

static void
chimera_link_source_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_client_thread *thread       = request->thread;
        chimera_link_callback_t       callback     = request->link.callback;
        void                         *callback_arg = request->link.private_data;

        chimera_client_request_free(thread, request);
        callback(thread, error_code, callback_arg);
        return;
    }

    chimera_vfs_lookup_path(
        request->thread->vfs_thread,
        root_fh,
        sizeof(root_fh),
        request->link.source_path,
        request->link.source_path_len,
        CHIMERA_VFS_ATTR_FH,
        chimera_link_source_complete,
        request);

} /* chimera_link_source_lookup_complete */

static inline void
chimera_dispatch_link(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{

    if (unlikely(request->link.dest_name_offset == -1)) {
        chimera_dispatch_error_link(thread, request, CHIMERA_VFS_EINVAL);
        return;
    }

    chimera_vfs_lookup_path(
        thread->vfs_thread,
        root_fh,
        sizeof(root_fh),
        request->link.source_path,
        request->link.source_path_len,
        CHIMERA_VFS_ATTR_FH,
        chimera_link_source_lookup_complete,
        request);
} /* chimera_dispatch_link */
