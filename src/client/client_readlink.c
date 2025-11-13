// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_internal.h"

static void
chimera_readlink_complete(
    enum chimera_vfs_error error_code,
    int                    targetlen,
    void                  *private_data)
{
    struct chimera_client_request *request = private_data;

    request->readlink.callback(request->thread,
                               error_code,
                               request->readlink.target,
                               targetlen,
                               request->readlink.private_data);

    chimera_vfs_release(request->thread->vfs_thread, request->readlink.handle);

    chimera_client_request_free(request->thread, request);

} /* chimera_readlink_complete */

static void
chimera_readlink_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        request->readlink.callback(request->thread,
                                   error_code,
                                   NULL,
                                   0,
                                   request->readlink.private_data);
        chimera_client_request_free(request->thread, request);
        return;
    }

    request->readlink.handle = oh;

    chimera_vfs_readlink(
        request->thread->vfs_thread,
        oh,
        request->readlink.target,
        request->readlink.target_maxlength,
        chimera_readlink_complete,
        request);

} /* chimera_readlink_open_complete */

static void
chimera_readlink_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        request->readlink.callback(request->thread,
                                   error_code,
                                   NULL,
                                   0,
                                   request->readlink.private_data);
        chimera_client_request_free(request->thread, request);
        return;
    }

    memcpy(request->fh, attr->va_fh, attr->va_fh_len);
    request->fh_len = attr->va_fh_len;

    chimera_vfs_open(
        request->thread->vfs_thread,
        request->fh,
        request->fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED,
        chimera_readlink_open_complete,
        request);

} /* chimera_readlink_lookup_complete */

SYMBOL_EXPORT void
chimera_readlink(
    struct chimera_client_thread *thread,
    const char                   *path,
    int                           path_len,
    char                         *target,
    uint32_t                      target_maxlength,
    chimera_readlink_callback_t   callback,
    void                         *private_data)
{
    struct chimera_client_request *request;

    if (unlikely(target_maxlength > CHIMERA_VFS_PATH_MAX)) {
        callback(thread, CHIMERA_VFS_EINVAL, NULL, 0, private_data);
        return;
    }

    request = chimera_client_request_alloc(thread);

    request->opcode                    = CHIMERA_CLIENT_OP_READLINK;
    request->readlink.callback         = callback;
    request->readlink.private_data     = private_data;
    request->readlink.target_maxlength = target_maxlength;
    request->readlink.target           = target;

    chimera_vfs_lookup_path(
        thread->vfs_thread,
        root_fh,
        sizeof(root_fh),
        path,
        path_len,
        CHIMERA_VFS_ATTR_FH,
        chimera_readlink_lookup_complete,
        request);
} /* chimera_readlink */

