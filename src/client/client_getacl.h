// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <string.h>

#include "client_internal.h"
#include "vfs/vfs_acl.h"

/*
 * GETACL dispatch: resolve a path, open an O_PATH handle, getattr the canonical
 * NFSv4/Windows ACL (CHIMERA_VFS_ATTR_ACL), and copy the returned ACL into the
 * caller's buffer.  The returned va_acl is valid only for the duration of the
 * getattr completion (same contract as va_fh), so it is copied out here before
 * the request is released.
 */

static void chimera_getacl_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data);

static void
chimera_getacl_getattr_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request  *request        = private_data;
    struct chimera_client_thread   *thread         = request->thread;
    struct chimera_vfs_open_handle *handle         = request->getacl.handle;
    chimera_setattr_callback_t      callback       = request->getacl.callback;
    void                           *callback_arg   = request->getacl.private_data;
    int                             heap_allocated = request->heap_allocated;
    enum chimera_vfs_error          status         = error_code;

    if (error_code == CHIMERA_VFS_OK) {
        if (!(attr->va_set_mask & CHIMERA_VFS_ATTR_ACL) || !attr->va_acl) {
            /* Backend returned no ACL (mode-only object / backend without ACL
             * storage): report an empty ACL rather than failing. */
            request->getacl.r_acl_aces = 0;
            if (request->getacl.acl_bufsize >= chimera_acl_size(0)) {
                request->getacl.acl_buf->num_aces   = 0;
                request->getacl.acl_buf->ctrl_flags = 0;
            } else {
                status = CHIMERA_VFS_ERANGE;
            }
        } else {
            const struct chimera_acl *acl  = attr->va_acl;
            size_t                    need = chimera_acl_size(acl->num_aces);

            request->getacl.r_acl_aces = acl->num_aces;

            if (request->getacl.acl_bufsize >= need) {
                memcpy(request->getacl.acl_buf, acl, need);
            } else {
                status = CHIMERA_VFS_ERANGE;
            }
        }
    }

    if (heap_allocated) {
        chimera_client_request_free(thread, request);
    }

    chimera_vfs_release(thread->vfs_thread, handle);

    callback(thread, status, callback_arg);
} /* chimera_getacl_getattr_complete */

static void
chimera_getacl_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_client_thread *thread       = request->thread;
        chimera_setattr_callback_t    callback     = request->getacl.callback;
        void                         *callback_arg = request->getacl.private_data;

        chimera_client_request_free(thread, request);
        callback(thread, error_code, callback_arg);
        return;
    }

    request->getacl.handle = oh;

    chimera_vfs_getattr(
        request->thread->vfs_thread,
        chimera_client_req_cred(request), NULL,
        oh,
        CHIMERA_VFS_ATTR_ACL,
        chimera_getacl_getattr_complete,
        request);
} /* chimera_getacl_open_complete */

static void
chimera_getacl_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_client_thread *thread       = request->thread;
        chimera_setattr_callback_t    callback     = request->getacl.callback;
        void                         *callback_arg = request->getacl.private_data;

        chimera_client_request_free(thread, request);
        callback(thread, error_code, callback_arg);
        return;
    }

    memcpy(request->fh, attr->va_fh, attr->va_fh_len);
    request->fh_len = attr->va_fh_len;

    chimera_vfs_open_fh(
        request->thread->vfs_thread,
        chimera_client_req_cred(request), NULL,
        request->fh,
        request->fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED,
        chimera_getacl_open_complete,
        request);
} /* chimera_getacl_lookup_complete */

static inline void
chimera_dispatch_getacl(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_lookup(
        thread->vfs_thread,
        chimera_client_req_cred(request), NULL,
        thread->client->root_fh,
        thread->client->root_fh_len,
        request->getacl.path,
        request->getacl.path_len,
        CHIMERA_VFS_ATTR_FH,
        CHIMERA_VFS_LOOKUP_FOLLOW,
        chimera_getacl_lookup_complete,
        request);
} /* chimera_dispatch_getacl */
