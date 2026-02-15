// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_release.h"
#include "common/misc.h"
#include "common/macros.h"

static void
chimera_vfs_mknod_op_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    struct chimera_vfs_request  *request  = private_data;
    struct chimera_vfs_thread   *thread   = request->thread;
    chimera_vfs_mknod_callback_t callback = request->mknod.callback;
    void                        *priv     = request->mknod.private_data;

    chimera_vfs_release(thread, request->mknod.parent_handle);
    chimera_vfs_request_free(thread, request);

    callback(error_code, attr, priv);
} /* chimera_vfs_mknod_op_complete */

static void
chimera_vfs_mknod_parent_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct chimera_vfs_thread  *thread  = request->thread;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_mknod_callback_t callback = request->mknod.callback;
        void                        *priv     = request->mknod.private_data;

        chimera_vfs_request_free(thread, request);
        callback(error_code, NULL, priv);
        return;
    }

    request->mknod.parent_handle = oh;

    chimera_vfs_mknod_at(
        thread,
        request->cred,
        oh,
        request->mknod.path + request->mknod.name_offset,
        request->mknod.pathlen - request->mknod.name_offset,
        request->mknod.set_attr,
        request->mknod.attr_mask,
        0,
        0,
        chimera_vfs_mknod_op_complete,
        request);
} /* chimera_vfs_mknod_parent_open_complete */

static void
chimera_vfs_mknod_parent_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct chimera_vfs_thread  *thread  = request->thread;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_mknod_callback_t callback = request->mknod.callback;
        void                        *priv     = request->mknod.private_data;

        chimera_vfs_request_free(thread, request);
        callback(error_code, NULL, priv);
        return;
    }

    memcpy(request->mknod.parent_fh, attr->va_fh, attr->va_fh_len);
    request->mknod.parent_fh_len = attr->va_fh_len;

    chimera_vfs_open_fh(
        thread,
        request->cred,
        request->mknod.parent_fh,
        request->mknod.parent_fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
        chimera_vfs_mknod_parent_open_complete,
        request);
} /* chimera_vfs_mknod_parent_lookup_complete */

SYMBOL_EXPORT void
chimera_vfs_mknod(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const char                    *path,
    int                            pathlen,
    struct chimera_vfs_attrs      *set_attr,
    uint64_t                       attr_mask,
    chimera_vfs_mknod_callback_t   callback,
    void                          *private_data)
{
    struct chimera_vfs_request *request;
    const char                 *slash;

    while (pathlen > 0 && *path == '/') {
        path++;
        pathlen--;
    }

    while (pathlen > 0 && path[pathlen - 1] == '/') {
        pathlen--;
    }

    if (pathlen > CHIMERA_VFS_PATH_MAX) {
        callback(CHIMERA_VFS_ENAMETOOLONG, NULL, private_data);
        return;
    }

    if (pathlen == 0) {
        callback(CHIMERA_VFS_EINVAL, NULL, private_data);
        return;
    }

    request = chimera_vfs_request_alloc(thread, cred, fh, fhlen);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), NULL, private_data);
        return;
    }

    memcpy(request->plugin_data, path, pathlen);
    ((char *) request->plugin_data)[pathlen] = '\0';

    request->mknod.path         = request->plugin_data;
    request->mknod.pathlen      = pathlen;
    request->mknod.set_attr     = set_attr;
    request->mknod.attr_mask    = attr_mask;
    request->mknod.callback     = callback;
    request->mknod.private_data = private_data;

    if (request->module->capabilities & CHIMERA_VFS_CAP_FS_PATH_OP) {
        request->mknod.name_offset = 0;

        memcpy(request->mknod.parent_fh, fh, fhlen);
        request->mknod.parent_fh_len = fhlen;

        chimera_vfs_open_fh(
            thread,
            cred,
            request->mknod.parent_fh,
            request->mknod.parent_fh_len,
            CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
            chimera_vfs_mknod_parent_open_complete,
            request);
    } else {
        slash = strrchr(request->mknod.path, '/');

        if (slash) {
            request->mknod.parent_len  = slash - request->mknod.path;
            request->mknod.name_offset = (slash + 1) - request->mknod.path;
        } else {
            request->mknod.parent_len  = 0;
            request->mknod.name_offset = 0;
        }

        chimera_vfs_lookup(
            thread,
            cred,
            fh,
            fhlen,
            request->mknod.path,
            request->mknod.parent_len,
            CHIMERA_VFS_ATTR_FH,
            CHIMERA_VFS_LOOKUP_FOLLOW,
            chimera_vfs_mknod_parent_lookup_complete,
            request);
    }
} /* chimera_vfs_mknod */
