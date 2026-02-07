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
chimera_vfs_open_root_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_vfs_request *request  = private_data;
    struct chimera_vfs_thread  *thread   = request->thread;
    chimera_vfs_open_callback_t callback = request->open.callback;
    void                       *priv     = request->open.private_data;

    chimera_vfs_request_free(thread, request);

    callback(error_code, oh, NULL, priv);
} /* chimera_vfs_open_root_complete */

static void
chimera_vfs_open_op_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data)
{
    struct chimera_vfs_request *request  = private_data;
    struct chimera_vfs_thread  *thread   = request->thread;
    chimera_vfs_open_callback_t callback = request->open.callback;
    void                       *priv     = request->open.private_data;

    chimera_vfs_release(thread, request->open.parent_handle);
    chimera_vfs_request_free(thread, request);

    callback(error_code, oh, attr, priv);
} /* chimera_vfs_open_op_complete */

static void
chimera_vfs_open_parent_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct chimera_vfs_thread  *thread  = request->thread;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_open_callback_t callback = request->open.callback;
        void                       *priv     = request->open.private_data;

        chimera_vfs_request_free(thread, request);
        callback(error_code, NULL, NULL, priv);
        return;
    }

    request->open.parent_handle = oh;

    chimera_vfs_open_at(
        thread,
        request->cred,
        oh,
        request->open.path + request->open.name_offset,
        request->open.pathlen - request->open.name_offset,
        request->open.flags,
        request->open.set_attr,
        request->open.attr_mask,
        0,
        0,
        chimera_vfs_open_op_complete,
        request);
} /* chimera_vfs_open_parent_open_complete */

static void
chimera_vfs_open_parent_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct chimera_vfs_thread  *thread  = request->thread;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_open_callback_t callback = request->open.callback;
        void                       *priv     = request->open.private_data;

        chimera_vfs_request_free(thread, request);
        callback(error_code, NULL, NULL, priv);
        return;
    }

    memcpy(request->open.parent_fh, attr->va_fh, attr->va_fh_len);
    request->open.parent_fh_len = attr->va_fh_len;

    chimera_vfs_open_fh(
        thread,
        request->cred,
        request->open.parent_fh,
        request->open.parent_fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
        chimera_vfs_open_parent_open_complete,
        request);
} /* chimera_vfs_open_parent_lookup_complete */

static void
chimera_vfs_open_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct chimera_vfs_thread  *thread  = request->thread;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_open_callback_t callback = request->open.callback;
        void                       *priv     = request->open.private_data;

        chimera_vfs_request_free(thread, request);
        callback(error_code, NULL, NULL, priv);
        return;
    }

    memcpy(request->open.parent_fh, attr->va_fh, attr->va_fh_len);
    request->open.parent_fh_len = attr->va_fh_len;

    chimera_vfs_open_fh(
        thread,
        request->cred,
        request->open.parent_fh,
        request->open.parent_fh_len,
        request->open.flags,
        chimera_vfs_open_root_complete,
        request);
} /* chimera_vfs_open_lookup_complete */

SYMBOL_EXPORT void
chimera_vfs_open(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const char                    *path,
    int                            pathlen,
    unsigned int                   flags,
    struct chimera_vfs_attrs      *set_attr,
    uint64_t                       attr_mask,
    chimera_vfs_open_callback_t    callback,
    void                          *private_data)
{
    struct chimera_vfs_request *request;
    const char                 *slash;

    while (pathlen > 0 && *path == '/') {
        path++;
        pathlen--;
    }

    if (pathlen == 0) {
        /* Path is root "/" - open the provided FH directly */
        request = chimera_vfs_request_alloc(thread, cred, fh, fhlen);

        if (CHIMERA_VFS_IS_ERR(request)) {
            callback(CHIMERA_VFS_PTR_ERR(request), NULL, NULL, private_data);
            return;
        }

        request->open.callback     = callback;
        request->open.private_data = private_data;

        chimera_vfs_open_fh(
            thread,
            cred,
            fh,
            fhlen,
            flags,
            chimera_vfs_open_root_complete,
            request);
        return;
    }

    while (pathlen > 0 && path[pathlen - 1] == '/') {
        pathlen--;
    }

    request = chimera_vfs_request_alloc(thread, cred, fh, fhlen);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), NULL, NULL, private_data);
        return;
    }

    memcpy(request->plugin_data, path, pathlen);
    ((char *) request->plugin_data)[pathlen] = '\0';

    request->open.path         = request->plugin_data;
    request->open.pathlen      = pathlen;
    request->open.flags        = flags;
    request->open.set_attr     = set_attr;
    request->open.attr_mask    = attr_mask;
    request->open.callback     = callback;
    request->open.private_data = private_data;

    if (request->module->capabilities & CHIMERA_VFS_CAP_FS_PATH_OP) {
        request->open.name_offset = 0;

        memcpy(request->open.parent_fh, fh, fhlen);
        request->open.parent_fh_len = fhlen;

        chimera_vfs_open_fh(
            thread,
            cred,
            request->open.parent_fh,
            request->open.parent_fh_len,
            CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
            chimera_vfs_open_parent_open_complete,
            request);
    } else if (flags & CHIMERA_VFS_OPEN_CREATE) {
        /* Create needs parent handle + name for open_at */
        slash = strrchr(request->open.path, '/');

        if (slash) {
            request->open.parent_len  = slash - request->open.path;
            request->open.name_offset = (slash + 1) - request->open.path;
        } else {
            request->open.parent_len  = 0;
            request->open.name_offset = 0;
        }

        chimera_vfs_lookup(
            thread,
            cred,
            fh,
            fhlen,
            request->open.path,
            request->open.parent_len,
            CHIMERA_VFS_ATTR_FH,
            CHIMERA_VFS_LOOKUP_FOLLOW,
            chimera_vfs_open_parent_lookup_complete,
            request);
    } else {
        /* Non-create: resolve full path via lookup, then open the result */
        chimera_vfs_lookup(
            thread,
            cred,
            fh,
            fhlen,
            request->open.path,
            request->open.pathlen,
            CHIMERA_VFS_ATTR_FH,
            CHIMERA_VFS_LOOKUP_FOLLOW,
            chimera_vfs_open_lookup_complete,
            request);
    }
} /* chimera_vfs_open */
