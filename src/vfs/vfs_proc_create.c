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
chimera_vfs_create_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data);

static void
chimera_vfs_create_mkdir_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data);

static inline void
chimera_vfs_create_open_dispatch(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_vfs_request *cp_request = private_data;
    struct chimera_vfs_thread  *thread     = cp_request->thread;
    const char                 *component;
    int                         componentlen;
    int                         final;

    if (error_code != CHIMERA_VFS_OK) {
        cp_request->create.callback(error_code,
                                    NULL,
                                    cp_request->create.private_data);
        chimera_vfs_request_free(thread, cp_request);
        return;
    }

    cp_request->create.handle = oh;

    component = cp_request->create.pathc;

    while (*cp_request->create.pathc != '/' && *cp_request->create.pathc != '\0') {
        cp_request->create.pathc++;
    }

    componentlen = cp_request->create.pathc - component;

    while (*cp_request->create.pathc == '/' && *cp_request->create.pathc != '\0') {
        cp_request->create.pathc++;
    }

    final = (*cp_request->create.pathc == '\0');

    chimera_vfs_mkdir_at(
        thread,
        cp_request->cred,
        oh,
        component,
        componentlen,
        cp_request->create.set_attr,
        final ? cp_request->create.attr_mask : CHIMERA_VFS_ATTR_FH,
        0,
        0,
        chimera_vfs_create_mkdir_complete,
        cp_request);

} /* chimera_vfs_create_open_dispatch */

static void
chimera_vfs_create_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct chimera_vfs_request *cp_request = private_data;
    struct chimera_vfs_thread  *thread     = cp_request->thread;
    int                         final      = (*cp_request->create.pathc == '\0');

    chimera_vfs_release(thread, cp_request->create.handle);

    if (error_code != CHIMERA_VFS_OK) {
        cp_request->create.callback(error_code,
                                    NULL,
                                    cp_request->create.private_data);

        chimera_vfs_request_free(thread, cp_request);
        return;
    }

    if (final) {
        cp_request->create.callback(CHIMERA_VFS_OK,
                                    attr,
                                    cp_request->create.private_data);

        chimera_vfs_request_free(thread, cp_request);

    } else {

        memcpy(cp_request->create.next_fh, attr->va_fh, attr->va_fh_len);

        chimera_vfs_open_fh(thread,
                            cp_request->cred,
                            cp_request->create.next_fh,
                            attr->va_fh_len,
                            CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
                            chimera_vfs_create_open_dispatch,
                            cp_request);
    }
} /* chimera_vfs_create_complete */

static void
chimera_vfs_create_mkdir_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    struct chimera_vfs_request *cp_request = private_data;
    struct chimera_vfs_thread  *thread     = cp_request->thread;
    int                         final      = (*cp_request->create.pathc == '\0');

    chimera_vfs_release(thread, cp_request->create.handle);

    if (error_code == CHIMERA_VFS_EEXIST) {
        error_code = CHIMERA_VFS_OK;
    }

    if (error_code != CHIMERA_VFS_OK) {
        cp_request->create.callback(error_code,
                                    NULL,
                                    cp_request->create.private_data);

        chimera_vfs_request_free(thread, cp_request);
        return;
    }

    if (final) {
        cp_request->create.callback(CHIMERA_VFS_OK,
                                    attr,
                                    cp_request->create.private_data);

        chimera_vfs_request_free(thread, cp_request);

    } else {

        memcpy(cp_request->create.next_fh, attr->va_fh, attr->va_fh_len);

        chimera_vfs_open_fh(thread,
                            cp_request->cred,
                            cp_request->create.next_fh,
                            attr->va_fh_len,
                            CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
                            chimera_vfs_create_open_dispatch,
                            cp_request);
    }
} /* chimera_vfs_create_mkdir_complete */

SYMBOL_EXPORT void
chimera_vfs_create(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const char                    *path,
    int                            pathlen,
    struct chimera_vfs_attrs      *set_attr,
    uint64_t                       attr_mask,
    chimera_vfs_create_callback_t  callback,
    void                          *private_data)
{
    struct chimera_vfs_request *cp_request;

    while (pathlen > 0 && *path == '/') {
        path++;
        pathlen--;
    }

    if (pathlen > CHIMERA_VFS_PATH_MAX) {
        callback(CHIMERA_VFS_ENAMETOOLONG, NULL, private_data);
        return;
    }

    if (pathlen == 0) {
        struct chimera_vfs_attrs attr;

        attr.va_req_mask = attr_mask;
        attr.va_set_mask = CHIMERA_VFS_ATTR_FH;
        memcpy(attr.va_fh, fh, fhlen);
        attr.va_fh_len = fhlen;
        callback(CHIMERA_VFS_OK,
                 &attr,
                 private_data);
        return;
    }

    cp_request = chimera_vfs_request_alloc(thread, cred, fh, fhlen);

    if (CHIMERA_VFS_IS_ERR(cp_request)) {
        callback(CHIMERA_VFS_PTR_ERR(cp_request), NULL, private_data);
        return;
    }

    cp_request->create.path         = cp_request->plugin_data;
    cp_request->create.pathlen      = pathlen;
    cp_request->create.pathc        = cp_request->create.path;
    cp_request->create.handle       = NULL;
    cp_request->create.set_attr     = set_attr;
    cp_request->create.attr_mask    = attr_mask;
    cp_request->create.private_data = private_data;
    cp_request->create.callback     = callback;

    memcpy(cp_request->create.path, path, pathlen);

    cp_request->create.path[pathlen] = '\0';

    chimera_vfs_open_fh(thread,
                        cred,
                        fh,
                        fhlen,
                        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_vfs_create_open_dispatch,
                        cp_request);

} /* chimera_vfs_create */
