// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_release.h"
#include "common/misc.h"
#include "common/macros.h"

static void
chimera_vfs_create_path_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data);

static void
chimera_vfs_create_path_mkdir_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data);

static inline void
chimera_vfs_create_path_open_dispatch(
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
        cp_request->create_path.callback(error_code,
                                         NULL,
                                         cp_request->create_path.private_data);
        chimera_vfs_request_free(thread, cp_request);
        return;
    }

    cp_request->create_path.handle = oh;

    component = cp_request->create_path.pathc;

    while (*cp_request->create_path.pathc != '/' && *cp_request->create_path.pathc != '\0') {
        cp_request->create_path.pathc++;
    }

    componentlen = cp_request->create_path.pathc - component;

    while (*cp_request->create_path.pathc == '/' && *cp_request->create_path.pathc != '\0') {
        cp_request->create_path.pathc++;
    }

    final = (*cp_request->create_path.pathc == '\0');

    chimera_vfs_mkdir(
        thread,
        oh,
        component,
        componentlen,
        cp_request->create_path.set_attr,
        final ? cp_request->create_path.attr_mask : CHIMERA_VFS_ATTR_FH,
        0,
        0,
        chimera_vfs_create_path_mkdir_complete,
        cp_request);

} /* chimera_vfs_create_path_open_dispatch */

static void
chimera_vfs_create_path_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct chimera_vfs_request *cp_request = private_data;
    struct chimera_vfs_thread  *thread     = cp_request->thread;
    int                         final      = (*cp_request->create_path.pathc == '\0');

    chimera_vfs_release(thread, cp_request->create_path.handle);

    if (error_code != CHIMERA_VFS_OK) {
        cp_request->create_path.callback(error_code,
                                         NULL,
                                         cp_request->create_path.private_data);

        chimera_vfs_request_free(thread, cp_request);
        return;
    }

    if (final) {
        cp_request->create_path.callback(CHIMERA_VFS_OK,
                                         attr,
                                         cp_request->create_path.private_data);

        chimera_vfs_request_free(thread, cp_request);

    } else {

        memcpy(cp_request->create_path.next_fh, attr->va_fh, attr->va_fh_len);

        chimera_vfs_open(thread,
                         cp_request->create_path.next_fh,
                         attr->va_fh_len,
                         CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
                         chimera_vfs_create_path_open_dispatch,
                         cp_request);
    }
} /* chimera_vfs_create_path_complete */

static void
chimera_vfs_create_path_mkdir_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    struct chimera_vfs_request *cp_request = private_data;
    struct chimera_vfs_thread  *thread     = cp_request->thread;
    int                         final      = (*cp_request->create_path.pathc == '\0');

    chimera_vfs_release(thread, cp_request->create_path.handle);

    if (error_code == CHIMERA_VFS_EEXIST) {
        error_code = CHIMERA_VFS_OK;
    }

    if (error_code != CHIMERA_VFS_OK) {
        cp_request->create_path.callback(error_code,
                                         NULL,
                                         cp_request->create_path.private_data);

        chimera_vfs_request_free(thread, cp_request);
        return;
    }

    if (final) {
        cp_request->create_path.callback(CHIMERA_VFS_OK,
                                         attr,
                                         cp_request->create_path.private_data);

        chimera_vfs_request_free(thread, cp_request);

    } else {

        memcpy(cp_request->create_path.next_fh, attr->va_fh, attr->va_fh_len);

        chimera_vfs_open(thread,
                         cp_request->create_path.next_fh,
                         attr->va_fh_len,
                         CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
                         chimera_vfs_create_path_open_dispatch,
                         cp_request);
    }
} /* chimera_vfs_create_path_complete */

SYMBOL_EXPORT void
chimera_vfs_create_path(
    struct chimera_vfs_thread         *thread,
    const void                        *fh,
    int                                fhlen,
    const char                        *path,
    int                                pathlen,
    struct chimera_vfs_attrs          *set_attr,
    uint64_t                           attr_mask,
    chimera_vfs_create_path_callback_t callback,
    void                              *private_data)
{
    struct chimera_vfs_request *cp_request;

    while (*path == '/') {
        path++;
        pathlen--;
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

    cp_request = chimera_vfs_request_alloc(thread, fh, fhlen);

    cp_request->create_path.path         = cp_request->plugin_data;
    cp_request->create_path.pathlen      = pathlen;
    cp_request->create_path.pathc        = cp_request->create_path.path;
    cp_request->create_path.handle       = NULL;
    cp_request->create_path.set_attr     = set_attr;
    cp_request->create_path.attr_mask    = attr_mask;
    cp_request->create_path.private_data = private_data;
    cp_request->create_path.callback     = callback;

    memcpy(cp_request->create_path.path, path, pathlen);

    cp_request->create_path.path[pathlen] = '\0';

    chimera_vfs_open(thread,
                     fh,
                     fhlen,
                     CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
                     chimera_vfs_create_path_open_dispatch,
                     cp_request);

} /* chimera_vfs_create_path */
