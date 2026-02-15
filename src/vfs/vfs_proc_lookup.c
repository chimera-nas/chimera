// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <sys/stat.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_release.h"
#include "common/misc.h"
#include "common/macros.h"

static void
chimera_vfs_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data);

static void
chimera_vfs_lookup_readlink_complete(
    enum chimera_vfs_error    error_code,
    int                       target_length,
    struct chimera_vfs_attrs *attr,
    void                     *private_data);

static inline void
chimera_vfs_lookup_open_dispatch(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_vfs_request *lp_request = private_data;
    struct chimera_vfs_thread  *thread     = lp_request->thread;
    const char                 *component;
    int                         componentlen;
    int                         final;

    if (error_code != CHIMERA_VFS_OK) {
        lp_request->lookup.callback(error_code,
                                    NULL,
                                    lp_request->lookup.private_data);
        chimera_vfs_request_free(thread, lp_request);
        return;
    }

    lp_request->lookup.handle = oh;

    /* Save parent fh for potential relative symlink resolution */
    memcpy(lp_request->lookup.parent_fh, oh->fh, oh->fh_len);
    lp_request->lookup.parent_fh_len = oh->fh_len;

    component = lp_request->lookup.pathc;

    while (*lp_request->lookup.pathc != '/' && *lp_request->lookup.pathc != '\0') {
        lp_request->lookup.pathc++;
    }

    componentlen = lp_request->lookup.pathc - component;

    while (*lp_request->lookup.pathc == '/' && *lp_request->lookup.pathc != '\0') {
        lp_request->lookup.pathc++;
    }

    final = (*lp_request->lookup.pathc == '\0');

    /* Always request mode so we can detect symlinks */
    chimera_vfs_lookup_at(
        thread,
        lp_request->cred,
        oh,
        component,
        componentlen,
        (final ? lp_request->lookup.attr_mask : CHIMERA_VFS_ATTR_FH) | CHIMERA_VFS_ATTR_MODE,
        0,
        chimera_vfs_lookup_complete,
        lp_request);

} /* chimera_vfs_lookup_open_dispatch */

static void
chimera_vfs_lookup_symlink_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_vfs_request *lp_request = private_data;
    struct chimera_vfs_thread  *thread     = lp_request->thread;
    char                       *target;

    if (error_code != CHIMERA_VFS_OK) {
        lp_request->lookup.callback(error_code,
                                    NULL,
                                    lp_request->lookup.private_data);
        chimera_vfs_request_free(thread, lp_request);
        return;
    }

    lp_request->lookup.handle = oh;

    /* Use the path buffer after the current path for symlink target storage */
    target = lp_request->lookup.path + strlen(lp_request->lookup.path) + 1;

    chimera_vfs_readlink(
        thread,
        lp_request->cred,
        oh,
        target,
        CHIMERA_VFS_PATH_MAX,
        0,
        chimera_vfs_lookup_readlink_complete,
        lp_request);
} /* chimera_vfs_lookup_symlink_open_complete */

static void
chimera_vfs_lookup_readlink_complete(
    enum chimera_vfs_error    error_code,
    int                       target_length,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_request *lp_request = private_data;
    struct chimera_vfs_thread  *thread     = lp_request->thread;
    char                       *target;
    char                       *new_path;
    int                         remaining_len;
    int                         new_pathlen;
    const uint8_t              *start_fh;
    int                         start_fh_len;

    chimera_vfs_release(thread, lp_request->lookup.handle);
    lp_request->lookup.handle = NULL;

    if (error_code != CHIMERA_VFS_OK) {
        lp_request->lookup.callback(error_code,
                                    NULL,
                                    lp_request->lookup.private_data);
        chimera_vfs_request_free(thread, lp_request);
        return;
    }

    /* Target is stored after the original path */
    target                = lp_request->lookup.path + strlen(lp_request->lookup.path) + 1;
    target[target_length] = '\0';

    /* Calculate remaining path after symlink */
    remaining_len = strlen(lp_request->lookup.pathc);

    /* Build new path: symlink target + remaining path */
    if (target[0] == '/') {
        /* Absolute symlink - restart from root */
        start_fh     = lp_request->fh;
        start_fh_len = lp_request->fh_len;

        /* Skip leading slashes in target */
        while (*target == '/') {
            target++;
            target_length--;
        }
    } else {
        /* Relative symlink - continue from parent directory */
        start_fh     = lp_request->lookup.parent_fh;
        start_fh_len = lp_request->lookup.parent_fh_len;
    }

    /* Construct new path in plugin_data buffer */
    new_path = lp_request->plugin_data;

    if (remaining_len > 0) {
        /* Target + "/" + remaining */
        new_pathlen = target_length + 1 + remaining_len;
        if (new_pathlen >= CHIMERA_VFS_PATH_MAX) {
            lp_request->lookup.callback(CHIMERA_VFS_ENAMETOOLONG,
                                        NULL,
                                        lp_request->lookup.private_data);
            chimera_vfs_request_free(thread, lp_request);
            return;
        }
        memcpy(new_path, target, target_length);
        new_path[target_length] = '/';
        memcpy(new_path + target_length + 1, lp_request->lookup.pathc, remaining_len);
        new_path[new_pathlen] = '\0';
    } else {
        /* Just the target */
        new_pathlen = target_length;
        memcpy(new_path, target, target_length);
        new_path[new_pathlen] = '\0';
    }

    /* Reset path pointer */
    lp_request->lookup.path    = new_path;
    lp_request->lookup.pathc   = new_path;
    lp_request->lookup.pathlen = new_pathlen;

    /* Continue walking from start point */
    chimera_vfs_open_fh(thread,
                        lp_request->cred,
                        start_fh,
                        start_fh_len,
                        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_vfs_lookup_open_dispatch,
                        lp_request);
} /* chimera_vfs_lookup_readlink_complete */

static void
chimera_vfs_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct chimera_vfs_request *lp_request = private_data;
    struct chimera_vfs_thread  *thread     = lp_request->thread;
    int                         final      = (*lp_request->lookup.pathc == '\0');
    unsigned int                open_flags;
    int                         follow_symlink;

    chimera_vfs_release(thread, lp_request->lookup.handle);
    lp_request->lookup.handle = NULL;

    if (error_code != CHIMERA_VFS_OK) {
        lp_request->lookup.callback(error_code,
                                    NULL,
                                    lp_request->lookup.private_data);

        chimera_vfs_request_free(thread, lp_request);
        return;
    }

    /* Check if this is a symlink that needs to be followed */
    follow_symlink = 0;
    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) && S_ISLNK(attr->va_mode)) {
        if (!final) {
            /* Always follow symlinks in path components */
            follow_symlink = 1;
        } else if (lp_request->lookup.flags & CHIMERA_VFS_LOOKUP_FOLLOW) {
            /* Follow final symlink if requested */
            follow_symlink = 1;
        }
    }

    if (follow_symlink) {
        /* Check for symlink loop */
        lp_request->lookup.symlink_count++;
        if (lp_request->lookup.symlink_count > CHIMERA_VFS_SYMLOOP_MAX) {
            lp_request->lookup.callback(CHIMERA_VFS_ELOOP,
                                        NULL,
                                        lp_request->lookup.private_data);
            chimera_vfs_request_free(thread, lp_request);
            return;
        }

        /* Open the symlink to read its target */
        memcpy(lp_request->lookup.next_fh, attr->va_fh, attr->va_fh_len);
        chimera_vfs_open_fh(thread,
                            lp_request->cred,
                            lp_request->lookup.next_fh,
                            attr->va_fh_len,
                            CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED,
                            chimera_vfs_lookup_symlink_open_complete,
                            lp_request);
        return;
    }

    if (final) {
        lp_request->lookup.callback(CHIMERA_VFS_OK,
                                    attr,
                                    lp_request->lookup.private_data);

        chimera_vfs_request_free(thread, lp_request);

    } else {

        open_flags = CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED;

        if (!final) {
            open_flags |= CHIMERA_VFS_OPEN_DIRECTORY;
        }

        memcpy(lp_request->lookup.next_fh, attr->va_fh, attr->va_fh_len);
        chimera_vfs_open_fh(thread,
                            lp_request->cred,
                            lp_request->lookup.next_fh,
                            attr->va_fh_len,
                            open_flags,
                            chimera_vfs_lookup_open_dispatch,
                            lp_request);
    }
} /* chimera_vfs_lookup_complete */

SYMBOL_EXPORT void
chimera_vfs_lookup(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const char                    *path,
    int                            pathlen,
    uint64_t                       attr_mask,
    uint32_t                       flags,
    chimera_vfs_lookup_callback_t  callback,
    void                          *private_data)
{
    struct chimera_vfs_request *lp_request;

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

    lp_request = chimera_vfs_request_alloc(thread, cred, fh, fhlen);

    if (CHIMERA_VFS_IS_ERR(lp_request)) {
        callback(CHIMERA_VFS_PTR_ERR(lp_request), NULL, private_data);
        return;
    }

    lp_request->lookup.path          = lp_request->plugin_data;
    lp_request->lookup.pathlen       = pathlen;
    lp_request->lookup.pathc         = lp_request->lookup.path;
    lp_request->lookup.handle        = NULL;
    lp_request->lookup.attr_mask     = attr_mask;
    lp_request->lookup.flags         = flags;
    lp_request->lookup.symlink_count = 0;
    lp_request->lookup.private_data  = private_data;
    lp_request->lookup.callback      = callback;
    lp_request->lookup.parent_fh_len = 0;

    memcpy(lp_request->lookup.path, path, pathlen);

    lp_request->lookup.path[pathlen] = '\0';

    chimera_vfs_open_fh(thread,
                        cred,
                        fh,
                        fhlen,
                        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_vfs_lookup_open_dispatch,
                        lp_request);

} /* chimera_vfs_lookup */
