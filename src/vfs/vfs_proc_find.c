// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_release.h"
#include "common/misc.h"
#include "common/macros.h"

static void
chimera_vfs_find_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data);

static inline void
chimera_vfs_find_drain(
    struct chimera_vfs_thread  *thread,
    struct chimera_vfs_request *root)
{
    struct chimera_vfs_request     *cur = root;
    struct chimera_vfs_find_result *result;

    while (cur->find.results) {
        result = cur->find.results;

        if (!result->emitted) {
            cur->find.callback(result->path,
                               result->path_len,
                               &result->attrs,
                               cur->find.private_data);
            result->emitted = 1;
        }

        if (result->child_request && !result->child_request->find.is_complete) {
            cur = result->child_request;
            continue;
        }

        DL_DELETE(cur->find.results, result);

        if (result->child_request) {
            DL_CONCAT(result->child_request->find.results, cur->find.results);
            cur->find.results = result->child_request->find.results;
            chimera_vfs_request_free(thread, result->child_request);
        }

        chimera_vfs_find_result_free(thread, result);
    }

    if (!root->find.results &&
        root->find.is_complete &&
        !root->find.complete_called) {
        root->find.complete(CHIMERA_VFS_OK, root->find.private_data);
        root->find.complete_called = 1;
        chimera_vfs_request_free(thread, root);
    }

} /* chimera_vfs_find_drain */

static inline void
chimera_vfs_find_dispatch(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    const void                     *fh,
    int                             fhlen,
    const char                     *path_prefix,
    int                             path_prefix_len,
    uint64_t                        attr_mask,
    struct chimera_vfs_request     *root,
    struct chimera_vfs_find_result *parent,
    chimera_vfs_filter_callback_t   filter,
    chimera_vfs_find_callback_t     callback,
    chimera_vfs_find_complete_t     complete,
    void                           *private_data)
{
    struct chimera_vfs_request *find_request;

    find_request = chimera_vfs_request_alloc(thread, cred, fh, fhlen);

    find_request->find.path            = find_request->plugin_data;
    find_request->find.attr_mask       = attr_mask;
    find_request->find.private_data    = private_data;
    find_request->find.root            = root ? root : find_request;
    find_request->find.parent          = parent;
    find_request->find.is_complete     = 0;
    find_request->find.complete_called = 0;
    find_request->find.results         = NULL;
    find_request->find.filter          = filter;
    find_request->find.callback        = callback;
    find_request->find.complete        = complete;

    memcpy(find_request->find.path, path_prefix, path_prefix_len);
    find_request->find.path_len = path_prefix_len;

    if (parent) {
        parent->child_request = find_request;
    }

    chimera_vfs_open(
        thread,
        cred,
        find_request->fh,
        find_request->fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
        chimera_vfs_find_open_callback,
        find_request);

} /* chimera_vfs_find_dispatch */

static int
chimera_vfs_find_readdir_callback(
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg)
{
    struct chimera_vfs_request     *find_request = arg;
    struct chimera_vfs_thread      *thread       = find_request->thread;
    struct chimera_vfs_find_result *result;
    int                             filter_result;

    if ((namelen == 1 && name[0] == '.') ||
        (namelen == 2 && name[0] == '.' && name[1] == '.')) {
        return 0;
    }

    result = chimera_vfs_find_result_alloc(thread);

    result->attrs         = *attrs;
    result->emitted       = 0;
    result->child_request = NULL;

    result->path_len = snprintf(result->path,
                                CHIMERA_VFS_PATH_MAX,
                                "%.*s/%.*s",
                                find_request->find.path_len,
                                find_request->find.path,
                                namelen,
                                name);

    DL_APPEND(find_request->find.results, result);

    if ((attrs->va_mode & S_IFMT) == S_IFDIR) {

        filter_result = find_request->find.filter(result->path,
                                                  result->path_len,
                                                  &result->attrs,
                                                  find_request->find.private_data);

        if (filter_result == 0) {
            chimera_vfs_find_dispatch(thread,
                                      find_request->cred,
                                      attrs->va_fh,
                                      attrs->va_fh_len,
                                      result->path,
                                      result->path_len,
                                      find_request->find.attr_mask,
                                      find_request->find.root,
                                      result,
                                      find_request->find.filter,
                                      find_request->find.callback,
                                      find_request->find.complete,
                                      find_request->find.private_data);
        }
    }

    chimera_vfs_find_drain(thread, find_request->find.root);

    return 0;
} /* chimera_vfs_find_readdir_callback */

static void
chimera_vfs_find_readdir_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        cookie,
    uint64_t                        verifier,
    uint32_t                        eof,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data)
{
    struct chimera_vfs_request *find_request = private_data;
    struct chimera_vfs_thread  *thread       = find_request->thread;

    chimera_vfs_release(thread, handle);

    find_request->find.is_complete = 1;

    chimera_vfs_find_drain(thread, find_request->find.root);


} /* chimera_vfs_find_readdir_complete */ /* chimera_vfs_find_readdir_complete */

static void
chimera_vfs_find_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_vfs_request *find_request = private_data;
    struct chimera_vfs_thread  *thread       = find_request->thread;

    if (error_code != CHIMERA_VFS_OK) {
        find_request->find.complete(error_code,
                                    find_request->find.private_data);
        chimera_vfs_request_free(thread, find_request);
        return;
    }

    chimera_vfs_readdir(
        thread,
        find_request->cred,
        oh,
        find_request->find.attr_mask,
        0,
        0,
        0, /* verifier */
        0, /* flags: don't emit . and .. entries */
        chimera_vfs_find_readdir_callback,
        chimera_vfs_find_readdir_complete,
        find_request);

} /* chimera_vfs_find_open_callback */

SYMBOL_EXPORT void
chimera_vfs_find(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    uint64_t                       attr_mask,
    chimera_vfs_filter_callback_t  filter,
    chimera_vfs_find_callback_t    callback,
    chimera_vfs_find_complete_t    complete,
    void                          *private_data)
{
    chimera_vfs_find_dispatch(thread,
                              cred,
                              fh, fhlen,
                              "", 0,
                              attr_mask, NULL, NULL,
                              filter, callback, complete, private_data);
} /* chimera_vfs_find */