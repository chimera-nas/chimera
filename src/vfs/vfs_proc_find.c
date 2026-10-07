// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#ifdef _WIN32
#include "common/platform.h"
#endif /* ifdef _WIN32 */
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

/* Record one directory's error on the walk's root; the walk completes with
 * the first one recorded.  A subdirectory that vanished after its parent
 * listed it -- removed by another client mid-walk -- is not an error: its
 * entries went with it, as they would had it gone before the parent's
 * listing. */
static inline void
chimera_vfs_find_record(
    struct chimera_vfs_request *root,
    int                         subdir,
    enum chimera_vfs_error      error_code)
{
    if (error_code == CHIMERA_VFS_OK) {
        return;
    }

    if (subdir && (error_code == CHIMERA_VFS_ENOENT ||
                   error_code == CHIMERA_VFS_ESTALE)) {
        return;
    }

    if (root->find.status == CHIMERA_VFS_OK) {
        root->find.status = error_code;
    }
} /* chimera_vfs_find_record */

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
        root->find.complete(root->find.status, root->find.private_data);
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

    if (CHIMERA_VFS_IS_ERR(find_request)) {
        /* A subdirectory's walk could not start.  Record that on the root
         * rather than completing the caller while the rest of the walk runs
         * on: no child is linked from the parent's result yet, so the drain
         * passes over it. */
        if (root) {
            chimera_vfs_find_record(root, 1, CHIMERA_VFS_PTR_ERR(find_request));
            return;
        }
        complete(CHIMERA_VFS_PTR_ERR(find_request), private_data);
        return;
    }

    find_request->find.path            = find_request->plugin_data;
    find_request->find.attr_mask       = attr_mask;
    find_request->find.private_data    = private_data;
    find_request->find.root            = root ? root : find_request;
    find_request->find.parent          = parent;
    find_request->find.is_complete     = 0;
    find_request->find.complete_called = 0;
    find_request->find.status          = CHIMERA_VFS_OK;
    find_request->find.results         = NULL;
    find_request->find.filter          = filter;
    find_request->find.callback        = callback;
    find_request->find.complete        = complete;

    memcpy(find_request->find.path, path_prefix, path_prefix_len);
    find_request->find.path_len = path_prefix_len;

    if (parent) {
        parent->child_request = find_request;
    }

    chimera_vfs_open_fh(
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
    struct chimera_vfs_request     *root         = find_request->find.root;
    struct chimera_vfs_thread      *thread       = find_request->thread;
    struct chimera_vfs_find_result *result;
    int                             filter_result;

    if ((namelen == 1 && name[0] == '.') ||
        (namelen == 2 && name[0] == '.' && name[1] == '.')) {
        return 0;
    }

    /* The walk has already failed, and will complete with that error:
     * listing and descending further cannot change the answer. */
    if (root->find.status != CHIMERA_VFS_OK) {
        return -1;
    }

    /* The caller may list this directory but not search it, so the entries
     * come without the attributes the walk reports and descends by.  Fail
     * the walk, as for a directory it may not list, rather than report
     * entries it could not stat or reach. */
    if (attrs->va_set_mask & CHIMERA_VFS_ATTR_WITHHELD) {
        root->find.status = CHIMERA_VFS_EACCES;
        return -1;
    }

    /* Nor can the walk report or descend an entry that came without its
     * type or handle for any other reason -- a proxy whose server sent none,
     * or a handle too large to re-encode -- without passing it off as
     * something it may not be. */
    if ((attrs->va_set_mask & (CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MODE)) !=
        (CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MODE)) {
        root->find.status = CHIMERA_VFS_EIO;
        return -1;
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

/* One directory of the walk is done.  An error (the directory could not be
 * opened, or the caller may not list it) is recorded on the root, and the
 * walk completes with the first one once every directory has finished:
 * reporting success would pass off a partial listing as the whole tree. */
static void
chimera_vfs_find_finish(
    struct chimera_vfs_request *find_request,
    enum chimera_vfs_error      error_code)
{
    struct chimera_vfs_request *root = find_request->find.root;

    chimera_vfs_find_record(root, find_request != root, error_code);

    find_request->find.is_complete = 1;

    chimera_vfs_find_drain(find_request->thread, root);
} /* chimera_vfs_find_finish */

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

    chimera_vfs_find_finish(find_request, error_code);
} /* chimera_vfs_find_readdir_complete */

static void
chimera_vfs_find_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_vfs_request *find_request = private_data;
    struct chimera_vfs_thread  *thread       = find_request->thread;

    /* Not completed or freed here: a subdirectory's request is still linked
     * from its parent's result, so the drain must be the one to retire it. */
    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_find_finish(find_request, error_code);
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
        NULL, 0, /* no search-pattern filter */
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