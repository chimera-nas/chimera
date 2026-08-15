// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"
#include "../client/client_mkdir.h"

static void
chimera_posix_mkdir_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_completion *comp = private_data;

    chimera_posix_complete(comp, status);
} /* chimera_posix_mkdir_callback */

static void
chimera_posix_mkdir_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_mkdir(thread, request);
} /* chimera_posix_mkdir_exec */

SYMBOL_EXPORT int
chimera_posix_mkdir(
    const char *path,
    mode_t      mode)
{
    struct chimera_posix_client    *posix  = chimera_posix_get_global();
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request   req;
    struct chimera_posix_completion comp;
    const char                     *slash;
    int                             path_len;

    path_len = chimera_posix_check_path(path);
    if (path_len < 0) {
        return -1;
    }

    chimera_posix_completion_init(&comp, &req);

    slash = rindex(path, '/');

    req.opcode             = CHIMERA_CLIENT_OP_MKDIR;
    req.mkdir.callback     = chimera_posix_mkdir_callback;
    req.mkdir.private_data = &comp;
    req.mkdir.path_len     = path_len;
    req.mkdir.parent_len   = slash ? slash - path : path_len;

    while (slash && *slash == '/') {
        slash++;
    }

    req.mkdir.name_offset = slash ? slash - path : -1;

    chimera_posix_set_create_mode(&req.mkdir.set_attr, mode);

    memcpy(req.mkdir.path, path, path_len);

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_mkdir_exec);

    int err = chimera_posix_wait(&comp);

    chimera_posix_completion_destroy(&comp);

    if (err) {
        if ((err == EEXIST || err == EACCES) &&
            path_len > 1 && path[path_len - 1] == '/') {
            /* The backend judged the slash-stripped name.  XBD 4.16: a
             * trailing slash makes the pathname resolve through the final
             * component -- a non-directory there is ENOTDIR and a symlink
             * loop is ELOOP, and resolution errors precede the operation's
             * own existence/permission errors.  stat() with the slash
             * preserved applies exactly those rules; any other stat
             * outcome (a directory, a dangling link) keeps the backend's
             * errno. */
            struct stat st;

            if (chimera_posix_stat(path, &st) < 0) {
                if (errno == ENOTDIR || errno == ELOOP) {
                    return -1;
                }
                if (errno == ENOENT) {
                    /* The final component is a symbolic link whose target does
                     * not exist.  The trailing slash resolves through the link
                     * (XBD 4.16), so mkdir(2) must create the link's *target*,
                     * not fail EEXIST on the link itself.  Read the link and
                     * retry the create on the resolved path. */
                    char    link[CHIMERA_VFS_PATH_MAX];
                    char    tgt[CHIMERA_VFS_PATH_MAX];
                    char    full[CHIMERA_VFS_PATH_MAX];
                    ssize_t n;
                    int     llen = path_len;

                    while (llen > 1 && path[llen - 1] == '/') {
                        llen--;
                    }
                    memcpy(link, path, llen);
                    link[llen] = '\0';

                    n = chimera_posix_readlink(link, tgt, sizeof(tgt) - 1);
                    if (n > 0) {
                        tgt[n] = '\0';
                        if (tgt[0] == '/') {
                            return chimera_posix_mkdir(tgt, mode);
                        } else {
                            const char *ls   = rindex(link, '/');
                            int         dlen = ls ? (int) (ls - link) : 0;

                            if (snprintf(full, sizeof(full), "%.*s/%s", dlen,
                                         link, tgt) >= (int) sizeof(full)) {
                                errno = ENAMETOOLONG;
                                return -1;
                            }
                            return chimera_posix_mkdir(full, mode);
                        }
                    }
                }
            }
        }
        errno = err;
        return -1;
    }

    return 0;
} /* chimera_posix_mkdir */
