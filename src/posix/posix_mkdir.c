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
        /* The backend's answer stands, trailing slash and all.  This used to
         * re-judge a slash-terminated path with stat() and turn EEXIST into
         * ENOTDIR or ELOOP -- and, for a dangling symlink, follow the link
         * and create its target -- on the reading that XBD 4.16 makes a
         * trailing slash resolve THROUGH the final component.  It does not,
         * on the create path: XSH mkdir lists EEXIST as "the named file
         * exists" with no qualification, and Linux answers EEXIST for
         * mkdir("f/"), mkdir("dir/"), mkdir("lnk2dir/") and
         * mkdir("dangling/") alike, creating nothing in any of them.  The
         * old reading was shared with the quint model, which is why the MBT
         * suite passed: the two agreed on something POSIX never said, and it
         * took replaying the corpus at ext4 to break the tie.
         */
        errno = err;
        return -1;
    }

    return 0;
} /* chimera_posix_mkdir */
