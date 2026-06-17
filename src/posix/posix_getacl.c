// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"
#include "../client/client_getacl.h"
#include "vfs/vfs_acl.h"

/*
 * Read the canonical NFSv4/Windows ACL of `path` into the caller-owned `buf`
 * (capacity `bufsize` bytes).  Returns the number of ACEs in the object's ACL
 * on success; -1 with errno set on failure (ERANGE if `buf` is too small to
 * hold the whole ACL -- the caller can size a retry with chimera_acl_size()).
 * Chimera-specific extension (see posix_setacl.c).
 */

static void
chimera_posix_getacl_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_completion *comp = private_data;

    chimera_posix_complete(comp, status);
} /* chimera_posix_getacl_callback */

static void
chimera_posix_getacl_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_getacl(thread, request);
} /* chimera_posix_getacl_exec */

SYMBOL_EXPORT int
chimera_posix_getacl(
    const char         *path,
    struct chimera_acl *buf,
    size_t              bufsize)
{
    struct chimera_posix_client    *posix  = chimera_posix_get_global();
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request   req;
    struct chimera_posix_completion comp;
    int                             path_len;

    if (!buf) {
        errno = EINVAL;
        return -1;
    }

    path_len = chimera_posix_check_path(path);
    if (path_len < 0) {
        return -1;
    }

    chimera_posix_completion_init(&comp, &req);

    req.opcode              = CHIMERA_CLIENT_OP_STAT;
    req.getacl.callback     = chimera_posix_getacl_callback;
    req.getacl.private_data = &comp;
    req.getacl.path_len     = path_len;
    req.getacl.acl_buf      = buf;
    req.getacl.acl_bufsize  = bufsize;
    req.getacl.r_acl_aces   = 0;

    memcpy(req.getacl.path, path, path_len);

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_getacl_exec);

    int err = chimera_posix_wait(&comp);

    int naces = req.getacl.r_acl_aces;

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    return naces;
} /* chimera_posix_getacl */
