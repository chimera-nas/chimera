// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"
#include "../client/client_setattr.h"
#include "vfs/vfs_acl.h"

/*
 * Set the canonical NFSv4/Windows ACL on `path`.  This is a Chimera-specific
 * extension: NFSv4 ACLs are not part of standard POSIX, but they are a
 * first-class Chimera VFS attribute (CHIMERA_VFS_ATTR_ACL).  The ACL set is
 * authorized by the VFS setattr gate (WRITE_ACL, or file ownership), running
 * under the calling thread's effective credential (chimera_posix_set_cred).
 */

static void
chimera_posix_setacl_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_completion *comp = private_data;

    chimera_posix_complete(comp, status);
} /* chimera_posix_setacl_callback */

static void
chimera_posix_setacl_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_setattr(thread, request);
} /* chimera_posix_setacl_exec */

SYMBOL_EXPORT int
chimera_posix_setacl(
    const char               *path,
    const struct chimera_acl *acl)
{
    struct chimera_posix_client    *posix  = chimera_posix_get_global();
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request   req;
    struct chimera_posix_completion comp;
    int                             path_len;

    if (!acl) {
        errno = EINVAL;
        return -1;
    }

    path_len = chimera_posix_check_path(path);
    if (path_len < 0) {
        return -1;
    }

    chimera_posix_completion_init(&comp, &req);

    req.opcode               = CHIMERA_CLIENT_OP_SETATTR;
    req.setattr.callback     = chimera_posix_setacl_callback;
    req.setattr.private_data = &comp;
    req.setattr.path_len     = path_len;

    memcpy(req.setattr.path, path, path_len);

    /* va_acl points at the caller's buffer, which outlives the synchronous
    * wait below (the setattr gate copies/applies it before completing). */
    req.setattr.set_attr.va_req_mask = 0;
    req.setattr.set_attr.va_set_mask = CHIMERA_VFS_ATTR_ACL;
    req.setattr.set_attr.va_acl      = (struct chimera_acl *) acl;

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_setacl_exec);

    int err = chimera_posix_wait(&comp);

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
} /* chimera_posix_setacl */
