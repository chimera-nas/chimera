// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"
#include "../client/client_setattr.h"

static void
chimera_posix_chown_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_completion *comp = private_data;

    chimera_posix_complete(comp, status);
}

static void
chimera_posix_chown_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_setattr(thread, request);
}

SYMBOL_EXPORT int
chimera_posix_chown(
    const char *path,
    uid_t       owner,
    gid_t       group)
{
    struct chimera_posix_client     *posix  = chimera_posix_get_global();
    struct chimera_posix_worker     *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request    req;
    struct chimera_posix_completion  comp;
    int                              path_len;

    chimera_posix_completion_init(&comp, &req);

    path_len = strlen(path);

    req.opcode               = CHIMERA_CLIENT_OP_SETATTR;
    req.setattr.callback     = chimera_posix_chown_callback;
    req.setattr.private_data = &comp;
    req.setattr.path_len     = path_len;

    memcpy(req.setattr.path, path, path_len);

    req.setattr.set_attr.va_req_mask = 0;
    req.setattr.set_attr.va_set_mask = 0;

    if (owner != (uid_t) -1) {
        req.setattr.set_attr.va_req_mask |= CHIMERA_VFS_ATTR_UID;
        req.setattr.set_attr.va_uid       = owner;
    }

    if (group != (gid_t) -1) {
        req.setattr.set_attr.va_req_mask |= CHIMERA_VFS_ATTR_GID;
        req.setattr.set_attr.va_gid       = group;
    }

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_chown_exec);

    int err = chimera_posix_wait(&comp);

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
} /* chimera_posix_chown */
