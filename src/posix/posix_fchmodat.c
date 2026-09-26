// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"
#include "../client/client_setattr.h"

#ifndef AT_FDCWD
#define AT_FDCWD            -100
#endif /* ifndef AT_FDCWD */

#ifndef AT_SYMLINK_NOFOLLOW
#define AT_SYMLINK_NOFOLLOW 0x100
#endif /* ifndef AT_SYMLINK_NOFOLLOW */

struct chimera_posix_fchmodat_ctx {
    struct chimera_posix_completion comp;
    struct chimera_vfs_attrs        set_attr;
};

static void
chimera_posix_fchmodat_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_completion *comp = private_data;

    chimera_posix_complete(comp, status);
} /* chimera_posix_fchmodat_callback */

static void
chimera_posix_fchmodat_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_posix_fchmodat_ctx *ctx    = private_data;
    enum chimera_vfs_error             status = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);
    chimera_posix_complete(&ctx->comp, status);
} /* chimera_posix_fchmodat_sequence_complete */

static void
chimera_posix_fchmodat_at_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_posix_fchmodat_ctx *ctx      = request->setattr.private_data;
    struct chimera_vfs_compound       *compound = chimera_vfs_compound_alloc(
        thread->vfs_thread, chimera_client_req_cred(request));

    request->compound = compound;
    chimera_vfs_compound_add_puthandle(compound, request->setattr.parent_handle,
                                       CHIMERA_VFS_OPEN_INFERRED);
    int                                opened = chimera_vfs_compound_add_open_at(compound, request->setattr.path,
                                                                                 request->setattr.path_len,
                                                                                 CHIMERA_VFS_OPEN_PATH |
                                                                                 CHIMERA_VFS_OPEN_INFERRED,
                                                                                 NULL, 0);
    int                                attr = chimera_vfs_compound_add_setattr(compound, NULL, &ctx->set_attr, 0, 0);
    if (opened >= 0 && attr >= 0) {
        chimera_vfs_compound_op_set_handle(compound, opened, request->setattr.parent_handle);
        chimera_vfs_compound_op_use_handle(compound, attr, opened);
    }
    chimera_frontend_compound_submit(compound, chimera_posix_fchmodat_sequence_complete, ctx);
} /* chimera_posix_fchmodat_at_exec */

static void
chimera_posix_fchmodat_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_setattr(thread, request);
} /* chimera_posix_fchmodat_exec */

SYMBOL_EXPORT int
chimera_posix_fchmodat(
    int         dirfd,
    const char *pathname,
    mode_t      mode,
    int         flags)
{
    struct chimera_posix_client      *posix  = chimera_posix_get_global();
    struct chimera_posix_worker      *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request     req;
    struct chimera_posix_fchmodat_ctx ctx;
    struct chimera_posix_fd_entry    *dir_entry = NULL;
    int                               path_len;
    const char                       *slash;
    int                               err;

    // AT_SYMLINK_NOFOLLOW is not implemented
    (void) flags;

    chimera_posix_completion_init(&ctx.comp, &req);

    // Handle AT_FDCWD case - use simple path-based setattr
    if (dirfd == AT_FDCWD) {
        if (pathname[0] == '/') {
            path_len = strlen(pathname);
            memcpy(req.setattr.path, pathname, path_len);
        } else {
            req.setattr.path[0] = '/';
            path_len            = strlen(pathname);
            memcpy(req.setattr.path + 1, pathname, path_len);
            path_len++;
        }

        req.setattr.path[path_len] = '\0';
        slash                      = rindex(req.setattr.path, '/');

        req.setattr.parent_handle = NULL;
        req.setattr.path_len      = path_len;
        req.setattr.parent_len    = slash ? slash - req.setattr.path : path_len;

        while (slash && *slash == '/') {
            slash++;
        }

        req.setattr.name_offset = slash ? slash - req.setattr.path : -1;

        req.opcode               = CHIMERA_CLIENT_OP_SETATTR;
        req.setattr.callback     = chimera_posix_fchmodat_callback;
        req.setattr.private_data = &ctx.comp;

        req.setattr.set_attr.va_req_mask = 0;
        req.setattr.set_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        req.setattr.set_attr.va_mode     = mode;

        chimera_posix_worker_enqueue(worker, &req, chimera_posix_fchmodat_exec);
    } else {
        // Real fd case - need to open file, setattr, then close
        dir_entry = chimera_posix_fd_acquire(posix, dirfd, 0);
        if (!dir_entry) {
            errno = EBADF;
            chimera_posix_completion_destroy(&ctx.comp);
            return -1;
        }

        path_len = strlen(pathname);
        memcpy(req.setattr.path, pathname, path_len);

        req.setattr.parent_handle = dir_entry->handle;
        req.setattr.path_len      = path_len;
        req.setattr.parent_len    = 0;
        req.setattr.name_offset   = 0;

        req.opcode               = CHIMERA_CLIENT_OP_SETATTR;
        req.setattr.callback     = chimera_posix_fchmodat_callback;
        req.setattr.private_data = &ctx;

        // Store mode in ctx for the async chain
        ctx.set_attr.va_req_mask = 0;
        ctx.set_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        ctx.set_attr.va_mode     = mode;

        chimera_posix_worker_enqueue(worker, &req, chimera_posix_fchmodat_at_exec);
    }

    err = chimera_posix_wait(&ctx.comp);

    if (dir_entry) {
        chimera_posix_fd_release(dir_entry, 0);
    }

    chimera_posix_completion_destroy(&ctx.comp);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
} /* chimera_posix_fchmodat */
