// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
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

#ifndef AT_EMPTY_PATH
#define AT_EMPTY_PATH       0x1000
#endif /* ifndef AT_EMPTY_PATH */

struct chimera_posix_fchownat_ctx {
    struct chimera_posix_completion comp;
    struct chimera_vfs_open_handle *file_handle;
    struct chimera_vfs_attrs        set_attr;
};

static void
chimera_posix_fchownat_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_completion *comp = private_data;

    chimera_posix_complete(comp, status);
} /* chimera_posix_fchownat_callback */

/* Callback after setattr completes - release the file handle */
static void
chimera_posix_fchownat_setattr_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_posix_fchownat_ctx *ctx = private_data;

    chimera_vfs_release(ctx->comp.request->thread->vfs_thread, ctx->file_handle);
    chimera_posix_complete(&ctx->comp, error_code);
} /* chimera_posix_fchownat_setattr_complete */

/* Callback after opening the target file - now call setattr */
static void
chimera_posix_fchownat_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data)
{
    struct chimera_posix_fchownat_ctx *ctx     = private_data;
    struct chimera_client_request     *request = ctx->comp.request;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_posix_complete(&ctx->comp, error_code);
        return;
    }

    ctx->file_handle = oh;

    chimera_vfs_setattr(
        request->thread->vfs_thread,
        oh,
        &ctx->set_attr,
        0,  /* pre_attr_mask */
        0,  /* post_attr_mask */
        chimera_posix_fchownat_setattr_complete,
        ctx);
} /* chimera_posix_fchownat_open_complete */

static void
chimera_posix_fchownat_at_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_posix_fchownat_ctx *ctx = request->setattr.private_data;

    /* Set up the set_attr for the VFS call */
    ctx->set_attr.va_set_mask = 0;

    /* Open the target file relative to the parent directory */
    chimera_vfs_open_at(
        thread->vfs_thread,
        request->setattr.parent_handle,
        request->setattr.path,
        request->setattr.path_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED,
        &ctx->set_attr,
        0,
        0,
        0,
        chimera_posix_fchownat_open_complete,
        ctx);
} /* chimera_posix_fchownat_at_exec */

static void
chimera_posix_fchownat_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_setattr(thread, request);
} /* chimera_posix_fchownat_exec */

SYMBOL_EXPORT int
chimera_posix_fchownat(
    int         dirfd,
    const char *pathname,
    uid_t       owner,
    gid_t       group,
    int         flags)
{
    struct chimera_posix_client      *posix  = chimera_posix_get_global();
    struct chimera_posix_worker      *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request     req;
    struct chimera_posix_fchownat_ctx ctx;
    struct chimera_posix_fd_entry    *dir_entry = NULL;
    int                               path_len;
    const char                       *slash;
    int                               err;

    // AT_SYMLINK_NOFOLLOW and AT_EMPTY_PATH are not implemented
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
        req.setattr.callback     = chimera_posix_fchownat_callback;
        req.setattr.private_data = &ctx.comp;

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

        chimera_posix_worker_enqueue(worker, &req, chimera_posix_fchownat_exec);
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
        req.setattr.callback     = chimera_posix_fchownat_callback;
        req.setattr.private_data = &ctx;

        // Store uid/gid in ctx for the async chain
        ctx.set_attr.va_req_mask = 0;

        if (owner != (uid_t) -1) {
            ctx.set_attr.va_req_mask |= CHIMERA_VFS_ATTR_UID;
            ctx.set_attr.va_uid       = owner;
        }

        if (group != (gid_t) -1) {
            ctx.set_attr.va_req_mask |= CHIMERA_VFS_ATTR_GID;
            ctx.set_attr.va_gid       = group;
        }

        chimera_posix_worker_enqueue(worker, &req, chimera_posix_fchownat_at_exec);
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
} /* chimera_posix_fchownat */
