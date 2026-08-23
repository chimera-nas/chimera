// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <sys/stat.h>

#include "fuse_internal.h"
#include "fuse_attr.h"
#include "vfs/vfs_procs.h"

/* --- MKDIR / MKNOD --- */

static void
chimera_fuse_mkdir_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    struct chimera_fuse_request *req = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    chimera_fuse_reply_entry(req, attr, NULL, 0);
} /* chimera_fuse_mkdir_complete */

static void
chimera_fuse_mkdir_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_fuse_request *req  = private_data;
    const struct fuse_in_header *hdr  = chimera_fuse_request_hdr(req);
    const struct fuse_mkdir_in  *in   = (const struct fuse_mkdir_in *) (hdr + 1);
    const char                  *name = (const char *) (in + 1);

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    req->handle = oh;

    memset(&req->u.create.set_attr, 0, sizeof(req->u.create.set_attr));
    req->u.create.set_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    req->u.create.set_attr.va_mode     = (in->mode & 07777) & ~in->umask;

    chimera_vfs_mkdir_at(req->thread->vfs_thread, &req->cred, oh,
                         name, strlen(name),
                         &req->u.create.set_attr,
                         CHIMERA_FUSE_ATTR_MASK,
                         0, 0,
                         chimera_fuse_mkdir_complete, req);
} /* chimera_fuse_mkdir_open_callback */

void
chimera_fuse_op_mkdir(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    if (arglen < sizeof(struct fuse_mkdir_in)) {
        chimera_fuse_reply(req, EINVAL, NULL, 0);
        return;
    }

    if (chimera_fuse_resolve_nodeid(req) != 0) {
        chimera_fuse_reply(req, ESTALE, NULL, 0);
        return;
    }

    /* Watch the parent before the backend op: the new entry's dentry may
     * only carry a TTL when the watch predates the request (reply_entry). */
    req->entry_cover = chimera_fuse_watch_dir(req->thread, req->channel->mount,
                                              req->nodeid,
                                              req->fh, req->fh_len);

    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred,
                        req->fh, req->fh_len,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH |
                        CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_fuse_mkdir_open_callback, req);
} /* chimera_fuse_op_mkdir */

static void
chimera_fuse_mknod_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    struct chimera_fuse_request *req = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    chimera_fuse_reply_entry(req, attr, NULL, 0);
} /* chimera_fuse_mknod_complete */

static void
chimera_fuse_mknod_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_fuse_request *req  = private_data;
    const struct fuse_in_header *hdr  = chimera_fuse_request_hdr(req);
    const struct fuse_mknod_in  *in   = (const struct fuse_mknod_in *) (hdr + 1);
    const char                  *name = (const char *) (in + 1);

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    req->handle = oh;

    memset(&req->u.create.set_attr, 0, sizeof(req->u.create.set_attr));
    req->u.create.set_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE |
        CHIMERA_VFS_ATTR_RDEV;
    req->u.create.set_attr.va_mode = (in->mode & S_IFMT) |
        ((in->mode & 07777) & ~in->umask);
    req->u.create.set_attr.va_rdev = in->rdev;

    chimera_vfs_mknod_at(req->thread->vfs_thread, &req->cred, oh,
                         name, strlen(name),
                         &req->u.create.set_attr,
                         CHIMERA_FUSE_ATTR_MASK,
                         0, 0,
                         chimera_fuse_mknod_complete, req);
} /* chimera_fuse_mknod_open_callback */

void
chimera_fuse_op_mknod(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    if (arglen < sizeof(struct fuse_mknod_in)) {
        chimera_fuse_reply(req, EINVAL, NULL, 0);
        return;
    }

    if (chimera_fuse_resolve_nodeid(req) != 0) {
        chimera_fuse_reply(req, ESTALE, NULL, 0);
        return;
    }

    /* Watch the parent before the backend op: the new entry's dentry may
     * only carry a TTL when the watch predates the request (reply_entry). */
    req->entry_cover = chimera_fuse_watch_dir(req->thread, req->channel->mount,
                                              req->nodeid,
                                              req->fh, req->fh_len);

    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred,
                        req->fh, req->fh_len,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH |
                        CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_fuse_mknod_open_callback, req);
} /* chimera_fuse_op_mknod */

/* --- SYMLINK --- */

static void
chimera_fuse_symlink_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    struct chimera_fuse_request *req = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    chimera_fuse_reply_entry(req, attr, NULL, 0);
} /* chimera_fuse_symlink_complete */

static void
chimera_fuse_symlink_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_fuse_request *req    = private_data;
    const struct fuse_in_header *hdr    = chimera_fuse_request_hdr(req);
    const char                  *name   = (const char *) (hdr + 1);
    const char                  *target = name + strlen(name) + 1;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    req->handle = oh;

    memset(&req->u.create.set_attr, 0, sizeof(req->u.create.set_attr));
    req->u.create.set_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    req->u.create.set_attr.va_mode     = 0777;

    chimera_vfs_symlink_at(req->thread->vfs_thread, &req->cred, oh,
                           name, strlen(name),
                           target, strlen(target),
                           &req->u.create.set_attr,
                           CHIMERA_FUSE_ATTR_MASK,
                           0, 0,
                           chimera_fuse_symlink_complete, req);
} /* chimera_fuse_symlink_open_callback */

void
chimera_fuse_op_symlink(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    if (chimera_fuse_resolve_nodeid(req) != 0) {
        chimera_fuse_reply(req, ESTALE, NULL, 0);
        return;
    }

    /* Watch the parent before the backend op: the new entry's dentry may
     * only carry a TTL when the watch predates the request (reply_entry). */
    req->entry_cover = chimera_fuse_watch_dir(req->thread, req->channel->mount,
                                              req->nodeid,
                                              req->fh, req->fh_len);

    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred,
                        req->fh, req->fh_len,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH |
                        CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_fuse_symlink_open_callback, req);
} /* chimera_fuse_op_symlink */

/* --- LINK --- */

static void
chimera_fuse_link_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *r_attr,
    struct chimera_vfs_attrs *r_dir_pre_attr,
    struct chimera_vfs_attrs *r_dir_post_attr,
    void                     *private_data)
{
    struct chimera_fuse_request *req = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    chimera_fuse_reply_entry(req, r_attr, NULL, 0);
} /* chimera_fuse_link_complete */

void
chimera_fuse_op_link(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const struct fuse_link_in *in   = arg;
    const char                *name = (const char *) (in + 1);

    if (arglen < sizeof(*in)) {
        chimera_fuse_reply(req, EINVAL, NULL, 0);
        return;
    }

    /* fh = new parent directory (the request's nodeid), fh2 = the file. */
    if (chimera_fuse_resolve_nodeid(req) != 0 ||
        chimera_fuse_resolve_nodeid2(req, in->oldnodeid) != 0) {
        chimera_fuse_reply(req, ESTALE, NULL, 0);
        return;
    }

    /* Watch the parent before the backend op: the new entry's dentry may
     * only carry a TTL when the watch predates the request (reply_entry). */
    req->entry_cover = chimera_fuse_watch_dir(req->thread, req->channel->mount,
                                              req->nodeid,
                                              req->fh, req->fh_len);

    chimera_vfs_link_at(req->thread->vfs_thread, &req->cred,
                        req->fh2, req->fh2_len,
                        req->fh, req->fh_len,
                        name, strlen(name),
                        0,
                        CHIMERA_FUSE_ATTR_MASK,
                        0, 0,
                        NULL, NULL,
                        chimera_fuse_link_complete, req);
} /* chimera_fuse_op_link */

/* --- UNLINK / RMDIR --- */

static void
chimera_fuse_remove_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_fuse_request *req = private_data;

    chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
} /* chimera_fuse_remove_complete */

static void
chimera_fuse_remove_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_fuse_request *req  = private_data;
    const struct fuse_in_header *hdr  = chimera_fuse_request_hdr(req);
    const char                  *name = (const char *) (hdr + 1);
    unsigned int                 flags;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    req->handle = oh;

    flags = (req->opcode == FUSE_RMDIR) ?
        CHIMERA_VFS_REMOVE_ISDIR : CHIMERA_VFS_REMOVE_ISNOTDIR;

    chimera_vfs_remove_at(req->thread->vfs_thread, &req->cred, oh,
                          name, strlen(name),
                          NULL, 0,
                          flags,
                          0, 0,
                          NULL,
                          chimera_fuse_remove_complete, req);
} /* chimera_fuse_remove_open_callback */

static void
chimera_fuse_remove_common(struct chimera_fuse_request *req)
{
    if (chimera_fuse_resolve_nodeid(req) != 0) {
        chimera_fuse_reply(req, ESTALE, NULL, 0);
        return;
    }

    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred,
                        req->fh, req->fh_len,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH |
                        CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_fuse_remove_open_callback, req);
} /* chimera_fuse_remove_common */

void
chimera_fuse_op_unlink(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    chimera_fuse_remove_common(req);
} /* chimera_fuse_op_unlink */

void
chimera_fuse_op_rmdir(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    chimera_fuse_remove_common(req);
} /* chimera_fuse_op_rmdir */

/* --- RENAME / RENAME2 --- */

static void
chimera_fuse_rename_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *fromdir_pre_attr,
    struct chimera_vfs_attrs *fromdir_post_attr,
    struct chimera_vfs_attrs *todir_pre_attr,
    struct chimera_vfs_attrs *todir_post_attr,
    void                     *private_data)
{
    struct chimera_fuse_request *req = private_data;

    chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
} /* chimera_fuse_rename_complete */

void
chimera_fuse_op_rename(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    uint64_t    newdir;
    const char *oldname;

    if (req->opcode == FUSE_RENAME2) {
        const struct fuse_rename2_in *in = arg;

        if (arglen < sizeof(*in)) {
            chimera_fuse_reply(req, EINVAL, NULL, 0);
            return;
        }

        /* RENAME_NOREPLACE / RENAME_EXCHANGE / RENAME_WHITEOUT have no VFS
         * counterpart yet. */
        if (in->flags != 0) {
            chimera_fuse_reply(req, EINVAL, NULL, 0);
            return;
        }

        newdir  = in->newdir;
        oldname = (const char *) (in + 1);
    } else {
        const struct fuse_rename_in *in = arg;

        if (arglen < sizeof(*in)) {
            chimera_fuse_reply(req, EINVAL, NULL, 0);
            return;
        }

        newdir  = in->newdir;
        oldname = (const char *) (in + 1);
    }

    const char *newname = oldname + strlen(oldname) + 1;

    if (chimera_fuse_resolve_nodeid(req) != 0 ||
        chimera_fuse_resolve_nodeid2(req, newdir) != 0) {
        chimera_fuse_reply(req, ESTALE, NULL, 0);
        return;
    }

    chimera_vfs_rename_at(req->thread->vfs_thread, &req->cred,
                          req->fh, req->fh_len,
                          oldname, strlen(oldname),
                          req->fh2, req->fh2_len,
                          newname, strlen(newname),
                          NULL, 0,
                          0,
                          0, 0,
                          NULL, NULL,
                          chimera_fuse_rename_complete, req);
} /* chimera_fuse_op_rename */
