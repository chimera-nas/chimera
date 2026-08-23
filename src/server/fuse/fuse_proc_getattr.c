// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

#include "fuse_internal.h"
#include "fuse_attr.h"
#include "vfs/vfs_procs.h"
#include "vfs/sdk/vfs_access.h"
#include "vfs/sdk/vfs_acl.h"

static void
chimera_fuse_attr_out_reply(
    struct chimera_fuse_request    *req,
    const struct chimera_vfs_attrs *attr)
{
    struct chimera_fuse_mount *mount   = req->channel->mount;
    uint32_t                   attr_ms = mount->attr_timeout_ms;
    struct fuse_attr_out       out;

    memset(&out, 0, sizeof(out));

    /* coherence=sync: attributes get a TTL only when coverage existed at
     * request ENTRY (req->entry_cover -- so the backend fetch was
     * protected) AND, for a regular file, the grant is STILL unbroken now
     * (a break mid-flight means a foreign write raced the fetch).  A
     * directory's watch never breaks; its mid-flight races resolve through
     * the queued INVAL_INODE instead.  Everything uncovered -- first
     * touches, symlinks, contention -- serves uncached. */
    if (mount->coherence_sync) {
        if (req->entry_cover == CHIMERA_FUSE_COVER_NONE) {
            attr_ms = 0;
        } else if (S_ISREG(attr->va_mode) &&
                   !chimera_fuse_grant_active(mount, req->nodeid)) {
            attr_ms = 0;
        }
    }

    out.attr_valid      = attr_ms / 1000;
    out.attr_valid_nsec = (attr_ms % 1000) * 1000000;

    chimera_fuse_attr_from_vfs(&out.attr, attr);

    chimera_fuse_reply(req, 0, &out, sizeof(out));
} /* chimera_fuse_attr_out_reply */

/* --- GETATTR --- */

static void
chimera_fuse_getattr_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_fuse_request *req   = private_data;
    struct chimera_fuse_mount   *mount = req->channel->mount;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    /* The kernel re-fetches attributes exactly when its cache went stale
     * (or was invalidated), which makes this the natural rearm point for a
     * broken grant -- and for stat-only files, the point coverage begins. */
    if (req->fh_len) {
        if (S_ISREG(attr->va_mode)) {
            chimera_fuse_grant_ensure(req->thread, mount, req->nodeid,
                                      req->fh, req->fh_len,
                                      chimera_fuse_fh_hash(req->fh,
                                                           req->fh_len));
        } else if (S_ISDIR(attr->va_mode)) {
            chimera_fuse_watch_dir(req->thread, mount, req->nodeid,
                                   req->fh, req->fh_len);
        }
    }

    chimera_fuse_attr_out_reply(req, attr);
} /* chimera_fuse_getattr_complete */

static void
chimera_fuse_getattr_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_fuse_request *req = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    req->handle = oh;

    chimera_vfs_getattr(req->thread->vfs_thread, &req->cred, oh,
                        CHIMERA_VFS_ATTR_MASK_STAT,
                        chimera_fuse_getattr_complete, req);
} /* chimera_fuse_getattr_open_callback */

void
chimera_fuse_op_getattr(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const struct fuse_getattr_in *in = arg;

    if (arglen >= sizeof(*in) && (in->getattr_flags & FUSE_GETATTR_FH)) {
        struct chimera_vfs_open_handle *oh = chimera_fuse_file(in->fh)->handle;

        /* The completion's grant rearm reads the handle from req->fh. */
        memcpy(req->fh, oh->fh, oh->fh_len);
        req->fh_len = oh->fh_len;

        /* Arm coverage BEFORE the backend fetch so the reply may carry a
         * TTL (see chimera_fuse_attr_out_reply). */
        req->entry_cover = chimera_fuse_cover_touch(req->thread,
                                                    req->channel->mount,
                                                    req->nodeid,
                                                    req->fh, req->fh_len);

        /* The kernel named an open file; use its handle directly (it stays
         * owned by the open, so req->handle stays NULL). */
        chimera_vfs_getattr(req->thread->vfs_thread, &req->cred, oh,
                            CHIMERA_VFS_ATTR_MASK_STAT,
                            chimera_fuse_getattr_complete, req);
        return;
    }

    if (chimera_fuse_resolve_nodeid(req) != 0) {
        chimera_fuse_reply(req, ESTALE, NULL, 0);
        return;
    }

    req->entry_cover = chimera_fuse_cover_touch(req->thread,
                                                req->channel->mount,
                                                req->nodeid,
                                                req->fh, req->fh_len);

    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred,
                        req->fh, req->fh_len,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                        chimera_fuse_getattr_open_callback, req);
} /* chimera_fuse_op_getattr */

/* --- SETATTR --- */

static void
chimera_fuse_setattr_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_fuse_request *req = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    chimera_fuse_attr_out_reply(req, post_attr);
} /* chimera_fuse_setattr_complete */

static void
chimera_fuse_setattr_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_fuse_request *req = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    req->handle = oh;

    chimera_vfs_setattr(req->thread->vfs_thread, &req->cred, oh,
                        &req->u.setattr.set_attr,
                        0, CHIMERA_VFS_ATTR_MASK_STAT,
                        chimera_fuse_setattr_complete, req);
} /* chimera_fuse_setattr_open_callback */

void
chimera_fuse_op_setattr(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const struct fuse_setattr_in *in = arg;

    if (arglen < sizeof(*in)) {
        chimera_fuse_reply(req, EINVAL, NULL, 0);
        return;
    }

    chimera_fuse_setattr_to_vfs(&req->u.setattr.set_attr, in);

    if (in->valid & FATTR_FH) {
        chimera_vfs_setattr(req->thread->vfs_thread, &req->cred,
                            chimera_fuse_file(in->fh)->handle,
                            &req->u.setattr.set_attr,
                            0, CHIMERA_VFS_ATTR_MASK_STAT,
                            chimera_fuse_setattr_complete, req);
        return;
    }

    if (chimera_fuse_resolve_nodeid(req) != 0) {
        chimera_fuse_reply(req, ESTALE, NULL, 0);
        return;
    }

    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred,
                        req->fh, req->fh_len,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                        chimera_fuse_setattr_open_callback, req);
} /* chimera_fuse_op_setattr */

/* --- READLINK --- */

static void
chimera_fuse_readlink_complete(
    enum chimera_vfs_error    error_code,
    int                       targetlen,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_fuse_request *req = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    chimera_fuse_reply(req, 0, chimera_fuse_reply_space(req), targetlen);
} /* chimera_fuse_readlink_complete */

static void
chimera_fuse_readlink_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_fuse_request *req = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    req->handle = oh;

    chimera_vfs_readlink(req->thread->vfs_thread, &req->cred, oh,
                         chimera_fuse_reply_space(req), 4096, 0,
                         chimera_fuse_readlink_complete, req);
} /* chimera_fuse_readlink_open_callback */

void
chimera_fuse_op_readlink(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    if (chimera_fuse_resolve_nodeid(req) != 0) {
        chimera_fuse_reply(req, ESTALE, NULL, 0);
        return;
    }

    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred,
                        req->fh, req->fh_len,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                        chimera_fuse_readlink_open_callback, req);
} /* chimera_fuse_op_readlink */

/* --- STATFS --- */

static void
chimera_fuse_statfs_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_fuse_request *req = private_data;
    struct fuse_statfs_out       out;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    memset(&out, 0, sizeof(out));

    chimera_fuse_statfs_from_vfs(&out.st, attr);

    chimera_fuse_reply(req, 0, &out, sizeof(out));
} /* chimera_fuse_statfs_complete */

static void
chimera_fuse_statfs_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_fuse_request *req = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    req->handle = oh;

    chimera_vfs_getattr(req->thread->vfs_thread, &req->cred, oh,
                        CHIMERA_VFS_ATTR_MASK_STATFS,
                        chimera_fuse_statfs_complete, req);
} /* chimera_fuse_statfs_open_callback */

void
chimera_fuse_op_statfs(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    if (chimera_fuse_resolve_nodeid(req) != 0) {
        chimera_fuse_reply(req, ESTALE, NULL, 0);
        return;
    }

    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred,
                        req->fh, req->fh_len,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                        chimera_fuse_statfs_open_callback, req);
} /* chimera_fuse_op_statfs */

/* --- ACCESS --- */

static void
chimera_fuse_access_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_fuse_request *req       = private_data;
    const struct fuse_in_header *hdr       = chimera_fuse_request_hdr(req);
    const struct fuse_access_in *in        = (const struct fuse_access_in *) (hdr + 1);
    uint32_t                     requested = 0;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    if (in->mask & R_OK) {
        requested |= CHIMERA_ACE_READ_DATA;
    }
    if (in->mask & W_OK) {
        requested |= CHIMERA_ACE_WRITE_DATA;
    }
    if (in->mask & X_OK) {
        requested |= CHIMERA_ACE_EXECUTE;
    }

    if (requested &&
        !chimera_vfs_access_allowed(attr, &req->cred, requested)) {
        chimera_fuse_reply(req, EACCES, NULL, 0);
        return;
    }

    chimera_fuse_reply(req, 0, NULL, 0);
} /* chimera_fuse_access_complete */

static void
chimera_fuse_access_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_fuse_request *req = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    req->handle = oh;

    chimera_vfs_getattr(req->thread->vfs_thread, &req->cred, oh,
                        CHIMERA_VFS_ATTR_MASK_STAT,
                        chimera_fuse_access_complete, req);
} /* chimera_fuse_access_open_callback */

void
chimera_fuse_op_access(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    if (arglen < sizeof(struct fuse_access_in)) {
        chimera_fuse_reply(req, EINVAL, NULL, 0);
        return;
    }

    if (chimera_fuse_resolve_nodeid(req) != 0) {
        chimera_fuse_reply(req, ESTALE, NULL, 0);
        return;
    }

    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred,
                        req->fh, req->fh_len,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                        chimera_fuse_access_open_callback, req);
} /* chimera_fuse_op_access */
