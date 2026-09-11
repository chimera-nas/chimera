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

/*
 * One sequence, however the kernel named the object.
 *
 * When it named an open file the sequence addresses that handle and has no
 * current object at all; when it named a node the sequence starts from its file
 * handle and opens it itself.  Either way the request no longer carries the
 * open, or the stage that took it.
 */
static void
chimera_fuse_getattr_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_fuse_request          *req = private_data;
    const struct chimera_vfs_compound_op *op;

    op = chimera_vfs_compound_op(compound,
                                 chimera_vfs_compound_num_ops(compound) - 1);

    chimera_fuse_getattr_complete(chimera_vfs_compound_status(compound),
                                  (struct chimera_vfs_attrs *) &op->attr,
                                  req);
} /* chimera_fuse_getattr_sequence_complete */

void
chimera_fuse_op_getattr(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const struct fuse_getattr_in *in = arg;
    int                           idx;

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

        /* The kernel named an open file; the sequence acts on that handle,
         * which stays owned by the open. */
        req->compound = chimera_vfs_compound_alloc(req->thread->vfs_thread,
                                                   &req->cred);

        idx = chimera_vfs_compound_add_getattr(req->compound,
                                               CHIMERA_VFS_ATTR_MASK_STAT);
        chimera_vfs_compound_op_set_handle(req->compound, (uint32_t) idx, oh);

        chimera_vfs_compound_submit(req->compound,
                                    chimera_fuse_getattr_sequence_complete,
                                    req);
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

    req->compound = chimera_vfs_compound_alloc(req->thread->vfs_thread,
                                               &req->cred);

    chimera_vfs_compound_add_putfh(req->compound, req->fh, (int) req->fh_len);
    chimera_vfs_compound_add_getattr(req->compound,
                                     CHIMERA_VFS_ATTR_MASK_STAT);

    chimera_vfs_compound_submit(req->compound,
                                chimera_fuse_getattr_sequence_complete, req);
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
chimera_fuse_setattr_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_fuse_request          *req = private_data;
    const struct chimera_vfs_compound_op *op;

    op = chimera_vfs_compound_op(compound,
                                 chimera_vfs_compound_num_ops(compound) - 1);

    chimera_fuse_setattr_complete(chimera_vfs_compound_status(compound),
                                  NULL, NULL,
                                  (struct chimera_vfs_attrs *) &op->attr,
                                  req);
} /* chimera_fuse_setattr_sequence_complete */

void
chimera_fuse_op_setattr(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const struct fuse_setattr_in *in = arg;
    int                           idx;

    if (arglen < sizeof(*in)) {
        chimera_fuse_reply(req, EINVAL, NULL, 0);
        return;
    }

    chimera_fuse_setattr_to_vfs(&req->u.setattr.set_attr, in);

    if (in->valid & FATTR_FH) {
        /* A setattr through an open descriptor is descriptor-originated, so
         * it gets POSIX rights retention: chimera_vfs_fsetattr authorizes a
         * size change from the handle's open-time grant instead of the
         * file's current mode, which is what makes ftruncate through a
         * writable descriptor survive an intervening chmod.  The path-based
         * form (truncate(2)) deliberately does not, and still uses
         * chimera_vfs_setattr below. */
        req->compound = chimera_vfs_compound_alloc(req->thread->vfs_thread,
                                                   &req->cred);

        idx = chimera_vfs_compound_add_setattr(req->compound,
                                               chimera_fuse_file(in->fh)->handle,
                                               &req->u.setattr.set_attr,
                                               CHIMERA_VFS_ATTR_MASK_STAT);
        (void) idx;

        chimera_vfs_compound_submit(req->compound,
                                    chimera_fuse_setattr_sequence_complete,
                                    req);
        return;
    }

    if (chimera_fuse_resolve_nodeid(req) != 0) {
        chimera_fuse_reply(req, ESTALE, NULL, 0);
        return;
    }

    /* No handle: the sequence applies the attributes against the object's own
     * mode, which is truncate(2) rather than ftruncate(2) -- the distinction
     * the op's handle argument carries. */
    req->compound = chimera_vfs_compound_alloc(req->thread->vfs_thread,
                                               &req->cred);

    chimera_vfs_compound_add_putfh(req->compound, req->fh, (int) req->fh_len);
    chimera_vfs_compound_add_setattr(req->compound, NULL,
                                     &req->u.setattr.set_attr,
                                     CHIMERA_VFS_ATTR_MASK_STAT);

    chimera_vfs_compound_submit(req->compound,
                                chimera_fuse_setattr_sequence_complete, req);
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

/*
 * The sequence owns the target it read, so it is copied into the reply rather
 * than read into it -- one copy of a path, in exchange for the request no
 * longer opening the object itself.
 */
static void
chimera_fuse_readlink_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_fuse_request          *req = private_data;
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                status;
    uint32_t                              len;

    status = chimera_vfs_compound_status(compound);

    if (status != CHIMERA_VFS_OK) {
        chimera_fuse_readlink_complete(status, 0, NULL, req);
        return;
    }

    op = chimera_vfs_compound_op(compound,
                                 chimera_vfs_compound_num_ops(compound) - 1);
    len = op->target_len > 4096 ? 4096 : op->target_len;

    memcpy(chimera_fuse_reply_space(req), op->target, len);

    chimera_fuse_readlink_complete(CHIMERA_VFS_OK, (int) len, NULL, req);
} /* chimera_fuse_readlink_sequence_complete */

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

    req->compound = chimera_vfs_compound_alloc(req->thread->vfs_thread,
                                               &req->cred);

    chimera_vfs_compound_add_putfh(req->compound, req->fh, (int) req->fh_len);
    chimera_vfs_compound_add_readlink(req->compound);

    chimera_vfs_compound_submit(req->compound,
                                chimera_fuse_readlink_sequence_complete, req);
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

/*
 * The decision is the sequence's, not ours.
 *
 * An ACCESS op answers while the object's ACL is still the backend's to read;
 * a sequence's ATTRIBUTES deliberately carry no ACL, because a backend owns the
 * one it reports only for the duration of its own completion.  Asking for the
 * attributes and judging them here would therefore answer from mode bits alone
 * on any object that has an ACL.
 */
static uint32_t
chimera_fuse_access_requested(const struct fuse_access_in *in)
{
    uint32_t requested = 0;

    if (in->mask & R_OK) {
        requested |= CHIMERA_ACE_READ_DATA;
    }
    if (in->mask & W_OK) {
        requested |= CHIMERA_ACE_WRITE_DATA;
    }
    if (in->mask & X_OK) {
        requested |= CHIMERA_ACE_EXECUTE;
    }

    return requested;
} /* chimera_fuse_access_requested */

static void
chimera_fuse_access_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_fuse_request          *req = private_data;
    const struct fuse_in_header          *hdr = chimera_fuse_request_hdr(req);
    const struct fuse_access_in          *in  =
        (const struct fuse_access_in *) (hdr + 1);
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                status;
    uint32_t                              requested;

    status = chimera_vfs_compound_status(compound);

    if (status != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(status), NULL, 0);
        return;
    }

    op = chimera_vfs_compound_op(compound,
                                 chimera_vfs_compound_num_ops(compound) - 1);
    requested = chimera_fuse_access_requested(in);

    if (requested && (op->granted & requested) != requested) {
        chimera_fuse_reply(req, EACCES, NULL, 0);
        return;
    }

    chimera_fuse_reply(req, 0, NULL, 0);
} /* chimera_fuse_access_sequence_complete */

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

    req->compound = chimera_vfs_compound_alloc(req->thread->vfs_thread,
                                               &req->cred);

    chimera_vfs_compound_add_putfh(req->compound, req->fh, (int) req->fh_len);
    chimera_vfs_compound_add_access(req->compound,
                                    chimera_fuse_access_requested(arg));

    chimera_vfs_compound_submit(req->compound,
                                chimera_fuse_access_sequence_complete, req);
} /* chimera_fuse_op_access */
