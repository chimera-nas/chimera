// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <sys/stat.h>
#ifdef _WIN32
#include "common/platform.h"
#endif /* ifdef _WIN32 */

#include "fuse_internal.h"
#include "fuse_attr.h"
#include "vfs/vfs_procs.h"


/*
 * Every creating operation here ends the same way: reply with the new object's
 * attributes, which the sequence's last op carries.  The kernel is told about
 * the object, never about the directory, so the directory attributes the
 * per-op API returned had no reader.
 */
static void
chimera_fuse_entry_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_fuse_request          *req = private_data;
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                status;

    status = chimera_vfs_compound_status(compound);

    if (status != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(status), NULL, 0);
        return;
    }

    op = chimera_vfs_compound_op(compound,
                                 chimera_vfs_compound_num_ops(compound) - 1);

    chimera_fuse_reply_entry(req, (struct chimera_vfs_attrs *) &op->attr,
                             NULL, 0);
} /* chimera_fuse_entry_sequence_complete */

/*
 * The shape every creating operation in this file shares: start at the parent
 * the kernel named, create in it, reply with what was created.  The parent is
 * no longer opened by the request -- the sequence opens it, once, as part of
 * running the create.
 */
static void
chimera_fuse_create_submit(
    struct chimera_fuse_request    *req,
    uint8_t                         create_type,
    const char                     *name,
    const char                     *target,
    const struct chimera_vfs_attrs *set_attr)
{
    req->compound = chimera_vfs_compound_alloc(req->thread->vfs_thread,
                                               &req->cred);

    chimera_vfs_compound_add_putfh(req->compound, req->fh, (int) req->fh_len);
    /* The open the sequence used to do for this op, said out loud. */
    chimera_vfs_compound_add_open_current(req->compound,
                                          CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH |
                                          CHIMERA_VFS_OPEN_DIRECTORY,
                                          0);

    chimera_vfs_compound_add_create(req->compound, create_type,
                                    name, (int) strlen(name),
                                    target, target ? (int) strlen(target) : 0,
                                    set_attr, CHIMERA_FUSE_ATTR_MASK, 0, 0);

    chimera_vfs_compound_submit(req->compound,
                                chimera_fuse_entry_sequence_complete, req);
} /* chimera_fuse_create_submit */

/* --- MKDIR / MKNOD --- */

void
chimera_fuse_op_mkdir(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const struct fuse_mkdir_in *in   = arg;
    const char                 *name = (const char *) (in + 1);

    if (arglen < sizeof(*in)) {
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

    memset(&req->u.create.set_attr, 0, sizeof(req->u.create.set_attr));
    req->u.create.set_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    req->u.create.set_attr.va_mode     = (in->mode & 07777) & ~in->umask;

    chimera_fuse_create_submit(req, CHIMERA_VFS_COMPOUND_CREATE_DIR,
                               name, NULL, &req->u.create.set_attr);
} /* chimera_fuse_op_mkdir */

void
chimera_fuse_op_mknod(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const struct fuse_mknod_in *in   = arg;
    const char                 *name = (const char *) (in + 1);

    if (arglen < sizeof(*in)) {
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

    memset(&req->u.create.set_attr, 0, sizeof(req->u.create.set_attr));
    req->u.create.set_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE |
        CHIMERA_VFS_ATTR_RDEV;
    req->u.create.set_attr.va_mode = (in->mode & S_IFMT) |
        ((in->mode & 07777) & ~in->umask);
    req->u.create.set_attr.va_rdev = in->rdev;

    chimera_fuse_create_submit(req, CHIMERA_VFS_COMPOUND_CREATE_NODE,
                               name, NULL, &req->u.create.set_attr);
} /* chimera_fuse_op_mknod */

/* --- SYMLINK --- */

void
chimera_fuse_op_symlink(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const char *name   = (const char *) (hdr + 1);
    const char *target = name + strlen(name) + 1;

    if (chimera_fuse_resolve_nodeid(req) != 0) {
        chimera_fuse_reply(req, ESTALE, NULL, 0);
        return;
    }

    /* Watch the parent before the backend op: the new entry's dentry may
     * only carry a TTL when the watch predates the request (reply_entry). */
    req->entry_cover = chimera_fuse_watch_dir(req->thread, req->channel->mount,
                                              req->nodeid,
                                              req->fh, req->fh_len);

    memset(&req->u.create.set_attr, 0, sizeof(req->u.create.set_attr));
    req->u.create.set_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    req->u.create.set_attr.va_mode     = 0777;

    chimera_fuse_create_submit(req, CHIMERA_VFS_COMPOUND_CREATE_SYMLINK,
                               name, target, &req->u.create.set_attr);
} /* chimera_fuse_op_symlink */

/* --- LINK --- */

static void
chimera_fuse_link_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_fuse_request          *req = private_data;
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                status;

    status = chimera_vfs_compound_status(compound);

    if (status != CHIMERA_VFS_OK) {
        int err = chimera_fuse_errno(status);

        /* link(2) reports a directory source as EPERM.  The VFS deliberately
         * surfaces the physical condition as EISDIR instead, for NFS4
         * (NFS4ERR_ISDIR) and SMB (STATUS_FILE_IS_A_DIRECTORY) fidelity, and
         * leaves the POSIX spelling to the caller -- which is what
         * chimera_posix_link does.  FUSE speaks POSIX to the kernel, so it
         * owes the same mapping. */
        if (err == EISDIR) {
            err = EPERM;
        }

        chimera_fuse_reply(req, err, NULL, 0);
        return;
    }

    op = chimera_vfs_compound_op(compound,
                                 chimera_vfs_compound_num_ops(compound) - 1);

    chimera_fuse_reply_entry(req, (struct chimera_vfs_attrs *) &op->attr,
                             NULL, 0);
} /* chimera_fuse_link_sequence_complete */

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

    req->compound = chimera_vfs_compound_alloc(req->thread->vfs_thread,
                                               &req->cred);

    /* LINK takes its source from the saved slot and its target directory from
     * the current object, so the file is put first and saved, and the parent
     * selected after it. */
    chimera_vfs_compound_add_putfh(req->compound, req->fh2, (int) req->fh2_len);
    chimera_vfs_compound_add_savefh(req->compound);
    chimera_vfs_compound_add_putfh(req->compound, req->fh, (int) req->fh_len);
    chimera_vfs_compound_add_link(req->compound, name, (int) strlen(name),
                                  CHIMERA_FUSE_ATTR_MASK, 0, 0);

    chimera_vfs_compound_submit(req->compound,
                                chimera_fuse_link_sequence_complete, req);
} /* chimera_fuse_op_link */

/* --- UNLINK / RMDIR --- */

static void
chimera_fuse_status_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_fuse_request *req = private_data;

    chimera_fuse_reply(req,
                       chimera_fuse_errno(chimera_vfs_compound_status(compound)),
                       NULL, 0);
} /* chimera_fuse_status_sequence_complete */

static void
chimera_fuse_remove_common(struct chimera_fuse_request *req)
{
    const struct fuse_in_header *hdr  = chimera_fuse_request_hdr(req);
    const char                  *name = (const char *) (hdr + 1);
    unsigned int                 flags;

    if (chimera_fuse_resolve_nodeid(req) != 0) {
        chimera_fuse_reply(req, ESTALE, NULL, 0);
        return;
    }

    /* rmdir(2) and unlink(2) each assert what the target must be, and the VFS
     * is what enforces it -- FUSE never resolves the target itself. */
    flags = (req->opcode == FUSE_RMDIR) ?
        CHIMERA_VFS_REMOVE_ISDIR : CHIMERA_VFS_REMOVE_ISNOTDIR;

    req->compound = chimera_vfs_compound_alloc(req->thread->vfs_thread,
                                               &req->cred);

    chimera_vfs_compound_add_putfh(req->compound, req->fh, (int) req->fh_len);
    /* The open the sequence used to do for this op, said out loud. */
    chimera_vfs_compound_add_open_current(req->compound,
                                          CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH |
                                          CHIMERA_VFS_OPEN_DIRECTORY,
                                          0);

    /* The reply is a bare status; the directory's change has no reader. */
    chimera_vfs_compound_add_remove(req->compound, name, (int) strlen(name),
                                    flags, 0, 0);

    chimera_vfs_compound_submit(req->compound,
                                chimera_fuse_status_sequence_complete, req);
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

void
chimera_fuse_op_rename(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    uint64_t    newdir;
    const char *oldname;
    const char *newname;

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

    newname = oldname + strlen(oldname) + 1;

    if (chimera_fuse_resolve_nodeid(req) != 0 ||
        chimera_fuse_resolve_nodeid2(req, newdir) != 0) {
        chimera_fuse_reply(req, ESTALE, NULL, 0);
        return;
    }

    req->compound = chimera_vfs_compound_alloc(req->thread->vfs_thread,
                                               &req->cred);

    /* RENAME renames within the saved object into the current one, so the
     * source directory is put first and saved and the destination selected
     * after it. */
    chimera_vfs_compound_add_putfh(req->compound, req->fh, (int) req->fh_len);
    chimera_vfs_compound_add_savefh(req->compound);
    chimera_vfs_compound_add_putfh(req->compound, req->fh2, (int) req->fh2_len);

    /*
     * CHIMERA_VFS_REMOVE_RECALL: a rename that replaces an existing
     * destination unlinks that inode, which changes its link count while its
     * file handle stays valid -- an fd held across the rename keeps naming
     * it.  The VFS only recalls delegations on the doomed target and
     * invalidates its cached attributes when it knows which inode is being
     * clobbered, and a by-name caller either resolves that itself or asks the
     * VFS to (NFSv3 RENAME takes the same route).  Without it a later
     * GETATTR through the surviving open handle is answered from the attr
     * cache and still reports the pre-rename nlink.
     */
    chimera_vfs_compound_add_rename(req->compound,
                                    oldname, (int) strlen(oldname),
                                    newname, (int) strlen(newname),
                                    CHIMERA_VFS_REMOVE_RECALL, 0, 0);

    chimera_vfs_compound_submit(req->compound,
                                chimera_fuse_status_sequence_complete, req);
} /* chimera_fuse_op_rename */
