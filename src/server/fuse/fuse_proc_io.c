// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#define _GNU_SOURCE 1

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <linux/falloc.h>

#include "fuse_internal.h"
#include "fuse_attr.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

static unsigned int
chimera_fuse_open_flags(uint32_t flags)
{
    switch (flags & O_ACCMODE) {
        case O_RDONLY:
            return CHIMERA_VFS_OPEN_READ_ONLY;
        case O_WRONLY:
            return CHIMERA_VFS_OPEN_WRITE_ONLY;
        default:
            return 0;
    } /* switch */
} /* chimera_fuse_open_flags */

/* --- OPEN --- */

static void
chimera_fuse_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_fuse_request   *req    = private_data;
    struct chimera_fuse_thread    *thread = req->thread;
    struct chimera_fuse_mount     *mount  = req->channel->mount;
    struct chimera_fuse_open_file *file;
    struct fuse_open_out           out;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    file = calloc(1, sizeof(*file));

    file->handle = oh;
    file->mount  = mount;

    chimera_fuse_file_link(mount, file);
    chimera_fuse_grant_open(thread, mount, req->nodeid, oh);

    memset(&out, 0, sizeof(out));
    out.fh = (uint64_t) (uintptr_t) file;

    if (chimera_fuse_reply(req, 0, &out, sizeof(out)) != 0) {
        /* The kernel never learned this fh, so no RELEASE will come. */
        chimera_fuse_file_unlink(mount, file);
        chimera_vfs_release(thread->vfs_thread, file->handle);
        free(file);
    }
} /* chimera_fuse_open_callback */

void
chimera_fuse_op_open(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const struct fuse_open_in *in = arg;

    if (arglen < sizeof(*in)) {
        chimera_fuse_reply(req, EINVAL, NULL, 0);
        return;
    }

    if (chimera_fuse_resolve_nodeid(req) != 0) {
        chimera_fuse_reply(req, ESTALE, NULL, 0);
        return;
    }

    /* O_TRUNC arrives as a separate SETATTR(size=0) because we do not
     * advertise FUSE_ATOMIC_O_TRUNC. */
    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred,
                        req->fh, req->fh_len,
                        chimera_fuse_open_flags(in->flags),
                        chimera_fuse_open_callback, req);
} /* chimera_fuse_op_open */

/* --- CREATE --- */

static void
chimera_fuse_create_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data)
{
    struct chimera_fuse_request   *req    = private_data;
    struct chimera_fuse_thread    *thread = req->thread;
    struct chimera_fuse_mount     *mount  = req->channel->mount;
    struct chimera_fuse_open_file *file;
    struct fuse_open_out           out;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    file = calloc(1, sizeof(*file));

    file->handle = oh;
    file->mount  = mount;

    chimera_fuse_file_link(mount, file);

    memset(&out, 0, sizeof(out));
    out.fh = (uint64_t) (uintptr_t) file;

    /* No invalidation grant here: the child's nodeid is assigned inside
     * reply_entry, and the creator's own writes are self-coherent anyway.
     * Any other mount's open of the file builds its own grant. */
    if (chimera_fuse_reply_entry(req, attr, &out, sizeof(out)) != 0) {
        chimera_fuse_file_unlink(mount, file);
        chimera_vfs_release(thread->vfs_thread, file->handle);
        free(file);
    }
} /* chimera_fuse_create_callback */

static void
chimera_fuse_create_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_fuse_request *req  = private_data;
    const struct fuse_in_header *hdr  = evpl_iovec_data(&req->buf);
    const struct fuse_create_in *in   = (const struct fuse_create_in *) (hdr + 1);
    const char                  *name = (const char *) (in + 1);
    unsigned int                 flags;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    req->handle = oh;

    flags = CHIMERA_VFS_OPEN_CREATE | chimera_fuse_open_flags(in->flags);

    if (in->flags & O_EXCL) {
        flags |= CHIMERA_VFS_OPEN_EXCLUSIVE;
    }

    if (in->flags & O_TRUNC) {
        flags |= CHIMERA_VFS_OPEN_TRUNCATE;
    }

    memset(&req->u.create.set_attr, 0, sizeof(req->u.create.set_attr));
    req->u.create.set_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    req->u.create.set_attr.va_mode     = (in->mode & 07777) & ~in->umask;

    chimera_vfs_open_at(req->thread->vfs_thread, &req->cred, oh,
                        name, strlen(name),
                        flags,
                        &req->u.create.set_attr,
                        CHIMERA_FUSE_ATTR_MASK,
                        0, 0,
                        chimera_fuse_create_callback, req);
} /* chimera_fuse_create_open_callback */

void
chimera_fuse_op_create(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    if (arglen < sizeof(struct fuse_create_in)) {
        chimera_fuse_reply(req, EINVAL, NULL, 0);
        return;
    }

    if (chimera_fuse_resolve_nodeid(req) != 0) {
        chimera_fuse_reply(req, ESTALE, NULL, 0);
        return;
    }

    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred,
                        req->fh, req->fh_len,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH |
                        CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_fuse_create_open_callback, req);
} /* chimera_fuse_op_create */

/* --- READ --- */

static void
chimera_fuse_read_complete(
    enum chimera_vfs_error    error_code,
    uint32_t                  count,
    uint32_t                  eof,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_fuse_request *req = private_data;

    chimera_fuse_reply_read(req, chimera_fuse_errno(error_code),
                            iov, niov, count);
} /* chimera_fuse_read_complete */

void
chimera_fuse_op_read(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const struct fuse_read_in     *in = arg;
    struct chimera_fuse_open_file *file;
    struct chimera_vfs_lease_owner owner;

    if (arglen < sizeof(*in)) {
        chimera_fuse_reply(req, EINVAL, NULL, 0);
        return;
    }

    file = chimera_fuse_file(in->fh);

    /* Attributed to the mount's own lease identity so a read never breaks
     * this mount's invalidation grant (copied by value downstream). */
    chimera_fuse_grant_owner(&owner, req->channel->mount,
                             file->handle->fh_hash);

    chimera_vfs_read_owned(req->thread->vfs_thread, &req->cred,
                           file->handle,
                           in->offset, in->size,
                           req->u.read.iov, CHIMERA_FUSE_IOV_MAX,
                           0,
                           &owner,
                           chimera_fuse_read_complete, req);
} /* chimera_fuse_op_read */

/* --- WRITE --- */

static void
chimera_fuse_write_complete(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_fuse_request *req = private_data;
    struct fuse_write_out        out;

    evpl_iovec_release(req->thread->evpl, &req->u.write.iov);

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    memset(&out, 0, sizeof(out));
    out.size = length;

    chimera_fuse_reply(req, 0, &out, sizeof(out));
} /* chimera_fuse_write_complete */

void
chimera_fuse_op_write(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const struct fuse_write_in *in   = arg;
    uint32_t                    sync = 0;
    size_t                      data_off;

    if (arglen < sizeof(*in) || arglen - sizeof(*in) < in->size) {
        chimera_fuse_reply(req, EINVAL, NULL, 0);
        return;
    }

    if ((in->flags & O_SYNC) == O_SYNC) {
        sync = 2;
    } else if (in->flags & O_DSYNC) {
        sync = 1;
    }

    data_off = sizeof(struct fuse_in_header) + sizeof(*in);

    /* Borrow the payload straight out of the request buffer; the buffer is
     * not recycled until the request completes. */
    evpl_iovec_clone_segment(&req->u.write.iov, &req->buf, data_off, in->size);

    struct chimera_fuse_open_file *file = chimera_fuse_file(in->fh);
    struct chimera_vfs_lease_owner owner;

    /* Attributed to the mount's own lease identity: the kernel wrote
     * through us, so its cache is current and must not be invalidated;
     * every OTHER holder's read cache still breaks. */
    chimera_fuse_grant_owner(&owner, req->channel->mount,
                             file->handle->fh_hash);

    chimera_vfs_write_owned(req->thread->vfs_thread, &req->cred,
                            file->handle,
                            in->offset, in->size, sync,
                            0, 0,
                            &req->u.write.iov, 1,
                            &owner,
                            chimera_fuse_write_complete, req);
} /* chimera_fuse_op_write */

/* --- FLUSH / FSYNC --- */

static void
chimera_fuse_commit_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_fuse_request *req = private_data;

    chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
} /* chimera_fuse_commit_complete */

void
chimera_fuse_op_flush(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const struct fuse_flush_in    *in = arg;
    struct chimera_fuse_open_file *file;

    if (arglen < sizeof(*in)) {
        chimera_fuse_reply(req, EINVAL, NULL, 0);
        return;
    }

    file = chimera_fuse_file(in->fh);

    /* POSIX: any close by a process drops that process's locks on the
     * file; the kernel identifies the process via lock_owner. */
    chimera_fuse_locks_release_owner(req->thread, req->channel->mount,
                                     file->handle->fh_hash, in->lock_owner);

    /* close(2) must surface write errors, so flush commits. */
    chimera_vfs_commit(req->thread->vfs_thread, &req->cred,
                       file->handle,
                       0, 0, 0, 0,
                       chimera_fuse_commit_complete, req);
} /* chimera_fuse_op_flush */

void
chimera_fuse_op_fsync(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const struct fuse_fsync_in *in = arg;

    if (arglen < sizeof(*in)) {
        chimera_fuse_reply(req, EINVAL, NULL, 0);
        return;
    }

    chimera_vfs_commit(req->thread->vfs_thread, &req->cred,
                       chimera_fuse_file(in->fh)->handle,
                       0, 0, 0, 0,
                       chimera_fuse_commit_complete, req);
} /* chimera_fuse_op_fsync */

/* --- RELEASE --- */

void
chimera_fuse_op_release(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const struct fuse_release_in  *in = arg;
    struct chimera_fuse_open_file *file;

    if (arglen < sizeof(*in)) {
        chimera_fuse_reply(req, EINVAL, NULL, 0);
        return;
    }

    file = chimera_fuse_file(in->fh);

    chimera_fuse_file_unlink(file->mount, file);

    chimera_vfs_release(req->thread->vfs_thread, file->handle);

    free(file);

    chimera_fuse_reply(req, 0, NULL, 0);
} /* chimera_fuse_op_release */

/* --- FALLOCATE --- */

static void
chimera_fuse_fallocate_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_fuse_request *req = private_data;

    chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
} /* chimera_fuse_fallocate_complete */

void
chimera_fuse_op_fallocate(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const struct fuse_fallocate_in *in = arg;
    uint32_t                        flags;

    if (arglen < sizeof(*in)) {
        chimera_fuse_reply(req, EINVAL, NULL, 0);
        return;
    }

    if (in->mode == 0) {
        flags = 0;
    } else if (in->mode == (FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE)) {
        flags = CHIMERA_VFS_ALLOCATE_DEALLOCATE;
    } else {
        chimera_fuse_reply(req, EOPNOTSUPP, NULL, 0);
        return;
    }

    chimera_vfs_allocate(req->thread->vfs_thread, &req->cred,
                         chimera_fuse_file(in->fh)->handle,
                         in->offset, in->length, flags,
                         0, 0,
                         chimera_fuse_fallocate_complete, req);
} /* chimera_fuse_op_fallocate */

/* --- LSEEK (SEEK_DATA / SEEK_HOLE) --- */

static void
chimera_fuse_lseek_complete(
    enum chimera_vfs_error error_code,
    int                    sr_eof,
    uint64_t               sr_offset,
    void                  *private_data)
{
    struct chimera_fuse_request *req = private_data;
    const struct fuse_in_header *hdr = evpl_iovec_data(&req->buf);
    const struct fuse_lseek_in  *in  = (const struct fuse_lseek_in *) (hdr + 1);
    struct fuse_lseek_out        out;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    if (sr_eof && in->whence == SEEK_DATA) {
        chimera_fuse_reply(req, ENXIO, NULL, 0);
        return;
    }

    memset(&out, 0, sizeof(out));
    out.offset = sr_offset;

    chimera_fuse_reply(req, 0, &out, sizeof(out));
} /* chimera_fuse_lseek_complete */

void
chimera_fuse_op_lseek(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const struct fuse_lseek_in *in = arg;
    uint32_t                    what;

    if (arglen < sizeof(*in)) {
        chimera_fuse_reply(req, EINVAL, NULL, 0);
        return;
    }

    switch (in->whence) {
        case SEEK_DATA:
            what = 0;
            break;
        case SEEK_HOLE:
            what = 1;
            break;
        default:
            chimera_fuse_reply(req, EINVAL, NULL, 0);
            return;
    } /* switch */

    chimera_vfs_seek(req->thread->vfs_thread, &req->cred,
                     chimera_fuse_file(in->fh)->handle,
                     in->offset, what,
                     chimera_fuse_lseek_complete, req);
} /* chimera_fuse_op_lseek */
