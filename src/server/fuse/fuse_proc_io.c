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
    /* The VFS signals access intent positively and treats the two bits as
     * independent, so O_RDWR is BOTH -- not neither.  Returning 0 here left
     * an O_RDWR open requesting no access at all, which meant
     * chimera_vfs_open_required_access() found nothing to require: the open
     * was never gated and no grant was ever recorded for the commonest
     * read-write descriptor there is. */
    switch (flags & O_ACCMODE) {
        case O_RDONLY:
            return CHIMERA_VFS_OPEN_READ_ONLY;
        case O_WRONLY:
            return CHIMERA_VFS_OPEN_WRITE_ONLY;
        case O_RDWR:
            return CHIMERA_VFS_OPEN_READ_ONLY | CHIMERA_VFS_OPEN_WRITE_ONLY;
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
    struct chimera_fuse_request   *req   = private_data;
    struct chimera_fuse_mount     *mount = req->channel->mount;
    struct chimera_fuse_open_file *file;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    /* Bind what the open-time gate authorized onto the handle.  Without this
     * the first write or ftruncate re-derives the grant from the file's
     * current mode (chimera_vfs_write_gate_complete), which is how a chmod
     * after open used to revoke an already-open descriptor's write right.
     * The open cache is credential-keyed when gating is in force, so the
     * grant recorded here belongs to this caller alone. */
    if (req->u.open.granted) {
        oh->granted_access |= req->u.open.granted;
        oh->granted_valid   = 1;
        oh->granted_bound   = 1;
    }

    file = calloc(1, sizeof(*file));

    file->handle = oh;
    file->mount  = mount;

    chimera_fuse_file_link(mount, file);

    req->file       = file;
    req->file_owned = 1;

    /* The fuse_open_out -- including the invalidation-grant arm that decides
     * its cache flags -- is built by the deliver path once the compound end
     * settles; if the kernel never learns this fh (no RELEASE will come) --
     * or a conflict replays the request -- the open_file is undone there. */
    chimera_fuse_reply_open(req);
} /* chimera_fuse_open_callback */

static void
chimera_fuse_open_gated(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_fuse_request *req = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    /* O_TRUNC arrives as a separate SETATTR(size=0) because we do not
     * advertise FUSE_ATOMIC_O_TRUNC. */
    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred,
                        chimera_fuse_req_compound(req),
                        req->fh, req->fh_len,
                        req->u.open.vfs_flags,
                        chimera_fuse_open_callback, req);
} /* chimera_fuse_open_gated */

void
chimera_fuse_op_open(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const struct fuse_open_in *in = arg;
    uint32_t                   required;

    if (arglen < sizeof(*in)) {
        chimera_fuse_reply(req, EINVAL, NULL, 0);
        return;
    }

    if (chimera_fuse_resolve_nodeid(req) != 0) {
        chimera_fuse_reply(req, ESTALE, NULL, 0);
        return;
    }

    req->u.open.vfs_flags = chimera_fuse_open_flags(in->flags);

    /*
     * Authorize the access mode at OPEN, and remember what was granted.
     *
     * POSIX binds I/O rights when a file is opened: a descriptor opened for
     * writing stays writable across a later chmod.  Opening by file handle
     * skips the DAC gate entirely, so nothing was checked here and nothing
     * was recorded -- and the first write or ftruncate then fell into the
     * VFS's lazy grant derivation, which re-derives from the file's CURRENT
     * mode.  A chmod between open and first write therefore revoked a right
     * POSIX says is already bound, and an unreadable file opened fine on a
     * no_default_permissions mount.  Gate once here and stamp the outcome so
     * neither happens.
     */
    required = 0;
    if (req->u.open.vfs_flags & CHIMERA_VFS_OPEN_READ_ONLY) {
        required |= CHIMERA_ACE_READ_DATA;
    }
    if (req->u.open.vfs_flags & CHIMERA_VFS_OPEN_WRITE_ONLY) {
        required |= CHIMERA_ACE_WRITE_DATA;
    }

    req->u.open.granted = required;

    if (required == 0) {
        chimera_fuse_open_gated(CHIMERA_VFS_OK, req);
        return;
    }

    chimera_vfs_gate_fh_obj(&req->u.open.gate, req->thread->vfs_thread,
                            &req->cred, req->fh, req->fh_len, required,
                            chimera_fuse_open_gated, req);
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
    struct chimera_fuse_request   *req   = private_data;
    struct chimera_fuse_mount     *mount = req->channel->mount;
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

    req->file       = file;
    req->file_owned = 1;

    memset(&out, 0, sizeof(out));
    out.fh = (uint64_t) (uintptr_t) file;

    /* No invalidation grant here: the child's nodeid is assigned inside the
     * entry deliver, and the creator's own writes are self-coherent anyway.
     * Any other mount's open of the file builds its own grant.  If the
     * kernel never sees the entry -- or a conflict replays the request --
     * the deliver/replay path undoes the open_file. */
    chimera_fuse_reply_entry(req, attr, &out, sizeof(out));
} /* chimera_fuse_create_callback */

static void
chimera_fuse_create_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_fuse_request *req  = private_data;
    const struct fuse_in_header *hdr  = chimera_fuse_request_hdr(req);
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

    chimera_vfs_open_at(req->thread->vfs_thread, &req->cred,
                        chimera_fuse_req_compound(req), oh,
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

    /* Watch the parent before the backend op: the new entry's dentry may
     * only carry a TTL when the watch predates the request (reply_entry). */
    req->entry_cover = chimera_fuse_watch_dir(req->thread, req->channel->mount,
                                              req->nodeid,
                                              req->fh, req->fh_len);

    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred,
                        chimera_fuse_req_compound(req),
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
    struct chimera_claim_actor     actor;

    if (arglen < sizeof(*in)) {
        chimera_fuse_reply(req, EINVAL, NULL, 0);
        return;
    }

    file = chimera_fuse_file(in->fh);

    /* coherence=sync: pages this read seeds into the kernel's cache must be
     * covered, so re-arm the grant if a break dropped it.  A break during
     * the read is still safe: the invalidation write serializes behind the
     * in-flight read on the kernel's page locks. */
    if (req->channel->mount->coherence_sync) {
        chimera_fuse_grant_ensure(req->thread, req->channel->mount,
                                  req->nodeid,
                                  file->handle->fh, file->handle->fh_len,
                                  file->handle->fh_hash);
    }

    /* Attributed to the mount's own claim identity so a read never breaks
     * this mount's invalidation grant (copied by value downstream).  The
     * actor's op_handle stays NULL: a FUSE grant self-exempts at the CLIENT
     * circle, which the shared mount client_key already provides. */
    memset(&actor, 0, sizeof(actor));
    chimera_fuse_grant_owner(&actor.owner, req->channel->mount,
                             file->handle->fh_hash);

    chimera_vfs_read_owned(req->thread->vfs_thread, &req->cred,
                           chimera_fuse_req_compound(req),
                           file->handle,
                           in->offset, in->size,
                           req->u.read.iov, CHIMERA_FUSE_IOV_MAX,
                           0,
                           &actor,
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

    /* Page-aligned by construction (see CHIMERA_FUSE_REQ_OFF): the borrowed
     * segment below is what a backend DMAs from, and diskfs's zero-copy
     * device write rejects an unaligned source. */
    data_off = CHIMERA_FUSE_REQ_OFF + sizeof(struct fuse_in_header) +
        sizeof(*in);

    /* Borrow the payload straight out of the request buffer; the buffer is
     * not recycled until the request completes. */
    evpl_iovec_clone_segment(&req->u.write.iov, &req->buf, data_off, in->size);

    struct chimera_fuse_open_file *file = chimera_fuse_file(in->fh);
    struct chimera_claim_actor     actor;

    /* coherence=sync: the kernel retains the written pages in its cache, so
     * they need grant coverage exactly like read-seeded pages. */
    if (req->channel->mount->coherence_sync) {
        chimera_fuse_grant_ensure(req->thread, req->channel->mount,
                                  req->nodeid,
                                  file->handle->fh, file->handle->fh_len,
                                  file->handle->fh_hash);
    }

    /* Attributed to the mount's own claim identity: the kernel wrote
     * through us, so its cache is current and must not be invalidated;
     * every OTHER holder's read cache still breaks -- and under sync
     * coherence the write parks until those breaks ack. */
    memset(&actor, 0, sizeof(actor));
    chimera_fuse_grant_owner(&actor.owner, req->channel->mount,
                             file->handle->fh_hash);

    chimera_vfs_write_owned(req->thread->vfs_thread, &req->cred,
                            chimera_fuse_req_compound(req),
                            file->handle,
                            in->offset, in->size, sync,
                            0, 0,
                            &req->u.write.iov, 1,
                            &actor,
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
                       chimera_fuse_req_compound(req),
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
                       chimera_fuse_req_compound(req),
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
                         chimera_fuse_req_compound(req),
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
    const struct fuse_in_header *hdr = chimera_fuse_request_hdr(req);
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
                     chimera_fuse_req_compound(req),
                     chimera_fuse_file(in->fh)->handle,
                     in->offset, what,
                     chimera_fuse_lseek_complete, req);
} /* chimera_fuse_op_lseek */

/* --- COPY_FILE_RANGE --- */

static void
chimera_fuse_copy_range_complete(
    enum chimera_vfs_error    error_code,
    uint64_t                  length,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_fuse_request *req = private_data;
    struct fuse_write_out        out;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    memset(&out, 0, sizeof(out));
    out.size = (uint32_t) length;

    chimera_fuse_reply(req, 0, &out, sizeof(out));
} /* chimera_fuse_copy_range_complete */

/*
 * Server-side copy between two already-open descriptors.  Answering ENOSYS
 * is safe -- the kernel falls back to read+write and the copy still happens
 * -- but it moves every byte through the kernel and back, which is exactly
 * what the operation exists to avoid on a backend that can copy internally.
 * A backend without the capability still reports ENOTSUP from the VFS, so
 * the fallback remains available where it is genuinely needed.
 */
void
chimera_fuse_op_copy_file_range(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const struct fuse_copy_file_range_in *in = arg;
    struct chimera_fuse_open_file        *src, *dst;

    if (arglen < sizeof(*in)) {
        chimera_fuse_reply(req, EINVAL, NULL, 0);
        return;
    }

    src = chimera_fuse_file(in->fh_in);
    dst = chimera_fuse_file(in->fh_out);

    if (!src || !dst) {
        chimera_fuse_reply(req, EBADF, NULL, 0);
        return;
    }

    /* The destination's pages change underneath any kernel that has them
     * cached, including this one; the write triggers the usual claim break,
     * and this mount is exempt from its own invalidation through the
     * credential's origin stamp. */
    chimera_vfs_copy_range(req->thread->vfs_thread, &req->cred,
                           chimera_fuse_req_compound(req),
                           src->handle, in->off_in,
                           dst->handle, in->off_out,
                           in->len, 0,
                           0, 0,
                           chimera_fuse_copy_range_complete, req);
} /* chimera_fuse_op_copy_file_range */
