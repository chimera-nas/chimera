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
    struct chimera_fuse_request   *req    = private_data;
    struct chimera_fuse_thread    *thread = req->thread;
    struct chimera_fuse_mount     *mount  = req->channel->mount;
    struct chimera_fuse_open_file *file;
    struct fuse_open_out           out;

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

    memset(&out, 0, sizeof(out));
    out.fh = (uint64_t) (uintptr_t) file;

    /* With an invalidation grant in force from here on, the kernel's cached
     * pages are guaranteed to be dropped when any other party changes the
     * file, so letting them survive across open/close cycles is coherent --
     * and a real read-cache win.  (Pages, unlike attributes, are only
     * seeded through us AFTER the arm, so a fresh grant fully covers
     * them.)  No grant (contention) means no coverage: ttl mode keeps the
     * kernel's default invalidate-on-open behavior, sync mode goes further
     * and bypasses the page cache entirely so an uncovered open can never
     * serve stale data. */
    out.open_flags |= chimera_fuse_open_cache_flags(
        mount, chimera_fuse_grant_open(thread, mount, req->nodeid, oh));

    if (chimera_fuse_reply(req, 0, &out, sizeof(out)) != 0) {
        /* The kernel never learned this fh, so no RELEASE will come. */
        chimera_fuse_file_unlink(mount, file);
        chimera_vfs_release(thread->vfs_thread, file->handle);
        free(file);
    }
} /* chimera_fuse_open_callback */

/*
 * POSIX binds I/O rights when a file is opened, and opening by file handle
 * checks nothing -- see the note in chimera_fuse_op_open.  The sequence asks
 * for the rights and then opens; this veto is what turns a refusal into one.
 * It reads only the ACCESS result already in the sequence.
 */
static void
chimera_fuse_open_gate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct chimera_fuse_request          *req = private_data;
    const struct chimera_vfs_compound_op *op;

    if (*status != CHIMERA_VFS_OK) {
        return;
    }

    op = chimera_vfs_compound_op(compound, index);

    if (op->type != CHIMERA_VFS_COMPOUND_OP_ACCESS) {
        return;
    }

    if ((op->granted & req->u.open.granted) != req->u.open.granted) {
        *status = CHIMERA_VFS_EACCES;
    }
} /* chimera_fuse_open_gate */

static void
chimera_fuse_open_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_fuse_request    *req = private_data;
    struct chimera_vfs_open_handle *oh;
    enum chimera_vfs_error          status;

    status = chimera_vfs_compound_status(compound);

    if (status != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(status), NULL, 0);
        return;
    }

    /* The handle is what the kernel's fh will name, so it outlives the
     * sequence and has to be taken from the compound. */
    oh = chimera_vfs_compound_take_handle(
        compound, chimera_vfs_compound_num_ops(compound) - 1);

    chimera_fuse_open_callback(CHIMERA_VFS_OK, oh, req);
} /* chimera_fuse_open_sequence_complete */

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

    req->compound = chimera_vfs_compound_alloc(req->thread->vfs_thread,
                                               &req->cred);

    chimera_vfs_compound_add_putfh(req->compound, req->fh, (int) req->fh_len);

    /* Nothing to authorize for an O_PATH-ish open, and asking anyway would
     * make the sequence refuse opens POSIX permits. */
    if (required) {
        chimera_vfs_compound_set_gate(req->compound, chimera_fuse_open_gate,
                                      req);

        /* A metadata open to ask the question; the data open the caller
         * actually wants follows, and cannot share it -- they are handles
         * from two different caches. */
        chimera_vfs_compound_add_open_current(req->compound,
                                              CHIMERA_VFS_OPEN_INFERRED |
                                              CHIMERA_VFS_OPEN_PATH,
                                              0);

        chimera_vfs_compound_add_access(req->compound, required);
    }

    /* O_TRUNC arrives as a separate SETATTR(size=0) because we do not
     * advertise FUSE_ATOMIC_O_TRUNC. */
    chimera_vfs_compound_add_open_current(req->compound,
                                          req->u.open.vfs_flags, 0);

    /* The handle is what the kernel's fh will name, so it outlives us. */
    chimera_vfs_compound_add_gethandle(req->compound);

    chimera_vfs_compound_submit(req->compound,
                                chimera_fuse_open_sequence_complete, req);
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
chimera_fuse_create_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_fuse_request          *req = private_data;
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_open_handle       *oh;
    enum chimera_vfs_error                status;
    uint32_t                              last;

    status = chimera_vfs_compound_status(compound);

    if (status != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(status), NULL, 0);
        return;
    }

    last = chimera_vfs_compound_num_ops(compound) - 1;
    op   = chimera_vfs_compound_op(compound, last);
    oh   = chimera_vfs_compound_take_handle(compound, last);

    chimera_fuse_create_callback(CHIMERA_VFS_OK, oh, NULL,
                                 (struct chimera_vfs_attrs *) &op->attr,
                                 NULL, NULL, req);
} /* chimera_fuse_create_sequence_complete */

void
chimera_fuse_op_create(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const struct fuse_create_in *in   = arg;
    const char                  *name = (const char *) (in + 1);
    unsigned int                 flags;

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

    req->compound = chimera_vfs_compound_alloc(req->thread->vfs_thread,
                                               &req->cred);

    chimera_vfs_compound_add_putfh(req->compound, req->fh, (int) req->fh_len);

    /* The parent, which a named OPEN resolves the new name in. */
    chimera_vfs_compound_add_open_current(req->compound,
                                          CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_PATH |
                                          CHIMERA_VFS_OPEN_DIRECTORY,
                                          0);

    chimera_vfs_compound_add_open(req->compound, name, (int) strlen(name),
                                  flags, 0, &req->u.create.set_attr,
                                  CHIMERA_FUSE_ATTR_MASK);

    chimera_vfs_compound_submit(req->compound,
                                chimera_fuse_create_sequence_complete, req);
} /* chimera_fuse_op_create */

/* --- READ --- */

static void
chimera_fuse_read_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_fuse_request          *req = private_data;
    const struct chimera_vfs_compound_op *op;
    struct evpl_iovec                    *iov;
    enum chimera_vfs_error                status;
    uint32_t                              count;
    int                                   niov;
    uint32_t                              last;

    status = chimera_vfs_compound_status(compound);

    if (status != CHIMERA_VFS_OK) {
        chimera_fuse_reply_read(req, chimera_fuse_errno(status), NULL, 0, 0);
        return;
    }

    last  = chimera_vfs_compound_num_ops(compound) - 1;
    op    = chimera_vfs_compound_op(compound, last);
    count = op->read_len;

    /* chimera_fuse_reply_read releases the buffers, so they have to leave the
     * compound's ownership first -- otherwise both would release them. */
    chimera_vfs_compound_take_iov(compound, last, &iov, &niov);

    chimera_fuse_reply_read(req, 0, iov, niov, count);
} /* chimera_fuse_read_sequence_complete */

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

    req->compound = chimera_vfs_compound_alloc(req->thread->vfs_thread,
                                               &req->cred);

    /* The kernel named an open file, so the sequence addresses that handle
     * and has no current object.  The descriptor array is the request's and
     * stays the request's -- an evpl_iovec records its owner's address, so it
     * cannot live in the compound and be copied out. */
    chimera_vfs_compound_add_read(req->compound, file->handle,
                                  in->offset, in->size,
                                  req->u.read.iov, CHIMERA_FUSE_IOV_MAX,
                                  &actor);

    chimera_vfs_compound_submit(req->compound,
                                chimera_fuse_read_sequence_complete, req);
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

static void
chimera_fuse_write_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_fuse_request          *req = private_data;
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                status;

    status = chimera_vfs_compound_status(compound);

    if (status != CHIMERA_VFS_OK) {
        chimera_fuse_write_complete(status, 0, 0, NULL, NULL, req);
        return;
    }

    op = chimera_vfs_compound_op(compound,
                                 chimera_vfs_compound_num_ops(compound) - 1);

    chimera_fuse_write_complete(status, op->written, op->committed,
                                NULL, NULL, req);
} /* chimera_fuse_write_sequence_complete */

void
chimera_fuse_op_write(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const struct fuse_write_in    *in   = arg;
    uint32_t                       sync = 0;
    size_t                         data_off;
    struct chimera_fuse_open_file *file;
    struct chimera_claim_actor     actor;

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

    file = chimera_fuse_file(in->fh);

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

    req->compound = chimera_vfs_compound_alloc(req->thread->vfs_thread,
                                               &req->cred);

    /* Handle and payload are both borrowed for the length of the sequence;
     * the payload is released by chimera_fuse_write_complete. */
    chimera_vfs_compound_add_write(req->compound, file->handle,
                                   in->offset, in->size, sync,
                                   &req->u.write.iov, 1, &actor);

    chimera_vfs_compound_submit(req->compound,
                                chimera_fuse_write_sequence_complete, req);
} /* chimera_fuse_op_write */

/* --- FLUSH / FSYNC --- */

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

/* FLUSH and FSYNC are the same sequence against an already-open file. */
static void
chimera_fuse_commit_submit(
    struct chimera_fuse_request    *req,
    struct chimera_vfs_open_handle *oh)
{
    req->compound = chimera_vfs_compound_alloc(req->thread->vfs_thread,
                                               &req->cred);

    chimera_vfs_compound_add_puthandle(req->compound, oh,
                                       CHIMERA_VFS_OPEN_INFERRED);

    chimera_vfs_compound_add_commit(req->compound, 0, 0, 0);

    chimera_vfs_compound_submit(req->compound,
                                chimera_fuse_status_sequence_complete, req);
} /* chimera_fuse_commit_submit */

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
    chimera_fuse_commit_submit(req, file->handle);
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

    chimera_fuse_commit_submit(req, chimera_fuse_file(in->fh)->handle);
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

    req->compound = chimera_vfs_compound_alloc(req->thread->vfs_thread,
                                               &req->cred);

    /* The kernel named an open file; the sequence borrows it.  fallocate(2)
     * reports only success, so neither attribute set is asked for. */
    chimera_vfs_compound_add_puthandle(req->compound,
                                       chimera_fuse_file(in->fh)->handle,
                                       CHIMERA_VFS_OPEN_INFERRED);

    chimera_vfs_compound_add_allocate(req->compound, NULL,
                                      in->offset, in->length, flags,
                                      0, 0);

    chimera_vfs_compound_submit(req->compound,
                                chimera_fuse_status_sequence_complete, req);
} /* chimera_fuse_op_fallocate */

/* --- LSEEK (SEEK_DATA / SEEK_HOLE) --- */

static void
chimera_fuse_lseek_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_fuse_request          *req = private_data;
    const struct fuse_in_header          *hdr = chimera_fuse_request_hdr(req);
    const struct fuse_lseek_in           *in  = (const struct fuse_lseek_in *) (hdr + 1);
    const struct chimera_vfs_compound_op *op;
    struct fuse_lseek_out                 out;
    enum chimera_vfs_error                status;

    status = chimera_vfs_compound_status(compound);

    if (status != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(status), NULL, 0);
        return;
    }

    op = chimera_vfs_compound_op(compound,
                                 chimera_vfs_compound_num_ops(compound) - 1);

    if (op->seek_eof && in->whence == SEEK_DATA) {
        chimera_fuse_reply(req, ENXIO, NULL, 0);
        return;
    }

    memset(&out, 0, sizeof(out));
    out.offset = op->seek_offset;

    chimera_fuse_reply(req, 0, &out, sizeof(out));
} /* chimera_fuse_lseek_sequence_complete */

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
            /* The kernel resolves SEEK_SET/CUR/END itself and only asks us
             * where data and holes are; nothing here tracks a position. */
            chimera_fuse_reply(req, EINVAL, NULL, 0);
            return;
    } /* switch */

    req->compound = chimera_vfs_compound_alloc(req->thread->vfs_thread,
                                               &req->cred);

    chimera_vfs_compound_add_puthandle(req->compound,
                                       chimera_fuse_file(in->fh)->handle,
                                       CHIMERA_VFS_OPEN_INFERRED);

    chimera_vfs_compound_add_seek(req->compound, NULL, in->offset, what);

    chimera_vfs_compound_submit(req->compound,
                                chimera_fuse_lseek_sequence_complete, req);
} /* chimera_fuse_op_lseek */

/* --- COPY_FILE_RANGE --- */

static void
chimera_fuse_copy_range_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_fuse_request          *req = private_data;
    const struct chimera_vfs_compound_op *op;
    struct fuse_write_out                 out;
    enum chimera_vfs_error                status;

    status = chimera_vfs_compound_status(compound);

    if (status != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(status), NULL, 0);
        return;
    }

    op = chimera_vfs_compound_op(compound,
                                 chimera_vfs_compound_num_ops(compound) - 1);

    memset(&out, 0, sizeof(out));
    out.size = op->written;

    chimera_fuse_reply(req, 0, &out, sizeof(out));
} /* chimera_fuse_copy_range_sequence_complete */

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

    req->compound = chimera_vfs_compound_alloc(req->thread->vfs_thread,
                                               &req->cred);

    /* Both objects come from the kernel's open files, which is why this op
     * never addresses the sequence's current one -- there is no single current
     * object that could stand for two.
     *
     * The destination's pages change underneath any kernel that has them
     * cached, including this one; the write triggers the usual claim break,
     * and this mount is exempt from its own invalidation through the
     * credential's origin stamp. */
    chimera_vfs_compound_add_copy_range(req->compound,
                                        src->handle, in->off_in,
                                        dst->handle, in->off_out,
                                        in->len, 0, 0, 0);

    chimera_vfs_compound_submit(req->compound,
                                chimera_fuse_copy_range_sequence_complete,
                                req);
} /* chimera_fuse_op_copy_file_range */
