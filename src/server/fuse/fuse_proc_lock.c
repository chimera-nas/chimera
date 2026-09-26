// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

/* POSIX byte-range locks use the shared VFS owner journal. The frontend owns
 * only wire validation, cancellation lookup and terminal reply formatting;
 * ranges, waiting, retry rollback and accepted publication belong to VFS. */
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>

#include "fuse_internal.h"
#include "common/compound_retry.h"
#include "vfs/vfs_lock.h"
#include "vfs/vfs_release.h"

#define CHIMERA_FUSE_LOCK_LEN(start, end) \
        ((end) == CHIMERA_FUSE_LOCK_EOF ? UINT64_MAX : (end) - (start) + 1)

static void
chimera_fuse_lock_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_fuse_request             *req   = private_data;
    struct chimera_fuse_mount               *mount = req->channel->mount;
    const struct chimera_vfs_compound_op    *op;
    const struct chimera_vfs_claim_conflict *conflict;
    struct fuse_lk_out                       out;
    enum chimera_vfs_error                   status = chimera_vfs_compound_status(compound);

    evpl_mutex_lock(&mount->lock_lock);
    if (req->u.lock.parked) {
        DL_DELETE2(mount->parked_locks, req, u.lock.park_prev, u.lock.park_next);
        req->u.lock.parked = 0;
    }
    evpl_mutex_unlock(&mount->lock_lock);

    if (status != CHIMERA_VFS_OK || req->opcode != FUSE_GETLK) {
        chimera_fuse_reply(req, chimera_fuse_errno(status), NULL, 0);
        return;
    }

    op       = chimera_vfs_compound_op(compound, req->u.lock.op_index);
    conflict = &op->claim_conflict;
    memset(&out, 0, sizeof(out));
    out.lk.type = F_UNLCK;
    if (conflict->construct == CHIMERA_CONSTRUCT_LOCK_ADVISORY ||
        conflict->construct == CHIMERA_CONSTRUCT_LOCK_SMB) {
        out.lk.type  = (conflict->used & CHIMERA_CLAIM_LW) ? F_WRLCK : F_RDLCK;
        out.lk.start = conflict->offset;
        if (conflict->length == UINT64_MAX || conflict->offset >= CHIMERA_FUSE_LOCK_EOF ||
            conflict->length - 1 >= CHIMERA_FUSE_LOCK_EOF - conflict->offset) {
            out.lk.end = CHIMERA_FUSE_LOCK_EOF;
        } else {
            out.lk.end = conflict->offset + conflict->length - 1;
        }
        out.lk.pid = op->lock_pid;
    }
    chimera_fuse_reply(req, 0, &out, sizeof(out));
} /* chimera_fuse_lock_complete */

static void
chimera_fuse_lock_submit(
    struct chimera_fuse_request *req,
    const struct fuse_lk_in     *in,
    uint32_t                     arglen,
    bool                         test)
{
    struct chimera_fuse_mount      *mount = req->channel->mount;
    struct chimera_fuse_open_file  *file;
    struct chimera_vfs_lock_request lock = { 0 };

    if (arglen < sizeof(*in) || in->lk.start > in->lk.end ||
        in->lk.end > CHIMERA_FUSE_LOCK_EOF) {
        chimera_fuse_reply(req, EINVAL, NULL, 0);
        return;
    }
    if (in->lk_flags & FUSE_LK_FLOCK) {
        chimera_fuse_reply(req, ENOSYS, NULL, 0);
        return;
    }
    switch (in->lk.type) {
        case F_RDLCK:
            lock.type = CHIMERA_VFS_LOCK_READ;
            break;
        case F_WRLCK:
            lock.type = CHIMERA_VFS_LOCK_WRITE;
            break;
        case F_UNLCK:
            if (!test) {
                lock.type = CHIMERA_VFS_LOCK_UNLOCK;
                break;
            }
            /* GETLK asks whether a read or write lock would conflict. */
            chimera_fuse_reply(req, EINVAL, NULL, 0);
            return;
        default:
            chimera_fuse_reply(req, EINVAL, NULL, 0);
            return;
    } /* switch */

    file = chimera_fuse_file(in->fh);
    chimera_fuse_lock_owner(&lock.owner, mount, in->owner);
    lock.whence          = SEEK_SET;
    lock.offset          = in->lk.start;
    lock.length          = CHIMERA_FUSE_LOCK_LEN(in->lk.start, in->lk.end);
    lock.wait            = req->opcode == FUSE_SETLKW;
    lock.project_backend = false;
    lock.generation      = chimera_vfs_lock_domain_admit(mount->lock_domain,
                                                         file->handle, &lock.owner);
    if (!lock.generation) {
        chimera_fuse_reply(req, ENOMEM, NULL, 0);
        return;
    }

    /* A blocked request outlives RELEASE/FLUSH on another descriptor. */
    chimera_vfs_dup_handle(req->thread->vfs_thread, file->handle);
    req->handle           = file->handle;
    req->compound         = chimera_vfs_compound_alloc(req->thread->vfs_thread, &req->cred);
    req->u.lock.parked    = 0;
    req->u.lock.park_prev = req->u.lock.park_next = NULL;
    if (test) {
        req->u.lock.op_index = chimera_vfs_compound_add_lock_test(req->compound,
                                                                  mount->lock_domain, req->handle, &lock);
    } else {
        req->u.lock.op_index = chimera_vfs_compound_add_lock_change(req->compound,
                                                                    mount->lock_domain, req->handle, &lock);
    }

    /* Register before submit: interrupt/close may race an unsubmitted request.
     * VFS cancellation only schedules its worker; it never invokes completion
     * inline while this registry mutex protects the compound's lifetime. */
    if (!test && req->u.lock.op_index >= 0) {
        evpl_mutex_lock(&mount->lock_lock);
        req->u.lock.parked = 1;
        DL_APPEND2(mount->parked_locks, req, u.lock.park_prev, u.lock.park_next);
        evpl_mutex_unlock(&mount->lock_lock);
    }
    chimera_frontend_compound_submit(req->compound, chimera_fuse_lock_complete, req);
} /* chimera_fuse_lock_submit */

void
chimera_fuse_op_getlk(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    chimera_fuse_lock_submit(req, arg, arglen, true);
} /* chimera_fuse_op_getlk */

void
chimera_fuse_op_setlk(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    chimera_fuse_lock_submit(req, arg, arglen, false);
} /* chimera_fuse_op_setlk */

void
chimera_fuse_locks_release_owner(
    struct chimera_fuse_thread *thread,
    struct chimera_fuse_mount  *mount,
    const uint8_t              *fh,
    uint32_t                    fh_len,
    uint64_t                    token)
{
    struct chimera_claim_owner owner;

    chimera_fuse_lock_owner(&owner, mount, token);
    /* Mandatory close cleanup: invalidate admissions before retiring committed
     * coverage, including granted attempts awaiting backend finish. */
    chimera_vfs_lock_domain_retire(mount->lock_domain, fh, fh_len, &owner);
} /* chimera_fuse_locks_release_owner */

int
chimera_fuse_locks_interrupt(
    struct chimera_fuse_mount *mount,
    struct chimera_vfs_state  *state,
    uint64_t                   unique)
{
    struct chimera_fuse_request *req;
    int                          found = 0;

    evpl_mutex_lock(&mount->lock_lock);
    DL_FOREACH2(mount->parked_locks, req, u.lock.park_next)
    {
        if (req->unique == unique) {
            chimera_vfs_compound_lock_cancel(req->compound, req->u.lock.op_index);
            found = 1;
            break;
        }
    }
    evpl_mutex_unlock(&mount->lock_lock);
    return found;
} /* chimera_fuse_locks_interrupt */

void
chimera_fuse_locks_shutdown(
    struct chimera_fuse_shared *shared,
    struct chimera_fuse_mount  *mount)
{
    /* Local-only domains require no backend thread to release their ranges.
     * Cancelled operations finish on their original, still-live workers. */
    chimera_vfs_lock_domain_shutdown(NULL, mount->lock_domain);
} /* chimera_fuse_locks_shutdown */
