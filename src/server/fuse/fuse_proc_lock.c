// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * POSIX byte-range locks (FUSE_GETLK / FUSE_SETLK / FUSE_SETLKW).
 *
 * Locks live in the shared claim core as RANGE claims -- the same conflict
 * matrix NLM, NFSv4, and SMB2 use -- so a FUSE lock genuinely conflicts
 * with locks taken over the other protocols, on every backend.  The lease
 * owner identity is (mount, kernel lock-owner token), which is exactly the
 * per-process identity POSIX wants: the kernel hands us the same token for
 * every fd of one process.
 *
 * Taking one is a VFS SEQUENCE: PUTHANDLE lends the kernel's own open file,
 * and a CLAIM (or, for GETLK, a CLAIM_TEST) asks the claim core against it.
 * GETLK is a probe; SETLK is CLAIM(WAIT), which rides out a breakable
 * caching holder and reports a hard lock conflict as it stands; SETLKW is
 * CLAIM(WAIT|WAIT_HARD), which additionally queues behind that lock.
 *
 * What stays OUT OF BAND is what a sequence op cannot be.  vfs_state stores
 * opaque per-holder ranges and never merges or splits, so POSIX range
 * surgery is done here (the NFSv4 LOCK/LOCKU precedent): a new lock or
 * unlock trims every overlapping range the same owner holds, re-inserting
 * the kept sub-ranges.  A same-owner re-insert can never fail -- the owner
 * held the covering range throughout, so no conflicting claim can have
 * appeared inside it -- and a release is not reversible, so neither belongs
 * behind an op that can fail.  F_UNLCK and the FLUSH lock_owner release are
 * releases outright.
 *
 * A blocking SETLKW parks the run, and the park callback is what puts the
 * request where FUSE_INTERRUPT can find it.  The interrupt arrives on
 * whatever thread read it from the kernel's shared queue, while the cancel
 * belongs to the thread that submitted -- it finishes the run inline when it
 * takes -- so the interrupt only ASKS, marshalling the request home through
 * the per-thread resume doorbell.  There, chimera_vfs_compound_cancel's
 * return value decides exactly one of the two racers (grant vs cancel)
 * completes the request, exactly as chimera_vfs_claim_cancel's did beneath
 * it before the sequence owned the acquire.
 */

#include "common/thread.h"
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>

#include "fuse_internal.h"
#include "vfs/vfs_claim.h"

#define CHIMERA_FUSE_LOCK_LEN(start, end) \
        ((end) >= CHIMERA_FUSE_LOCK_EOF ? UINT64_MAX : (end) - (start) + 1)

/* True when a reported conflict is an actual byte-range lock rather than a
 * cache claim.  A cache claim breaks transparently when a lock is taken, so
 * GETLK must not report it and SETLK must not fail on it. */
static inline bool
chimera_fuse_conflict_is_lock(const struct chimera_vfs_claim_conflict *c)
{
    return c->construct == CHIMERA_CONSTRUCT_LOCK_ADVISORY ||
           c->construct == CHIMERA_CONSTRUCT_LOCK_SMB;
} /* chimera_fuse_conflict_is_lock */

static inline struct chimera_vfs_state *
chimera_fuse_vfs_state(struct chimera_fuse_request *req)
{
    return req->thread->vfs_thread->vfs->vfs_state;
} /* chimera_fuse_vfs_state */

static void
chimera_fuse_lock_lease_init(
    struct chimera_vfs_claim  *lease,
    struct chimera_fuse_mount *mount,
    uint64_t                   token,
    uint64_t                   start,
    uint64_t                   end,
    int                        exclusive)
{
    struct chimera_claim_owner owner;

    chimera_fuse_lock_owner(&owner, mount, token);

    /* Advisory (not LOCK_SMB): FUSE byte-range locks are POSIX locks, so
     * they carry no mandatory-I/O rows.  The claim core's half-open range
     * uses UINT64_MAX for to-EOF, which is what LOCK_LEN already yields. */
    chimera_vfs_claim_init_range(lease, exclusive, false, start,
                                 CHIMERA_FUSE_LOCK_LEN(start, end), &owner);
} /* chimera_fuse_lock_lease_init */

/* Find or create the (owner, file) bucket.  Caller holds mount->lock_lock. */
static struct chimera_fuse_lock_file *
chimera_fuse_lock_file_get(
    struct chimera_fuse_mount      *mount,
    struct chimera_vfs_state       *state,
    uint64_t                        token,
    struct chimera_vfs_open_handle *handle,
    int                             create)
{
    struct chimera_fuse_lock_file *lf;
    struct chimera_fuse_lock_key   key;

    /* Zero the whole key before filling it: uthash hashes the raw bytes, so
     * any padding must be defined -- and the analyser cannot otherwise prove
     * it is. */
    memset(&key, 0, sizeof(key));
    key.owner   = token;
    key.fh_hash = handle->fh_hash;

    HASH_FIND(hh, mount->lock_files, &key, sizeof(key), lf);

    if (lf || !create) {
        return lf;
    }

    lf = calloc(1, sizeof(*lf));

    lf->key    = key;
    lf->fh_len = handle->fh_len;
    memcpy(lf->fh, handle->fh, handle->fh_len);
    lf->file_state = chimera_vfs_state_get(state, lf->fh, lf->fh_len,
                                           handle->fh_hash, true);

    if (!lf->file_state) {
        free(lf);
        return NULL;
    }

    HASH_ADD(hh, mount->lock_files, key, sizeof(lf->key), lf);

    return lf;
} /* chimera_fuse_lock_file_get */

/* Free the bucket once nothing references it.  Caller holds lock_lock. */
static void
chimera_fuse_lock_file_maybe_free(
    struct chimera_fuse_mount     *mount,
    struct chimera_vfs_state      *state,
    struct chimera_fuse_lock_file *lf)
{
    if (lf->locks || lf->pending) {
        return;
    }

    HASH_DELETE(hh, mount->lock_files, lf);

    chimera_vfs_state_put(state, lf->file_state);

    free(lf);
} /* chimera_fuse_lock_file_maybe_free */

/*
 * Trim every lock the owner holds that overlaps [start, end], keeping the
 * sub-ranges outside it (POSIX lock replacement).  `skip` is the freshly
 * granted entry, already on the list.  Caller holds mount->lock_lock.
 */
static void
chimera_fuse_lock_trim(
    struct chimera_vfs_state      *state,
    struct chimera_fuse_lock_file *lf,
    uint64_t                       start,
    uint64_t                       end,
    struct chimera_fuse_lock      *skip)
{
    struct chimera_fuse_lock     *lock, *tmp, *piece;
    enum chimera_vfs_claim_result result;
    uint64_t                      keep_start[2], keep_end[2];
    int                           npiece, i;

    DL_FOREACH_SAFE(lf->locks, lock, tmp)
    {
        if (lock == skip || lock->end < start || lock->start > end) {
            continue;
        }

        npiece = 0;

        if (lock->start < start) {
            keep_start[npiece] = lock->start;
            keep_end[npiece]   = start - 1;
            npiece++;
        }

        if (lock->end > end) {
            keep_start[npiece] = end + 1;
            keep_end[npiece]   = lock->end;
            npiece++;
        }

        for (i = 0; i < npiece; i++) {
            piece = calloc(1, sizeof(*piece));

            piece->lf        = lf;
            piece->start     = keep_start[i];
            piece->end       = keep_end[i];
            piece->exclusive = lock->exclusive;

            /* Rebuild precisely from the surviving range: same owner and
             * mode as the lock being trimmed. */
            piece->claim             = lock->claim;
            piece->claim.offset      = piece->start;
            piece->claim.length      = CHIMERA_FUSE_LOCK_LEN(piece->start, piece->end);
            piece->claim.prev        = NULL;
            piece->claim.next        = NULL;
            piece->claim.file        = NULL;
            piece->claim.break_state = CHIMERA_CLAIM_BREAK_IDLE;

            result = chimera_vfs_claim_try_acquire(state, lf->file_state,
                                                   &piece->claim, NULL);

            /* The owner held the covering range for the whole time, so no
             * other owner can hold anything conflicting inside it. */
            chimera_fuse_abort_if(result != CHIMERA_CLAIM_GRANTED,
                                  "same-owner lock trim re-insert failed (%d)",
                                  result);

            DL_APPEND(lf->locks, piece);
        }

        chimera_vfs_claim_release(state, lf->file_state, &lock->claim);

#ifdef __clang_analyzer__
        chimera_fuse_abort_if(lf->locks == NULL,
                              "clang static analysis thinks this can happen");
#endif /* ifdef __clang_analyzer__ */
        DL_DELETE(lf->locks, lock);
        free(lock);
    }
} /* chimera_fuse_lock_trim */

/* --- GETLK --- */

/* The CLAIM_TEST's index in the GETLK sequence (PUTHANDLE is 0). */
#define CHIMERA_FUSE_GETLK_OP_CLAIM 1

static void
chimera_fuse_getlk_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_fuse_request             *req = private_data;
    const struct chimera_vfs_compound_op    *op;
    const struct chimera_vfs_claim_conflict *conflict;
    enum chimera_vfs_error                   status;
    struct fuse_lk_out                       out;

    status = chimera_vfs_compound_status(compound);

    if (status != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(status), NULL, 0);
        return;
    }

    memset(&out, 0, sizeof(out));
    out.lk.type = F_UNLCK;

    op       = chimera_vfs_compound_op(compound, CHIMERA_FUSE_GETLK_OP_CLAIM);
    conflict = &op->conflict;

    /* Only a byte-range lock is a lock: a conflicting cache claim (some
     * client's read cache, possibly this mount's own) breaks transparently
     * when a real lock is taken and must not be reported as one. */
    if (op->claim_result != CHIMERA_CLAIM_GRANTED &&
        chimera_fuse_conflict_is_lock(conflict)) {
        out.lk.type  = (conflict->used & CHIMERA_CLAIM_LW) ? F_WRLCK : F_RDLCK;
        out.lk.start = conflict->offset;
        if (conflict->length == UINT64_MAX ||
            conflict->offset + conflict->length - 1 >= CHIMERA_FUSE_LOCK_EOF) {
            out.lk.end = CHIMERA_FUSE_LOCK_EOF;
        } else {
            out.lk.end = conflict->offset + conflict->length - 1;
        }
        /* The claim core reports conflicts by value without a pid; NFS
         * clients give the same answer for remote holders. */
        out.lk.pid = 0;
    }

    chimera_fuse_reply(req, 0, &out, sizeof(out));
} /* chimera_fuse_getlk_sequence_complete */

void
chimera_fuse_op_getlk(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const struct fuse_lk_in       *in    = arg;
    struct chimera_fuse_mount     *mount = req->channel->mount;
    struct chimera_fuse_open_file *file;

    if (arglen < sizeof(*in)) {
        chimera_fuse_reply(req, EINVAL, NULL, 0);
        return;
    }

    if (in->lk_flags & FUSE_LK_FLOCK) {
        chimera_fuse_reply(req, ENOSYS, NULL, 0);
        return;
    }

    file = chimera_fuse_file(in->fh);

    /* The probe is never inserted, so it need only outlive the sequence --
     * which the request does, being recycled by the reply. */
    chimera_fuse_lock_lease_init(&req->u.lock.probe, mount, in->owner,
                                 in->lk.start, in->lk.end,
                                 in->lk.type == F_WRLCK);

    req->compound = chimera_vfs_compound_alloc(req->thread->vfs_thread,
                                               &req->cred);

    /* The kernel named an open file; the sequence borrows it, flagged as
     * it was opened.  A claim wants only the fh, so a data handle serves
     * the PATH want and nothing is re-opened. */
    chimera_vfs_compound_add_puthandle(req->compound, file->handle,
                                       file->open_flags);

    /* Local only: no TEST_BACKEND.  A FUSE kernel asking F_GETLK wants what
     * this server arbitrates, which is what the per-op probe answered. */
    chimera_vfs_compound_add_claim_test(req->compound, &req->u.lock.probe, 0);

    chimera_vfs_compound_submit(req->compound,
                                chimera_fuse_getlk_sequence_complete, req);
} /* chimera_fuse_op_getlk */

/* --- SETLK / SETLKW --- */

/* The CLAIM's index in the SETLK sequence (PUTHANDLE is 0). */
#define CHIMERA_FUSE_SETLK_OP_CLAIM 1

/*
 * Settle a finished lock request on its owning thread: commit or discard the
 * entry, do the POSIX range surgery, and reply.
 *
 * Unless an interrupt is in flight.  chimera_fuse_locks_interrupt marshals
 * the request to this thread with the mount lock held, so from the moment it
 * posts until the drain consumes it there is a doorbell holding this pointer
 * -- and replying would recycle the request underneath it.  So the outcome is
 * parked on the request instead and the drain settles it, which it does
 * having first cleared the flag.
 */
static void
chimera_fuse_lock_settle(
    struct chimera_fuse_request *req,
    int                          error)
{
    struct chimera_fuse_mount     *mount = req->channel->mount;
    struct chimera_vfs_state      *state = chimera_fuse_vfs_state(req);
    struct chimera_fuse_lock      *entry = req->u.lock.entry;
    struct chimera_fuse_lock_file *lf    = req->u.lock.lf;

    evpl_mutex_lock(&mount->lock_lock);

    if (req->u.lock.cancel_posted) {
        req->u.lock.result_errno = error;
        req->u.lock.done         = 1;
        evpl_mutex_unlock(&mount->lock_lock);
        return;
    }

    if (req->u.lock.parked) {
        DL_DELETE2(mount->parked_locks, req, u.lock.park_prev, u.lock.park_next);
        req->u.lock.parked = 0;
    }

    lf->pending--;

    if (error == 0) {
        DL_APPEND(lf->locks, entry);
        chimera_fuse_lock_trim(state, lf, entry->start, entry->end, entry);
    } else {
        free(entry);
    }

    chimera_fuse_lock_file_maybe_free(mount, state, lf);

    evpl_mutex_unlock(&mount->lock_lock);

    chimera_fuse_reply(req, error, NULL, 0);
} /* chimera_fuse_lock_settle */

/*
 * The doorbell drain, on the request's own thread: an interrupt asked for
 * this run to be cancelled, or the completion handed us an outcome it could
 * not reply with.
 */
void
chimera_fuse_lock_resume(struct chimera_fuse_request *req)
{
    struct chimera_fuse_mount *mount = req->channel->mount;
    int                        done, error;

    evpl_mutex_lock(&mount->lock_lock);
    req->u.lock.cancel_posted = 0;
    done                      = req->u.lock.done;
    error                     = req->u.lock.result_errno;
    evpl_mutex_unlock(&mount->lock_lock);

    if (done) {
        /* The run finished while the interrupt was in flight and left the
         * outcome for us rather than free a request the doorbell still
         * pointed at.  Whichever answer it carries is the one that stands:
         * the interrupt never reached the cancel. */
        chimera_fuse_lock_settle(req, error);
        return;
    }

    /*
     * Still parked, so the cancel decides -- and its return value is the
     * whole arbitration, never a guess.  Non-zero: we took the park back and
     * the completion has ALREADY RUN inside this call, with ECANCELED, which
     * settled and replied EINTR (cancel_posted is clear again, so it did not
     * defer).  Zero: the grant owns the completion, it is running or about to
     * on whatever thread released the conflict, and it comes home to this one
     * carrying whatever the run actually did.  Exactly one of the two
     * completes the request, and neither may be touched here afterwards.
     */
    chimera_vfs_compound_cancel(req->compound);
} /* chimera_fuse_lock_resume */

/*
 * The run parked: put the request where FUSE_INTERRUPT can find it.  Fires
 * once, on this thread, from inside the acquire -- before anything can
 * answer it, because the answer comes home through this thread's doorbell.
 */
static void
chimera_fuse_lock_park_cb(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    void                        *private_data)
{
    struct chimera_fuse_request *req   = private_data;
    struct chimera_fuse_mount   *mount = req->channel->mount;

    evpl_mutex_lock(&mount->lock_lock);
    req->u.lock.parked = 1;
    DL_APPEND2(mount->parked_locks, req, u.lock.park_prev, u.lock.park_next);
    evpl_mutex_unlock(&mount->lock_lock);
} /* chimera_fuse_lock_park_cb */

static void
chimera_fuse_lock_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_fuse_request   *req   = private_data;
    struct chimera_vfs_state      *state = chimera_fuse_vfs_state(req);
    struct chimera_vfs_file_state *taken;
    enum chimera_vfs_error         status;
    int                            error;

    status = chimera_vfs_compound_status(compound);

    switch (status) {
        case CHIMERA_VFS_OK:
            error = 0;
            break;
        case CHIMERA_VFS_EAGAIN:
            /* The claim was refused: a hard lock conflict a non-blocking
             * SETLK will not wait for.  A breakable caching holder never
             * lands here -- WAIT rides those out. */
            error = EAGAIN;
            break;
        case CHIMERA_VFS_ECANCELED:
            /* FUSE_INTERRUPT took the park back. */
            error = EINTR;
            break;
        default:
            error = chimera_fuse_errno(status);
            break;
    } /* switch */

    /*
     * On GRANTED the claim is inserted and the file state comes with it; the
     * contract is that the caller takes it.  We then PUT it, because the
     * (owner, file) bucket already holds a reference to the very same state
     * -- chimera_vfs_state_get is refcounted per fh, and the executor
     * resolved it from the same handle the bucket was built from.  The
     * bucket's is the one to keep: it covers every lock in the bucket at
     * once and a parked acquire holding none, it is what the release paths
     * that never ran through a sequence use (the trim's re-insert, the FLUSH
     * owner release, shutdown), and its lifetime is already exactly the
     * bucket's.  Keeping the sequence's instead would mean a reference per
     * lock and a bucket with none before the first grant.
     */
    taken = chimera_vfs_compound_take_file_state(compound,
                                                 CHIMERA_FUSE_SETLK_OP_CLAIM);

    if (taken) {
        chimera_vfs_state_put(state, taken);
    }

    chimera_fuse_lock_settle(req, error);
} /* chimera_fuse_lock_sequence_complete */

static void
chimera_fuse_setlk_unlock(
    struct chimera_fuse_request *req,
    const struct fuse_lk_in     *in)
{
    struct chimera_fuse_mount     *mount = req->channel->mount;
    struct chimera_vfs_state      *state = chimera_fuse_vfs_state(req);
    struct chimera_fuse_open_file *file  = chimera_fuse_file(in->fh);
    struct chimera_fuse_lock_file *lf;

    evpl_mutex_lock(&mount->lock_lock);

    lf = chimera_fuse_lock_file_get(mount, state, in->owner,
                                    file->handle, 0);

    if (lf) {
        chimera_fuse_lock_trim(state, lf, in->lk.start, in->lk.end, NULL);
        chimera_fuse_lock_file_maybe_free(mount, state, lf);
    }

    evpl_mutex_unlock(&mount->lock_lock);

    /* Unlocking a range with no locks in it succeeds (POSIX). */
    chimera_fuse_reply(req, 0, NULL, 0);
} /* chimera_fuse_setlk_unlock */

void
chimera_fuse_op_setlk(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const struct fuse_lk_in       *in    = arg;
    struct chimera_fuse_mount     *mount = req->channel->mount;
    struct chimera_vfs_state      *state = chimera_fuse_vfs_state(req);
    struct chimera_fuse_open_file *file;
    struct chimera_fuse_lock_file *lf;
    struct chimera_fuse_lock      *entry;
    unsigned int                   claim_flags;
    int                            wait;

    if (arglen < sizeof(*in)) {
        chimera_fuse_reply(req, EINVAL, NULL, 0);
        return;
    }

    if (in->lk_flags & FUSE_LK_FLOCK) {
        chimera_fuse_reply(req, ENOSYS, NULL, 0);
        return;
    }

    switch (in->lk.type) {
        case F_UNLCK:
            chimera_fuse_setlk_unlock(req, in);
            return;
        case F_RDLCK:
        case F_WRLCK:
            break;
        default:
            chimera_fuse_reply(req, EINVAL, NULL, 0);
            return;
    } /* switch */

    file = chimera_fuse_file(in->fh);
    wait = (req->opcode == FUSE_SETLKW);

    entry = calloc(1, sizeof(*entry));

    entry->start     = in->lk.start;
    entry->end       = in->lk.end;
    entry->exclusive = (in->lk.type == F_WRLCK);

    chimera_fuse_lock_lease_init(&entry->claim, mount, in->owner,
                                 entry->start, entry->end, entry->exclusive);

    evpl_mutex_lock(&mount->lock_lock);

    lf = chimera_fuse_lock_file_get(mount, state, in->owner,
                                    file->handle, 1);

    if (!lf) {
        evpl_mutex_unlock(&mount->lock_lock);
        free(entry);
        chimera_fuse_reply(req, EIO, NULL, 0);
        return;
    }

    entry->lf = lf;
    lf->pending++;

    evpl_mutex_unlock(&mount->lock_lock);

    req->u.lock.entry         = entry;
    req->u.lock.lf            = lf;
    req->u.lock.result_errno  = EIO;
    req->u.lock.parked        = 0;
    req->u.lock.cancel_posted = 0;
    req->u.lock.done          = 0;

    /*
     * WAIT so breakable cache holders are recalled and waited through: a
     * conflicting CACHING lease is not a lock.  It is someone's read cache
     * (possibly this very mount's own invalidation grant, whose owner
     * identity deliberately differs from lock owners), it breaks in
     * milliseconds, and reporting it as EAGAIN would fabricate a lock that
     * does not exist.
     *
     * WAIT_HARD only for SETLKW.  Without it the executor's answer to a hard
     * conflict IS the EAGAIN this used to pre-probe for: the claim core
     * denies synchronously on a conflicting byte-range lock rather than
     * queueing, and a BREAKING answer can never name one (a range claim is
     * never revocable -- no consumer gives it a break_cb -- and the ACCESS
     * escape that produces the other BREAKING conflicts reports the cache it
     * escaped onto).  So the probe answered exactly what the acquire answers,
     * one arbitration later, and is gone with its own race.
     */
    claim_flags = CHIMERA_VFS_COMPOUND_CLAIM_WAIT;

    if (wait) {
        claim_flags |= CHIMERA_VFS_COMPOUND_CLAIM_WAIT_HARD;
    }

    req->compound = chimera_vfs_compound_alloc(req->thread->vfs_thread,
                                               &req->cred);

    /* The kernel named an open file; the sequence borrows it, flagged as
     * it was opened.  A claim wants only the fh, so a data handle serves
     * the PATH want and nothing is re-opened. */
    chimera_vfs_compound_add_puthandle(req->compound, file->handle,
                                       file->open_flags);

    /* The claim and its ticket live in the heap entry, not the sequence and
     * not this frame: the claim core keeps pointers into an inserted claim,
     * so its address is its identity for as long as the lock is held. */
    chimera_vfs_compound_add_claim(req->compound, &entry->claim, &entry->ticket,
                                   claim_flags, 0, 0, 0, 0);

    chimera_vfs_compound_set_park_cb(req->compound,
                                     chimera_fuse_lock_park_cb, req);

    /* The completion can run inside this call, replying and recycling the
     * request, so nothing may follow. */
    chimera_vfs_compound_submit(req->compound,
                                chimera_fuse_lock_sequence_complete, req);
} /* chimera_fuse_op_setlk */

/* --- FLUSH / teardown integration --- */

void
chimera_fuse_locks_release_owner(
    struct chimera_fuse_thread *thread,
    struct chimera_fuse_mount  *mount,
    uint64_t                    fh_hash,
    uint64_t                    owner)
{
    struct chimera_vfs_state      *state = thread->vfs_thread->vfs->vfs_state;
    struct chimera_fuse_lock_file *lf;
    struct chimera_fuse_lock      *lock, *tmp;
    struct chimera_fuse_lock_key   key;

    memset(&key, 0, sizeof(key));
    key.owner   = owner;
    key.fh_hash = fh_hash;

    evpl_mutex_lock(&mount->lock_lock);

    HASH_FIND(hh, mount->lock_files, &key, sizeof(key), lf);

    if (!lf) {
        evpl_mutex_unlock(&mount->lock_lock);
        return;
    }

    /* Every lock this owner holds on this file goes away together, so detach
     * the list in one step and walk the detached chain: splicing each node out
     * of a list that is about to be empty anyway only gives the head pointer
     * intermediate states to reason about. */
    lock      = lf->locks;
    lf->locks = NULL;

    while (lock) {
        tmp = lock->next;
        chimera_vfs_claim_release(state, lf->file_state, &lock->claim);
        free(lock);
        lock = tmp;
    }

    chimera_fuse_lock_file_maybe_free(mount, state, lf);

    evpl_mutex_unlock(&mount->lock_lock);
} /* chimera_fuse_locks_release_owner */

/*
 * Ask for a parked lock to be cancelled.  The cancel itself is not done here:
 * it finishes the run inline when it takes, and the completion replies on the
 * channel and recycles the request, both of which belong to the thread that
 * submitted -- while an INTERRUPT arrives on whatever thread read it from the
 * kernel's shared queue.  So this marshals the request home and the drain
 * (chimera_fuse_lock_resume) calls the cancel there.
 *
 * Posting under lock_lock is what makes the pointer safe.  A request leaves
 * parked_locks only in its own settle, which takes this lock, so while it is
 * on the list and we hold the lock it cannot be replied to and recycled; and
 * from the flag on, the settle defers to the drain instead of freeing
 * underneath it.
 */
int
chimera_fuse_locks_interrupt(
    struct chimera_fuse_mount *mount,
    uint64_t                   unique)
{
    struct chimera_fuse_request *parked;

    evpl_mutex_lock(&mount->lock_lock);

    DL_FOREACH2(mount->parked_locks, parked, u.lock.park_next)
    {
        if (parked->unique == unique) {
            break;
        }
    }

    if (!parked) {
        evpl_mutex_unlock(&mount->lock_lock);
        return 0;
    }

    if (!parked->u.lock.cancel_posted) {
        parked->u.lock.cancel_posted = 1;
        chimera_fuse_resume_post(parked);
    }

    evpl_mutex_unlock(&mount->lock_lock);

    return 1;
} /* chimera_fuse_locks_interrupt */

void
chimera_fuse_locks_shutdown(
    struct chimera_fuse_shared *shared,
    struct chimera_fuse_mount  *mount)
{
    struct chimera_vfs_state      *state = shared->vfs->vfs_state;
    struct chimera_fuse_request   *parked, *ptmp;
    struct chimera_fuse_lock_file *lf, *lftmp;
    struct chimera_fuse_lock      *lock, *ltmp;

    /* Ask for every parked acquire to be cancelled, on the same terms as an
     * interrupt: the cancel runs on the request's own thread, which is still
     * alive (stop() runs before the thread pool is torn down) and drains the
     * doorbell.  Whichever of cancel and grant wins there, the request is
     * replied to and the run's claims are its own to release. */
    evpl_mutex_lock(&mount->lock_lock);

    DL_FOREACH_SAFE2(mount->parked_locks, parked, ptmp, u.lock.park_next)
    {
        if (!parked->u.lock.cancel_posted) {
            parked->u.lock.cancel_posted = 1;
            chimera_fuse_resume_post(parked);
        }
    }

    HASH_ITER(hh, mount->lock_files, lf, lftmp)
    {
        DL_FOREACH_SAFE(lf->locks, lock, ltmp)
        {
            chimera_vfs_claim_release(state, lf->file_state, &lock->claim);
#ifdef __clang_analyzer__
            chimera_fuse_abort_if(lf->locks == NULL,
                                  "clang static analysis thinks this can happen");
#endif /* ifdef __clang_analyzer__ */
            DL_DELETE(lf->locks, lock);
            free(lock);
        }

        chimera_fuse_lock_file_maybe_free(mount, state, lf);
    }

    evpl_mutex_unlock(&mount->lock_lock);
} /* chimera_fuse_locks_shutdown */
