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
 * vfs_state stores opaque per-holder ranges and never merges or splits, so
 * POSIX range surgery is done here (the NFSv4 LOCK/LOCKU precedent): a new
 * lock or unlock trims every overlapping range the same owner holds,
 * re-inserting the kept sub-ranges.  A same-owner re-insert can never fail:
 * the owner held the covering range throughout, so no conflicting claim can
 * have appeared inside it.
 *
 * A blocking SETLKW parks in vfs_state's pending queue with the request
 * held open; the grant callback can fire on any thread, so completion is
 * marshalled home through the per-thread resume doorbell.  FUSE_INTERRUPT
 * cancels a parked lock through chimera_vfs_claim_cancel, whose
 * return value decides exactly one of the two racers (grant vs cancel)
 * completes the request.
 */

#include <stdlib.h>
#include <string.h>
#include <fcntl.h>

#include "fuse_internal.h"
#include "vfs/vfs_claim.h"
#include "vfs/vfs_procs.h"

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

void
chimera_fuse_op_getlk(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const struct fuse_lk_in          *in    = arg;
    struct chimera_fuse_mount        *mount = req->channel->mount;
    struct chimera_vfs_state         *state = chimera_fuse_vfs_state(req);
    struct chimera_fuse_open_file    *file;
    struct chimera_vfs_file_state    *file_state;
    struct chimera_vfs_claim          probe;
    struct chimera_vfs_claim_conflict conflict;
    enum chimera_vfs_claim_result     result;
    struct fuse_lk_out                out;

    if (arglen < sizeof(*in)) {
        chimera_fuse_reply(req, EINVAL, NULL, 0);
        return;
    }

    if (in->lk_flags & FUSE_LK_FLOCK) {
        chimera_fuse_reply(req, ENOSYS, NULL, 0);
        return;
    }

    file = chimera_fuse_file(in->fh);

    memset(&out, 0, sizeof(out));
    out.lk.type = F_UNLCK;

    file_state = chimera_vfs_state_get(state, file->handle->fh,
                                       file->handle->fh_len,
                                       file->handle->fh_hash, false);

    if (!file_state) {
        /* No lease state on the file at all: nothing can conflict. */
        chimera_fuse_reply(req, 0, &out, sizeof(out));
        return;
    }

    chimera_fuse_lock_lease_init(&probe, mount, in->owner,
                                 in->lk.start, in->lk.end,
                                 in->lk.type == F_WRLCK);

    memset(&conflict, 0, sizeof(conflict));

    result = chimera_vfs_claim_test(file_state, &probe, &conflict);

    /* Only a byte-range lock is a lock: a conflicting cache claim (some
     * client's read cache, possibly this mount's own) breaks transparently
     * when a real lock is taken and must not be reported as one. */
    if (result != CHIMERA_CLAIM_GRANTED &&
        chimera_fuse_conflict_is_lock(&conflict)) {
        out.lk.type  = (conflict.used & CHIMERA_CLAIM_LW) ? F_WRLCK : F_RDLCK;
        out.lk.start = conflict.offset;
        if (conflict.length == UINT64_MAX ||
            conflict.offset + conflict.length - 1 >= CHIMERA_FUSE_LOCK_EOF) {
            out.lk.end = CHIMERA_FUSE_LOCK_EOF;
        } else {
            out.lk.end = conflict.offset + conflict.length - 1;
        }
        /* The claim core reports conflicts by value without a pid; NFS
         * clients give the same answer for remote holders. */
        out.lk.pid = 0;
    }

    chimera_vfs_state_put(state, file_state);

    chimera_fuse_reply(req, 0, &out, sizeof(out));
} /* chimera_fuse_op_getlk */

/* --- SETLK / SETLKW --- */

/* Complete a lock request on its owning thread: commit or discard the
 * entry, do the POSIX range surgery, and reply. */
static void
chimera_fuse_lock_finish(struct chimera_fuse_request *req)
{
    struct chimera_fuse_mount     *mount = req->channel->mount;
    struct chimera_vfs_state      *state = chimera_fuse_vfs_state(req);
    struct chimera_fuse_lock      *entry = req->u.lock.entry;
    struct chimera_fuse_lock_file *lf    = req->u.lock.lf;
    int                            error = req->u.lock.result_errno;

    pthread_mutex_lock(&mount->lock_lock);

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

    pthread_mutex_unlock(&mount->lock_lock);

    chimera_fuse_reply(req, error, NULL, 0);
} /* chimera_fuse_lock_finish */

void
chimera_fuse_lock_resume(struct chimera_fuse_request *req)
{
    chimera_fuse_lock_finish(req);
} /* chimera_fuse_lock_resume */

static void
chimera_fuse_setlk_acquire_cb(
    enum chimera_vfs_claim_result            result,
    struct chimera_vfs_claim                *granted_claim,
    const struct chimera_vfs_claim_conflict *conflict,
    void                                    *private_data)
{
    struct chimera_fuse_request *req      = private_data;
    int                          expected = 0;

    switch (result) {
        case CHIMERA_CLAIM_GRANTED:
            req->u.lock.result_errno = 0;
            break;
        case CHIMERA_CLAIM_BREAKING:
            /* Non-blocking lock against a breakable caching holder: the
             * break has been kicked; report unavailable-now. */
            req->u.lock.result_errno = EAGAIN;
            break;
        case CHIMERA_CLAIM_DENIED:
        default:
            req->u.lock.result_errno = EAGAIN;
            break;
    } /* switch */

    if (!atomic_compare_exchange_strong(&req->u.lock.phase, &expected, 1)) {
        /* The dispatch path has returned; marshal home for the reply. */
        chimera_fuse_resume_post(req);
    }
} /* chimera_fuse_setlk_acquire_cb */

static void
chimera_fuse_setlk_unlock(
    struct chimera_fuse_request *req,
    const struct fuse_lk_in     *in)
{
    struct chimera_fuse_mount     *mount = req->channel->mount;
    struct chimera_vfs_state      *state = chimera_fuse_vfs_state(req);
    struct chimera_fuse_open_file *file  = chimera_fuse_file(in->fh);
    struct chimera_fuse_lock_file *lf;

    pthread_mutex_lock(&mount->lock_lock);

    lf = chimera_fuse_lock_file_get(mount, state, in->owner,
                                    file->handle, 0);

    if (lf) {
        chimera_fuse_lock_trim(state, lf, in->lk.start, in->lk.end, NULL);
        chimera_fuse_lock_file_maybe_free(mount, state, lf);
    }

    pthread_mutex_unlock(&mount->lock_lock);

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
    int                            wait, expected;

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

    pthread_mutex_lock(&mount->lock_lock);

    lf = chimera_fuse_lock_file_get(mount, state, in->owner,
                                    file->handle, 1);

    if (!lf) {
        pthread_mutex_unlock(&mount->lock_lock);
        free(entry);
        chimera_fuse_reply(req, EIO, NULL, 0);
        return;
    }

    entry->lf = lf;
    lf->pending++;

    /*
     * F_SETLK must fail EAGAIN on a real lock conflict without waiting --
     * but a conflicting CACHING lease is not a lock: it is someone's read
     * cache (possibly this very mount's own invalidation grant, whose
     * owner identity deliberately differs from lock owners), it breaks in
     * milliseconds, and reporting it as EAGAIN would fabricate a lock that
     * does not exist.  So probe first: a hard conflict answers EAGAIN
     * immediately, anything else acquires with wait so breakable holders
     * are recalled and waited through.  A conflicting lock that lands in
     * the probe-to-acquire window turns this SETLK into a short wait; a
     * delayed success is more faithful than a spurious failure.
     */
    if (!wait) {
        struct chimera_vfs_claim_conflict conflict;
        enum chimera_vfs_claim_result     probe;

        memset(&conflict, 0, sizeof(conflict));

        probe = chimera_vfs_claim_test(lf->file_state, &entry->claim,
                                       &conflict);

        if (probe == CHIMERA_CLAIM_DENIED ||
            (probe == CHIMERA_CLAIM_BREAKING &&
             chimera_fuse_conflict_is_lock(&conflict))) {
            lf->pending--;
            chimera_fuse_lock_file_maybe_free(mount, state, lf);
            pthread_mutex_unlock(&mount->lock_lock);
            free(entry);
            chimera_fuse_reply(req, EAGAIN, NULL, 0);
            return;
        }
    }

    req->u.lock.entry        = entry;
    req->u.lock.lf           = lf;
    req->u.lock.result_errno = EIO;
    req->u.lock.wait         = wait;
    req->u.lock.start        = entry->start;
    req->u.lock.end          = entry->end;
    req->u.lock.exclusive    = entry->exclusive;
    atomic_store(&req->u.lock.phase, 0);

    /* Findable by FUSE_INTERRUPT while parked (any acquire may park at
     * least for the duration of a caching-lease break). */
    req->u.lock.parked = 1;
    DL_APPEND2(mount->parked_locks, req, u.lock.park_prev, u.lock.park_next);

    pthread_mutex_unlock(&mount->lock_lock);

    /* wait=true so breakable cache holders are recalled and waited through;
     * wait_hard=false so a genuine lock conflict still completes rather than
     * queueing forever (SETLKW's blocking is the caller's own retry). */
    chimera_vfs_claim_acquire(req->thread->vfs_thread, state, lf->file_state,
                              &entry->claim, &entry->ticket,
                              true, wait,
                              chimera_fuse_setlk_acquire_cb,
                              NULL,
                              req);

    expected = 0;

    if (!atomic_compare_exchange_strong(&req->u.lock.phase, &expected, 2)) {
        /* Callback already ran inline on this thread. */
        chimera_fuse_lock_finish(req);
    }
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

    pthread_mutex_lock(&mount->lock_lock);

    HASH_FIND(hh, mount->lock_files, &key, sizeof(key), lf);

    if (!lf) {
        pthread_mutex_unlock(&mount->lock_lock);
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

    pthread_mutex_unlock(&mount->lock_lock);
} /* chimera_fuse_locks_release_owner */

int
chimera_fuse_locks_interrupt(
    struct chimera_fuse_mount *mount,
    struct chimera_vfs_state  *state,
    uint64_t                   unique)
{
    struct chimera_fuse_request *parked;
    bool                         cancelled = false;

    pthread_mutex_lock(&mount->lock_lock);

    DL_FOREACH2(mount->parked_locks, parked, u.lock.park_next)
    {
        if (parked->unique == unique) {
            break;
        }
    }

    if (!parked) {
        pthread_mutex_unlock(&mount->lock_lock);
        return 0;
    }

    cancelled = chimera_vfs_claim_cancel(state,
                                         &parked->u.lock.entry->ticket);

    if (cancelled) {
        parked->u.lock.result_errno = EINTR;
    }

    pthread_mutex_unlock(&mount->lock_lock);

    if (cancelled) {
        /* The grant callback will never fire; complete with EINTR on the
         * request's owning thread. */
        chimera_fuse_resume_post(parked);
    }

    /* Either way the interrupt is handled: not cancelled means the grant
     * won the race and the reply is already on its way. */
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

    /* Cancel parked acquires first; each cancelled request completes with
     * EINTR on its owning thread (still alive: stop() runs before the
     * thread pool is torn down). */
    pthread_mutex_lock(&mount->lock_lock);

    DL_FOREACH_SAFE2(mount->parked_locks, parked, ptmp, u.lock.park_next)
    {
        if (chimera_vfs_claim_cancel(state,
                                     &parked->u.lock.entry->ticket)) {
            parked->u.lock.result_errno = EINTR;
            pthread_mutex_unlock(&mount->lock_lock);
            chimera_fuse_resume_post(parked);
            pthread_mutex_lock(&mount->lock_lock);
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

    pthread_mutex_unlock(&mount->lock_lock);
} /* chimera_fuse_locks_shutdown */
