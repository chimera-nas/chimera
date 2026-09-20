// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "common/thread.h"
#include <stdlib.h>
#include <string.h>

#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_session.h"
#include "smb_async_interim.h"
#include "smb_common/smb2.h"
#include "common/misc.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_claim.h"
#include "vfs/vfs_compound.h"

#define SMB2_LOCK_REQUEST_SIZE        48 /* fixed; LockCount==1 in this build */
#define SMB2_LOCK_REPLY_SIZE          4

/* Per MS-SMB2 §2.2.26.1 */
#define SMB2_LOCKFLAG_SHARED_LOCK     0x00000001
#define SMB2_LOCKFLAG_EXCLUSIVE       0x00000002
#define SMB2_LOCKFLAG_UNLOCK          0x00000004
#define SMB2_LOCKFLAG_FAIL_IMM        0x00000010

#define SMB2_LOCKFLAG_KIND_MASK       (SMB2_LOCKFLAG_SHARED_LOCK | \
                                       SMB2_LOCKFLAG_EXCLUSIVE   | \
                                       SMB2_LOCKFLAG_UNLOCK)

/*
 * The shape of a LOCK request as a VFS sequence: one PUTHANDLE lending the
 * open's own handle, then one CLAIM per lock element.  A SHARED / EXCLUSIVE
 * element is a claim; an UNLOCK element is a claim RELEASE, which is not a
 * sequence op at all, so an unlock request builds no run (see chimera_smb_lock
 * and the unlock arm of chimera_smb_lock_multi).
 *
 * Every request chimera accepts fits in ONE run: the parser rejects a LockCount
 * above CHIMERA_SMB_LOCK_MAX_ELEMENTS, so the widest sequence is 1 + 16 = 17 of
 * the executor's CHIMERA_VFS_COMPOUND_MAX_OPS (32).  There is therefore no
 * split-across-consecutive-runs path -- which is the better answer as well as
 * the simpler one, because the multi-element rollback IS the run: splitting
 * would put the abort release on the wrong side of the boundary and hand the
 * rollback back to this file.  The assertion keeps that true if either constant
 * moves.
 */
#define CHIMERA_SMB_LOCK_OP_PUTHANDLE 0
#define CHIMERA_SMB_LOCK_OP_CLAIM(i) (1 + (i))

_Static_assert(1 + CHIMERA_SMB_LOCK_MAX_ELEMENTS <=
               CHIMERA_VFS_COMPOUND_MAX_OPS,
               "an SMB2 LOCK request must fit in one VFS sequence");

/*
 * Guards open_file->parked_lock_req and the cancel_status / run pair beside it.
 *
 * One process-wide mutex rather than a per-thread or per-open one, because the
 * two sides it serializes genuinely run anywhere: a sequence's completion runs
 * on the thread that submitted the LOCK, while the three cancel triggers arrive
 * wherever their own event landed -- an SMB2 CANCEL on another channel, a tree
 * disconnect or logoff on whichever thread saw the disconnect.  The rule the
 * cross-thread cancel asks for is exactly that a post be made while the run is
 * known not to have been freed: the run leaves the parked slot only in its own
 * completion, which takes this lock, and a post is made under it.
 *
 * It is taken only when a blocking lock parks, is cancelled, or completes, and
 * never while any VFS lock is held -- the completion drops it before releasing
 * anything, and chimera_vfs_compound_cancel_post arbitrates nothing and takes
 * no claim-core lock.
 */
static evpl_mutex_t chimera_smb_lock_park_lock = EVPL_MUTEX_INITIALIZER;

/* MS-SMB2 3.3.5.14 LockSequence replay detection.  The 32-bit LockSequence is a
 * 28-bit LockSequenceNumber (the "bucket", valid 1..64) in bits [4..31] and a
 * 4-bit LockSequenceIndex in bits [0..3].  Replay verification is performed only
 * for a durable / persistent / resilient handle, or on an SMB 3.x connection
 * that negotiated multichannel (per the spec and Note <314>). */
static inline uint32_t
chimera_smb_lock_seq_bucket(uint32_t lock_sequence)
{
    /* The full 28-bit LockSequenceNumber (bits 4..31); the 1..64 range check at
     * the call site rejects out-of-extent values instead of aliasing them via an
     * 8-bit truncation (MS-SMB2 2.2.26 / 3.3.5.14). */
    return lock_sequence >> 4;
} /* chimera_smb_lock_seq_bucket */

static inline uint8_t
chimera_smb_lock_seq_index(uint32_t lock_sequence)
{
    return (uint8_t) (lock_sequence & 0x0F);
} /* chimera_smb_lock_seq_index */

/* True when this open performs LockSequence replay verification (MS-SMB2
 * 3.3.5.14): a durable, persistent, or resilient handle, or any handle on an
 * SMB 3.x connection that negotiated SMB2_GLOBAL_CAP_MULTI_CHANNEL. */
static inline bool
chimera_smb_lock_replay_active(
    struct chimera_smb_request   *request,
    struct chimera_smb_open_file *open_file)
{
    struct chimera_smb_conn *conn = request->compound->conn;

    if (open_file->durable_flags || open_file->resilient ||
        (open_file->flags & CHIMERA_SMB_OPEN_FILE_PERSISTED)) {
        return true;
    }
    return conn && conn->dialect >= 0x300 &&
           (conn->capabilities & SMB2_GLOBAL_CAP_MULTI_CHANNEL);
} /* chimera_smb_lock_replay_active */

/* Record the outcome `status` of a non-replayed single-element lock op in the
 * open's per-bucket replay cache.  No-op when the request carried no valid
 * bucket (replay inactive, or bucket 0 / >64). */
static inline void
chimera_smb_lock_seq_record(
    struct chimera_smb_request   *request,
    struct chimera_smb_open_file *open_file,
    uint32_t                      status)
{
    uint8_t b = request->lock.seq_bucket;

    if (b >= 1 && b <= 64) {
        open_file->lock_seq_valid[b - 1]  = 1;
        open_file->lock_seq_index[b - 1]  = request->lock.seq_index;
        open_file->lock_seq_status[b - 1] = status;
    }
} /* chimera_smb_lock_seq_record */

/* Complete a multi-element LOCK/UNLOCK: record the outcome in the replay cache
 * (so a retransmit on a durable/persistent/resilient/multichannel handle returns
 * the cached status without re-applying the ranges -- issue #1119), drop the
 * handle, and reply.  seq_record is a no-op unless the dispatch captured a valid
 * replay bucket, so this is also correct for non-replay-active opens. */
static inline void
chimera_smb_lock_multi_complete(
    struct chimera_smb_request   *request,
    struct chimera_smb_open_file *open_file,
    uint32_t                      status)
{
    chimera_smb_lock_seq_record(request, open_file, status);
    chimera_smb_open_file_release(request, open_file);
    chimera_smb_complete_request(request, status);
} /* chimera_smb_lock_multi_complete */

/* Half-open byte-range overlap, mirroring chimera_vfs_range_overlap: length 0
 * is a genuine zero-byte range and the exclusive end saturates at UINT64_MAX on
 * overflow.  Used for the SMB same-handle conflict rule, which the VFS conflict
 * matrix (POSIX: an owner never conflicts with itself) does not enforce. */
static inline bool
smb_lock_ranges_overlap(
    uint64_t a_off,
    uint64_t a_len,
    uint64_t b_off,
    uint64_t b_len)
{
    return chimera_vfs_claim_range_overlap_i(a_off, a_len, b_off, b_len);
} /* smb_lock_ranges_overlap */

/* One held byte-range, owned by the open_file.  Released on UNLOCK or
 * at close via chimera_smb_open_file_drain_locks. */
struct chimera_smb_lock_entry {
    struct chimera_vfs_claim           lease;
    struct chimera_vfs_pending_acquire ticket;
    struct chimera_vfs_file_state     *file_state;
    struct chimera_smb_open_file      *open_file;
    /* Set while the acquire is in flight; cleared once cb has fired. */
    struct chimera_smb_request        *pending_req;
    bool                               lease_inserted;
    struct chimera_smb_lock_entry     *prev;
    struct chimera_smb_lock_entry     *next;
};

SYMBOL_EXPORT void
chimera_smb_open_file_drain_locks(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_open_file     *open_file)
{
    struct chimera_smb_lock_entry *entry, *tmp;
    struct chimera_vfs_state      *vfs_state = thread->vfs_thread->vfs->vfs_state;

    /* A pending CREATE can resume as soon as the share reservation is removed.
     * Drop the cache grant in the same critical section, so that CREATE cannot
     * cap its oplock against a lease belonging to the closing open. Detach the
     * protocol member first so a break cannot notify it during teardown. */
    if (open_file->grant) {
        chimera_smb_grant_remove_member(open_file->grant, open_file);
    }
    if (open_file->share_lease_inserted) {
        chimera_vfs_claim_release_open(vfs_state, open_file->share_file_state,
                                       &open_file->share_lease, open_file->grant);
        open_file->share_lease_inserted = false;
    } else if (open_file->grant) {
        chimera_vfs_claim_grant_release(vfs_state, open_file->grant, true /*pump*/);
    }
    open_file->grant                  = NULL;
    open_file->caching_lease_inserted = false;
    if (open_file->share_file_state) {
        chimera_vfs_state_put(vfs_state, open_file->share_file_state);
        open_file->share_file_state = NULL;
    }

    /* A named stream's file-level DELETE reservation on the base file's state
     * (smb2.streams.delete). */
    if (open_file->base_share_lease_inserted) {
        chimera_vfs_claim_release(vfs_state, open_file->base_share_file_state,
                                  &open_file->base_share_lease);
        chimera_vfs_state_stream_holder_dec(open_file->base_share_file_state);
        open_file->base_share_lease_inserted = false;
    }
    if (open_file->base_share_file_state) {
        chimera_vfs_state_put(vfs_state, open_file->base_share_file_state);
        open_file->base_share_file_state = NULL;
    }

    if (open_file->caching_file_state) {
        chimera_vfs_state_put(vfs_state, open_file->caching_file_state);
        open_file->caching_file_state = NULL;
    }

    if (!open_file->lock_entries) {
        return;
    }

    /* Detach the whole list head before walking — the loop frees each
     * entry, so leaving them linked would invite use-after-free both at
     * the analyzer level and in any concurrent iteration. */
    entry                   = open_file->lock_entries;
    open_file->lock_entries = NULL;

    while (entry) {
        tmp = entry->next;
        if (entry->lease_inserted) {
            chimera_vfs_claim_release_ranged(thread->vfs_thread, vfs_state,
                                             entry->file_state, &entry->lease);
        }
        if (entry->file_state) {
            chimera_vfs_state_put(vfs_state, entry->file_state);
        }
        free(entry);
        entry = tmp;
    }
} /* chimera_smb_open_file_drain_locks */

int
chimera_smb_parse_lock(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{
    uint16_t reserved_lock_seq_lo = 0;
    uint16_t reserved_lock_seq_hi = 0;
    uint32_t reserved;

    if (unlikely(request->request_struct_size != SMB2_LOCK_REQUEST_SIZE)) {
        chimera_smb_error("Received SMB2 LOCK request with invalid struct size (%u expected %u)",
                          request->request_struct_size,
                          SMB2_LOCK_REQUEST_SIZE);
        request->status = SMB2_STATUS_INVALID_PARAMETER;
        return -1;
    }

    int prc = 0;
    prc |= evpl_iovec_cursor_try_get_uint16(request_cursor, &request->lock.lock_count);
    /* LockSequenceNumber is 4 bits + LockSequenceIndex 28 bits = 32 bits.
     * We don't replay-detect by sequence in Stage B; just store. */
    prc |= evpl_iovec_cursor_try_get_uint16(request_cursor, &reserved_lock_seq_lo);
    prc |= evpl_iovec_cursor_try_get_uint16(request_cursor, &reserved_lock_seq_hi);
    prc |= evpl_iovec_cursor_try_get_uint64(request_cursor, &request->lock.file_id.pid);
    prc |= evpl_iovec_cursor_try_get_uint64(request_cursor, &request->lock.file_id.vid);

    if (unlikely(prc)) {
        chimera_smb_error("Received SMB2 LOCK request truncated in fixed body");
        return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
    }

    /* Combine the LockSequence halves only once every field parsed cleanly, so a
     * short read can never feed an uninitialized value into the shift/or. */
    request->lock.lock_sequence = ((uint32_t) reserved_lock_seq_lo) |
        (((uint32_t) reserved_lock_seq_hi) << 16);

    /* Read the lock elements (24 bytes each: Offset, Length, Flags, Reserved).
     * A zero-lock request (which smbtorture sends to probe rejection) carries
     * no element; an over-long LockCount is flagged so the handler can reply
     * INVALID_PARAMETER rather than tearing the connection down with a -1.  The
     * first element is mirrored into l_offset/l_length/l_flags for the common
     * single-lock path.  The elements are pulled with the bounds-checked reader
     * so a LockCount that runs past the received bytes rejects cleanly. */
    request->lock.l_offset      = 0;
    request->lock.l_length      = 0;
    request->lock.l_flags       = 0;
    request->lock.lock_too_many = false;

    if (request->lock.lock_count >= 1 &&
        request->lock.lock_count <= CHIMERA_SMB_LOCK_MAX_ELEMENTS) {
        for (uint16_t i = 0; i < request->lock.lock_count; i++) {
            prc |= evpl_iovec_cursor_try_get_uint64(request_cursor,
                                                    &request->lock.elements[i].offset);
            prc |= evpl_iovec_cursor_try_get_uint64(request_cursor,
                                                    &request->lock.elements[i].length);
            prc |= evpl_iovec_cursor_try_get_uint32(request_cursor,
                                                    &request->lock.elements[i].flags);
            prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &reserved);
        }

        if (unlikely(prc)) {
            chimera_smb_error("Received SMB2 LOCK elements past message");
            return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
        }

        request->lock.l_offset = request->lock.elements[0].offset;
        request->lock.l_length = request->lock.elements[0].length;
        request->lock.l_flags  = request->lock.elements[0].flags;
    } else if (request->lock.lock_count > CHIMERA_SMB_LOCK_MAX_ELEMENTS) {
        request->lock.lock_too_many = true;
    }

    return 0;
} /* chimera_smb_parse_lock */

/* Release (if it was inserted) and free one lock entry.  The entry's claim is
 * BORROWED by the sequence for as long as it runs, so this is only ever reached
 * from the completion or from a build that never submitted. */
static void
chimera_smb_lock_entry_free(
    struct chimera_server_smb_thread *thread,
    struct chimera_vfs_state         *vfs_state,
    struct chimera_smb_lock_entry    *entry)
{
    if (entry->lease_inserted) {
        chimera_vfs_claim_release_ranged(thread->vfs_thread, vfs_state,
                                         entry->file_state, &entry->lease);
    }
    if (entry->file_state) {
        chimera_vfs_state_put(vfs_state, entry->file_state);
    }
    free(entry);
} /* chimera_smb_lock_entry_free */

/* How a sequence's failure reads on the wire.  A CLAIM the arbiter refused
 * stops the run EAGAIN, which MS-SMB2 3.3.5.14 maps to STATUS_LOCK_NOT_GRANTED
 * (Windows accepts _NOT_GRANTED for the _RANGE variant).  Everything else is a
 * failure to reach the object at all -- the PUTHANDLE refusing the lent handle,
 * a backend error -- and answers for itself. */
static uint32_t
chimera_smb_lock_error_status(enum chimera_vfs_error error)
{
    switch (error) {
        case CHIMERA_VFS_EAGAIN:
        case CHIMERA_VFS_EACCES:
            return SMB2_STATUS_LOCK_NOT_GRANTED;
        case CHIMERA_VFS_EBADF:
        case CHIMERA_VFS_ESTALE:
            return SMB2_STATUS_FILE_CLOSED;
        case CHIMERA_VFS_ENOSPC:
        case CHIMERA_VFS_EMFILE:
            return SMB2_STATUS_INSUFFICIENT_RESOURCES;
        case CHIMERA_VFS_EINVAL:
            return SMB2_STATUS_INVALID_PARAMETER;
        default:
            return SMB2_STATUS_LOCK_NOT_GRANTED;
    } /* switch */
} /* chimera_smb_lock_error_status */

/*
 * Ask the VFS to take back the sequence a blocking LOCK parked on `open_file`
 * is waiting in, and to tell the client `status`.
 *
 * The cancel is POSTED, never arbitrated here: this is reached from an SMB2
 * CANCEL on another channel, from a CLOSE, and from a tree-disconnect / logoff
 * / connection teardown that holds the tree's open-file bucket lock, and a
 * completion that replies, frees and recycles the LOCK's request must not run
 * inside any of them.  So nothing of the parked request's finishes in this
 * call; its own sequence completes on the thread that submitted it.
 *
 * WHICH RACER ANSWERS THE CLIENT.  A queued ticket is cancellable right up to
 * the moment the claim core answers it, and which of the two won is the core's
 * arbitration -- never a guess made here.  That answer now reaches this file
 * through the sequence's completion status: CHIMERA_VFS_ECANCELED when the
 * cancel took the park back, and the run's real outcome when the grant was
 * already in flight.  `cancel_status` is recorded BEFORE the post so the
 * completion sees it either way, and a grant that won the race is released
 * again and the client still told `status` -- exactly what the hand-rolled
 * abort did with a ticket that would not dequeue, because the event that asked
 * for the cancel (the handle is closing, the tree is gone) has already moved
 * past the point where a lock could be held.
 */
SYMBOL_EXPORT void
chimera_smb_lock_cancel_parked(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_open_file     *open_file,
    uint32_t                          status)
{
    struct chimera_smb_request *request;

    (void) thread;

    evpl_mutex_lock(&chimera_smb_lock_park_lock);

    request = open_file->parked_lock_req;

    if (request && request->lock.run) {
        request->lock.cancel_status = status;
        chimera_vfs_compound_cancel_post(request->lock.run);
    }

    evpl_mutex_unlock(&chimera_smb_lock_park_lock);
} /* chimera_smb_lock_cancel_parked */

SYMBOL_EXPORT struct chimera_smb_request *
chimera_smb_lock_abort_parked(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_open_file     *open_file)
{
    /* Closing a handle whose blocking lock is still pending completes that lock
     * with RANGE_NOT_LOCKED (MS-SMB2 / smb2.lock.cancel "cancel by close"; the
     * tree-disconnect and logoff variants accept RANGE_NOT_LOCKED too). */
    chimera_smb_lock_cancel_parked(thread, open_file,
                                   SMB2_STATUS_RANGE_NOT_LOCKED);

    /* NULL always: the parked LOCK replies from its own sequence's completion,
     * so there is nothing here for the caller to finish. */
    return NULL;
} /* chimera_smb_lock_abort_parked */

SYMBOL_EXPORT void
chimera_smb_lock_park_finish(
    struct chimera_smb_request *request,
    uint32_t                    status)
{
    /* Retired with the hand-rolled park.  Reached only from a caller still
     * carrying the old "abort, then finish what it returned" shape, which now
     * gets NULL and never calls this; left as a cancel post so that a caller
     * which does reach it asks for the right thing rather than completing a
     * request the sequence still owns. */
    evpl_mutex_lock(&chimera_smb_lock_park_lock);

    if (request->lock.parked && request->lock.run) {
        request->lock.cancel_status = status;
        chimera_vfs_compound_cancel_post(request->lock.run);
    }

    evpl_mutex_unlock(&chimera_smb_lock_park_lock);
} /* chimera_smb_lock_park_finish */

/* Build one lock element's entry: the range claim, its ticket, and the per-file
 * claim state a later release needs.  Nothing is acquired here -- the CLAIM ops
 * of the run do that -- so a built entry owns only its own state reference
 * until the sequence answers. */
static struct chimera_smb_lock_entry *
chimera_smb_lock_entry_alloc(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_request       *request,
    struct chimera_smb_open_file     *open_file,
    uint64_t                          offset,
    uint64_t                          length,
    bool                              exclusive)
{
    struct chimera_vfs_state      *vfs_state = thread->vfs_thread->vfs->vfs_state;
    struct chimera_smb_lock_entry *entry;
    struct chimera_claim_owner     owner;

    entry = calloc(1, sizeof(*entry));
    if (!entry) {
        return NULL;
    }

    entry->file_state = chimera_vfs_state_get(vfs_state,
                                              open_file->handle->fh,
                                              open_file->handle->fh_len,
                                              open_file->handle->fh_hash, true);
    if (!entry->file_state) {
        free(entry);
        return NULL;
    }

    entry->open_file   = open_file;
    entry->pending_req = request;

    memset(&owner, 0, sizeof(owner));
    owner.proto      = CHIMERA_CLAIM_PROTO_SMB2;
    owner.client_key = request->session_handle->session->client_key;
    /* The owner identity is the open — different opens (even by the same
     * client) get different owner_lo/owner_hi and lock independently, matching
     * Windows handle-based lock semantics. */
    owner.owner_lo = open_file->file_id.pid;
    owner.owner_hi = open_file->file_id.vid;
    /* Carry the open's grant LeaseKey (when it holds one) so the range-vs-
     * caching displacement can tell that a byte-range lock and the same open's
     * -- or a coalesced peer's -- caching lease (whose owner identity is the
     * lease key, not the file id) belong to one grant and must not break each
     * other (smb2.oplock.brl2). */
    if (open_file->grant) {
        memcpy(owner.key, open_file->grant->claim.owner.key, 16);
    }

    chimera_vfs_claim_init_range(&entry->lease, exclusive, true /* smb */,
                                 offset, length, &owner);
    /* HOLDER-circle anchor for the brl2 exemptions and the MAND holder-exempt
     * row.  A range claim is never op_handle-stamped by the executor -- open
     * handles are cached per (fh, access mode, cred) and shared, so a stamp it
     * made would fold distinct lock owners into one holder -- so the stamp SMB
     * wants, against the handle it knows is this open's, is made here. */
    entry->lease.op_handle  = open_file->handle;
    entry->lease.policy_tag = open_file->file_id.pid;

    return entry;
} /* chimera_smb_lock_entry_alloc */

/*
 * The run has parked: the first CLAIM that could not be answered queued its
 * ticket, either behind a caching holder that is still breaking or behind
 * another owner's incompatible range.  This is the SMB2 blocking lock (MS-SMB2
 * 3.3.5.14): emit the STATUS_PENDING interim the client is waiting for and
 * record the park on the open so a CLOSE, a tree disconnect, a logoff, a
 * connection teardown or an SMB2 CANCEL can find the run and post against it.
 *
 * Runs on the submitting thread, from inside the acquire that is parking.
 */
static void
chimera_smb_lock_park_cb(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    void                        *private_data)
{
    struct chimera_smb_request *request = private_data;

    (void) compound;
    (void) index;

    evpl_mutex_lock(&chimera_smb_lock_park_lock);
    request->lock.parked                     = 1;
    request->lock.open_file->parked_lock_req = request;
    evpl_mutex_unlock(&chimera_smb_lock_park_lock);

    chimera_smb_async_interim_begin(request);
} /* chimera_smb_lock_park_cb */

/*
 * The run is over, on the thread that submitted it.
 *
 * WHAT THE SEQUENCE DID.  chimera_vfs_compound_take_file_state answers non-NULL
 * for exactly the CLAIMs that were GRANTED in a run that finished OK.  On any
 * other outcome -- a later element denied, a cancel that took the park back --
 * the executor has already released every claim its own CLAIMs inserted, which
 * IS the multi-element rollback this file used to hand-roll: n CLAIMs go out
 * TRY, and if element k denies, the k-1 already granted are released before
 * this callback is entered.
 *
 * THE FILE STATE.  The state handed over is the same per-file state the entry
 * already holds a reference on (chimera_vfs_state_get is refcounted per fh, and
 * the executor resolved it from the same handle the entry did), so it is taken
 * and put straight back.  The ENTRY's reference is the one to keep: it is taken
 * when the entry is built and put when the entry is freed, so it covers the
 * entry's whole life -- including the refused acquire that never reached a
 * GRANTED op -- and it is the one every release path that never ran through a
 * sequence already uses (UNLOCK, and chimera_smb_open_file_drain_locks).
 */
static void
chimera_smb_lock_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request       *request   = private_data;
    struct chimera_server_smb_thread *thread    = request->compound->thread;
    struct chimera_vfs_state         *vfs_state = thread->vfs_thread->vfs->vfs_state;
    struct chimera_smb_open_file     *open_file = request->lock.open_file;
    struct chimera_smb_lock_entry    *entry;
    struct chimera_vfs_file_state    *taken;
    enum chimera_vfs_error            error;
    uint32_t                          status, cancel_status;
    uint16_t                          i;

    error = chimera_vfs_compound_status(compound);

    /* Leave the parked slot, and read the cancel intent, under the lock the
     * cancel triggers post from: a run leaves that slot only here, so a post
     * made while the slot still named this request cannot land on a run that
     * has been freed. */
    evpl_mutex_lock(&chimera_smb_lock_park_lock);
    if (open_file->parked_lock_req == request) {
        open_file->parked_lock_req = NULL;
    }
    cancel_status        = request->lock.cancel_status;
    request->lock.parked = 0;
    request->lock.run    = NULL;
    evpl_mutex_unlock(&chimera_smb_lock_park_lock);

    for (i = 0; i < request->lock.nentries; i++) {
        entry = request->lock.entries[i];
        taken = chimera_vfs_compound_take_file_state(
            compound, CHIMERA_SMB_LOCK_OP_CLAIM(i));

        if (taken) {
            entry->lease_inserted = true;
            chimera_vfs_state_put(vfs_state, taken);
        }
        entry->pending_req = NULL;
    }

    chimera_vfs_compound_free(compound);

    if (error == CHIMERA_VFS_OK) {
        status = SMB2_STATUS_SUCCESS;
    } else if (error == CHIMERA_VFS_ECANCELED) {
        status = cancel_status ? cancel_status : SMB2_STATUS_CANCELLED;
    } else {
        status = chimera_smb_lock_error_status(error);
    }

    /* A cancel that LOST the race still decides what the client is told: the
     * close or teardown that asked for it has already gone past the point where
     * this open could hold a lock, so release the grant that landed and answer
     * with the cancel's status. */
    if (cancel_status && status == SMB2_STATUS_SUCCESS) {
        status = cancel_status;
    }

    if (status == SMB2_STATUS_SUCCESS) {
        for (i = 0; i < request->lock.nentries; i++) {
            DL_APPEND(open_file->lock_entries, request->lock.entries[i]);
        }
    } else {
        for (i = 0; i < request->lock.nentries; i++) {
            chimera_smb_lock_entry_free(thread, vfs_state,
                                        request->lock.entries[i]);
        }
    }
    request->lock.nentries = 0;

    /* Cache the outcome for LockSequence replay (issue #1119).  An aborted or
     * cancelled lock is not a real outcome and is not recorded. */
    if (!cancel_status) {
        chimera_smb_lock_seq_record(request, open_file, status);
    }

    chimera_smb_open_file_release(request, open_file);
    chimera_smb_complete_request(request, status);
} /* chimera_smb_lock_complete */

/*
 * Submit the run: PUTHANDLE, then one CLAIM per element already built on
 * request->lock.entries.
 *
 * `wait` is the single blocking lock -- an SMB2 LOCK without FAIL_IMMEDIATELY,
 * which must complete only once the conflicting range is released rather than
 * bounce back denied, so it rides out a breaking caching holder (WAIT) and
 * queues behind another owner's incompatible lock too (WAIT_HARD).  Everything
 * else is TRY: FAIL_IMMEDIATELY says so outright, and a multi-element request
 * is all-or-nothing, which is what the abort release gives a run of TRY CLAIMs.
 *
 * `pre` is the read-cache revocation a lock request performs (MS-SMB2 3.3.4.x /
 * [MS-FSA] 2.1.5.1.2: a cached reader cannot be trusted once another open
 * coordinates access via locks -- break to NONE, no acknowledgment).  It rides
 * as the CLAIM's own pre trigger rather than a separate invalidate call, and
 * the actor is derived from the claim: the locker self-exempts at the KEY
 * circle through owner.key (its RqLs lease key) and at the HOLDER circle
 * through op_handle, so a client locking through its own caching handle does
 * not break itself.
 */
static void
chimera_smb_lock_submit(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_request       *request,
    struct chimera_smb_open_file     *open_file,
    bool                              wait,
    uint8_t                           pre)
{
    struct chimera_vfs_compound *compound;
    unsigned int                 flags;
    uint16_t                     i;

    compound = chimera_vfs_compound_alloc(
        thread->vfs_thread, &request->session_handle->session->cred);

    /* The open's own handle, lent with the flags it was really opened with.  A
     * claim is arbitrated per FILE and uses only the fh, so a data handle
     * serves the CLAIM's PATH want and nothing is re-opened. */
    chimera_vfs_compound_add_puthandle(compound, open_file->handle,
                                       open_file->open_flags);

    flags = wait
        ? (CHIMERA_VFS_COMPOUND_CLAIM_WAIT |
           CHIMERA_VFS_COMPOUND_CLAIM_WAIT_HARD)
        : CHIMERA_VFS_COMPOUND_CLAIM_TRY;

    for (i = 0; i < request->lock.nentries; i++) {
        struct chimera_smb_lock_entry *entry = request->lock.entries[i];

        chimera_vfs_compound_add_claim(compound, &entry->lease, &entry->ticket,
                                       flags, pre, 0 /* pre_retain */,
                                       0 /* deny */, 0 /* deny_retain */);
    }

    /* Publish the run before it can park or be cancelled, and start it from a
     * known state: `parked` is what the SMB2 CANCEL path screens on, and a
     * recycled request must not carry a previous lock's into this one. */
    evpl_mutex_lock(&chimera_smb_lock_park_lock);
    request->lock.run           = compound;
    request->lock.cancel_status = 0;
    request->lock.parked        = 0;
    evpl_mutex_unlock(&chimera_smb_lock_park_lock);

    /* Registered before the submit: a run can park inside it. */
    chimera_vfs_compound_set_park_cb(compound, chimera_smb_lock_park_cb,
                                     request);

    chimera_vfs_compound_submit(compound, chimera_smb_lock_complete, request);
} /* chimera_smb_lock_submit */

/* Free every entry built for this request without submitting a run: the
 * build-time failure path, where nothing was ever claimed. */
static void
chimera_smb_lock_entries_discard(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_request       *request)
{
    struct chimera_vfs_state *vfs_state = thread->vfs_thread->vfs->vfs_state;
    uint16_t                  i;

    for (i = 0; i < request->lock.nentries; i++) {
        chimera_smb_lock_entry_free(thread, vfs_state,
                                    request->lock.entries[i]);
    }
    request->lock.nentries = 0;
} /* chimera_smb_lock_entries_discard */

/* Multi-element (LockCount>1) lock/unlock.  All elements share one direction,
 * taken from element 0 (MS-SMB2 3.3.5.14).
 *
 * A LOCK request validates every element's flags and range up front, then takes
 * them ATOMICALLY -- which is now what the sequence is for: n CLAIM ops in one
 * run, all TRY, and if element k denies, the run stops there and the executor
 * releases the k-1 claims its own earlier CLAIMs inserted.  That abort release
 * is exactly the rollback loop this function used to run by hand, and it is
 * safe for the reason a rolled-back acquire generally is: it blocked other
 * owners for a moment and handed them nothing they could act on.
 *
 * An UNLOCK request is not a run at all.  A release is not a sequence op -- it
 * pumps waiters, it is not reversible, and it must not sit behind an op that
 * can fail -- so each named range is released in order, out of band, stopping
 * with RANGE_NOT_LOCKED at the first range this open does not hold.
 *
 * Every element of a multi-element LOCK is FAIL_IMMEDIATELY: a blocking element
 * is not supported in a multi-element request. */
static void
chimera_smb_lock_multi(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_request       *request,
    struct chimera_smb_open_file     *open_file)
{
    struct chimera_vfs_state *vfs_state = thread->vfs_thread->vfs->vfs_state;
    uint16_t                  n         = request->lock.lock_count;
    bool                      is_unlock;
    uint16_t                  i;

    is_unlock = (request->lock.elements[0].flags & SMB2_LOCKFLAG_UNLOCK) != 0;

    if (is_unlock) {
        struct chimera_smb_lock_entry *freelist = NULL;
        uint32_t                       status   = SMB2_STATUS_SUCCESS;

        for (i = 0; i < n; i++) {
            uint64_t                       off = request->lock.elements[i].offset;
            uint64_t                       len = request->lock.elements[i].length;
            struct chimera_smb_lock_entry *match = NULL, *e;

            /* Every element of an unlock request must itself be an unlock
             * (MS-SMB2 3.3.5.14): a lock element mixed in fails the request --
             * checked in order, so any earlier unlocks have already taken
             * effect (smb2.lock.multiple-unlock). */
            if (!(request->lock.elements[i].flags & SMB2_LOCKFLAG_UNLOCK)) {
                status = SMB2_STATUS_INVALID_PARAMETER;
                break;
            }

            DL_FOREACH(open_file->lock_entries, e)
            {
                if (e->lease.offset == off && e->lease.length == len) {
                    match = e;
                    break;
                }
            }
            if (!match) {
                status = SMB2_STATUS_RANGE_NOT_LOCKED;
                break;
            }

            /* Unlink and release the lock now (it is gone), but defer the free
             * past the loop: the static analyzer cannot follow utlist's
             * DL_DELETE and would otherwise treat a node freed here as still
             * reachable from the next iteration's traversal -- the same reason
             * chimera_smb_open_file_drain_locks frees from a detached list. */
            DL_DELETE(open_file->lock_entries, match);
            if (match->lease_inserted) {
                chimera_vfs_claim_release_ranged(thread->vfs_thread, vfs_state,
                                                 match->file_state,
                                                 &match->lease);
            }
            if (match->file_state) {
                chimera_vfs_state_put(vfs_state, match->file_state);
            }
            match->next = freelist;
            freelist    = match;
        }

        while (freelist) {
            struct chimera_smb_lock_entry *t = freelist->next;
            free(freelist);
            freelist = t;
        }

        chimera_smb_lock_multi_complete(request, open_file, status);
        return;
    }

    /* LOCK request: validate every element first. */
    for (i = 0; i < n; i++) {
        uint32_t kind = request->lock.elements[i].flags & SMB2_LOCKFLAG_KIND_MASK;
        uint64_t off  = request->lock.elements[i].offset;
        uint64_t len  = request->lock.elements[i].length;

        if (kind != SMB2_LOCKFLAG_SHARED_LOCK && kind != SMB2_LOCKFLAG_EXCLUSIVE) {
            chimera_smb_lock_multi_complete(request, open_file, SMB2_STATUS_INVALID_PARAMETER);
            return;
        }
        if (len && off > UINT64_MAX - (len - 1)) {
            chimera_smb_lock_multi_complete(request, open_file, SMB2_STATUS_INVALID_LOCK_RANGE);
            return;
        }
    }

    /* The ranges within a single lock request must not overlap each other
     * (MS-SMB2 3.3.5.14): an overlap is INVALID_PARAMETER, independent of lock
     * type (smb2.lock.valid-request locks two shared ranges at the same offset). */
    for (i = 0; i < n; i++) {
        for (uint16_t j = i + 1; j < n; j++) {
            if (smb_lock_ranges_overlap(request->lock.elements[i].offset,
                                        request->lock.elements[i].length,
                                        request->lock.elements[j].offset,
                                        request->lock.elements[j].length)) {
                chimera_smb_lock_multi_complete(request, open_file,
                                                SMB2_STATUS_INVALID_PARAMETER);
                return;
            }
        }
    }

    /* The SMB same-handle conflict rule, which the VFS matrix does not enforce
     * (POSIX: an owner never conflicts with itself): a new EXCLUSIVE element
     * conflicts with any range this handle already holds that it overlaps.  The
     * elements of this request cannot overlap EACH OTHER -- that was refused
     * just above -- so the check is against what the open held before the
     * request, and it is made here rather than per-acquire. */
    for (i = 0; i < n; i++) {
        struct chimera_smb_lock_entry *held;

        if (!(request->lock.elements[i].flags & SMB2_LOCKFLAG_EXCLUSIVE)) {
            continue;
        }

        DL_FOREACH(open_file->lock_entries, held)
        {
            if (smb_lock_ranges_overlap(held->lease.offset, held->lease.length,
                                        request->lock.elements[i].offset,
                                        request->lock.elements[i].length)) {
                chimera_smb_lock_multi_complete(request, open_file,
                                                SMB2_STATUS_LOCK_NOT_GRANTED);
                return;
            }
        }
    }

    /* Build every element's claim, then take them all in one run.  The rollback
     * is the executor's abort release; there is none to write here. */
    request->lock.nentries = 0;

    for (i = 0; i < n; i++) {
        bool                           excl = (request->lock.elements[i].flags & SMB2_LOCKFLAG_EXCLUSIVE) != 0;
        struct chimera_smb_lock_entry *e    =
            chimera_smb_lock_entry_alloc(thread, request, open_file,
                                         request->lock.elements[i].offset,
                                         request->lock.elements[i].length, excl);

        if (!e) {
            chimera_smb_lock_entries_discard(thread, request);
            chimera_smb_lock_multi_complete(request, open_file,
                                            SMB2_STATUS_INSUFFICIENT_RESOURCES);
            return;
        }
        request->lock.entries[request->lock.nentries++] = e;
    }

    chimera_smb_lock_submit(thread, request, open_file, false /* wait */,
                            0 /* pre */);
} /* chimera_smb_lock_multi */

void
chimera_smb_lock(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread    = request->compound->thread;
    struct chimera_vfs_state         *vfs_state = thread->vfs_thread->vfs->vfs_state;
    struct chimera_smb_open_file     *open_file;
    struct chimera_smb_lock_entry    *entry;
    uint32_t                          kind;
    uint64_t                          want_length;

    open_file = chimera_smb_open_file_resolve(request, &request->lock.file_id);
    if (unlikely(!open_file)) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }

    request->lock.open_file = open_file;

    /* Byte-range locking applies to a file (data stream); a directory handle has
     * no byte range to lock.  Reject LOCK against a directory FID with
     * STATUS_INVALID_DEVICE_REQUEST -- locking is not a directory operation
     * (MS-SMB2 3.3.5.14; issue #1254). */
    if (unlikely(open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY)) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_DEVICE_REQUEST);
        return;
    }

    /* MS-SMB2 3.3.5.14: a byte-range lock is a data operation, so the handle
     * must carry FILE_READ_DATA or FILE_WRITE_DATA.  A DELETE-only or
     * attribute-only handle has no business locking a range and is refused
     * ACCESS_DENIED.  Samba refuses these too (it answers INVALID_HANDLE, which
     * the spec does not sanction); chimera had no access check here at all and
     * granted the lock. */
    if (unlikely(!(open_file->granted_access &
                   (SMB2_FILE_READ_DATA | SMB2_FILE_WRITE_DATA)))) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_ACCESS_DENIED);
        return;
    }

    /* LockCount==0 is illegal, and more than CHIMERA_SMB_LOCK_MAX_ELEMENTS is
     * rejected rather than parsed; both reply INVALID_PARAMETER without dropping
     * the connection. */
    if (unlikely(request->lock.lock_count == 0 || request->lock.lock_too_many)) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    /* MS-SMB2 3.3.5.14 LockSequence replay detection.  Capture the replay bucket
     * and short-circuit a replay BEFORE dispatching either the single- or
     * multi-element path: the spec mandates replay verification for EVERY LOCK
     * request on a durable/persistent/resilient/multichannel handle regardless of
     * LockCount, and the multi path records its outcome under the same bucket
     * (issue #1119).  On a replay of the last op recorded under this bucket (same
     * index), return the cached status without re-applying the lock(s) — making a
     * retransmit after a lost reply idempotent. */
    request->lock.seq_bucket = 0;
    request->lock.seq_index  = chimera_smb_lock_seq_index(request->lock.lock_sequence);
    if (chimera_smb_lock_replay_active(request, open_file)) {
        uint32_t b = chimera_smb_lock_seq_bucket(request->lock.lock_sequence);

        if (b >= 1 && b <= 64) {
            request->lock.seq_bucket = b;
            if (open_file->lock_seq_valid[b - 1] &&
                open_file->lock_seq_index[b - 1] == request->lock.seq_index) {
                uint32_t cached = open_file->lock_seq_status[b - 1];
                chimera_smb_open_file_release(request, open_file);
                chimera_smb_complete_request(request, cached);
                return;
            }
        }
    }

    /* A multi-element request is handled synchronously (all FAIL_IMMEDIATELY). */
    if (request->lock.lock_count > 1) {
        chimera_smb_lock_multi(thread, request, open_file);
        return;
    }

    kind = request->lock.l_flags & SMB2_LOCKFLAG_KIND_MASK;
    if (kind != SMB2_LOCKFLAG_SHARED_LOCK &&
        kind != SMB2_LOCKFLAG_EXCLUSIVE &&
        kind != SMB2_LOCKFLAG_UNLOCK) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    /* An UNLOCK element must carry the UNLOCK flag and nothing else — not a
     * lock-type bit (caught above as a bad kind) and not FAIL_IMMEDIATELY
     * (MS-SMB2 2.2.26.1 / 3.3.5.14.2: FAIL_IMMEDIATELY is meaningful only for a
     * lock).  smbtorture smb2.lock.valid-request probes UNLOCK|FAIL_IMMEDIATELY
     * expecting INVALID_PARAMETER. */
    if (kind == SMB2_LOCKFLAG_UNLOCK &&
        (request->lock.l_flags & ~SMB2_LOCKFLAG_UNLOCK)) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    /* A byte range whose end strictly exceeds 2^64 is invalid (MS-SMB2
     * §3.3.5.14.{1,2}): reject it with INVALID_LOCK_RANGE before touching the
     * lock table.  A range that ends exactly at 2^64 (e.g. offset=2^64-1,
     * length=1, the last byte) is valid; length 0 is a zero-byte range. */
    if (request->lock.l_length && request->lock.l_offset >
        UINT64_MAX - (request->lock.l_length - 1)) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_LOCK_RANGE);
        return;
    }

    /* SMB2 length 0 is a legal zero-byte lock: it locks the empty range
     * [offset, offset) and conflicts only with a lock that strictly contains
     * that point.  The VFS range layer represents this directly (length 0 ==
     * zero bytes; to-EOF is UINT64_MAX), so pass the length through verbatim. */
    want_length = request->lock.l_length;

    if (kind == SMB2_LOCKFLAG_UNLOCK) {
        struct chimera_smb_lock_entry *match = NULL;
        DL_FOREACH(open_file->lock_entries, entry)
        {
            if (entry->lease.offset == request->lock.l_offset &&
                entry->lease.length == want_length) {
                match = entry;
                break;
            }
        }

        if (!match) {
            chimera_smb_lock_seq_record(request, open_file,
                                        SMB2_STATUS_RANGE_NOT_LOCKED);
            chimera_smb_open_file_release(request, open_file);
            /* No matching range — Windows returns RANGE_NOT_LOCKED. */
            chimera_smb_complete_request(request, SMB2_STATUS_RANGE_NOT_LOCKED);
            return;
        }

        DL_DELETE(open_file->lock_entries, match);
        if (match->lease_inserted) {
            chimera_vfs_claim_release_ranged(thread->vfs_thread, vfs_state,
                                             match->file_state, &match->lease);
        }
        if (match->file_state) {
            chimera_vfs_state_put(vfs_state, match->file_state);
        }
        free(match);

        chimera_smb_lock_seq_record(request, open_file, SMB2_STATUS_SUCCESS);
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
        return;
    }

    /* SMB same-handle conflict rule (MS-SMB2 / Windows): a new EXCLUSIVE lock
     * conflicts with any of this handle's own locks whose range it overlaps --
     * including re-locking an identical non-empty range (smb2.lock.errorcode).
     * A new SHARED lock never conflicts with the handle's own locks, so an
     * exclusive range can be re-locked shared and stacks (smb2.lock.unlock).  A
     * zero-length lock owns no bytes and so overlaps nothing, including another
     * zero-length lock at the same offset (smb2.lock.zerobytelength).  The VFS
     * conflict matrix skips a lock's own owner (POSIX semantics: a process never
     * conflicts with itself and overlapping locks coalesce), so the SMB rule is
     * enforced here before the (cross-handle) acquire. */
    if (kind == SMB2_LOCKFLAG_EXCLUSIVE) {
        struct chimera_smb_lock_entry *held;
        DL_FOREACH(open_file->lock_entries, held)
        {
            if (smb_lock_ranges_overlap(held->lease.offset, held->lease.length,
                                        request->lock.l_offset, want_length)) {
                chimera_smb_lock_seq_record(request, open_file,
                                            SMB2_STATUS_LOCK_NOT_GRANTED);
                chimera_smb_open_file_release(request, open_file);
                chimera_smb_complete_request(request,
                                             SMB2_STATUS_LOCK_NOT_GRANTED);
                return;
            }
        }
    }

    /* Build the single element's claim and take it in a one-CLAIM run.
     *
     * The read-cache revocation a lock request performs (MS-SMB2 3.3.4.x /
     * [MS-FSA] 2.1.5.1.2: a cached reader cannot be trusted once another open
     * coordinates access via locks -- break to NONE, no acknowledgment
     * required) rides as that CLAIM's `pre` trigger rather than a separate
     * invalidate call, so it fires in the same breath as the admission attempt
     * instead of a step before it.  The locker self-exempts through the claim's
     * own identity: owner.key carries its RqLs lease key, which is what the
     * KEY circle keys on, and op_handle is this open's handle for the HOLDER
     * circle -- so a client locking through its own caching handle does not
     * break itself.  (WPTS MS-SMB2Model BreakRead*HandleLease: after the handle
     * break settles the holder to R, a peer RANGE_LOCK must then break that R
     * lease to NONE.) */
    entry = chimera_smb_lock_entry_alloc(thread, request, open_file,
                                         request->lock.l_offset, want_length,
                                         kind == SMB2_LOCKFLAG_EXCLUSIVE);

    if (!entry) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INSUFFICIENT_RESOURCES);
        return;
    }

    request->lock.entries[0] = entry;
    request->lock.nentries   = 1;

    /* An SMB2 LOCK without FAIL_IMMEDIATELY is a blocking lock (MS-SMB2
     * 3.3.5.14): it must complete only once the conflicting range is released,
     * not bounce back denied, so its CLAIM rides out a breaking caching holder
     * AND queues behind another owner's incompatible range.  If it does queue,
     * the park callback emits the STATUS_PENDING interim and records the park;
     * either way the run answers through chimera_smb_lock_complete on this
     * thread. */
    chimera_smb_lock_submit(thread, request, open_file,
                            !(request->lock.l_flags & SMB2_LOCKFLAG_FAIL_IMM),
                            CHIMERA_TRIGGER_SMB_LOCK);
} /* chimera_smb_lock */

void
chimera_smb_lock_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{
    (void) request;
    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_LOCK_REPLY_SIZE);
    evpl_iovec_cursor_append_uint16(reply_cursor, 0);
} /* chimera_smb_lock_reply */
