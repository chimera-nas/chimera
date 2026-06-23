// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>

#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_session.h"
#include "smb_async_interim.h"
#include "smb2.h"
#include "common/misc.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_state.h"

#define SMB2_LOCK_REQUEST_SIZE    48   /* fixed; LockCount==1 in this build */
#define SMB2_LOCK_REPLY_SIZE      4

/* Sentinel stored in request->lock.resume_status before a (possibly blocking)
 * single-element acquire: chimera_smb_lock_acquire_cb overwrites it with the
 * real status if it fires synchronously, so a value still equal to this after
 * chimera_vfs_lease_acquire returns means the acquire parked on the VFS pending
 * queue and the callback will fire later. */
#define CHIMERA_SMB_LOCK_PENDING  0xFFFFFFFFu

/* Per MS-SMB2 §2.2.26.1 */
#define SMB2_LOCKFLAG_SHARED_LOCK 0x00000001
#define SMB2_LOCKFLAG_EXCLUSIVE   0x00000002
#define SMB2_LOCKFLAG_UNLOCK      0x00000004
#define SMB2_LOCKFLAG_FAIL_IMM    0x00000010

#define SMB2_LOCKFLAG_KIND_MASK   (SMB2_LOCKFLAG_SHARED_LOCK | \
                                   SMB2_LOCKFLAG_EXCLUSIVE   | \
                                   SMB2_LOCKFLAG_UNLOCK)

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
    __uint128_t a_end = (a_len == UINT64_MAX)
        ? ((__uint128_t) 1 << 64) : (__uint128_t) a_off + a_len;
    __uint128_t b_end = (b_len == UINT64_MAX)
        ? ((__uint128_t) 1 << 64) : (__uint128_t) b_off + b_len;

    return a_off < b_end && b_off < a_end;
} /* smb_lock_ranges_overlap */

/* One held byte-range, owned by the open_file.  Released on UNLOCK or
 * at close via chimera_smb_open_file_drain_locks. */
struct chimera_smb_lock_entry {
    struct chimera_vfs_lease           lease;
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

    /* Drop the SHARE-mode reservation first.  Release any byte-range
     * locks after — they may share the same file_state. */
    if (open_file->share_lease_inserted) {
        chimera_vfs_lease_release(vfs_state, open_file->share_file_state,
                                  &open_file->share_lease);
        open_file->share_lease_inserted = false;
    }
    if (open_file->share_file_state) {
        chimera_vfs_state_put(vfs_state, open_file->share_file_state);
        open_file->share_file_state = NULL;
    }

    /* A named stream's file-level DELETE reservation on the base file's state
     * (smb2.streams.delete). */
    if (open_file->base_share_lease_inserted) {
        chimera_vfs_lease_release(vfs_state, open_file->base_share_file_state,
                                  &open_file->base_share_lease);
        chimera_vfs_state_stream_holder_dec(open_file->base_share_file_state);
        open_file->base_share_lease_inserted = false;
    }
    if (open_file->base_share_file_state) {
        chimera_vfs_state_put(vfs_state, open_file->base_share_file_state);
        open_file->base_share_file_state = NULL;
    }

    /* Drop this open's reference on the caching grant (oplock / SMB2 lease).
     * On the grant's last reference the embedded lease is unlinked and the grant
     * freed; the per-file state reference (caching_file_state) is balanced
     * separately below. */
    if (open_file->grant) {
        /* Unthread this open from the grant's member list before dropping its
         * reference: once removed, the break callback will not try to notify a
         * closing open, and on the grant's last reference the lease is freed. */
        chimera_smb_grant_remove_member(open_file->grant, open_file);
        chimera_vfs_caching_grant_release(vfs_state, open_file->grant, true /*pump*/);
        open_file->grant                  = NULL;
        open_file->caching_lease_inserted = false;
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
            chimera_vfs_lease_release(vfs_state, entry->file_state, &entry->lease);
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

/* Map a finished single-element acquire to its SMB2 status and either install or
 * tear down the lock entry.  Caller owns completing the request afterwards. */
static uint32_t
chimera_smb_lock_settle_entry(
    struct chimera_vfs_state      *vfs_state,
    struct chimera_smb_request    *request,
    struct chimera_smb_open_file  *open_file,
    struct chimera_smb_lock_entry *entry,
    enum chimera_vfs_lease_result  result)
{
    if (result == CHIMERA_VFS_LEASE_GRANTED) {
        entry->lease_inserted = true;
        DL_APPEND(open_file->lock_entries, entry);
        return SMB2_STATUS_SUCCESS;
    }

    /* Tear the entry down.  Normally the lease was never inserted (a
     * FAIL_IMMEDIATELY denial or an unbreakable-caching conflict), but a parked
     * lock that was GRANTED by the VFS and then aborted in the same instant the
     * grant landed (handle close / teardown racing the conflict release) reaches
     * here with lease_inserted set — release the inserted lease so it is not
     * leaked into the VFS range table. */
    if (entry->lease_inserted) {
        chimera_vfs_lease_release(vfs_state, entry->file_state, &entry->lease);
    }
    chimera_vfs_state_put(vfs_state, entry->file_state);
    free(entry);
    request->lock.entry = NULL;
    /* MS-SMB2 §3.3.5.14 maps a lock denied by an existing range to
     * STATUS_LOCK_NOT_GRANTED (or _RANGE for SMB2 v.s. SMB1 spec
     * variants).  Use _NOT_GRANTED — accepted by Windows clients.  A waiting
     * lock never lands here DENIED (it parks); only a FAIL_IMMEDIATELY acquire
     * or an unbreakable-caching conflict does. */
    return SMB2_STATUS_LOCK_NOT_GRANTED;
} /* chimera_smb_lock_settle_entry */

/* Complete a parked blocking lock on its OWNING thread (driven by the resume
 * doorbell, the close/teardown abort path, or an SMB2 CANCEL).  `status` is the
 * stashed grant result, SMB2_STATUS_CANCELLED, or SMB2_STATUS_RANGE_NOT_LOCKED.
 * Unlinks the open's parked-lock reference, installs or tears down the entry,
 * and drops the open_file reference the park held. */
SYMBOL_EXPORT void
chimera_smb_lock_park_finish(
    struct chimera_smb_request *request,
    uint32_t                    status)
{
    struct chimera_server_smb_thread *thread    = request->compound->thread;
    struct chimera_vfs_state         *vfs_state = thread->vfs_thread->vfs->vfs_state;
    struct chimera_smb_lock_entry    *entry     = request->lock.entry;
    struct chimera_smb_open_file     *open_file = request->lock.open_file;

    /* open_file is always valid here: a park stores it on request->lock.open_file
     * and holds a reference that is dropped only by this function, and every
     * caller (resume doorbell, close/teardown abort, SMB2 CANCEL) reaches a
     * parked request via that same open_file. */
    request->lock.parked = 0;
    if (open_file->parked_lock_req == request) {
        open_file->parked_lock_req = NULL;
    }

    if (entry) {
        entry->pending_req = NULL;
        if (status == SMB2_STATUS_SUCCESS) {
            status = chimera_smb_lock_settle_entry(vfs_state, request, open_file,
                                                   entry, CHIMERA_VFS_LEASE_GRANTED);
        } else {
            /* Aborted/cancelled before grant: tear the entry down. */
            chimera_smb_lock_settle_entry(vfs_state, request, open_file,
                                          entry, CHIMERA_VFS_LEASE_DENIED);
        }
    }

    /* Cache a granted blocking-lock outcome for LockSequence replay (an aborted /
     * cancelled lock is not a real outcome and is not recorded). */
    if (status == SMB2_STATUS_SUCCESS) {
        chimera_smb_lock_seq_record(request, open_file, status);
    }

    chimera_smb_open_file_release(request, open_file);
    chimera_smb_complete_request(request, status);
} /* chimera_smb_lock_park_finish */

SYMBOL_EXPORT struct chimera_smb_request *
chimera_smb_lock_abort_parked(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_open_file     *open_file)
{
    struct chimera_vfs_state      *vfs_state = thread->vfs_thread->vfs->vfs_state;
    struct chimera_smb_request    *request   = open_file->parked_lock_req;
    struct chimera_smb_lock_entry *entry;

    if (!request) {
        return NULL;
    }

    open_file->parked_lock_req = NULL;
    entry                      = request->lock.entry;

    /* Try to pull the ticket off the VFS pending-acquire queue.  If it dequeues,
     * the grant callback will never fire and we own the request outright.  If it
     * does NOT dequeue, the grant raced in: the callback already fired (on this
     * or another thread), stashed SUCCESS, and queued the request on this
     * thread's lock_resume_head.  Both this abort and the resume drain run on the
     * owning thread, so unlink it from lock_resume_head here and abort it rather
     * than letting the drain complete it (the open is being torn down). */
    if (entry && !chimera_vfs_lease_acquire_cancel(vfs_state, &entry->ticket)) {
        struct chimera_smb_request **pp = &thread->lock_resume_head;

        pthread_mutex_lock(&thread->lease_break_lock);
        while (*pp) {
            if (*pp == request) {
                *pp = request->lock.lock_resume_next;
                request->lock.lock_resume_next = NULL;
                break;
            }
            pp = &(*pp)->lock.lock_resume_next;
        }
        pthread_mutex_unlock(&thread->lease_break_lock);

        /* The ticket did not dequeue because the grant already landed: the VFS
         * range lease IS inserted in the table even though the SMB-side
         * lease_inserted flag (set only by the GRANTED settle path) is still
         * clear.  Mark it so chimera_smb_lock_park_finish's teardown releases the
         * inserted lease instead of leaking it. */
        if (entry && request->lock.resume_status == SMB2_STATUS_SUCCESS) {
            entry->lease_inserted = true;
        }
    }

    /* Closing a handle whose blocking lock is still pending completes that lock
     * with RANGE_NOT_LOCKED (MS-SMB2 / smb2.lock.cancel "cancel by close"; the
     * tree-disconnect and logoff variants accept RANGE_NOT_LOCKED too). */
    request->lock.resume_status = SMB2_STATUS_RANGE_NOT_LOCKED;
    return request;
} /* chimera_smb_lock_abort_parked */

static void
chimera_smb_lock_acquire_cb(
    enum chimera_vfs_lease_result result,
    struct chimera_vfs_lease     *granted,
    struct chimera_vfs_lease     *conflict,
    void                         *private_data)
{
    struct chimera_smb_lock_entry    *entry   = private_data;
    struct chimera_smb_request       *request = entry->pending_req;
    struct chimera_server_smb_thread *thread  = request->compound->thread;
    uint32_t                          status;

    (void) granted;
    (void) conflict;

    status = (result == CHIMERA_VFS_LEASE_GRANTED)
        ? SMB2_STATUS_SUCCESS : SMB2_STATUS_LOCK_NOT_GRANTED;

    if (!request->lock.parked) {
        /* Synchronous outcome: the acquire has not returned yet.  Stash the
         * status and let chimera_smb_lock act on it inline (no interim was
         * emitted), so the common grant/deny path stays a single thread hop. */
        request->lock.resume_status = status;
        return;
    }

    /* Deferred grant: this lock had parked (interim already sent) and the
     * conflicting range was just released.  The release — and therefore this
     * callback — can run on ANY thread (whichever owner unlocked); the LOCK
     * response's iovecs are thread-local, so stash the result and bounce to the
     * request's owning thread, which drains lock_resume_head off its resume
     * doorbell and completes the lock there. */
    request->lock.resume_status = status;

    pthread_mutex_lock(&thread->lease_break_lock);
    request->lock.lock_resume_next = thread->lock_resume_head;
    thread->lock_resume_head       = request;
    pthread_mutex_unlock(&thread->lease_break_lock);

    evpl_ring_doorbell(&thread->lease_resume_doorbell);
} /* chimera_smb_lock_acquire_cb */

/* Records a synchronous lease-acquire outcome.  chimera_vfs_lease_acquire fires
 * the callback inline when wait==false, so the result is readable right after
 * the call returns. */
static void
chimera_smb_lock_sync_cb(
    enum chimera_vfs_lease_result result,
    struct chimera_vfs_lease     *granted,
    struct chimera_vfs_lease     *conflict,
    void                         *private_data)
{
    (void) granted;
    (void) conflict;
    *(enum chimera_vfs_lease_result *) private_data = result;
} /* chimera_smb_lock_sync_cb */

/* Take one byte-range lock for this open synchronously.  Returns the held entry
 * (appended to open_file->lock_entries) or NULL on a conflict -- with the SMB
 * same-handle rule applied first, then the cross-handle VFS acquire. */
static struct chimera_smb_lock_entry *
chimera_smb_lock_take_one(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_request       *request,
    struct chimera_smb_open_file     *open_file,
    uint64_t                          offset,
    uint64_t                          length,
    bool                              exclusive)
{
    struct chimera_vfs_state      *vfs_state = thread->vfs_thread->vfs->vfs_state;
    struct chimera_smb_lock_entry *entry, *held;
    enum chimera_vfs_lease_result  result = CHIMERA_VFS_LEASE_DENIED;

    /* The only caller (chimera_smb_lock_multi) is reached from chimera_smb_lock
     * after open_file has been resolved and null-checked, so open_file is an
     * invariant here.  Make it explicit for the static analyzer (which cannot
     * carry the upstream check across the two call boundaries): a missing open
     * fails the acquire, which the caller maps to STATUS_LOCK_NOT_GRANTED. */
    if (unlikely(!open_file)) {
        return NULL;
    }

    if (exclusive) {
        DL_FOREACH(open_file->lock_entries, held)
        {
            if (smb_lock_ranges_overlap(held->lease.offset, held->lease.length,
                                        offset, length)) {
                return NULL;
            }
        }
    }

    entry = calloc(1, sizeof(*entry));
    if (!entry) {
        return NULL;
    }
    entry->file_state = chimera_vfs_state_get(vfs_state, open_file->handle->fh,
                                              open_file->handle->fh_len,
                                              open_file->handle->fh_hash, true);
    if (!entry->file_state) {
        free(entry);
        return NULL;
    }
    entry->open_file          = open_file;
    entry->lease.kind         = CHIMERA_VFS_LEASE_RANGE;
    entry->lease.mode.granted = exclusive ? CHIMERA_VFS_LEASE_MODE_W
                                              : CHIMERA_VFS_LEASE_MODE_R;
    entry->lease.offset           = offset;
    entry->lease.length           = length;
    entry->lease.owner.protocol   = CHIMERA_VFS_LEASE_PROTO_SMB2;
    entry->lease.owner.client_key = request->session_handle->session->session_id;
    entry->lease.owner.owner_lo   = open_file->file_id.pid;
    entry->lease.owner.owner_hi   = open_file->file_id.vid;
    /* Point at the open's caching grant (NULL if it holds no oplock/lease), not
     * the open_file, so the range-vs-caching conflict check sees that this
     * byte-range lock and the same open's (or a coalesced peer's) caching lease
     * -- whose owner identity is the lease key, not the file id -- share one
     * grant and must not break each other (vfs_state.c same-cb_private
     * exemption; smb2.oplock.brl2).  The grant's lease carries the same pointer.
     * Mirrors the single-element path. */
    entry->lease.owner.cb_private = open_file->grant;

    chimera_vfs_lease_acquire(vfs_state, entry->file_state, &entry->lease,
                              &entry->ticket, false /* no wait */,
                              chimera_smb_lock_sync_cb, &result);

    if (result != CHIMERA_VFS_LEASE_GRANTED) {
        chimera_vfs_state_put(vfs_state, entry->file_state);
        free(entry);
        return NULL;
    }
    entry->lease_inserted = true;
    DL_APPEND(open_file->lock_entries, entry);
    return entry;
} /* chimera_smb_lock_take_one */

static void
chimera_smb_lock_entry_drop(
    struct chimera_vfs_state      *vfs_state,
    struct chimera_smb_open_file  *open_file,
    struct chimera_smb_lock_entry *e)
{
    DL_DELETE(open_file->lock_entries, e);
    if (e->lease_inserted) {
        chimera_vfs_lease_release(vfs_state, e->file_state, &e->lease);
    }
    if (e->file_state) {
        chimera_vfs_state_put(vfs_state, e->file_state);
    }
    free(e);
} /* chimera_smb_lock_entry_drop */

/* Multi-element (LockCount>1) lock/unlock.  All elements share one direction,
 * taken from element 0 (MS-SMB2 3.3.5.14): a LOCK request validates every
 * element's flags/range up front then acquires them atomically (rolling all of
 * them back on the first conflict); an UNLOCK request removes each named range
 * in order and stops with RANGE_NOT_LOCKED at the first range it does not hold.
 * All acquires are FAIL_IMMEDIATELY (synchronous) -- a blocking last element is
 * not supported in a multi-element request. */
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
                chimera_vfs_lease_release(vfs_state, match->file_state,
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
        if ((__uint128_t) off + len > ((__uint128_t) 1 << 64)) {
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

    /* Acquire all; roll back every lock taken in this request on a conflict. */
    struct chimera_smb_lock_entry *taken[CHIMERA_SMB_LOCK_MAX_ELEMENTS];
    uint16_t                       ntaken = 0;

    for (i = 0; i < n; i++) {
        bool                           excl = (request->lock.elements[i].flags & SMB2_LOCKFLAG_EXCLUSIVE) != 0;
        struct chimera_smb_lock_entry *e    =
            chimera_smb_lock_take_one(thread, request, open_file,
                                      request->lock.elements[i].offset,
                                      request->lock.elements[i].length, excl);

        if (!e) {
            while (ntaken > 0) {
                chimera_smb_lock_entry_drop(vfs_state, open_file, taken[--ntaken]);
            }
            chimera_smb_lock_multi_complete(request, open_file, SMB2_STATUS_LOCK_NOT_GRANTED);
            return;
        }
        taken[ntaken++] = e;
    }

    chimera_smb_lock_multi_complete(request, open_file, SMB2_STATUS_SUCCESS);
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
    bool                              wait;

    open_file = chimera_smb_open_file_resolve(request, &request->lock.file_id);
    if (unlikely(!open_file)) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }

    request->lock.open_file = open_file;

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
    if ((__uint128_t) request->lock.l_offset + request->lock.l_length >
        ((__uint128_t) 1 << 64)) {
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
            chimera_vfs_lease_release(vfs_state, match->file_state, &match->lease);
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

    /* LOCK acquire. */
    entry = calloc(1, sizeof(*entry));
    if (!entry) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INSUFFICIENT_RESOURCES);
        return;
    }

    entry->file_state = chimera_vfs_state_get(vfs_state,
                                              open_file->handle->fh,
                                              open_file->handle->fh_len,
                                              open_file->handle->fh_hash, true);
    if (!entry->file_state) {
        free(entry);
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INSUFFICIENT_RESOURCES);
        return;
    }

    entry->open_file          = open_file;
    entry->pending_req        = request;
    entry->lease.kind         = CHIMERA_VFS_LEASE_RANGE;
    entry->lease.mode.granted = (kind == SMB2_LOCKFLAG_EXCLUSIVE)
                                    ? CHIMERA_VFS_LEASE_MODE_W
                                    : CHIMERA_VFS_LEASE_MODE_R;
    entry->lease.offset           = request->lock.l_offset;
    entry->lease.length           = want_length;
    entry->lease.owner.protocol   = CHIMERA_VFS_LEASE_PROTO_SMB2;
    entry->lease.owner.client_key = request->session_handle->session->client_key;
    /* The owner identity is the open — different opens (even by the
     * same client) get different owner_lo/owner_hi and lock
     * independently, matching Windows handle-based lock semantics. */
    entry->lease.owner.owner_lo = open_file->file_id.pid;
    entry->lease.owner.owner_hi = open_file->file_id.vid;
    /* Point at the open's caching grant (NULL if it holds no oplock/lease) so the
     * range-vs-caching conflict check can tell that a byte-range lock and the same
     * open's -- or a coalesced peer's -- caching lease (whose owner identity is the
     * lease key, not the file id) belong to one grant and must not break each
     * other.  The grant's lease carries the same pointer in cb_private. */
    entry->lease.owner.cb_private = open_file->grant;

    request->lock.entry         = entry;
    request->lock.resume_status = CHIMERA_SMB_LOCK_PENDING;

    wait = !(request->lock.l_flags & SMB2_LOCKFLAG_FAIL_IMM);

    /* A waiting acquire may queue in the VFS and have its grant callback fire
     * later on ANOTHER thread (whoever releases the conflict).  Arm `parked`
     * BEFORE the call so the callback always takes the cross-thread bounce path,
     * even if a concurrent release fires it the instant the ticket queues — this
     * closes the window where the callback would otherwise mistake a deferred
     * grant for a synchronous one.  A FAIL_IMMEDIATELY acquire never queues, so
     * its callback always fires inline; leave it on the synchronous path. */
    request->lock.parked = wait ? 1 : 0;

    chimera_vfs_lease_acquire(vfs_state, entry->file_state,
                              &entry->lease, &entry->ticket, wait,
                              chimera_smb_lock_acquire_cb, entry);

    if (request->lock.parked && request->lock.resume_status == CHIMERA_SMB_LOCK_PENDING) {
        /* The acquire genuinely queued (a conflicting range held by another
         * owner, or a breakable caching lease being recalled) and its callback
         * has not fired.  This is an SMB2 blocking lock (MS-SMB2 3.3.5.14): emit
         * the STATUS_PENDING interim and record the park on the open so close /
         * tree-disconnect / logoff / connection teardown can abort it.  On grant
         * the callback bounces to this thread's resume doorbell; an SMB2 CANCEL
         * or an abort completes it with CANCELLED / RANGE_NOT_LOCKED. */
        open_file->parked_lock_req = request;
        chimera_smb_async_interim_begin(request);
        return;
    }

    if (request->lock.parked) {
        /* Waiting acquire that resolved (granted or unbreakable-caching denial)
         * before/at return: the callback already stashed the result and queued
         * the request on this thread's lock_resume_head + rang the doorbell.  No
         * interim is needed (no real wait occurred); the doorbell drain completes
         * it.  request->lock.parked stays 1 until the drain. */
        return;
    }

    /* FAIL_IMMEDIATELY: a synchronous grant or denial.  Install or tear down the
     * entry and complete inline.  Clear pending_req BEFORE settle — a DENIED
     * settle frees the entry, so touching it afterwards would be a UAF. */
    {
        uint32_t status;

        entry->pending_req = NULL;
        status             = chimera_smb_lock_settle_entry(
            vfs_state, request, open_file, entry,
            request->lock.resume_status == SMB2_STATUS_SUCCESS
            ? CHIMERA_VFS_LEASE_GRANTED : CHIMERA_VFS_LEASE_DENIED);

        chimera_smb_lock_seq_record(request, open_file, status);
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, status);
    }
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
