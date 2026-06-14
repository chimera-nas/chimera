// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>

#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_session.h"
#include "common/misc.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_state.h"

#define SMB2_LOCK_REQUEST_SIZE    48   /* fixed; LockCount==1 in this build */
#define SMB2_LOCK_REPLY_SIZE      4

/* Per MS-SMB2 §2.2.26.1 */
#define SMB2_LOCKFLAG_SHARED_LOCK 0x00000001
#define SMB2_LOCKFLAG_EXCLUSIVE   0x00000002
#define SMB2_LOCKFLAG_UNLOCK      0x00000004
#define SMB2_LOCKFLAG_FAIL_IMM    0x00000010

#define SMB2_LOCKFLAG_KIND_MASK   (SMB2_LOCKFLAG_SHARED_LOCK | \
                                   SMB2_LOCKFLAG_EXCLUSIVE   | \
                                   SMB2_LOCKFLAG_UNLOCK)

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

static void
chimera_smb_lock_acquire_cb(
    enum chimera_vfs_lease_result result,
    struct chimera_vfs_lease     *granted,
    struct chimera_vfs_lease     *conflict,
    void                         *private_data)
{
    struct chimera_smb_lock_entry *entry     = private_data;
    struct chimera_smb_request    *request   = entry->pending_req;
    struct chimera_smb_open_file  *open_file = entry->open_file;
    struct chimera_vfs_state      *vfs_state = request->compound->thread->vfs_thread->vfs->vfs_state;
    uint32_t                       status;

    (void) granted;
    (void) conflict;

    entry->pending_req = NULL;

    if (result == CHIMERA_VFS_LEASE_GRANTED) {
        entry->lease_inserted = true;
        DL_APPEND(open_file->lock_entries, entry);
        status = SMB2_STATUS_SUCCESS;
    } else {
        chimera_vfs_state_put(vfs_state, entry->file_state);
        free(entry);
        request->lock.entry = NULL;
        /* MS-SMB2 §3.3.5.14 maps a lock denied by an existing range to
         * STATUS_LOCK_NOT_GRANTED (or _RANGE for SMB2 v.s. SMB1 spec
         * variants).  Use _NOT_GRANTED — accepted by Windows clients. */
        status = SMB2_STATUS_LOCK_NOT_GRANTED;
    }

    chimera_smb_open_file_release(request, open_file);
    chimera_smb_complete_request(request, status);
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
    entry->lease.owner.cb_private = open_file;

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

        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, status);
        return;
    }

    /* LOCK request: validate every element first. */
    for (i = 0; i < n; i++) {
        uint32_t kind = request->lock.elements[i].flags & SMB2_LOCKFLAG_KIND_MASK;
        uint64_t off  = request->lock.elements[i].offset;
        uint64_t len  = request->lock.elements[i].length;

        if (kind != SMB2_LOCKFLAG_SHARED_LOCK && kind != SMB2_LOCKFLAG_EXCLUSIVE) {
            chimera_smb_open_file_release(request, open_file);
            chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
            return;
        }
        if ((__uint128_t) off + len > ((__uint128_t) 1 << 64)) {
            chimera_smb_open_file_release(request, open_file);
            chimera_smb_complete_request(request, SMB2_STATUS_INVALID_LOCK_RANGE);
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
                chimera_smb_open_file_release(request, open_file);
                chimera_smb_complete_request(request,
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
            chimera_smb_open_file_release(request, open_file);
            chimera_smb_complete_request(request, SMB2_STATUS_LOCK_NOT_GRANTED);
            return;
        }
        taken[ntaken++] = e;
    }

    chimera_smb_open_file_release(request, open_file);
    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
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

    request->lock.entry = entry;

    wait = !(request->lock.l_flags & SMB2_LOCKFLAG_FAIL_IMM);

    chimera_vfs_lease_acquire(vfs_state, entry->file_state,
                              &entry->lease, &entry->ticket, wait,
                              chimera_smb_lock_acquire_cb, entry);
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
