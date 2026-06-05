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
        chimera_vfs_caching_grant_release(vfs_state, open_file->grant);
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
    uint16_t reserved_lock_seq_lo;
    uint32_t reserved_lock_seq_hi;
    uint32_t reserved;

    if (unlikely(request->request_struct_size != SMB2_LOCK_REQUEST_SIZE)) {
        chimera_smb_error("Received SMB2 LOCK request with invalid struct size (%u expected %u)",
                          request->request_struct_size,
                          SMB2_LOCK_REQUEST_SIZE);
        request->status = SMB2_STATUS_INVALID_PARAMETER;
        return -1;
    }

    evpl_iovec_cursor_get_uint16(request_cursor, &request->lock.lock_count);
    /* LockSequenceNumber is 4 bits + LockSequenceIndex 28 bits = 32 bits.
     * We don't replay-detect by sequence in Stage B; just store. */
    evpl_iovec_cursor_get_uint16(request_cursor, &reserved_lock_seq_lo);
    evpl_iovec_cursor_get_uint16(request_cursor, (uint16_t *) &reserved_lock_seq_hi);
    request->lock.lock_sequence = ((uint32_t) reserved_lock_seq_lo) |
        (((uint32_t) reserved_lock_seq_hi) << 16);
    evpl_iovec_cursor_get_uint64(request_cursor, &request->lock.file_id.pid);
    evpl_iovec_cursor_get_uint64(request_cursor, &request->lock.file_id.vid);

    /* Only a LockCount of 1 carries a lock element we can read; a
     * zero-lock request (which smbtorture sends to probe rejection) has
     * no element, so reading one would over-run the buffer.  Invalid
     * LockCount values are reported by the handler as a normal
     * INVALID_PARAMETER response rather than a parse failure — returning
     * -1 here would tear the connection down instead of replying. */
    if (request->lock.lock_count == 1) {
        evpl_iovec_cursor_get_uint64(request_cursor, &request->lock.l_offset);
        evpl_iovec_cursor_get_uint64(request_cursor, &request->lock.l_length);
        evpl_iovec_cursor_get_uint32(request_cursor, &request->lock.l_flags);
        evpl_iovec_cursor_get_uint32(request_cursor, &reserved);
    } else {
        request->lock.l_offset = 0;
        request->lock.l_length = 0;
        request->lock.l_flags  = 0;
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

    /* LockCount==0 is illegal; >1 (multi-lock) is not yet supported.  Both
    * are reported as INVALID_PARAMETER without dropping the connection. */
    if (unlikely(request->lock.lock_count != 1)) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
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

    /* SMB2 length 0 is a legal "zero-byte" lock — we treat it as a
     * to-EOF lock to match the vfs_state convention (length==0 => EOF).
     * Windows clients use length==0 sparingly; the practical effect is
     * the same. */
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
    entry->lease.owner.client_key = request->session_handle->session->session_id;
    /* The owner identity is the open — different opens (even by the
     * same client) get different owner_lo/owner_hi and lock
     * independently, matching Windows handle-based lock semantics. */
    entry->lease.owner.owner_lo = open_file->file_id.pid;
    entry->lease.owner.owner_hi = open_file->file_id.vid;
    /* Point at the owning open so the range-vs-caching conflict check can tell
     * that a byte-range lock and the same open's caching lease (whose owner
     * identity is the lease key, not the file id) belong together and must not
     * break each other. */
    entry->lease.owner.cb_private = open_file;

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
