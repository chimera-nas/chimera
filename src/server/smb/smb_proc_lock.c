// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "common/range.h"
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
#include "vfs/vfs_internal_procs.h"
#include "vfs/vfs_claim.h"

#define SMB2_LOCK_REQUEST_SIZE    48
#define SMB2_LOCK_REPLY_SIZE      4

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

/* Cut off the closing open's cache participation without freeing storage that
 * a journal-pinned ACCESS claim can still address through own_cache. */
SYMBOL_EXPORT void
chimera_smb_open_file_revoke_cache(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_open_file     *open_file)
{
    (void) thread;
    if (!open_file->grant) {
        return;
    }
    chimera_smb_grant_remove_member(open_file->grant, open_file);
    chimera_vfs_claim_grant_revoke_empty(open_file->grant);
} /* chimera_smb_open_file_revoke_cache */

/* Called by the serialized CLOSE/finalize owner while it still pins the open.
 * A cache break must be able to settle before waiting on a DOC namespace fence;
 * ACCESS/range claims, file identities and namespace membership stay intact. */
SYMBOL_EXPORT void
chimera_smb_open_file_drain_cache(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_open_file     *open_file)
{
    struct chimera_vfs_state       *state = thread->vfs_thread->vfs->vfs_state;
    struct chimera_vfs_claim_grant *grant = open_file->grant;
    struct chimera_vfs_file_state  *file  = open_file->caching_file_state;

    if (grant) {
        chimera_smb_grant_remove_member(grant, open_file);
    }
    /* Clear before grant_release pumps callbacks, making reentrant drain inert. */
    open_file->grant                  = NULL;
    open_file->caching_lease_inserted = false;
    open_file->caching_file_state     = NULL;
    if (grant) {
        chimera_vfs_claim_grant_release(state, grant, true /* pump */);
    }
    if (file) {
        chimera_vfs_state_put(state, file);
    }
} /* chimera_smb_open_file_drain_cache */

SYMBOL_EXPORT void
chimera_smb_open_file_drain_locks(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_open_file     *open_file)
{
    struct chimera_vfs_state *vfs_state = thread->vfs_thread->vfs->vfs_state;

    /* Drop the SHARE-mode reservation first.  Release any byte-range
     * locks after — they may share the same file_state. */
    if (open_file->access_owner) {
        chimera_vfs_claim_access_owner_retire(open_file->access_owner);
        chimera_smb_abort_if(!chimera_vfs_claim_access_owner_is_retired(open_file->access_owner),
                             "ACCESS teardown before journal retirement");
        chimera_vfs_claim_access_owner_put(open_file->access_owner);
        open_file->access_owner         = NULL;
        open_file->share_lease_inserted = false;
    } else if (open_file->share_lease_inserted) {
        chimera_vfs_claim_release(vfs_state, open_file->share_file_state,
                                  &open_file->share_lease);
        open_file->share_lease_inserted = false;
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
    if (open_file->base_access_owner) {
        chimera_vfs_claim_access_owner_retire(open_file->base_access_owner);
        chimera_smb_abort_if(!chimera_vfs_claim_access_owner_is_retired(open_file->base_access_owner),
                             "base ACCESS teardown before journal retirement");
        chimera_vfs_claim_access_owner_put(open_file->base_access_owner);
        open_file->base_access_owner = NULL;
        if (open_file->base_share_lease_inserted) {
            chimera_vfs_state_stream_holder_dec(open_file->base_share_file_state);
            open_file->base_share_lease_inserted = false;
        }
    } else if (open_file->base_share_lease_inserted) {
        chimera_vfs_claim_release(vfs_state, open_file->base_share_file_state,
                                  &open_file->base_share_lease);
        chimera_vfs_state_stream_holder_dec(open_file->base_share_file_state);
        open_file->base_share_lease_inserted = false;
    }
    if (open_file->base_share_file_state) {
        chimera_vfs_state_put(vfs_state, open_file->base_share_file_state);
        open_file->base_share_file_state = NULL;
    }
    /* The base ACCESS claim borrows this exact handle as its actor anchor.
     * Retain it through journal retirement and stream-holder removal, even
     * when only the stream's data handle remains visible to the protocol. */
    struct chimera_vfs_open_handle *base_handle = open_file->base_handle;
    open_file->base_handle = NULL;
    if (base_handle) {
        chimera_vfs_release(thread->vfs_thread, base_handle);
    }

    chimera_smb_open_file_drain_cache(thread, open_file);

    if (open_file->range_owner) {
        chimera_vfs_claim_owner_retire(open_file->range_owner, NULL, NULL);
        chimera_vfs_claim_owner_put(open_file->range_owner);
        open_file->range_owner = NULL;
    }
} /* chimera_smb_open_file_drain_locks */

int
chimera_smb_parse_lock(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{
    request->lock.vfs_compound           = NULL;
    request->lock.compound_cancel_status = 0;
    request->lock.compound_index         = 0;
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
    /* Capture the 28-bit sequence number and 4-bit index for replay checks
     * against the private compound state. */
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

/* Caller holds the open's bucket lock. The compound owns the request and
 * completes on its worker; teardown only records a cancellation reason. */
void
chimera_smb_lock_abort_parked(struct chimera_smb_open_file *open_file)
{
    struct chimera_smb_request *request = open_file->parked_lock_req;

    if (!request) {
        return;
    }
    __atomic_store_n(&request->lock.compound_cancel_status,
                     SMB2_STATUS_RANGE_NOT_LOCKED, __ATOMIC_RELEASE);
    open_file->parked_lock_req = NULL;
} /* chimera_smb_lock_abort_parked */

void
chimera_smb_lock(struct chimera_smb_request *request)
{
    /* Every file-backed LOCK/UNLOCK uses the native compound adapter. A failed
     * batch allocation must not create an alternate, unjournaled lock owner.
     * Named pipes have no filesystem byte ranges and are rejected separately. */
    struct chimera_smb_open_file *open =
        chimera_smb_open_file_resolve(request, &request->lock.file_id);

    if (!open) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }
    /* Preserve FileId inheritance and ordinary protocol validation even when
     * the batch cannot be allocated. No range or LockSequence state changes. */
    unsigned status = SMB2_STATUS_INSUFFICIENT_RESOURCES;
    if (open->type == CHIMERA_SMB_OPEN_FILE_TYPE_PIPE ||
        (open->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY)) {
        status = SMB2_STATUS_INVALID_DEVICE_REQUEST;
    } else if (!(open->granted_access & (SMB2_FILE_READ_DATA | SMB2_FILE_WRITE_DATA))) {
        status = SMB2_STATUS_ACCESS_DENIED;
    } else if (!request->lock.lock_count || request->lock.lock_too_many) {
        status = SMB2_STATUS_INVALID_PARAMETER;
    }
    chimera_smb_open_file_release(request, open);
    chimera_smb_complete_request(request, status);
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

/* SMB exact ranges are local under the VFS projection policy and share one
 * attempt journal with READ/WRITE and other file-backed commands. */
struct smb_lock_compound {
    struct chimera_vfs_claim_exact_range ranges[CHIMERA_SMB_LOCK_MAX_ELEMENTS];
    uint32_t                             count, post_status;
    uint8_t                              replay, seq_bucket, seq_index;
    bool                                 unlock;
};

static int
smb_lock_compound_eligible(struct chimera_smb_request *request)
{
    (void) request;
    return 1;
} /* smb_lock_compound_eligible */

static struct chimera_smb_file_id
smb_lock_compound_file_id(struct chimera_smb_request *request)
{
    return request->lock.file_id;
} /* smb_lock_compound_file_id */

static unsigned int
smb_lock_compound_error(enum chimera_vfs_error error)
{
    switch (error) {
        case CHIMERA_VFS_OK: return SMB2_STATUS_SUCCESS;
        case CHIMERA_VFS_ENOENT: return SMB2_STATUS_RANGE_NOT_LOCKED;
        case CHIMERA_VFS_EAGAIN:
        case CHIMERA_VFS_EBUSY: return SMB2_STATUS_LOCK_NOT_GRANTED;
        case CHIMERA_VFS_EINTR: return SMB2_STATUS_CANCELLED;
        case CHIMERA_VFS_EINVAL: return SMB2_STATUS_INVALID_PARAMETER;
        case CHIMERA_VFS_ENOSPC: return SMB2_STATUS_INSUFFICIENT_RESOURCES;
        case CHIMERA_VFS_ENOTSUP: return SMB2_STATUS_NOT_SUPPORTED;
        default: return SMB2_STATUS_INTERNAL_ERROR;
    } /* switch */
} /* smb_lock_compound_error */

static void
smb_lock_compound_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command     *command = private_data;
    struct chimera_smb_request *request = command->request;
    struct smb_lock_compound   *lock    = command->private_data;

    (void) compound;
    (void) index;
    lock->replay     = 0;
    lock->seq_bucket = 0;
    if (command->state->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY) {
        command->status = SMB2_STATUS_INVALID_DEVICE_REQUEST;
    } else if (!(command->open->granted_access & (SMB2_FILE_READ_DATA | SMB2_FILE_WRITE_DATA))) {
        command->status = SMB2_STATUS_ACCESS_DENIED;
    } else if (!request->lock.lock_count || request->lock.lock_too_many) {
        command->status = SMB2_STATUS_INVALID_PARAMETER;
    } else {
        uint32_t bucket = chimera_smb_lock_seq_bucket(request->lock.lock_sequence);
        lock->seq_index = chimera_smb_lock_seq_index(request->lock.lock_sequence);
        if (chimera_smb_lock_replay_active(request, command->open) && bucket >= 1 && bucket <= 64) {
            lock->seq_bucket = bucket;
            if (command->state->lock_seq_valid[bucket - 1] &&
                command->state->lock_seq_index[bucket - 1] == lock->seq_index) {
                lock->replay    = 1;
                command->status = command->state->lock_seq_status[bucket - 1];
            }
        }
        if (!lock->replay) {
            if (!lock->unlock) {
                for (uint32_t i = 0; i < request->lock.lock_count; i++) {
                    uint32_t kind = request->lock.elements[i].flags & SMB2_LOCKFLAG_KIND_MASK;
                    if (kind != SMB2_LOCKFLAG_SHARED_LOCK && kind != SMB2_LOCKFLAG_EXCLUSIVE) {
                        command->status = SMB2_STATUS_INVALID_PARAMETER;
                        break;
                    }
                    if (chimera_range_compare(chimera_range_end(lock->ranges[i].offset, lock->ranges[i].length),
                                              chimera_range_eof()) > 0) {
                        command->status = SMB2_STATUS_INVALID_LOCK_RANGE;
                        break;
                    }
                }
            } else if (request->lock.lock_count == 1) {
                if (request->lock.l_flags != SMB2_LOCKFLAG_UNLOCK) {
                    command->status = SMB2_STATUS_INVALID_PARAMETER;
                } else if (chimera_range_compare(chimera_range_end(lock->ranges[0].offset, lock->ranges[0].length),
                                                 chimera_range_eof()) > 0) {
                    command->status = SMB2_STATUS_INVALID_LOCK_RANGE;
                }
            }
            if (!lock->count && lock->post_status) {
                command->status = lock->post_status;
            }
        }
    }
    if (command->status != SMB2_STATUS_SUCCESS) {
        *status = CHIMERA_VFS_EINVAL;
    }
} /* smb_lock_compound_prepare */

static void
smb_lock_compound_coordinate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    uint64_t                     token,
    const uint8_t               *fh,
    uint32_t                     fh_len,
    void                        *private_data)
{
    struct smb_vfs_command       *command = private_data;
    struct smb_lock_compound     *lock    = command->private_data;
    struct chimera_smb_open_file *open    = command->open;
    struct chimera_vfs_state     *state   = command->request->compound->thread->vfs_thread->vfs->vfs_state;
    struct chimera_smb_tree      *tree    = open->tree;
    unsigned                      bucket  = open->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;
    enum chimera_vfs_error        status  = CHIMERA_VFS_OK;

    (void) index;
    if (lock->replay) {
        chimera_vfs_compound_coordinate_done(compound, token, status);
        return;
    }
    /* Canonical owner installation is explicit execution coordination, never
     * a pure callout. Its empty identity survives rejection; claims do not. */
    if (!command->state->producer) {
        evpl_mutex_lock(&tree->open_files_lock[bucket]);
        if (open->flags & CHIMERA_SMB_OPEN_FILE_CLOSED) {
            status = CHIMERA_VFS_EINTR;
        } else if (!open->range_owner) {
            struct chimera_vfs_file_state *file = chimera_vfs_state_get(state, fh, fh_len,
                                                                        chimera_vfs_hash(fh, fh_len), true);
            if (file) {
                struct chimera_claim_actor actor = command->actor;
                actor.op_handle   = open->handle;
                open->range_owner = chimera_vfs_claim_owner_create_compat(file, &actor, true);
                chimera_vfs_state_put(state, file);
            }
            if (!open->range_owner) {
                status = CHIMERA_VFS_ENOSPC;
            }
        }
        evpl_mutex_unlock(&tree->open_files_lock[bucket]);
    }
    if (status == CHIMERA_VFS_OK && !lock->unlock) {
        /* No reservation is acquired here. Awaited cache floors remain an
         * admission blocker observed by the typed range operation. */
        uint64_t hash = chimera_vfs_hash(fh, fh_len);
        chimera_vfs_claim_invalidate(state, fh, fh_len, hash,
                                     CHIMERA_TRIGGER_SMB_LOCK, &command->actor, 0);
        chimera_vfs_claim_invalidate(state, fh, fh_len, hash,
                                     CHIMERA_TRIGGER_OPEN_W, &command->actor, 0);
        chimera_vfs_claim_invalidate(state, fh, fh_len, hash,
                                     CHIMERA_TRIGGER_OPEN_H_FORCE, &command->actor, 0);
    }
    chimera_vfs_compound_coordinate_done(compound, token, status);
} /* smb_lock_compound_coordinate */

static void
smb_lock_owner_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command         *command = private_data;
    struct smb_lock_compound       *lock    = command->private_data;

    (void) status;
    if (lock->replay || command->state->range_owner) {
        chimera_vfs_compound_op_skip(compound, index);
        return;
    }
    struct chimera_vfs_compound_op *op = chimera_vfs_compound_op_args(compound, index);
    op->io_owner           = command->actor;
    op->io_owner.op_handle = command->handle;
    op->have_io_owner      = true;
} /* smb_lock_owner_prepare */

static void
smb_lock_owner_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command               *command = private_data;
    const struct chimera_vfs_compound_op *op      = chimera_vfs_compound_op(compound, index);

    if (*status == CHIMERA_VFS_OK && op->out_range_owner) {
        command->state->range_owner      = op->out_range_owner;
        command->state->range_owner_from = index;
    }
} /* smb_lock_owner_complete */

static void
smb_lock_compound_range_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command         *command = private_data;
    struct smb_lock_compound       *lock    = command->private_data;
    struct chimera_vfs_compound_op *op      = chimera_vfs_compound_op_args(compound, index);

    (void) status;
    if (lock->replay) {
        /* Replayed success must not allocate an owner or reserve any range.
         * The immutable builder snapshot restores execution on retry. */
        op->skipped = 1;
    } else {
        if (!command->state->producer) {
            command->state->range_owner = command->open->range_owner;
        }
        op->range_owner      = command->state->range_owner;
        op->num_exact_ranges = lock->count;
    }
} /* smb_lock_compound_range_prepare */

static bool
smb_lock_compound_is_canceled(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    void                        *private_data)
{
    struct smb_vfs_command     *command = private_data;
    struct chimera_smb_request *request = command->request;

    (void) compound;
    (void) index;
    return __atomic_load_n(&request->lock.compound_cancel_status, __ATOMIC_ACQUIRE) ||
           request->compound->conn->generation != request->compound->conn_generation ||
           request->compound->conn->disconnecting ||
           request->tree->compound_tearing_down ||
           (request->session_handle->session->flags & CHIMERA_SMB_SESSION_DELETED) ||
           (__atomic_load_n(&command->open->flags, __ATOMIC_ACQUIRE) & CHIMERA_SMB_OPEN_FILE_CLOSED);
} /* smb_lock_compound_is_canceled */

static void
smb_lock_compound_wait(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    void                        *private_data)
{
    struct smb_vfs_command       *command = private_data;
    struct chimera_smb_request   *request = command->request;
    struct chimera_smb_open_file *open    = command->open;
    unsigned                      bucket  = open->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;

    evpl_mutex_lock(&open->tree->open_files_lock[bucket]);
    request->lock.vfs_compound   = compound;
    request->lock.compound_index = index;
    request->lock.open_file      = open;
    request->lock.parked         = 1;
    /* Other waiters remain reachable by connection AsyncId/MessageId;
     * retirement/closed/session predicates settle every waiter on teardown. */
    if (!open->parked_lock_req) {
        open->parked_lock_req = request;
    }
    evpl_mutex_unlock(&open->tree->open_files_lock[bucket]);
    /* Memo is request-scoped and survives a finish retry. */
    if (!request->async_id) {
        chimera_smb_async_interim_begin(request);
    }
} /* smb_lock_compound_wait */

static void
smb_lock_compound_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command     *command = private_data;
    struct smb_lock_compound   *lock    = command->private_data;
    struct chimera_smb_request *request = command->request;

    (void) compound;
    (void) index;
    if (lock->replay) {
        return;
    }
    command->status = smb_lock_compound_error(*status);
    if (*status == CHIMERA_VFS_EINTR) {
        unsigned cancel = __atomic_load_n(&request->lock.compound_cancel_status, __ATOMIC_ACQUIRE);
        command->status = cancel ? cancel : SMB2_STATUS_RANGE_NOT_LOCKED;
    } else if (*status == CHIMERA_VFS_OK && lock->post_status) {
        command->status = lock->post_status;
        *status         = CHIMERA_VFS_EINVAL;
    }
    if (lock->seq_bucket && *status != CHIMERA_VFS_EINTR) {
        unsigned b = lock->seq_bucket - 1;
        command->state->lock_seq_valid[b]  = 1;
        command->state->lock_seq_index[b]  = lock->seq_index;
        command->state->lock_seq_status[b] = command->status;
        command->state->lock_seq_dirty    |= UINT64_C(1) << b;
    }
} /* smb_lock_compound_complete */

static int
smb_lock_compound_build(
    struct chimera_vfs_compound *compound,
    struct smb_vfs_command      *command)
{
    struct chimera_smb_request *request = command->request;
    struct smb_lock_compound   *lock    = calloc(1, sizeof(*lock));

    if (!lock) {
        command->input_status = SMB2_STATUS_INSUFFICIENT_RESOURCES;
        return chimera_vfs_compound_add_checkpoint(compound);
    }
    command->private_data = lock;
    lock->count           = request->lock.lock_too_many ? 0 : request->lock.lock_count;
    lock->unlock          = (request->lock.l_flags & SMB2_LOCKFLAG_UNLOCK) != 0;
    for (unsigned i = 0; i < lock->count; i++) {
        lock->ranges[i] = (struct chimera_vfs_claim_exact_range) {
            request->lock.elements[i].offset, request->lock.elements[i].length,
            (request->lock.elements[i].flags & SMB2_LOCKFLAG_EXCLUSIVE) != 0,
        };
        if (lock->unlock && !(request->lock.elements[i].flags & SMB2_LOCKFLAG_UNLOCK)) {
            lock->count       = i;
            lock->post_status = SMB2_STATUS_INVALID_PARAMETER;
            break;
        }
    }
    chimera_vfs_compound_add_coordinate(compound, smb_lock_compound_coordinate, command);
    if (command->state && command->state->producer) {
        int owner = chimera_vfs_compound_add_range_owner(compound, NULL, true);
        if (owner >= 0) {
            chimera_vfs_compound_set_op_callbacks(compound, owner,
                                                  smb_lock_owner_prepare, smb_lock_owner_complete, command);
        }
    }
    int op = chimera_vfs_compound_add_range_batch(compound, NULL, lock->ranges,
                                                  lock->count ? lock->count : 1, lock->unlock);
    if (op >= 0) {
        struct chimera_vfs_compound_op *args = chimera_vfs_compound_op_args(compound, op);
        args->range_wait = !lock->unlock && request->lock.lock_count == 1 &&
            !(request->lock.l_flags & SMB2_LOCKFLAG_FAIL_IMM);
        args->range_on_wait      = smb_lock_compound_wait;
        args->range_is_canceled  = smb_lock_compound_is_canceled;
        args->range_wait_private = command;
        chimera_vfs_compound_set_op_prepare(compound, op, smb_lock_compound_range_prepare, command);
    }
    return op;
} /* smb_lock_compound_build */

static void
smb_lock_compound_release(struct smb_vfs_command *command)
{
    struct chimera_smb_request   *request = command->request;
    struct chimera_smb_open_file *open    = command->open;

    if (request->lock.vfs_compound && open) {
        unsigned bucket = open->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;
        evpl_mutex_lock(&open->tree->open_files_lock[bucket]);
        if (open->parked_lock_req == request) {
            open->parked_lock_req = NULL;
        }
        request->lock.parked       = 0;
        request->lock.vfs_compound = NULL;
        request->lock.open_file    = NULL;
        evpl_mutex_unlock(&open->tree->open_files_lock[bucket]);
    }
    free(command->private_data);
    command->private_data = NULL;
} /* smb_lock_compound_release */

const struct smb_vfs_command_ops chimera_smb_lock_compound_ops = {
    .eligible  = smb_lock_compound_eligible,
    .file_id   = smb_lock_compound_file_id,
    .map_error = smb_lock_compound_error,
    .build     = smb_lock_compound_build,
    .prepare   = smb_lock_compound_prepare,
    .complete  = smb_lock_compound_complete,
    .release   = smb_lock_compound_release,
};
