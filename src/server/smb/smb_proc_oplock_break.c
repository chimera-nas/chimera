// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>

#include "smb2.h"
#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_session.h"
#include "common/misc.h"
#include "vfs/vfs.h"
#include "vfs/vfs_state.h"

/*
 * Server-to-client SMB2 OPLOCK_BREAK Notification.
 *
 * Stage D.1: send the lease-variant notification (MS-SMB2 §2.2.23.2) on
 * the conn that owns the broken lease.  The break callback is wired
 * onto every SMB CACHING lease at CREATE time; vfs_state invokes it
 * synchronously when a new acquire conflicts with the lease.  The
 * client's OPLOCK_BREAK Acknowledgment arrives later through the
 * regular SMB2 LOCK dispatch path (handler below).
 */

#define SMB2_OPLOCK_BREAK_FLAG_ACK_REQUIRED 0x01

/* chimera_smb_vfs_to_lease_bits / _lease_bits_to_vfs / _vfs_to_oplock_level live
 * in smb_internal.h -- one canonical SMB<->VFS caching-grant encoding shared with
 * the create grant path. */

/* Build the 4-byte NetBIOS header + 64-byte SMB2 header for an
 * unsolicited OPLOCK_BREAK Notification.  Returns pointer past the
 * SMB2 header into the body region. */
static uint8_t *
chimera_smb_oplock_break_build_header(uint8_t *buf)
{
    struct smb2_header *hdr;

    buf += 4; /* NetBIOS header — filled in by caller */

    hdr = (struct smb2_header *) buf;
    memset(hdr, 0, sizeof(*hdr));

    hdr->protocol_id[0]          = 0xFE;
    hdr->protocol_id[1]          = 'S';
    hdr->protocol_id[2]          = 'M';
    hdr->protocol_id[3]          = 'B';
    hdr->struct_size             = 64;
    hdr->credit_charge           = 0;
    hdr->status                  = 0;
    hdr->command                 = SMB2_OPLOCK_BREAK;
    hdr->credit_request_response = 0;
    hdr->flags                   = SMB2_FLAGS_SERVER_TO_REDIR;
    hdr->next_command            = 0;
    /* Unsolicited messages use 0xFFFFFFFFFFFFFFFF as the message_id. */
    hdr->message_id = UINT64_MAX;
    hdr->session_id = 0;

    return buf + sizeof(struct smb2_header);
} /* chimera_smb_oplock_break_build_header */

/* Send an SMB2 OPLOCK_BREAK Notification (lease variant) on `conn`. */
static void
chimera_smb_send_oplock_break_lease(
    struct chimera_smb_conn *conn,
    const uint8_t           *lease_key,
    uint8_t                  current_state, /* SMB lease bits */
    uint8_t                  new_state,     /* SMB lease bits */
    bool                     ack_required,
    uint16_t                 new_epoch)
{
    struct evpl_iovec iov;
    uint8_t          *buf;
    uint8_t          *p;
    int               total = 4 + 64 + SMB2_OPLOCK_BREAK_NOTIFY_LEASE_SIZE;
    uint32_t          nb_len;

    evpl_iovec_alloc(conn->thread->evpl, total, 8, 1, 0, &iov);
    buf = iov.data;
    memset(buf, 0, total);

    p = chimera_smb_oplock_break_build_header(buf);

    /* Body (44 bytes): MS-SMB2 §2.2.23.2 */
    /* StructureSize = 0x2C (44) */
    p[0] = 0x2C;
    p[1] = 0x00;
    /* NewEpoch (2) */
    p[2] = new_epoch & 0xff;
    p[3] = (new_epoch >> 8) & 0xff;
    /* Flags (4) — ACK_REQUIRED only when the break removes write or handle
     * caching (the client must flush / close); a break that only drops read
     * caching (e.g. to NONE) needs no acknowledgment. */
    p[4] = ack_required ? SMB2_OPLOCK_BREAK_FLAG_ACK_REQUIRED : 0;
    p[5] = 0; p[6] = 0; p[7] = 0;
    /* LeaseKey (16) */
    memcpy(p + 8, lease_key, 16);
    /* CurrentLeaseState (4) */
    p[24] = current_state;
    p[25] = 0; p[26] = 0; p[27] = 0;
    /* NewLeaseState (4) */
    p[28] = new_state;
    p[29] = 0; p[30] = 0; p[31] = 0;
    /* BreakReason, AccessMaskHint, ShareMaskHint (12) — reserved */
    memset(p + 32, 0, 12);

    p += SMB2_OPLOCK_BREAK_NOTIFY_LEASE_SIZE;

    /* NetBIOS header = big-endian length of everything after it. */
    nb_len = __builtin_bswap32((uint32_t) (p - (buf + 4)));
    memcpy(buf, &nb_len, 4);

    iov.length = (int) (p - buf);
    evpl_sendv(conn->thread->evpl, conn->bind, &iov, 1, iov.length,
               EVPL_SEND_FLAG_TAKE_REF);
} /* chimera_smb_send_oplock_break_lease */

/* Send a legacy (non-lease) SMB2 OPLOCK_BREAK Notification (MS-SMB2
 * §2.2.23.1) on `conn`, identifying the open by FileId. */
static void
chimera_smb_send_oplock_break_legacy(
    struct chimera_smb_conn *conn,
    uint64_t                 file_id_pid,
    uint64_t                 file_id_vid,
    uint8_t                  new_oplock_level)
{
    struct evpl_iovec iov;
    uint8_t          *buf;
    uint8_t          *p;
    int               total = 4 + 64 + SMB2_OPLOCK_BREAK_NOTIFY_LEGACY_SIZE;
    uint32_t          nb_len;

    evpl_iovec_alloc(conn->thread->evpl, total, 8, 1, 0, &iov);
    buf = iov.data;
    memset(buf, 0, total);

    p = chimera_smb_oplock_break_build_header(buf);

    /* Body (24 bytes): MS-SMB2 §2.2.23.1 */
    p[0] = 0x18;             /* StructureSize = 24 */
    p[1] = 0x00;
    p[2] = new_oplock_level; /* OplockLevel the holder breaks to */
    p[3] = 0;                /* Reserved */
    p[4] = 0; p[5] = 0; p[6] = 0; p[7] = 0; /* Reserved2 */
    memcpy(p + 8,  &file_id_pid, 8); /* FileId.Persistent */
    memcpy(p + 16, &file_id_vid, 8); /* FileId.Volatile */

    p += SMB2_OPLOCK_BREAK_NOTIFY_LEGACY_SIZE;

    nb_len = __builtin_bswap32((uint32_t) (p - (buf + 4)));
    memcpy(buf, &nb_len, 4);

    iov.length = (int) (p - buf);
    evpl_sendv(conn->thread->evpl, conn->bind, &iov, 1, iov.length,
               EVPL_SEND_FLAG_TAKE_REF);
} /* chimera_smb_send_oplock_break_legacy */

/* break_cb wired onto every SMB CACHING lease at CREATE time.  The cb_private is
 * the VFS-owned caching grant (chimera_vfs_caching_grant), which may be shared by
 * several opens under one lease key.  Pick a member whose connection is still
 * live to carry the OPLOCK_BREAK Notification; the grant outlives any single open,
 * so the open that created it may already be gone while a coalesced peer keeps the
 * lease alive. */
SYMBOL_EXPORT void
chimera_smb_lease_break_cb(
    struct chimera_vfs_lease *lease,
    uint8_t                   needed_mode,
    void                     *private_data)
{
    /* Resolve the grant from the lease's own back-pointer rather than cb_private:
     * a fresh grant is linked (and so becomes breakable) before its SMB cb_private
     * is stamped, but lease->grant is set under the file lock at allocation, so it
     * is always valid here. */
    struct chimera_vfs_caching_grant *grant = lease->grant;
    struct chimera_vfs_file_state    *file  = grant->file;

    (void) private_data;
    struct chimera_smb_open_file     *open_file;
    uint8_t                           new_vfs;

    /* Select a live member under the grant's file->lock (the holder list is
     * mutated by add/remove-member under the same lock).  A member is live when
     * its create_conn is still set and it has not been closed. */
    pthread_mutex_lock(&file->lock);
    for (open_file = grant->holders; open_file;
         open_file = open_file->grant_member_next) {
        if (!(open_file->flags & CHIMERA_SMB_OPEN_FILE_CLOSED) &&
            open_file->create_conn) {
            break;
        }
    }
    pthread_mutex_unlock(&file->lock);

    /* No live member can notify the client; the pragmatic recovery is to forcibly
     * revoke the lease so the pending acquire (or future acquires) can proceed.
     * This mirrors the Linux kernel's F_SETLEASE forcible expiry path. */
    if (!open_file) {
        chimera_vfs_lease_revoke(lease);
        return;
    }

    /* needed_mode is the *retained* mask the holder may keep, intersected with
     * what it currently holds:
     *   - A conflicting OPEN passes R: the holder keeps a shared read cache and
     *     gives up write/handle caching -- it downgrades to LEVEL_II.
     *   - A WRITE invalidation (and a namespace recall) passes 0: the cache is
     *     stale / the name is going away, so it breaks all the way to NONE.
     *   - A FLUSH recall (data setattr) passes R|H: the holder writes back its
     *     dirty data but keeps its read + handle cache (no full re-lease).
     * Intersecting with granted means a holder that does not hold a retained bit
     * simply loses it (e.g. an exclusive W-only oplock flushed with R|H keeps
     * only R). */
    new_vfs = lease->mode.granted & needed_mode;

    if (open_file->oplock_level == SMB2_OPLOCK_LEVEL_LEASE) {
        /* SMB2 lease (RqLs) — §2.2.23.2 lease-variant notification. */
        uint8_t current_smb = chimera_smb_vfs_to_lease_bits(lease->mode.granted);
        uint8_t new_smb     = chimera_smb_vfs_to_lease_bits(new_vfs);

        /* The lease epoch lives on the GRANT so all coalesced opens share one
         * monotonic counter (MS-SMB2 3.3.5.9.11): bump it once per break.  Only a
         * v2 lease versions its state; a v1 lease breaks with epoch 0. */
        if (grant->is_v2) {
            grant->epoch++;
        }
        open_file->lease_epoch = grant->is_v2 ? grant->epoch : 0;
        open_file->lease_state = new_smb;

        /* The client must acknowledge only when the break strips write or handle
         * caching; dropping read caching alone needs no ack.  Record this on the
         * grant so an open that triggered the break knows whether to park waiting
         * for an ack (chimera_vfs_state_caching_breaking). */
        bool ack_req = ((current_smb & ~new_smb) &
                        (SMB2_LEASE_WRITE_CACHING |
                         SMB2_LEASE_HANDLE_CACHING)) != 0;
        grant->break_ack_required = ack_req;

        /* break_cb may run on the breaker's thread, but the OPLOCK_BREAK
         * notification must be sent on the holder connection's owning thread
         * because evpl iovec pools and binds are thread-local. */
        {
            struct chimera_smb_conn            *conn = open_file->create_conn;
            struct chimera_server_smb_thread   *bt   = conn->thread;
            struct chimera_smb_lease_break_msg *msg;

            pthread_mutex_lock(&bt->lease_break_lock);
            if (!conn->lease_break_tearing_down) {
                msg           = calloc(1, sizeof(*msg));
                msg->conn     = conn;
                msg->is_lease = true;
                memcpy(msg->lease_key, open_file->lease_key, 16);
                msg->current_state    = current_smb;
                msg->new_state        = new_smb;
                msg->ack_required     = ack_req;
                msg->new_epoch        = open_file->lease_epoch;
                msg->next             = bt->lease_break_ready;
                bt->lease_break_ready = msg;
                pthread_mutex_unlock(&bt->lease_break_lock);
                evpl_ring_doorbell(&bt->lease_break_doorbell);
            } else {
                pthread_mutex_unlock(&bt->lease_break_lock);
            }
        }
    } else {
        /* Legacy oplock — §2.2.23.1 notification keyed by FileId.  Break
         * to LEVEL_II when a read cache survives, otherwise to NONE. */
        uint8_t new_level = (new_vfs & CHIMERA_VFS_LEASE_MODE_R)
                            ? SMB2_OPLOCK_LEVEL_II
                            : SMB2_OPLOCK_LEVEL_NONE;

        /* Breaking an exclusive/batch oplock expects the client to
         * acknowledge; breaking a LEVEL_II oplock (to NONE) does not -- a
         * client ack for that is a protocol error (see the ack handler). */
        open_file->oplock_break_ack_required =
            (open_file->oplock_level == SMB2_OPLOCK_LEVEL_EXCLUSIVE ||
             open_file->oplock_level == SMB2_OPLOCK_LEVEL_BATCH);
        grant->break_ack_required = open_file->oplock_break_ack_required;

        {
            struct chimera_smb_conn            *conn = open_file->create_conn;
            struct chimera_server_smb_thread   *bt   = conn->thread;
            struct chimera_smb_lease_break_msg *msg;

            pthread_mutex_lock(&bt->lease_break_lock);
            if (!conn->lease_break_tearing_down) {
                msg                   = calloc(1, sizeof(*msg));
                msg->conn             = conn;
                msg->is_lease         = false;
                msg->file_id_pid      = open_file->file_id.pid;
                msg->file_id_vid      = open_file->file_id.vid;
                msg->new_oplock_level = new_level;
                msg->next             = bt->lease_break_ready;
                bt->lease_break_ready = msg;
                pthread_mutex_unlock(&bt->lease_break_lock);
                evpl_ring_doorbell(&bt->lease_break_doorbell);
            } else {
                pthread_mutex_unlock(&bt->lease_break_lock);
            }
        }
        open_file->oplock_level = new_level;
    }

    /* A break that needs no client ack (it stripped only read caching, e.g. the
     * R->NONE tail of an RWH->RH->R->NONE cascade) will never be resolved by an
     * inbound OPLOCK_BREAK ack, so the ack-driven resume of parked CREATEs
     * (chimera_smb_create_resume_parked, only called from the ack handler) never
     * fires for it.  But the lease is now at its retained level, so
     * chimera_vfs_state_caching_breaking() returns false and any CREATE parked on
     * this file is resumable.  Ring the resume doorbell on this thread AND every
     * peer (the broadcast skips its origin) so each re-sweeps and completes its
     * parked CREATEs instead of stalling to the break deadline (cthon special
     * hang). */
    if (!grant->break_ack_required) {
        struct chimera_server_smb_thread *rt = open_file->create_conn->thread;
        evpl_ring_doorbell(&rt->lease_resume_doorbell);
        chimera_smb_create_resume_parked_broadcast(rt);
    }

    /* The lease stays BREAKING (awaiting the client's real OPLOCK_BREAK ack or
     * close); the conflict matrix treats a BREAKING SMB lease as already at its
     * retained (break_needed_mode) level, so a coexisting acquirer proceeds
     * immediately without waiting for the ack.  The client's response resolves
     * the break via chimera_smb_oplock_break -> chimera_vfs_lease_ack, and the
     * deadline driver revokes it if the client never responds.  (This is the
     * same real break/wait model NFSv4 delegations use; the previous code acked
     * optimistically here, which made true wait-for-close semantics
     * impossible.) */
    (void) new_vfs;
} /* chimera_smb_lease_break_cb */

/* Doorbell handler: runs on a connection-owning SMB thread and sends the
 * lease-break notifications that break_cbs (possibly on other threads) queued
 * for connections owned by this thread.  Runs on the same thread as conn_free,
 * so a connection in the drained list is guaranteed live here. */
SYMBOL_EXPORT void
chimera_smb_lease_break_doorbell_callback(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct chimera_server_smb_thread   *thread;
    struct chimera_smb_lease_break_msg *list, *msg;

    (void) evpl;

    thread = container_of(doorbell, struct chimera_server_smb_thread,
                          lease_break_doorbell);

    pthread_mutex_lock(&thread->lease_break_lock);
    list                      = thread->lease_break_ready;
    thread->lease_break_ready = NULL;
    pthread_mutex_unlock(&thread->lease_break_lock);

    while (list) {
        msg  = list;
        list = list->next;
        if (msg->is_lease) {
            chimera_smb_send_oplock_break_lease(msg->conn, msg->lease_key,
                                                msg->current_state, msg->new_state,
                                                msg->ack_required, msg->new_epoch);
        } else {
            chimera_smb_send_oplock_break_legacy(msg->conn,
                                                 msg->file_id_pid,
                                                 msg->file_id_vid,
                                                 msg->new_oplock_level);
        }
        free(msg);
    }
} /* chimera_smb_lease_break_doorbell_callback */

void
chimera_smb_lease_break_thread_init(struct chimera_server_smb_thread *thread)
{
    thread->lease_break_ready = NULL;
    pthread_mutex_init(&thread->lease_break_lock, NULL);
    evpl_add_doorbell(thread->evpl, &thread->lease_break_doorbell,
                      chimera_smb_lease_break_doorbell_callback);
} /* chimera_smb_lease_break_thread_init */

void
chimera_smb_lease_break_thread_destroy(struct chimera_server_smb_thread *thread)
{
    struct chimera_smb_lease_break_msg *msg;

    evpl_remove_doorbell(thread->evpl, &thread->lease_break_doorbell);

    while (thread->lease_break_ready) {
        msg                       = thread->lease_break_ready;
        thread->lease_break_ready = msg->next;
        free(msg);
    }
    pthread_mutex_destroy(&thread->lease_break_lock);
} /* chimera_smb_lease_break_thread_destroy */

/* ----------------------------------------------------------------------
 * Inbound SMB2 OPLOCK_BREAK Acknowledgment from client
 * ----------------------------------------------------------------------
 *
 * Lease ack arrives via the regular dispatch path with the SMB2_OPLOCK_BREAK
 * opcode and struct_size=36 (lease) or 24 (legacy).  Since the server-side
 * lease was already optimistically acked inside chimera_smb_lease_break_cb,
 * the client ack here only needs to reply with the matching OPLOCK_BREAK
 * Response packet.  A future refinement would defer the lease_ack until
 * this point to honor the client's chosen NewLeaseState verbatim. */

SYMBOL_EXPORT int
chimera_smb_parse_oplock_break(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{
    uint16_t reserved;
    uint32_t reserved32;

    if (request->request_struct_size == SMB2_OPLOCK_BREAK_ACK_LEASE_SIZE) {
        request->oplock_break.is_lease = true;
        evpl_iovec_cursor_get_uint16(request_cursor, &reserved);
        evpl_iovec_cursor_get_uint32(request_cursor, &request->oplock_break.lease_flags);
        evpl_iovec_cursor_copy(request_cursor, request->oplock_break.lease_key, 16);
        evpl_iovec_cursor_get_uint32(request_cursor, &reserved32);
        request->oplock_break.lease_state = (uint8_t) (reserved32 & 0xff);
        /* LeaseDuration (8 bytes, reserved) sits at a 4-aligned offset; the
         * aligning get_uint64 would skip to the next 8-boundary and overrun the
         * 36-byte request.  It is unused, so just advance past it. */
        evpl_iovec_cursor_skip(request_cursor, 8);
    } else if (request->request_struct_size == SMB2_OPLOCK_BREAK_ACK_LEGACY_SIZE) {
        request->oplock_break.is_lease = false;
        evpl_iovec_cursor_get_uint8(request_cursor, &request->oplock_break.oplock_level);
        evpl_iovec_cursor_get_uint8(request_cursor, (uint8_t *) &reserved);
        evpl_iovec_cursor_get_uint32(request_cursor, &reserved32);
        evpl_iovec_cursor_get_uint64(request_cursor, &request->oplock_break.file_id.pid);
        evpl_iovec_cursor_get_uint64(request_cursor, &request->oplock_break.file_id.vid);
    } else {
        chimera_smb_error("SMB2 OPLOCK_BREAK ack with unexpected struct size %u",
                          request->request_struct_size);
        request->status = SMB2_STATUS_INVALID_PARAMETER;
        return -1;
    }

    return 0;
} /* chimera_smb_parse_oplock_break */

SYMBOL_EXPORT void
chimera_smb_oplock_break(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread    = request->compound->thread;
    struct chimera_vfs_state         *vfs_state = thread->vfs_thread->vfs->vfs_state;
    struct chimera_smb_open_file     *open_file;

    if (request->oplock_break.is_lease) {
        /* Lease-break ack: resolve the lease by its key and apply the exact
         * NewLeaseState the client kept (it may drop further than we asked, e.g.
         * straight to NONE).  The lease is genuinely BREAKING (Phase 1 dropped
         * optimistic-ack), so chimera_vfs_lease_ack settles it -- re-arming a
         * surviving lease and pumping any acquirer parked on the break. */
        open_file = chimera_smb_open_file_resolve_by_lease_key(
            request, request->oplock_break.lease_key);

        if (open_file) {
            if (open_file->grant) {
                struct chimera_vfs_lease *lease    = &open_file->grant->lease;
                uint8_t                   kept_vfs = chimera_smb_lease_bits_to_vfs(
                    request->oplock_break.lease_state);

                /* MS-SMB2 3.3.5.22.2: an ack is valid only while a break is
                 * outstanding.  A duplicate ack (the lease already settled) is
                 * STATUS_UNSUCCESSFUL. */
                if (lease->break_state != CHIMERA_VFS_BREAK_BREAKING) {
                    chimera_smb_open_file_release(request, open_file);
                    chimera_smb_complete_request(request, SMB2_STATUS_UNSUCCESSFUL);
                    return;
                }

                /* The acknowledged state may only DROP bits the break asked the
                * holder to give up -- it must be a subset of the retained mask
                * (break_needed_mode).  An ack that tries to keep a bit being
                * broken (e.g. acking RWH to a W->RH break) is rejected with
                * STATUS_REQUEST_NOT_ACCEPTED and the lease is left BREAKING. */
                if (kept_vfs & ~lease->break_needed_mode) {
                    chimera_smb_open_file_release(request, open_file);
                    chimera_smb_complete_request(request,
                                                 SMB2_STATUS_REQUEST_NOT_ACCEPTED);
                    return;
                }

                struct chimera_vfs_lease_mode kept = {
                    .granted = kept_vfs,
                    .denied  = 0,
                };

                chimera_vfs_lease_ack(lease, kept);
            }
            open_file->lease_state = request->oplock_break.lease_state;
            chimera_smb_open_file_release(request, open_file);
        }

        /* The ack settled the lease; resume any CREATE that parked waiting for
         * this break to complete (MS-SMB2 pending-open). */
        chimera_smb_create_resume_parked(request);
        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
        return;
    }

    open_file = chimera_smb_open_file_resolve(request, &request->oplock_break.file_id);

    if (open_file) {
        if (!open_file->oplock_break_ack_required) {
            /* No acknowledgment was expected for this break -- e.g. it broke a
             * LEVEL_II oplock to NONE, which MS-SMB2 says the client must not
             * acknowledge.  Reply with the protocol error (3.3.5.22.2). */
            chimera_smb_open_file_release(request, open_file);
            chimera_smb_complete_request(request,
                                         SMB2_STATUS_INVALID_OPLOCK_PROTOCOL);
            return;
        }

        /* Resolve the outstanding break with the level the client chose to
         * keep (it may drop further than what we asked -- e.g. straight to
         * NONE).  The lease is genuinely BREAKING here (we no longer ack
         * optimistically), so the canonical ack applies the mode, re-arms a
         * surviving lease, and pumps any parked acquirer. */
        if (open_file->grant) {
            struct chimera_vfs_lease_mode kept = {
                .granted = (request->oplock_break.oplock_level ==
                            SMB2_OPLOCK_LEVEL_II) ? CHIMERA_VFS_LEASE_MODE_R : 0,
                .denied  = 0,
            };

            chimera_vfs_lease_ack(&open_file->grant->lease, kept);
        }
        (void) vfs_state;

        open_file->oplock_level              = request->oplock_break.oplock_level;
        open_file->oplock_break_ack_required = 0;

        chimera_smb_open_file_release(request, open_file);
    }

    /* Resume any CREATE parked waiting for this break to complete. */
    chimera_smb_create_resume_parked(request);
    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_oplock_break */

SYMBOL_EXPORT void
chimera_smb_oplock_break_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{
    if (request->oplock_break.is_lease) {
        /* Lease OPLOCK_BREAK Response — MS-SMB2 §2.2.25.2, 36 bytes. */
        evpl_iovec_cursor_append_uint16(reply_cursor,
                                        SMB2_OPLOCK_BREAK_ACK_LEASE_SIZE);
        evpl_iovec_cursor_append_uint16(reply_cursor, 0);  /* Reserved */
        evpl_iovec_cursor_append_uint32(reply_cursor,
                                        request->oplock_break.lease_flags);
        /* LeaseKey (16) */
        evpl_iovec_cursor_append_blob(reply_cursor,
                                      request->oplock_break.lease_key, 16);
        /* LeaseState (4) — echo what the client asked for. */
        evpl_iovec_cursor_append_uint32(reply_cursor,
                                        request->oplock_break.lease_state);
        /* LeaseDuration (8) — reserved. */
        evpl_iovec_cursor_append_uint64(reply_cursor, 0);
    } else {
        /* Legacy OPLOCK_BREAK Response — MS-SMB2 §2.2.25.1, 24 bytes. */
        evpl_iovec_cursor_append_uint16(reply_cursor,
                                        SMB2_OPLOCK_BREAK_ACK_LEGACY_SIZE);
        evpl_iovec_cursor_append_uint8(reply_cursor,
                                       request->oplock_break.oplock_level);
        evpl_iovec_cursor_append_uint8(reply_cursor, 0);   /* Reserved */
        evpl_iovec_cursor_append_uint32(reply_cursor, 0);  /* Reserved2 */
        evpl_iovec_cursor_append_uint64(reply_cursor,
                                        request->oplock_break.file_id.pid);
        evpl_iovec_cursor_append_uint64(reply_cursor,
                                        request->oplock_break.file_id.vid);
    }
} /* chimera_smb_oplock_break_reply */
