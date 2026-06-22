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
    struct chimera_smb_conn          *conn;
    struct chimera_server_smb_thread *conn_thread;
    uint8_t                           new_vfs;
    uint8_t                           oplock_level;
    uint8_t                           lease_key[16];
    uint64_t                          file_id_pid, file_id_vid;

    /* Select a live member under the grant's file->lock (the holder list is
     * mutated by add/remove-member under the same lock).  A member is live when
     * its create_conn is still set and it has not been closed.
     *
     * Snapshot create_conn while holding the lock: conn_free clears
     * open_file->create_conn to NULL (and recycles the conn) under file->lock
     * before returning the conn to the free pool.  Reading create_conn after
     * the lock is dropped is a use-after-free if that race fires — the open
     * can be freed to the pool and re-stamped with a different create_conn by
     * then, or the conn itself can be recycled and reused for a new connection.
     * Snapshot all per-open fields needed after the lock here to close this
     * window. */
    pthread_mutex_lock(&file->lock);
    conn        = NULL;
    conn_thread = NULL;
    for (open_file = grant->holders; open_file;
         open_file = open_file->grant_member_next) {
        if (!(open_file->flags & CHIMERA_SMB_OPEN_FILE_CLOSED) &&
            open_file->create_conn) {
            conn        = open_file->create_conn;
            conn_thread = conn->thread;
            break;
        }
    }
    if (open_file && conn) {
        /* Snapshot the immutable fields we need after the lock is released.
         * oplock_level is set at CREATE time and transitions only under this
         * same file->lock (break_cb is serialized by vfs_state).  lease_key
         * and file_id are written once at CREATE time and never change. */
        oplock_level = open_file->oplock_level;
        memcpy(lease_key, open_file->lease_key, 16);
        file_id_pid = open_file->file_id.pid;
        file_id_vid = open_file->file_id.vid;
    }
    if (!open_file && (lease->mode.granted & CHIMERA_VFS_LEASE_MODE_W)) {
        /* Breaking a WRITE-caching holder none of whose opens can be notified:
         * the disconnected durable handle yields to the conflicting acquirer
         * (MS-SMB2 3.3.4.6/3.3.4.7 close a disconnected open whose batch
         * oplock / write-caching lease breaks).  The purge-on-conflict path
         * (chimera_smb_create_purge_parked_writers) evicts such a holder when
         * it is already parked; when this break races the disconnect teardown
         * instead, mark the members yielded so a durable reconnect is refused
         * (the grace-timer sweep reaps the carcass).  A holder with no write
         * cache (a parked RH lease) is courtesy-held: its lease is still
         * revoked below, but the handle stays reclaimable
         * (keep-disconnected-rh-*). */
        struct chimera_smb_open_file *member;

        for (member = grant->holders; member;
             member = member->grant_member_next) {
            member->flags |= CHIMERA_SMB_OPEN_FILE_YIELDED;
            /* Courtesy-hold the yielding holder's share reservation too, in the
             * same file->lock section that revokes its lease (below).  The
             * holder's connection is already gone but its disconnect teardown
             * has not yet removed its share reservation; without this a racing
             * batch/durable opener of the same file would see a surviving
             * sole-opener and be capped to LEVEL_II with no durable handle, even
             * though the holder has yielded (smb2.durable-open.oplock: io2 races
             * io1's TCP disconnect).  The parked flag makes the conflict matrix's
             * sole-opener check skip this reservation, so the new open takes the
             * full BATCH + durable grant -- matching the already-parked-holder
             * outcome of chimera_smb_create_purge_parked_writers.  The teardown
             * later removes the reservation outright; setting parked early is
             * idempotent with the park it would otherwise do. */
            if (member->share_lease_inserted) {
                member->share_lease.parked = 1;
            }
        }
    }
    pthread_mutex_unlock(&file->lock);

    /* No live member can notify the client; the pragmatic recovery is to forcibly
     * revoke the lease so the pending acquire (or future acquires) can proceed.
     * This mirrors the Linux kernel's F_SETLEASE forcible expiry path. */
    if (!open_file || !conn) {
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

    if (oplock_level == SMB2_OPLOCK_LEVEL_LEASE) {
        /* SMB2 lease (RqLs) — §2.2.23.2 lease-variant notification. */
        uint8_t current_smb = chimera_smb_vfs_to_lease_bits(lease->mode.granted);
        uint8_t new_smb     = chimera_smb_vfs_to_lease_bits(new_vfs);

        /* The lease epoch lives on the GRANT so all coalesced opens share one
         * monotonic counter (MS-SMB2 3.3.5.9.11).  It is advanced once per break
         * EVENT by the VFS layer (chimera_vfs_lease_begin_break_ex, on the
         * IDLE/ACKED -> BREAKING transition) so a multi-notification cascade
         * (RWH -> RH -> R -> NONE driven by a single conflicting open) keeps one
         * epoch across all of its steps -- this cb is re-invoked once per step.
         * Here we only SNAPSHOT the already-advanced epoch.  Only a v2 lease
         * versions its state; a v1 lease breaks with epoch 0. */
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
         * because evpl iovec pools and binds are thread-local.  Use the
         * snapshotted conn (captured under file->lock above) — do NOT re-read
         * open_file->create_conn here, which conn_free may have cleared to NULL
         * by this point. */
        {
            struct chimera_smb_lease_break_msg *msg;
            bool                                ring = false;

            pthread_mutex_lock(&conn_thread->lease_break_lock);
            if (!conn->lease_break_tearing_down) {
                msg           = calloc(1, sizeof(*msg));
                msg->conn     = conn;
                msg->is_lease = true;
                memcpy(msg->lease_key, lease_key, 16);
                msg->current_state             = current_smb;
                msg->new_state                 = new_smb;
                msg->ack_required              = ack_req;
                msg->new_epoch                 = open_file->lease_epoch;
                msg->next                      = conn_thread->lease_break_ready;
                conn_thread->lease_break_ready = msg;
                /* Defer ONLY a directory-lease break whose holder connection is
                 * itself mid-compound (a self-break: the same connection both
                 * triggered the op and holds the lease).  That op replies without
                 * waiting for the ack, so we let its reply path flush the break
                 * right after the reply (reply-before-break — MS-SMB2 dir-lease
                 * ordering; smbtorture rename/unlink).  Fire immediately when the
                 * holder connection is idle (in_compound == 0): the break was
                 * triggered by a DIFFERENT connection whose op may PARK on this
                 * ack (overwrite / v2_request / rename_dst_parent), and the idle
                 * holder will never produce a reply to trigger the flush — so a
                 * deferral here would deadlock.  File leases / legacy oplocks
                 * always doorbell immediately for the same parking reason. */
                ring = !lease->is_dir || conn->in_compound == 0;
            }
            pthread_mutex_unlock(&conn_thread->lease_break_lock);
            if (ring) {
                evpl_ring_doorbell(&conn_thread->lease_break_doorbell);
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
            (oplock_level == SMB2_OPLOCK_LEVEL_EXCLUSIVE ||
             oplock_level == SMB2_OPLOCK_LEVEL_BATCH);
        grant->break_ack_required = open_file->oplock_break_ack_required;

        {
            struct chimera_smb_lease_break_msg *msg;

            pthread_mutex_lock(&conn_thread->lease_break_lock);
            if (!conn->lease_break_tearing_down) {
                msg                            = calloc(1, sizeof(*msg));
                msg->conn                      = conn;
                msg->is_lease                  = false;
                msg->file_id_pid               = file_id_pid;
                msg->file_id_vid               = file_id_vid;
                msg->new_oplock_level          = new_level;
                msg->next                      = conn_thread->lease_break_ready;
                conn_thread->lease_break_ready = msg;
                pthread_mutex_unlock(&conn_thread->lease_break_lock);
                /* Legacy oplocks are never directory leases and may be waited on
                 * by a parking open, so always wake the thread immediately. */
                evpl_ring_doorbell(&conn_thread->lease_break_doorbell);
            } else {
                pthread_mutex_unlock(&conn_thread->lease_break_lock);
            }
        }
        open_file->oplock_level = new_level;
    }

    /* A break that needs no client ack (it stripped only read caching, e.g. the
     * R->NONE tail of an RWH->RH->R->NONE cascade, or a same-key reader broken
     * by a peer's write) will never be resolved by an inbound OPLOCK_BREAK ack.
     * Settle the lease NOW to its retained mode so it leaves the BREAKING state:
     *   - it must return to IDLE at its new level so a *later* conflicting op can
     *     break it again (smb2.lease.nobreakself writes twice through one handle,
     *     each expecting a fresh break of the other lease; begin_break is a no-op
     *     on a lease still stuck BREAKING), and
     *   - a same-key re-open must NOT report SMB2_LEASE_FLAG_BREAK_IN_PROGRESS
     *     once the (un-acked) downgrade has settled (smb2.lease.break's third
     *     open checks lease_flags == 0).
     * chimera_vfs_lease_ack with the retained mode is the canonical settle: it
     * re-arms a surviving lease to IDLE (or goes inert ACKED at NONE) and pumps
     * any acquirer parked on the break.  An ack-required break instead waits for
     * the client's real OPLOCK_BREAK ack (handled in chimera_smb_oplock_break). */
    if (!grant->break_ack_required) {
        struct chimera_vfs_lease_mode settled = {
            .granted = new_vfs,
            .denied  = 0,
        };

        chimera_vfs_lease_ack(lease, settled);

        evpl_ring_doorbell(&conn_thread->lease_resume_doorbell);
        chimera_smb_create_resume_parked_broadcast(conn_thread);
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

/* Detach and send every queued lease-break notification for `thread`.  Runs on
 * the thread that owns the target connections (the doorbell handler or, for
 * op-triggered breaks, the request's own reply path), so a connection in the
 * list is guaranteed live here (conn_free drains under the same lock). */
SYMBOL_EXPORT void
chimera_smb_lease_break_flush(struct chimera_server_smb_thread *thread)
{
    struct chimera_smb_lease_break_msg *list, *msg, *fifo = NULL;

    pthread_mutex_lock(&thread->lease_break_lock);
    list                      = thread->lease_break_ready;
    thread->lease_break_ready = NULL;
    pthread_mutex_unlock(&thread->lease_break_lock);

    /* The queue is built by prepending (LIFO).  Reverse it so notifications are
     * sent in the order they were triggered (FIFO) — a cross-directory rename
     * breaks the source parent before the destination parent, and the client
     * expects that order (smbtorture dirlease.rename otherdir cases). */
    while (list) {
        msg       = list;
        list      = list->next;
        msg->next = fifo;
        fifo      = msg;
    }
    list = fifo;

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
} /* chimera_smb_lease_break_flush */

/* Doorbell handler: runs on a connection-owning SMB thread and sends the
 * lease-break notifications that break_cbs (possibly on other threads) queued
 * for connections owned by this thread.  Used for breaks NOT tied to an
 * in-flight request on the holder's thread (cross-connection mutations, the
 * idle reaper); a break triggered by a request on the holder's own thread is
 * instead flushed after that request's reply (chimera_smb_compound_reply) so
 * the reply reaches the client before the break (MS-SMB2 ordering). */
SYMBOL_EXPORT void
chimera_smb_lease_break_doorbell_callback(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct chimera_server_smb_thread *thread;

    (void) evpl;

    thread = container_of(doorbell, struct chimera_server_smb_thread,
                          lease_break_doorbell);

    chimera_smb_lease_break_flush(thread);
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
    /* Initialized so a short read cannot feed an uninitialized value into the
     * lease_state computation below (which runs before the aggregate prc check). */
    uint16_t reserved   = 0;
    uint32_t reserved32 = 0;

    int      prc = 0;

    if (request->request_struct_size == SMB2_OPLOCK_BREAK_ACK_LEASE_SIZE) {
        request->oplock_break.is_lease = true;
        prc                           |= evpl_iovec_cursor_try_get_uint16(request_cursor, &reserved);
        prc                           |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->oplock_break.
                                                                          lease_flags);
        prc |= evpl_iovec_cursor_try_copy(request_cursor, request->oplock_break.lease_key,
                                          16);
        prc                              |= evpl_iovec_cursor_try_get_uint32(request_cursor, &reserved32);
        request->oplock_break.lease_state = (uint8_t) (reserved32 & 0xff);
        /* LeaseDuration (8 bytes, reserved) sits at a 4-aligned offset; the
         * aligning get_uint64 would skip to the next 8-boundary and overrun the
         * 36-byte request.  It is unused, so just advance past it. */
        prc |= evpl_iovec_cursor_try_skip(request_cursor, 8);
    } else if (request->request_struct_size == SMB2_OPLOCK_BREAK_ACK_LEGACY_SIZE) {
        request->oplock_break.is_lease = false;
        prc                           |= evpl_iovec_cursor_try_get_uint8(request_cursor, &request->oplock_break.
                                                                         oplock_level);
        prc |= evpl_iovec_cursor_try_get_uint8(request_cursor, (uint8_t *) &reserved);
        prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &reserved32);
        prc |= evpl_iovec_cursor_try_get_uint64(request_cursor, &request->oplock_break.file_id
                                                .pid);
        prc |= evpl_iovec_cursor_try_get_uint64(request_cursor, &request->oplock_break.file_id
                                                .vid);
    } else {
        chimera_smb_error("SMB2 OPLOCK_BREAK ack with unexpected struct size %u",
                          request->request_struct_size);
        request->status = SMB2_STATUS_INVALID_PARAMETER;
        return -1;
    }

    if (unlikely(prc)) {
        chimera_smb_error("Received SMB2 OPLOCK_BREAK ack truncated in body");
        return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
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
        /* A lease-break ack whose lease key resolves to no open is benign --
         * the holder closed before acking, or another handle still carries the
         * lease (Samba smb2.lease.v2_complex2 acks after the breaking handle is
         * gone and expects STATUS_OK).  FILE_CLOSED is reserved for the oplock
         * (FileId) ack path below per MS-SMB2 3.3.5.22.1; here we fall through to
         * settle/resume and reply SUCCESS. */

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
    } else {
        /* MS-SMB2 3.3.5.22.1 "Processing an Oplock Acknowledgment": if the
         * FileId in the acknowledgment does not map to an open in the table
         * (the open was closed before the client's ack arrived), the server
         * MUST fail the request with STATUS_FILE_CLOSED.  Previously this path
         * fell through to STATUS_SUCCESS, which the MS-SMB2Model suite flags. */
        chimera_smb_create_resume_parked(request);
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
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
