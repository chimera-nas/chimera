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

/* Map vfs_state RWH bits to the SMB2 lease state encoding (different
 * bit positions for H and W). */
static inline uint8_t
chimera_smb_vfs_to_lease_bits(uint8_t vfs_mode)
{
    uint8_t s = 0;

    if (vfs_mode & CHIMERA_VFS_LEASE_MODE_R) {
        s |= SMB2_LEASE_READ_CACHING;
    }
    if (vfs_mode & CHIMERA_VFS_LEASE_MODE_H) {
        s |= SMB2_LEASE_HANDLE_CACHING;
    }
    if (vfs_mode & CHIMERA_VFS_LEASE_MODE_W) {
        s |= SMB2_LEASE_WRITE_CACHING;
    }
    return s;
} /* chimera_smb_vfs_to_lease_bits */

static inline uint8_t
chimera_smb_lease_to_vfs_bits(uint8_t smb_state)
{
    uint8_t m = 0;

    if (smb_state & SMB2_LEASE_READ_CACHING) {
        m |= CHIMERA_VFS_LEASE_MODE_R;
    }
    if (smb_state & SMB2_LEASE_HANDLE_CACHING) {
        m |= CHIMERA_VFS_LEASE_MODE_H;
    }
    if (smb_state & SMB2_LEASE_WRITE_CACHING) {
        m |= CHIMERA_VFS_LEASE_MODE_W;
    }
    return m;
} /* chimera_smb_lease_to_vfs_bits */

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
    /* Flags (4) — ACK_REQUIRED so the client must respond */
    p[4] = SMB2_OPLOCK_BREAK_FLAG_ACK_REQUIRED;
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

/* break_cb wired onto every SMB CACHING lease at CREATE time.  The
 * cb_private is the owning open_file. */
SYMBOL_EXPORT void
chimera_smb_lease_break_cb(
    struct chimera_vfs_lease *lease,
    uint8_t                   needed_mode,
    void                     *private_data)
{
    struct chimera_smb_open_file *open_file = private_data;
    uint8_t                       new_vfs;

    /* If the conn is no longer live, we can't notify the client; the
     * pragmatic recovery is to forcibly revoke the lease so the pending
     * acquire (or future acquires) can proceed.  This mirrors the
     * Linux kernel's F_SETLEASE forcible expiry path. */
    if (!open_file->create_conn) {
        chimera_vfs_lease_revoke(lease);
        return;
    }

    /* needed_mode is the *retained* mask the holder may keep:
     *   - A conflicting OPEN passes a non-zero mask (the new acquirer's
     *     granted bits); the holder keeps a shared read cache (R) and
     *     gives up write/handle caching — i.e. it downgrades to LEVEL_II.
     *   - A WRITE invalidation passes 0; the holder's read cache is now
     *     stale, so it breaks all the way to NONE.  (See
     *     chimera_vfs_state_break_on_write.) */
    if (needed_mode == 0) {
        new_vfs = 0;
    } else {
        new_vfs = lease->mode.granted & CHIMERA_VFS_LEASE_MODE_R;
    }

    if (open_file->oplock_level == SMB2_OPLOCK_LEVEL_LEASE) {
        /* SMB2 lease (RqLs) — §2.2.23.2 lease-variant notification. */
        uint8_t current_smb = chimera_smb_vfs_to_lease_bits(lease->mode.granted);
        uint8_t new_smb     = chimera_smb_vfs_to_lease_bits(new_vfs);

        open_file->lease_epoch++;
        chimera_smb_send_oplock_break_lease(open_file->create_conn,
                                            open_file->lease_key,
                                            current_smb,
                                            new_smb,
                                            open_file->lease_epoch);
        open_file->lease_state = new_smb;
    } else {
        /* Legacy oplock — §2.2.23.1 notification keyed by FileId.  Break
         * to LEVEL_II when a read cache survives, otherwise to NONE. */
        uint8_t new_level = (new_vfs & CHIMERA_VFS_LEASE_MODE_R)
                            ? SMB2_OPLOCK_LEVEL_II
                            : SMB2_OPLOCK_LEVEL_NONE;

        chimera_smb_send_oplock_break_legacy(open_file->create_conn,
                                             open_file->file_id.pid,
                                             open_file->file_id.vid,
                                             new_level);
        open_file->oplock_level = new_level;
    }

    /* The holder is downgraded only when its OPLOCK_BREAK ack arrives.
     * vfs_state queues conflicting work until chimera_smb_oplock_break()
     * applies that ack below. */
} /* chimera_smb_lease_break_cb */

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
    uint64_t reserved64;

    if (request->request_struct_size == SMB2_OPLOCK_BREAK_ACK_LEASE_SIZE) {
        request->oplock_break.is_lease = true;
        evpl_iovec_cursor_get_uint16(request_cursor, &reserved);
        evpl_iovec_cursor_get_uint32(request_cursor, &request->oplock_break.lease_flags);
        evpl_iovec_cursor_copy(request_cursor, request->oplock_break.lease_key, 16);
        evpl_iovec_cursor_get_uint32(request_cursor, &reserved32);
        request->oplock_break.lease_state = (uint8_t) (reserved32 & 0xff);
        evpl_iovec_cursor_get_uint64(request_cursor, &reserved64);
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
    struct chimera_smb_open_file     *open_file = NULL;
    struct chimera_vfs_lease_mode     mode      = { 0, 0 };
    struct chimera_server_smb_thread *thread    = request->compound->thread;
    struct chimera_vfs_state         *vfs_state = thread->vfs_thread->vfs->vfs_state;

    if (request->oplock_break.is_lease) {
        uint32_t bucket;

        if (request->tree) {
            for (bucket = 0; bucket < CHIMERA_SMB_OPEN_FILE_BUCKETS; bucket++) {
                for (open_file = request->tree->open_files[bucket];
                     open_file;
                     open_file = open_file->next) {
                    if (open_file->caching_lease_inserted &&
                        open_file->oplock_level == SMB2_OPLOCK_LEVEL_LEASE &&
                        memcmp(open_file->lease_key,
                               request->oplock_break.lease_key, 16) == 0) {
                        goto found;
                    }
                }
            }
        }
        open_file = NULL;
 found:
        mode.granted = chimera_smb_lease_to_vfs_bits(request->oplock_break.lease_state);
    } else {
        open_file = chimera_smb_open_file_resolve(request,
                                                  &request->oplock_break.file_id);
        switch (request->oplock_break.oplock_level) {
            case SMB2_OPLOCK_LEVEL_II:
                mode.granted = CHIMERA_VFS_LEASE_MODE_R;
                break;
            case SMB2_OPLOCK_LEVEL_NONE:
            default:
                mode.granted = 0;
                break;
        } /* switch */
    }

    if (!open_file || !open_file->caching_lease_inserted) {
        if (open_file && !request->oplock_break.is_lease) {
            chimera_smb_open_file_release(request, open_file);
        }
        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
        return;
    }

    chimera_vfs_lease_ack(&open_file->caching_lease, mode);

    if (mode.granted == 0) {
        chimera_vfs_lease_release(vfs_state, open_file->caching_file_state,
                                  &open_file->caching_lease);
        open_file->caching_lease_inserted = false;
        chimera_vfs_state_put(vfs_state, open_file->caching_file_state);
        open_file->caching_file_state = NULL;
    }

    if (request->oplock_break.is_lease) {
        open_file->lease_state = request->oplock_break.lease_state;
    } else {
        open_file->oplock_level = request->oplock_break.oplock_level;
        chimera_smb_open_file_release(request, open_file);
    }

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
