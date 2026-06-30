// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Generic SMB2 async-interim (STATUS_PENDING) infrastructure.  See
 * smb_async_interim.h for the policy: an interim is emitted the instant a
 * handler decides it must block on an external event (oplock/lease break ack,
 * blocking lock, change notify) -- never on a latency threshold.
 *
 * The interim carries SMB2_FLAGS_ASYNC_COMMAND and an AsyncId = the original
 * MessageId (Samba's convention), so the client can correlate and cancel the
 * eventual final response (which the reply builder tags with the same AsyncId
 * because request->async_id is left set).
 *
 * Everything runs on the request's owning conn thread (the block decision, the
 * eventual completion, and SMB2_CANCEL all serialise on the single-threaded
 * evpl loop), so no locks are needed.
 *
 * CHANGE_NOTIFY keeps its own bespoke interim path in smb_notify.c for now; the
 * two do not conflict (separate parked lists, CANCEL walks both).
 */

#include <string.h>
#include <stddef.h>

#include "smb_internal.h"
#include "smb_async_interim.h"
#include "smb_signing.h"
#include "smb2.h"

#include "evpl/evpl.h"

/* Standalone SMB2 message: 4 byte NetBIOS header + 64 byte SMB2 header +
 * 9 byte SMB2 error response body.  Same shape as
 * chimera_smb_notify_send_interim. */
#define CHIMERA_SMB_ASYNC_INTERIM_LEN 77

static void
chimera_smb_async_interim_unlink(
    struct chimera_smb_conn    *conn,
    struct chimera_smb_request *request)
{
    struct chimera_smb_request **pp = &conn->parked_requests;

    while (*pp) {
        if (*pp == request) {
            *pp                      = request->async.park_next;
            request->async.park_next = NULL;
            return;
        }
        pp = &(*pp)->async.park_next;
    }
} /* chimera_smb_async_interim_unlink */

/* Build and send a standalone STATUS_PENDING interim for `request` on its conn.
 * Caller has populated the request->async snapshot fields and set async_id. */
static void
chimera_smb_async_interim_send(struct chimera_smb_request *request)
{
    struct chimera_smb_compound *compound = request->compound;
    struct chimera_smb_conn     *conn     = compound->conn;
    struct evpl                 *evpl     = conn->thread->evpl;
    struct smb2_header          *hdr;
    struct evpl_iovec            iov;
    uint8_t                     *buf;
    int                          smb2_len;
    uint32_t                     nb;

    evpl_iovec_alloc(evpl, CHIMERA_SMB_ASYNC_INTERIM_LEN, 8, 1, 0, &iov);
    buf = iov.data;
    memset(buf, 0, CHIMERA_SMB_ASYNC_INTERIM_LEN);

    /* NetBIOS framing length filled in below. */
    hdr                 = (struct smb2_header *) (buf + 4);
    hdr->protocol_id[0] = 0xFE;
    hdr->protocol_id[1] = 'S';
    hdr->protocol_id[2] = 'M';
    hdr->protocol_id[3] = 'B';
    hdr->struct_size    = 64;
    hdr->credit_charge  = request->async.credit_charge;
    hdr->status         = SMB2_STATUS_PENDING;
    hdr->command        = request->smb2_hdr.command;
    /* Grant credits (capped), debiting this request's CreditCharge once here;
     * the eventual final response passes consume=0 so the charge is not counted
     * twice (see chimera_smb_grant_credits / chimera_smb_compound_reply). */
    hdr->credit_request_response = chimera_smb_grant_credits(
        conn,
        request->async.credit_charge ? request->async.credit_charge : 1,
        request->async.credit_request);
    hdr->flags          = SMB2_FLAGS_SERVER_TO_REDIR | SMB2_FLAGS_ASYNC_COMMAND;
    hdr->message_id     = request->smb2_hdr.message_id;
    hdr->async.async_id = request->async_id;
    hdr->session_id     = request->async.session_id;

    /* Error response body: StructureSize(2)=9 + ErrorContextCount(1) +
     * Reserved(1) + ByteCount(4) + pad(1). */
    buf[4 + sizeof(*hdr) + 0] = 9;

    smb2_len = (int) sizeof(*hdr) + 9;
    nb       = __builtin_bswap32((uint32_t) smb2_len);
    memcpy(buf, &nb, 4);

    /* Sign or, on an encrypting session, wrap in a TRANSFORM header -- an
     * interim STATUS_PENDING is a response like any other and must be secured
     * the same way (MS-SMB2 §3.3.4.1.4).  This consumes iov. */
    iov.length = CHIMERA_SMB_ASYNC_INTERIM_LEN;
    chimera_smb_secure_send(conn, &iov, smb2_len, &request->async.secure);
} /* chimera_smb_async_interim_send */

void
chimera_smb_async_interim_begin(struct chimera_smb_request *request)
{
    struct chimera_smb_compound *compound = request->compound;
    struct chimera_smb_conn     *conn     = compound->conn;

    /* Already pending (e.g. begin called twice) -- nothing to do. */
    if (request->async.armed) {
        return;
    }

    request->async.armed          = 1;
    request->async.credit_charge  = request->smb2_hdr.credit_charge;
    request->async.credit_request = request->smb2_hdr.credit_request_response;
    request->async.session_id     = request->smb2_hdr.session_id;

    /* Snapshot signing/encryption state so both the interim and the eventual
     * final response are secured the way the synchronous path would secure
     * them (sign if signed; wrap in a TRANSFORM header if the session
     * encrypts). */
    chimera_smb_secure_send_snapshot(request, &request->async.secure);

    /* AsyncId = the original MessageId (Samba's convention; the client
     * correlates and cancels by it). */
    request->async_id = request->smb2_hdr.message_id;

    /* Link onto the conn's parked-requests list so SMB2_CANCEL can find this
     * request by AsyncId and conn_free can drain it on teardown. */
    request->async.park_next = conn->parked_requests;
    conn->parked_requests    = request;

    chimera_smb_async_interim_send(request);
} /* chimera_smb_async_interim_begin */

void
chimera_smb_async_interim_cancel(struct chimera_smb_request *request)
{
    struct chimera_smb_compound *compound = request->compound;
    struct chimera_smb_conn     *conn     = compound->conn;

    /* Safe whether or not a timer was ever armed: evpl_remove_timer no-ops when
     * the timer is not in the heap. */
    evpl_remove_timer(compound->thread->evpl, &request->async.timer);

    chimera_smb_async_interim_unlink(conn, request);

    request->async.armed = 0;

    /* Deliberately do NOT clear request->async_id: an interim is on the wire, so
     * the compound reply builder must set SMB2_FLAGS_ASYNC_COMMAND with this
     * AsyncId on the final response so the client correlates the two halves. */
} /* chimera_smb_async_interim_cancel */

void
chimera_smb_async_interim_drain(struct chimera_smb_conn *conn)
{
    struct chimera_server_smb_thread *thread = conn->thread;
    struct chimera_smb_request       *request;

    while ((request = conn->parked_requests) != NULL) {
        conn->parked_requests    = request->async.park_next;
        request->async.park_next = NULL;
        request->async.armed     = 0;
        evpl_remove_timer(thread->evpl, &request->async.timer);

        /* A parked CREATE holds an open_file reference (taken when it deferred
        * on the lease/oplock break it triggered) that only its resume path
        * would drop -- and with the deadline timer cancelled above, no resume
        * will ever come.  Drop it here or the reference pins the open past
        * its tree's teardown and the open's VFS handle leaks, wedging the
        * VFS close-thread drain at shutdown.  Clear r_open_file so a late
        * completion of the orphaned request cannot touch the released open. */
        if (request->smb2_hdr.command == SMB2_CREATE &&
            request->create.r_open_file) {
            chimera_smb_open_file_release(request, request->create.r_open_file);
            request->create.r_open_file = NULL;
        }
    }

    /* All parked requests are gone, including any blocking named-pipe READs that
     * counted against the async ceiling. */
    conn->async_outstanding = 0;
} /* chimera_smb_async_interim_drain */
