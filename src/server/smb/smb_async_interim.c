// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Generic SMB2 async-interim (STATUS_PENDING) infrastructure.
 *
 * The dispatch site arms a one-shot timer on a defer-eligible request just
 * before calling the command handler.  If the handler completes within the
 * defer window (the common case on memfs/io_uring), chimera_smb_complete_request
 * removes the timer and the request proceeds synchronously.  If the timer
 * fires first, this file emits a standalone interim response carrying
 * SMB2_FLAGS_ASYNC_COMMAND and an AsyncId = the original MessageId (Samba's
 * convention), so the client knows the request is in flight and can later
 * correlate the final response by AsyncId.  When the handler eventually
 * completes, the existing reply builder sees request->async_id != 0 and emits
 * the final response with the same flag.
 *
 * The whole machinery runs on the request's owning conn thread (the dispatch
 * site, the timer fire, and chimera_smb_complete_request all serialise on the
 * single-threaded evpl loop), so no locks are needed.
 *
 * CHANGE_NOTIFY keeps its own bespoke interim path in smb_notify.c for now;
 * that path predates this one and is correct.  A future cleanup can fold it
 * in once this path is exercised by an actually-async producer (oplock break,
 * lease break, blocking lock).
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

static void
chimera_smb_async_interim_fire(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct chimera_smb_request  *request = (struct chimera_smb_request *)
        ((char *) timer - offsetof(struct chimera_smb_request, async.timer));
    struct chimera_smb_compound *compound = request->compound;
    struct chimera_smb_conn     *conn     = compound->conn;
    struct smb2_header          *hdr;
    struct evpl_iovec            iov;
    uint8_t                     *buf;
    int                          smb2_len;
    uint32_t                     nb;

    /* Assign the AsyncId.  Samba uses the original MessageId; chimera follows
     * suit -- there is no separate id space and the client correlates by it. */
    request->async_id = request->smb2_hdr.message_id;

    evpl_iovec_alloc(evpl, CHIMERA_SMB_ASYNC_INTERIM_LEN, 8, 1, 0, &iov);
    buf = iov.data;
    memset(buf, 0, CHIMERA_SMB_ASYNC_INTERIM_LEN);

    /* NetBIOS framing length filled in below. */
    hdr                          = (struct smb2_header *) (buf + 4);
    hdr->protocol_id[0]          = 0xFE;
    hdr->protocol_id[1]          = 'S';
    hdr->protocol_id[2]          = 'M';
    hdr->protocol_id[3]          = 'B';
    hdr->struct_size             = 64;
    hdr->credit_charge           = request->async.credit_charge;
    hdr->status                  = SMB2_STATUS_PENDING;
    hdr->command                 = request->smb2_hdr.command;
    hdr->credit_request_response = request->async.credit_request
        ? request->async.credit_request : 1;
    hdr->flags = SMB2_FLAGS_SERVER_TO_REDIR |
        SMB2_FLAGS_ASYNC_COMMAND;
    hdr->message_id     = request->smb2_hdr.message_id;
    hdr->async.async_id = request->async_id;
    hdr->session_id     = request->async.session_id;

    /* Error response body: StructureSize(2) + ErrorContextCount(1) +
     * Reserved(1) + ByteCount(4) + pad(1). */
    buf[4 + sizeof(*hdr) + 0] = 9;

    smb2_len = (int) sizeof(*hdr) + 9;
    nb       = __builtin_bswap32((uint32_t) smb2_len);
    memcpy(buf, &nb, 4);

    if (request->async.signed_session) {
        chimera_smb_sign_message(conn->thread->signing_ctx,
                                 request->async.dialect,
                                 request->async.signing_key,
                                 buf + 4,
                                 smb2_len);
    }

    iov.length = CHIMERA_SMB_ASYNC_INTERIM_LEN;
    evpl_sendv(evpl, conn->bind, &iov, 1, iov.length, EVPL_SEND_FLAG_TAKE_REF);
} /* chimera_smb_async_interim_fire */

void
chimera_smb_async_interim_arm(
    struct chimera_smb_request *request,
    uint64_t                    delay_us)
{
    struct chimera_smb_compound *compound = request->compound;
    struct chimera_smb_conn     *conn     = compound->conn;

    request->async.armed          = 1;
    request->async.credit_charge  = request->smb2_hdr.credit_charge;
    request->async.credit_request = request->smb2_hdr.credit_request_response;
    request->async.dialect        = conn->dialect;
    request->async.session_id     = request->smb2_hdr.session_id;

    /* Sign the interim iff the original request was signed (same rule as the
     * change-notify path).  An unsigned async reply on a signed session is
     * rejected by Windows clients. */
    if ((request->smb2_hdr.flags & SMB2_FLAGS_SIGNED) &&
        request->session_handle) {
        request->async.signed_session = 1;
        memcpy(request->async.signing_key,
               request->session_handle->signing_key,
               sizeof(request->async.signing_key));
    } else {
        request->async.signed_session = 0;
    }

    /* Link onto the conn's parked-requests list so CANCEL can find this
     * request by AsyncId and conn_free can drain it on teardown. */
    request->async.park_next = conn->parked_requests;
    conn->parked_requests    = request;

    evpl_add_oneshot_timer(compound->thread->evpl,
                           &request->async.timer,
                           chimera_smb_async_interim_fire,
                           delay_us);
} /* chimera_smb_async_interim_arm */

void
chimera_smb_async_interim_cancel(struct chimera_smb_request *request)
{
    struct chimera_smb_compound *compound = request->compound;
    struct chimera_smb_conn     *conn     = compound->conn;

    /* Documented safe whether or not the oneshot has already fired. */
    evpl_remove_timer(compound->thread->evpl, &request->async.timer);

    chimera_smb_async_interim_unlink(conn, request);

    request->async.armed = 0;

    /* Deliberately do NOT clear request->async_id.  If the timer had already
     * fired, the interim is on the wire and the compound reply builder must
     * still set SMB2_FLAGS_ASYNC_COMMAND with this AsyncId on the final
     * response so the client correlates the two halves. */
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
    }
} /* chimera_smb_async_interim_drain */
