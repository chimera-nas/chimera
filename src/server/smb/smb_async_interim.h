// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>

struct chimera_smb_request;
struct chimera_smb_conn;

/*
 * Generic SMB2 async-interim (STATUS_PENDING) infrastructure.
 *
 * An SMB2 server sends an interim response in exactly one situation: the
 * request cannot be completed now because it must wait on an event the server
 * cannot satisfy synchronously -- an oplock/lease break acknowledgment
 * (MS-SMB2 3.3.5.9), a blocking byte-range lock (3.3.5.14), or a directory
 * change notification (3.3.5.19).  The interim is sent at the moment the server
 * decides to block: it carries STATUS_PENDING, SMB2_FLAGS_ASYNC_COMMAND, and an
 * AsyncId (= the original MessageId, Samba's convention) so the client can
 * correlate -- and cancel (SMB2_CANCEL) -- the eventual final response.  When
 * the handler later completes, the reply builder sees request->async_id != 0 and
 * tags the final response with the same AsyncId.
 *
 * There is deliberately NO latency-threshold trigger: an ordinary request that
 * is merely slow is not made async (the client's request timeout is on the order
 * of tens of seconds, so a sub-second op never needs an interim, and forcing one
 * just adds a wasted round-trip).  Async is driven by the block decision, never
 * by a timer.
 *
 * The whole machinery runs on the request's owning conn thread (dispatch, the
 * block decision, and chimera_smb_complete_request all serialise on the
 * single-threaded evpl loop), so no locks are needed.
 */

/* Emit an interim STATUS_PENDING response for `request` immediately and mark it
 * pending: assigns request->async_id, links the request on its conn's
 * parked_requests list (so SMB2_CANCEL can find it and conn_free can drain it),
 * and leaves request->async_id set so the eventual final response carries a
 * matching SMB2_FLAGS_ASYNC_COMMAND.  Call this the instant a handler decides it
 * must block on an external event, before returning without completing. */
void
chimera_smb_async_interim_begin(
    struct chimera_smb_request *request);

/* Cancel a pending interim: remove the (possibly never-armed) timer, unlink the
 * request from its conn's parked list, and clear the armed bit.  Idempotent.
 * Does NOT clear request->async_id -- if an interim was sent, the final response
 * must still echo the AsyncId.  Called from chimera_smb_complete_request. */
void
chimera_smb_async_interim_cancel(
    struct chimera_smb_request *request);

/* Drain all pending interim requests on a connection being torn down: unlink
 * them and remove any armed timers.  The requests themselves are not freed --
 * they remain owned by their compounds, which tear down through the normal
 * path. */
void
chimera_smb_async_interim_drain(
    struct chimera_smb_conn *conn);
