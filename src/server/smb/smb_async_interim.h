// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>

struct chimera_smb_request;

/* Arm a one-shot timer that, if not cancelled within delay_us, emits a
 * standalone SMB2 STATUS_PENDING interim response for the request and leaves
 * request->async_id set so the eventual final response carries a matching
 * SMB2_FLAGS_ASYNC_COMMAND.
 *
 * Snapshots the signing key, session_id, dialect, credit fields, and the
 * signed-session bit at arm time so the fire path does not race with
 * concurrent session teardown for the signing material.
 *
 * Must be called on the request's conn thread (the dispatch site).  The
 * timer fires on the same thread.  If chimera_smb_complete_request runs
 * first (the synchronous fast path) it calls _cancel and the timer is
 * removed before its first opportunity to fire. */
void
chimera_smb_async_interim_arm(
    struct chimera_smb_request *request,
    uint64_t                    delay_us);

/* Cancel an armed interim timer.  Idempotent: evpl_remove_timer after a
 * oneshot has fired is a documented no-op.  Unlinks the request from the
 * conn's parked list and clears the armed bit.  Does NOT clear
 * request->async_id: if the timer had already fired, an interim is on the
 * wire and the final response must echo the AsyncId. */
void
chimera_smb_async_interim_cancel(
    struct chimera_smb_request *request);
