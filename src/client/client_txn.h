// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * Shared transaction driver for the client API (mirror of the NFS3 server's
 * nfs3_txn.c).  One client operation == one VFS transaction: begin (read or
 * write mode) -> the op's VFS calls (all enlisted via request->txn) -> commit,
 * which must be durable before the user callback fires.  A wait-die (diskfs) or
 * optimistic-commit (cairn) conflict replays the whole op from the top, reusing
 * the stable txn_ts so it cannot starve.  Backends that are not transactional
 * return a NULL handle and every step degrades to the previous autocommit
 * behaviour, so a non-transactional mount is bit-for-bit unchanged.
 *
 * An op plugs in two callbacks:
 *   txn_start(thread, request) -- run the op's VFS chain (request->txn is set;
 *                                 pass it down to every enlisted VFS call).
 *   txn_reply(thread, request) -- release any op-owned resources, invoke the
 *                                 user callback from request->txn_op_status and
 *                                 the op's stashed result fields, and free the
 *                                 request.
 * The op's VFS terminal calls chimera_client_txn_finish(thread, request,
 * status); on success it has already populated its result fields (txn_reply only
 * fixes up the error status).
 *
 * These are static-inline because the dispatch helpers that drive them are
 * themselves static-inline in client_<op>.h and are shared verbatim by both the
 * client public API (src/client) and the POSIX layer (src/posix); keeping the
 * driver header-only avoids a cross-library link dependency.
 */

#include "client_internal.h"
#include "vfs/vfs_procs.h"

#define CHIMERA_CLIENT_TXN_MAX_RETRIES 8

static inline void chimera_client_txn_attempt(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request);

static inline void
chimera_client_txn_replay(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    if (++request->txn_attempt > CHIMERA_CLIENT_TXN_MAX_RETRIES) {
        /* Give up after too many conflicts; surface a retriable error. */
        request->txn_op_status = CHIMERA_VFS_EIO;
        request->txn_reply(thread, request);
        return;
    }

    chimera_client_txn_attempt(thread, request);
} /* chimera_client_txn_replay */

static inline void
chimera_client_txn_committed(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_client_request *request = private_data;
    struct chimera_client_thread  *thread  = request->thread;

    if (error_code == CHIMERA_VFS_ETXN_CONFLICT) {
        chimera_client_txn_replay(thread, request);
        return;
    }

    if (error_code != CHIMERA_VFS_OK) {
        request->txn_op_status = error_code;
    }

    request->txn_reply(thread, request);
} /* chimera_client_txn_committed */

static inline void
chimera_client_txn_aborted(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_client_request *request = private_data;
    struct chimera_client_thread  *thread  = request->thread;

    (void) error_code;

    if (request->txn_op_status == CHIMERA_VFS_ETXN_CONFLICT) {
        chimera_client_txn_replay(thread, request);
    } else {
        request->txn_reply(thread, request);
    }
} /* chimera_client_txn_aborted */

/*
 * Terminal hook for an op's VFS chain.  On success commit (durably for a write
 * transaction, async for a read), on failure abort; the user callback fires
 * only once the commit/abort settles (and a commit-time conflict replays).
 */
static inline void
chimera_client_txn_finish(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request,
    enum chimera_vfs_error         status)
{
    request->txn_op_status = status;

    if (status == CHIMERA_VFS_OK) {
        chimera_vfs_end_transaction(thread->vfs_thread,
                                    chimera_client_req_cred(request),
                                    request->txn,
                                    request->txn_mode == CHIMERA_VFS_TXN_WRITE ?
                                    CHIMERA_VFS_TXN_COMMIT_SYNC :
                                    CHIMERA_VFS_TXN_COMMIT_ASYNC,
                                    chimera_client_txn_committed, request);
    } else {
        chimera_vfs_end_transaction(thread->vfs_thread,
                                    chimera_client_req_cred(request),
                                    request->txn,
                                    CHIMERA_VFS_TXN_ABORT,
                                    chimera_client_txn_aborted, request);
    }
} /* chimera_client_txn_finish */

static inline void
chimera_client_txn_attempt(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    /* Begin is a fast, local action that returns the handle synchronously (NULL
     * for a non-transactional backend -> autocommit).  It cannot conflict; a
     * conflict can only surface later at commit, replaying from here. */
    request->txn = chimera_vfs_begin_transaction(thread->vfs_thread,
                                                 chimera_client_req_cred(request),
                                                 request->txn_fh,
                                                 request->txn_fhlen,
                                                 request->txn_mode,
                                                 request->txn_ts);
    request->txn_op_status = CHIMERA_VFS_OK;
    request->txn_start(thread, request);
} /* chimera_client_txn_attempt */

/*
 * Run `request` as one transaction.  `hint_fh` steers the owning thread (the
 * file's fh for a handle op, the resolution root for a path op); `mode` is the
 * begin mode; `start` runs the op's VFS chain and `reply` builds the final user
 * callback.  Both callbacks must be re-entrant: `start` may run several times
 * (once per conflict replay).
 */
static inline void
chimera_client_txn_run(
    struct chimera_client_thread   *thread,
    struct chimera_client_request  *request,
    const void                     *hint_fh,
    int                             hint_fhlen,
    enum chimera_vfs_txn_mode       mode,
    chimera_client_request_callback start,
    chimera_client_request_callback reply)
{
    request->txn_ts      = chimera_vfs_txn_alloc_ts(thread->vfs_thread);
    request->txn_attempt = 0;
    if (hint_fhlen) {
        memcpy(request->txn_fh, hint_fh, hint_fhlen);
    }
    request->txn_fhlen = hint_fhlen;
    request->txn_mode  = mode;
    request->txn_start = start;
    request->txn_reply = reply;

    chimera_client_txn_attempt(thread, request);
} /* chimera_client_txn_run */
