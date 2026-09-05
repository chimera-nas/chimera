// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * Shared compound driver for the client API (mirror of the NFS3 server's
 * nfs3_compound.c).  One client operation == one VFS compound: begin (read or
 * write mode) -> the op's VFS calls (all enlisted via request->compound) ->
 * commit, which must be durable before the user callback fires.  A wait-die
 * (diskfs) or optimistic-commit (cairn) conflict replays the whole op from the
 * top, reusing the stable compound_ts so it cannot starve.  Begin never
 * returns NULL: on a backend that is not compound-capable the handle comes
 * back UNBOUND, every op ejects individually (autocommits standalone,
 * exactly the previous behaviour) and end is a synchronous OK.  A path op
 * whose hint (the resolution root) is not compound-capable also starts
 * unbound but lazy-binds at the first component op on a capable mount, so
 * client path ops are grouped on such backends too.
 *
 * An op plugs in two callbacks:
 *   compound_start(thread, request) -- run the op's VFS chain
 *                                      (request->compound is set; pass it down
 *                                      to every enlisted VFS call).
 *   compound_reply(thread, request) -- release any op-owned resources, invoke
 *                                      the user callback from
 *                                      request->compound_op_status and the op's
 *                                      stashed result fields, and free the
 *                                      request.
 * The op's VFS terminal calls chimera_client_compound_finish(thread, request,
 * status); on success it has already populated its result fields
 * (compound_reply only fixes up the error status).
 *
 * These are static-inline because the dispatch helpers that drive them are
 * themselves static-inline in client_<op>.h and are shared verbatim by both the
 * client public API (src/client) and the POSIX layer (src/posix); keeping the
 * driver header-only avoids a cross-library link dependency.
 */

#include "client_internal.h"
#include "vfs/vfs_procs.h"

#define CHIMERA_CLIENT_COMPOUND_MAX_RETRIES 8

static inline void chimera_client_compound_attempt(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request);

static inline void
chimera_client_compound_replay(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    if (++request->compound_attempt > CHIMERA_CLIENT_COMPOUND_MAX_RETRIES) {
        /* Give up after too many conflicts; surface a retriable status
         * (EAGAIN at the POSIX layer) distinct from a real I/O failure. */
        request->compound_op_status = CHIMERA_VFS_ECOMPOUND_EXHAUSTED;
        request->compound_reply(thread, request);
        return;
    }

    chimera_client_compound_attempt(thread, request);
} /* chimera_client_compound_replay */

static inline void
chimera_client_compound_committed(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_client_request *request = private_data;
    struct chimera_client_thread  *thread  = request->thread;

    if (error_code == CHIMERA_VFS_ECOMPOUND_CONFLICT) {
        chimera_client_compound_replay(thread, request);
        return;
    }

    if (error_code != CHIMERA_VFS_OK) {
        request->compound_op_status = error_code;
    }

    request->compound_reply(thread, request);
} /* chimera_client_compound_committed */

static inline void
chimera_client_compound_aborted(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_client_request *request = private_data;
    struct chimera_client_thread  *thread  = request->thread;

    (void) error_code;

    if (request->compound_op_status == CHIMERA_VFS_ECOMPOUND_CONFLICT) {
        chimera_client_compound_replay(thread, request);
    } else {
        request->compound_reply(thread, request);
    }
} /* chimera_client_compound_aborted */

/*
 * Terminal hook for an op's VFS chain.  On success commit (durably for a write
 * compound, async for a read), on failure abort; the user callback fires
 * only once the commit/abort settles (and a commit-time conflict replays).
 */
static inline void
chimera_client_compound_finish(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request,
    enum chimera_vfs_error         status)
{
    request->compound_op_status = status;

    if (status == CHIMERA_VFS_OK) {
        chimera_vfs_compound_end(thread->vfs_thread,
                                 chimera_client_req_cred(request),
                                 request->compound,
                                 request->compound_mode == CHIMERA_VFS_COMPOUND_WRITE ?
                                 CHIMERA_VFS_COMPOUND_COMMIT_DURABLE :
                                 CHIMERA_VFS_COMPOUND_COMMIT,
                                 chimera_client_compound_committed, request);
    } else {
        chimera_vfs_compound_end(thread->vfs_thread,
                                 chimera_client_req_cred(request),
                                 request->compound,
                                 CHIMERA_VFS_COMPOUND_ABORT,
                                 chimera_client_compound_aborted, request);
    }
} /* chimera_client_compound_finish */

static inline void
chimera_client_compound_attempt(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    /* Begin is a fast, local action that returns the handle synchronously and
     * never NULL: on a non-compound backend the handle is unbound and every
     * op ejects (autocommit, as before); it may still lazy-bind at the first
     * op on a capable mount.  RETRYABLE: this driver replays the whole op on
     * ECOMPOUND_CONFLICT (below), reusing the stable ts.  Begin cannot
     * conflict; a conflict can only surface later at commit, replaying from
     * here. */
    request->compound = chimera_vfs_compound_begin(thread->vfs_thread,
                                                   chimera_client_req_cred(request),
                                                   request->compound_fh,
                                                   request->compound_fhlen,
                                                   request->compound_mode,
                                                   request->compound_ts,
                                                   CHIMERA_VFS_COMPOUND_RETRYABLE);
    request->compound_op_status = CHIMERA_VFS_OK;
    request->compound_start(thread, request);
} /* chimera_client_compound_attempt */

/*
 * Run `request` as one compound.  `hint_fh` steers the owning thread (the
 * file's fh for a handle op, the resolution root for a path op); `mode` is the
 * begin mode; `start` runs the op's VFS chain and `reply` builds the final user
 * callback.  Both callbacks must be re-entrant: `start` may run several times
 * (once per conflict replay).
 */
static inline void
chimera_client_compound_run(
    struct chimera_client_thread   *thread,
    struct chimera_client_request  *request,
    const void                     *hint_fh,
    int                             hint_fhlen,
    enum chimera_vfs_compound_mode  mode,
    chimera_client_request_callback start,
    chimera_client_request_callback reply)
{
    request->compound_ts      = chimera_vfs_compound_alloc_ts(thread->vfs_thread);
    request->compound_attempt = 0;
    if (hint_fhlen) {
        memcpy(request->compound_fh, hint_fh, hint_fhlen);
    }
    request->compound_fhlen = hint_fhlen;
    request->compound_mode  = mode;
    request->compound_start = start;
    request->compound_reply = reply;

    chimera_client_compound_attempt(thread, request);
} /* chimera_client_compound_run */
