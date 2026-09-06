// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * Shared compound driver for the S3 server (mirror of the NFS3 server's
 * nfs3_compound.c and the client's client_compound.h).  One replayable phase
 * of an S3 request == one VFS compound: begin -> the phase's VFS chain (all
 * enlisted via request->compound) -> commit (durable for a write compound)
 * before the phase's results escape.  A wait-die (diskfs) or
 * optimistic-commit (cairn) conflict replays the whole phase from
 * compound_start, reusing the stable compound_ts so it cannot starve.  Begin
 * never returns NULL: on a backend without compound support the handle comes
 * back UNBOUND, every op ejects individually (autocommits standalone,
 * exactly the legacy behaviour) and end is a synchronous OK.
 *
 * A phase plugs in two callbacks:
 *   compound_start(request) -- run the phase's VFS chain (request->compound
 *                              is set; pass it to every enlisted VFS call).
 *                              Must be re-entrant: it runs once per replay.
 *   compound_reply(request) -- consume request->compound_op_status once the
 *                              end has settled: continue the request (the
 *                              next phase, or the response) on OK, otherwise
 *                              release phase resources and answer the error
 *                              (ECOMPOUND_EXHAUSTED => 503 SlowDown).
 * The phase's VFS terminals call chimera_s3_compound_finish(request, status).
 *
 * Grouping phases (flags 0: the publish/finalize phases whose inputs are
 * gone once the body is consumed) use the same driver: the core rewrites
 * their conflicts to ECOMPOUND_EXHAUSTED, which lands in compound_reply
 * without ever taking the replay path.
 *
 * Header-only (static inline) for the same reason as client_compound.h: the
 * flows that drive it live in several translation units and the driver is
 * small.
 */

#include <string.h>

#include "s3_internal.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

#define CHIMERA_S3_COMPOUND_MAX_RETRIES 8

static inline void chimera_s3_compound_attempt(
    struct chimera_s3_request *request);

/* The compound the request's CURRENT phase should enlist an op in: the open
 * phase compound, or the per-thread LOOSE singleton (pure autocommit) when
 * no phase compound is open.  Sub-machines shared by several flows (the
 * metadata/tagging xattr walkers) route through this so they enlist exactly
 * when their caller's phase does. */
static inline struct chimera_vfs_compound *
chimera_s3_req_compound(struct chimera_s3_request *request)
{
    return request->compound ? request->compound
           : chimera_vfs_compound_loose(request->thread->vfs);
} /* chimera_s3_req_compound */

static inline void
chimera_s3_compound_replay(struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread *thread = request->thread;

    if (++request->compound_attempt > CHIMERA_S3_COMPOUND_MAX_RETRIES) {
        /* Give up after too many conflicts; surface the retriable
         * ECOMPOUND_EXHAUSTED (503 SlowDown on the wire) rather than a
         * hard InternalError. */
        request->compound_op_status = CHIMERA_VFS_ECOMPOUND_EXHAUSTED;
        request->compound_reply(request);
        return;
    }

    /* Drop any handles resolved during the failed attempt (the compound
     * that opened them is already rolled back) and reset the request-level
     * outcome before replaying.  Flow-specific state (a copy ctx's source
     * handle) is reset by the flow's own compound_start. */
    if (request->file_handle) {
        chimera_vfs_release(thread->vfs, request->file_handle);
        request->file_handle = NULL;
    }
    if (request->dir_handle) {
        chimera_vfs_release(thread->vfs, request->dir_handle);
        request->dir_handle = NULL;
    }
    request->status = CHIMERA_S3_STATUS_OK;

    chimera_s3_compound_attempt(request);
} /* chimera_s3_compound_replay */

static inline void
chimera_s3_compound_committed(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    CHIMERA_S3_HOLD_REQUEST(private_data);
    struct chimera_s3_request *request = private_data;

    if (error_code == CHIMERA_VFS_ECOMPOUND_CONFLICT) {
        chimera_s3_compound_replay(request);
        return;
    }

    if (error_code != CHIMERA_VFS_OK) {
        request->compound_op_status = error_code;
    }

    request->compound_reply(request);
} /* chimera_s3_compound_committed */

static inline void
chimera_s3_compound_aborted(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    CHIMERA_S3_HOLD_REQUEST(private_data);
    struct chimera_s3_request *request = private_data;

    (void) error_code;

    if (request->compound_op_status == CHIMERA_VFS_ECOMPOUND_CONFLICT) {
        chimera_s3_compound_replay(request);
    } else {
        request->compound_reply(request);
    }
} /* chimera_s3_compound_aborted */

/*
 * Terminal hook for a phase's VFS chain.  On success commit (durably for a
 * write compound), on failure abort; compound_reply fires only once the
 * commit/abort settles (and a commit-time conflict replays).  The compound
 * handle is recycled by the end, so request->compound is cleared here --
 * anything the flow issues afterwards (body lanes, the next phase) sees
 * NULL and takes the loose/next-compound path.
 */
static inline void
chimera_s3_compound_finish(
    struct chimera_s3_request *request,
    enum chimera_vfs_error     status)
{
    struct chimera_server_s3_thread *thread   = request->thread;
    struct chimera_vfs_compound     *compound = request->compound;

    request->compound_op_status = status;
    request->compound           = NULL;

    /* The end callback dereferences the request; hold it. */
    chimera_s3_request_get(request);

    if (status == CHIMERA_VFS_OK) {
        chimera_vfs_compound_end(thread->vfs, &thread->shared->cred, compound,
                                 request->compound_mode == CHIMERA_VFS_COMPOUND_WRITE ?
                                 CHIMERA_VFS_COMPOUND_COMMIT_DURABLE :
                                 CHIMERA_VFS_COMPOUND_COMMIT,
                                 chimera_s3_compound_committed, request);
    } else {
        chimera_vfs_compound_end(thread->vfs, &thread->shared->cred, compound,
                                 CHIMERA_VFS_COMPOUND_ABORT,
                                 chimera_s3_compound_aborted, request);
    }
} /* chimera_s3_compound_finish */

static inline void
chimera_s3_compound_attempt(struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread *thread = request->thread;

    /* Begin is a fast, local action that returns the handle synchronously
     * and never NULL: on a non-compound backend the handle is unbound and
     * every op ejects (autocommit, as before).  Begin cannot conflict; a
     * conflict can only surface at a member op or at commit, replaying from
     * here. */
    request->compound = chimera_vfs_compound_begin(thread->vfs,
                                                   &thread->shared->cred,
                                                   request->compound_fhlen ?
                                                   request->compound_fh : NULL,
                                                   request->compound_fhlen,
                                                   request->compound_mode,
                                                   request->compound_ts,
                                                   request->compound_flags);
    request->compound_op_status = CHIMERA_VFS_OK;
    request->compound_start(request);
} /* chimera_s3_compound_attempt */

/*
 * Run one phase of `request` as one compound.  `hint_fh` steers the owning
 * thread (the bucket/dir the phase operates under); `mode` is the begin
 * mode; `flags` is CHIMERA_VFS_COMPOUND_RETRYABLE for a genuinely
 * replayable phase, 0 (grouping) where results escape.  `start` runs the
 * phase's VFS chain (re-entrant: once per replay) and `reply` consumes
 * request->compound_op_status once the end settles.
 */
static inline void
chimera_s3_compound_run(
    struct chimera_s3_request     *request,
    const void                    *hint_fh,
    int                            hint_fhlen,
    enum chimera_vfs_compound_mode mode,
    uint32_t                       flags,
    void (                        *start )(struct chimera_s3_request *request),
    void (                        *reply )(struct chimera_s3_request *request))
{
    struct chimera_server_s3_thread *thread = request->thread;

    request->compound_ts      = chimera_vfs_compound_alloc_ts(thread->vfs);
    request->compound_attempt = 0;
    if (hint_fhlen) {
        memcpy(request->compound_fh, hint_fh, hint_fhlen);
    }
    request->compound_fhlen = hint_fhlen;
    request->compound_mode  = mode;
    request->compound_flags = flags;
    request->compound_start = start;
    request->compound_reply = reply;

    chimera_s3_compound_attempt(request);
} /* chimera_s3_compound_run */

static inline void
chimera_s3_compound_discard_cb(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    (void) error_code;
    (void) private_data;
} /* chimera_s3_compound_discard_cb */

/*
 * Fire-and-forget end of the request's open compound, for phases that do
 * not gate their result on the end status: the grouping READ compounds
 * (GET/HEAD/?tagging-GET -- no isolation was promised, so a read commit's
 * outcome changes nothing about an already-correct response) and the
 * refcount-zero teardown safety net (chimera_s3_request_put), which ABORTs
 * whatever an abandoned request's dead-end path left open.  The callback
 * never touches the request, so no reference is needed.  NULL-safe.
 */
static inline void
chimera_s3_compound_release(
    struct chimera_s3_request    *request,
    enum chimera_vfs_compound_end end_flag)
{
    struct chimera_server_s3_thread *thread   = request->thread;
    struct chimera_vfs_compound     *compound = request->compound;

    if (!compound) {
        return;
    }
    request->compound = NULL;

    chimera_vfs_compound_end(thread->vfs, &thread->shared->cred, compound,
                             end_flag, chimera_s3_compound_discard_cb, NULL);
} /* chimera_s3_compound_release */

/* Map a settled compound status onto the request's S3 status: EXHAUSTED is
 * the retriable 503 SlowDown; any other failure keeps the status a chain
 * terminal already recorded, defaulting to InternalError. */
static inline void
chimera_s3_compound_map_error(
    struct chimera_s3_request *request,
    enum chimera_vfs_error     status)
{
    if (status == CHIMERA_VFS_ECOMPOUND_EXHAUSTED) {
        request->status = CHIMERA_S3_STATUS_SLOW_DOWN;
    } else if (request->status == CHIMERA_S3_STATUS_OK) {
        request->status = CHIMERA_S3_STATUS_INTERNAL_ERROR;
    }
} /* chimera_s3_compound_map_error */
