// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Shared compound driver for NFS3 procedures.  One NFS3 RPC == one VFS
 * compound: begin (read or write mode) -> the op's VFS calls (all enlisted
 * via req->compound) -> commit, which must be durable before the reply is sent.  A
 * wait-die (diskfs) or optimistic-commit (cairn) conflict replays the whole op
 * from the top, reusing the stable compound_ts so it cannot starve.  Begin never
 * returns NULL: on a backend without compound support the handle comes back
 * UNBOUND, every op ejects individually (autocommits standalone, exactly
 * today's behaviour) and end is a synchronous OK.
 *
 * An op plugs in two callbacks:
 *   compound_start(req) -- run the op's VFS chain (req->compound is set; pass it down).
 *   compound_reply(req) -- build + send the reply from req->res_* / req->compound_op_status,
 *                          release req->handle, and free the request.
 * The op's VFS terminal calls chimera_nfs3_compound_finish(req, status); on success it
 * has already populated req->res_* (the reply only fixes up the error status).
 */

#include "nfs3_procs.h"
#include "nfs_internal.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

static void chimera_nfs3_compound_attempt(
    struct nfs_request *req);

static void
chimera_nfs3_compound_replay(struct nfs_request *req)
{
    struct chimera_server_nfs_thread *thread = req->thread;

    if (++req->compound_attempt > CHIMERA_NFS3_COMPOUND_MAX_RETRIES) {
        /* Give up after too many conflicts; surface a retriable status
         * (NFS3ERR_JUKEBOX on the wire, matching the hand-rolled write and
         * create drivers) rather than a hard NFS3ERR_IO. */
        req->compound_op_status = CHIMERA_VFS_ECOMPOUND_EXHAUSTED;
        req->compound_reply(req);
        return;
    }

    /* Drop any handle opened during the failed attempt before replaying. */
    if (req->handle) {
        chimera_vfs_release(thread->vfs_thread, req->handle);
        req->handle = NULL;
    }

    chimera_nfs3_compound_attempt(req);
} /* chimera_nfs3_compound_replay */

static void
chimera_nfs3_compound_committed(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs_request *req = private_data;

    if (error_code == CHIMERA_VFS_ECOMPOUND_CONFLICT) {
        chimera_nfs3_compound_replay(req);
        return;
    }

    if (error_code != CHIMERA_VFS_OK) {
        req->compound_op_status = error_code;
    }

    req->compound_reply(req);
} /* chimera_nfs3_compound_committed */

static void
chimera_nfs3_compound_aborted(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs_request *req = private_data;

    (void) error_code;

    if (req->compound_op_status == CHIMERA_VFS_ECOMPOUND_CONFLICT) {
        chimera_nfs3_compound_replay(req);
    } else {
        req->compound_reply(req);
    }
} /* chimera_nfs3_compound_aborted */

void
chimera_nfs3_compound_finish(
    struct nfs_request    *req,
    enum chimera_vfs_error status)
{
    struct chimera_server_nfs_thread *thread = req->thread;

    req->compound_op_status = status;

    if (status == CHIMERA_VFS_OK) {
        chimera_vfs_compound_end(thread->vfs_thread, &req->cred, req->compound,
                                 req->compound_mode == CHIMERA_VFS_COMPOUND_WRITE ?
                                 CHIMERA_VFS_COMPOUND_COMMIT_DURABLE :
                                 CHIMERA_VFS_COMPOUND_COMMIT,
                                 chimera_nfs3_compound_committed, req);
    } else {
        chimera_vfs_compound_end(thread->vfs_thread, &req->cred, req->compound,
                                 CHIMERA_VFS_COMPOUND_ABORT,
                                 chimera_nfs3_compound_aborted, req);
    }
} /* chimera_nfs3_compound_finish */

static void
chimera_nfs3_compound_attempt(struct nfs_request *req)
{
    struct chimera_server_nfs_thread *thread = req->thread;

    /* Begin is a fast, local action that returns the handle synchronously and
     * never NULL: on a non-compound backend the handle is unbound and every
     * op ejects (autocommit, as today).  RETRYABLE: this driver replays the
     * whole op on ECOMPOUND_CONFLICT, reusing the stable ts.  Begin cannot
     * conflict; a conflict can only surface later at commit, replaying from
     * here. */
    req->compound = chimera_vfs_compound_begin(thread->vfs_thread, &req->cred,
                                               req->compound_fh, req->compound_fhlen,
                                               req->compound_mode, req->compound_ts,
                                               CHIMERA_VFS_COMPOUND_RETRYABLE);
    req->compound_op_status = CHIMERA_VFS_OK;
    req->compound_start(req);
} /* chimera_nfs3_compound_attempt */

void
chimera_nfs3_compound_run(
    struct nfs_request            *req,
    const void                    *fh,
    int                            fhlen,
    enum chimera_vfs_compound_mode mode,
    void (                        *start )(struct nfs_request *req),
    void (                        *reply )(struct nfs_request *req))
{
    req->compound_ts      = chimera_vfs_compound_alloc_ts(req->thread->vfs_thread);
    req->compound_attempt = 0;
    req->handle           = NULL;
    memcpy(req->compound_fh, fh, fhlen);
    req->compound_fhlen = fhlen;
    req->compound_mode  = mode;
    req->compound_start = start;
    req->compound_reply = reply;

    chimera_nfs3_compound_attempt(req);
} /* chimera_nfs3_compound_run */
