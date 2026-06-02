// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Shared transaction driver for NFS3 procedures.  One NFS3 RPC == one VFS
 * transaction: begin (read or write mode) -> the op's VFS calls (all enlisted
 * via req->txn) -> commit, which must be durable before the reply is sent.  A
 * wait-die (diskfs) or optimistic-commit (cairn) conflict replays the whole op
 * from the top, reusing the stable txn_ts so it cannot starve.  Backends that
 * are not transactional return a NULL handle and every step degrades to today's
 * autocommit behaviour.
 *
 * An op plugs in two callbacks:
 *   txn_start(req) -- run the op's VFS chain (req->txn is set; pass it down).
 *   txn_reply(req) -- build + send the reply from req->res_* / req->txn_op_status,
 *                     release req->handle, and free the request.
 * The op's VFS terminal calls chimera_nfs3_txn_finish(req, status); on success it
 * has already populated req->res_* (the reply only fixes up the error status).
 */

#include "nfs3_procs.h"
#include "nfs_internal.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

static void chimera_nfs3_txn_attempt(
    struct nfs_request *req);

static void
chimera_nfs3_txn_replay(struct nfs_request *req)
{
    struct chimera_server_nfs_thread *thread = req->thread;

    if (++req->txn_attempt > CHIMERA_NFS3_TXN_MAX_RETRIES) {
        /* Give up after too many conflicts; surface a retriable error. */
        req->txn_op_status = CHIMERA_VFS_EIO;
        req->txn_reply(req);
        return;
    }

    /* Drop any handle opened during the failed attempt before replaying. */
    if (req->handle) {
        chimera_vfs_release(thread->vfs_thread, req->handle);
        req->handle = NULL;
    }

    chimera_nfs3_txn_attempt(req);
} /* chimera_nfs3_txn_replay */

static void
chimera_nfs3_txn_committed(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs_request *req = private_data;

    if (error_code == CHIMERA_VFS_ETXN_CONFLICT) {
        chimera_nfs3_txn_replay(req);
        return;
    }

    if (error_code != CHIMERA_VFS_OK) {
        req->txn_op_status = error_code;
    }

    req->txn_reply(req);
} /* chimera_nfs3_txn_committed */

static void
chimera_nfs3_txn_aborted(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs_request *req = private_data;

    (void) error_code;

    if (req->txn_op_status == CHIMERA_VFS_ETXN_CONFLICT) {
        chimera_nfs3_txn_replay(req);
    } else {
        req->txn_reply(req);
    }
} /* chimera_nfs3_txn_aborted */

void
chimera_nfs3_txn_finish(
    struct nfs_request    *req,
    enum chimera_vfs_error status)
{
    struct chimera_server_nfs_thread *thread = req->thread;

    req->txn_op_status = status;

    if (status == CHIMERA_VFS_OK) {
        chimera_vfs_end_transaction(thread->vfs_thread, &req->cred, req->txn,
                                    req->txn_mode == CHIMERA_VFS_TXN_WRITE ?
                                    CHIMERA_VFS_TXN_COMMIT_SYNC :
                                    CHIMERA_VFS_TXN_COMMIT_ASYNC,
                                    chimera_nfs3_txn_committed, req);
    } else {
        chimera_vfs_end_transaction(thread->vfs_thread, &req->cred, req->txn,
                                    CHIMERA_VFS_TXN_ABORT,
                                    chimera_nfs3_txn_aborted, req);
    }
} /* chimera_nfs3_txn_finish */

static void
chimera_nfs3_txn_began(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_transaction *txn,
    void                           *private_data)
{
    struct nfs_request *req = private_data;

    if (error_code == CHIMERA_VFS_ETXN_CONFLICT) {
        chimera_nfs3_txn_replay(req);
        return;
    }

    req->txn = txn;     /* NULL for a non-transactional backend (autocommit) */

    if (error_code != CHIMERA_VFS_OK) {
        req->txn_op_status = error_code;
        req->txn_reply(req);
        return;
    }

    req->txn_op_status = CHIMERA_VFS_OK;
    req->txn_start(req);
} /* chimera_nfs3_txn_began */

static void
chimera_nfs3_txn_attempt(struct nfs_request *req)
{
    struct chimera_server_nfs_thread *thread = req->thread;

    chimera_vfs_begin_transaction(thread->vfs_thread, &req->cred,
                                  req->txn_fh, req->txn_fhlen, req->txn_mode,
                                  req->txn_ts, chimera_nfs3_txn_began, req);
} /* chimera_nfs3_txn_attempt */

void
chimera_nfs3_txn_run(
    struct nfs_request       *req,
    const void               *fh,
    int                       fhlen,
    enum chimera_vfs_txn_mode mode,
    void (                   *start )(struct nfs_request *req),
    void (                   *reply )(struct nfs_request *req))
{
    req->txn_ts      = chimera_vfs_txn_alloc_ts(req->thread->vfs_thread);
    req->txn_attempt = 0;
    req->handle      = NULL;
    memcpy(req->txn_fh, fh, fhlen);
    req->txn_fhlen = fhlen;
    req->txn_mode  = mode;
    req->txn_start = start;
    req->txn_reply = reply;

    chimera_nfs3_txn_attempt(req);
} /* chimera_nfs3_txn_run */
