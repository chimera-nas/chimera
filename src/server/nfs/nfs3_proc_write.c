// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "nfs_internal.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"

/* Bound on how many times a single NFS3 op replays its transaction after a
 * wait-die / optimistic conflict before giving up with NFS3ERR_JUKEBOX (a
 * transient "try again later" the client retries). */
#define CHIMERA_NFS3_TXN_MAX_RETRIES 8

static void chimera_nfs3_write_begin_attempt(
    struct nfs_request *req);

/*
 * Terminal error path: release the write iovecs (taken from the RPC message)
 * and send a WRITE3res carrying `vfs_error`.  Used when the transaction
 * could not begin, an op failed logically, or the retry budget is exhausted.
 */
static void
chimera_nfs3_write_finish_error(
    struct nfs_request    *req,
    enum chimera_vfs_error vfs_error,
    nfsstat3               override_status)
{
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct WRITE3args                *args   = req->args_write;
    struct WRITE3res                  res;
    int                               rc;

    evpl_iovecs_release(evpl, args->data.iov, args->data.niov);

    res.status = override_status ? override_status
                 : chimera_vfs_error_to_nfsstat3(vfs_error);
    chimera_nfs3_set_wcc_data(&res.resfail.file_wcc, NULL, NULL);

    rc = shared->nfs_v3.send_reply_NFSPROC3_WRITE(evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_write_finish_error */

/* Conflict path: replay the whole op from BeginTransaction (reusing the stable
 * txn_ts so wait-die can't starve it), or give up after the retry budget. */
static void
chimera_nfs3_write_retry(struct nfs_request *req)
{
    if (++req->txn_attempt > CHIMERA_NFS3_TXN_MAX_RETRIES) {
        chimera_nfs3_write_finish_error(req, CHIMERA_VFS_OK, NFS3ERR_JUKEBOX);
        return;
    }
    chimera_nfs3_write_begin_attempt(req);
} /* chimera_nfs3_write_retry */

/* EndTransaction(ABORT) completion after an op failure or a mid-op conflict.
 * Abort always succeeds; decide retry-vs-error from the saved op status. */
static void
chimera_nfs3_write_aborted(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs_request *req = private_data;

    (void) error_code;

    if (req->txn_op_status == CHIMERA_VFS_ETXN_CONFLICT) {
        chimera_nfs3_write_retry(req);
    } else {
        chimera_nfs3_write_finish_error(req, req->txn_op_status, 0);
    }
} /* chimera_nfs3_write_aborted */

/* EndTransaction(COMMIT) completion: the durable point.  The client is ACKed
 * only here.  A commit-time conflict (cairn optimistic validation) replays. */
static void
chimera_nfs3_write_committed(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct WRITE3args                *args   = req->args_write;
    int                               rc;

    if (error_code == CHIMERA_VFS_ETXN_CONFLICT) {
        /* Already rolled back by EndTransaction; drop the handle and replay. */
        chimera_vfs_release(thread->vfs_thread, req->handle);
        req->handle = NULL;
        chimera_nfs3_write_retry(req);
        return;
    }

    evpl_iovecs_release(evpl, args->data.iov, args->data.niov);
    chimera_vfs_release(thread->vfs_thread, req->handle);

    if (error_code != CHIMERA_VFS_OK) {
        req->res_write.status = chimera_vfs_error_to_nfsstat3(error_code);
    }

    rc = shared->nfs_v3.send_reply_NFSPROC3_WRITE(evpl, NULL, &req->res_write,
                                                  req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_write_committed */

static void
chimera_nfs3_write_op_complete(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct WRITE3args                *args   = req->args_write;
    enum chimera_vfs_txn_end          end_flag;

    (void) sync;

    if (error_code == CHIMERA_VFS_ETXN_CONFLICT) {
        /* wait-die abort mid-op: drop the handle, abort the txn, then replay.
         * The write iovecs are NOT released -- the replay re-issues the write. */
        req->txn_op_status = error_code;
        chimera_vfs_release(thread->vfs_thread, req->handle);
        req->handle = NULL;
        chimera_vfs_end_transaction(thread->vfs_thread, &req->cred, req->txn,
                                    CHIMERA_VFS_TXN_ABORT,
                                    chimera_nfs3_write_aborted, req);
        return;
    }

    if (error_code != CHIMERA_VFS_OK) {
        /* Logical failure: one NFS3 op == one txn, so abort it, then reply. */
        req->txn_op_status = error_code;
        chimera_vfs_release(thread->vfs_thread, req->handle);
        req->handle = NULL;
        chimera_vfs_end_transaction(thread->vfs_thread, &req->cred, req->txn,
                                    CHIMERA_VFS_TXN_ABORT,
                                    chimera_nfs3_write_aborted, req);
        return;
    }

    /* Success.  Build the reply now (set_wcc_data copies the attr values, so
     * pre/post_attr need not survive past this callback) and commit; the data
     * is acknowledged only once the commit is durable.  Honor the requested
     * stability via the commit flag. */
    end_flag = (args->stable == CHIMERA_VFS_WRITE_UNSTABLE)
               ? CHIMERA_VFS_TXN_COMMIT_ASYNC : CHIMERA_VFS_TXN_COMMIT_SYNC;

    req->res_write.status          = NFS3_OK;
    req->res_write.resok.count     = length;
    req->res_write.resok.committed = (end_flag == CHIMERA_VFS_TXN_COMMIT_SYNC)
                                       ? CHIMERA_VFS_WRITE_FILESYNC
                                       : CHIMERA_VFS_WRITE_UNSTABLE;
    memcpy(req->res_write.resok.verf, &thread->shared->nfs_verifier,
           sizeof(req->res_write.resok.verf));
    chimera_nfs3_set_wcc_data(&req->res_write.resok.file_wcc, pre_attr, post_attr);

    chimera_vfs_end_transaction(thread->vfs_thread, &req->cred, req->txn,
                                end_flag, chimera_nfs3_write_committed, req);
} /* chimera_nfs3_write_op_complete */

static void
chimera_nfs3_write_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct WRITE3args                *args   = req->args_write;

    if (error_code != CHIMERA_VFS_OK) {
        /* open_fh failed (possibly a wait-die conflict): abort + retry/error. */
        req->txn_op_status = error_code;
        chimera_vfs_end_transaction(thread->vfs_thread, &req->cred, req->txn,
                                    CHIMERA_VFS_TXN_ABORT,
                                    chimera_nfs3_write_aborted, req);
        return;
    }

    req->handle = handle;

    chimera_vfs_thread_enlist(thread->vfs_thread, req->txn);
    chimera_vfs_write(thread->vfs_thread, &req->cred,
                      handle,
                      args->offset,
                      args->count,
                      args->stable,           /* 3-level requested stability */
                      CHIMERA_NFS3_ATTR_WCC_MASK,
                      CHIMERA_NFS3_ATTR_MASK,
                      args->data.iov,
                      args->data.niov,
                      chimera_nfs3_write_op_complete,
                      req);
    chimera_vfs_thread_enlist(thread->vfs_thread, NULL);
} /* chimera_nfs3_write_open_callback */

static void
chimera_nfs3_write_began(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_transaction *txn,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct WRITE3args                *args   = req->args_write;

    if (error_code != CHIMERA_VFS_OK) {
        if (error_code == CHIMERA_VFS_ETXN_CONFLICT) {
            chimera_nfs3_write_retry(req);
        } else {
            chimera_nfs3_write_finish_error(req, error_code, 0);
        }
        return;
    }

    req->txn = txn;     /* NULL for a non-transactional backend (autocommit) */

    chimera_vfs_thread_enlist(thread->vfs_thread, req->txn);
    chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                        args->file.data.data,
                        args->file.data.len,
                        CHIMERA_VFS_OPEN_INFERRED,
                        chimera_nfs3_write_open_callback,
                        req);
    chimera_vfs_thread_enlist(thread->vfs_thread, NULL);
} /* chimera_nfs3_write_began */

static void
chimera_nfs3_write_begin_attempt(struct nfs_request *req)
{
    struct chimera_server_nfs_thread *thread = req->thread;
    struct WRITE3args                *args   = req->args_write;

    chimera_vfs_begin_transaction(thread->vfs_thread, &req->cred,
                                  args->file.data.data,
                                  args->file.data.len,
                                  CHIMERA_VFS_TXN_WRITE,
                                  req->txn_ts,
                                  chimera_nfs3_write_began,
                                  req);
} /* chimera_nfs3_write_begin_attempt */

void
chimera_nfs3_write(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct WRITE3args         *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred(&req->cred, cred);

    nfs3_dump_write(req, args);

    req->args_write = args;
    req->handle     = NULL;

    /* Transfer ownership of write iovecs from the RPC2 message to prevent
     * msg_free from double-releasing (args->data.iov points to msg->read_chunk.iov
     * via XDR zerocopy). The iovecs are released only on final completion (after
     * the commit) or on a terminal error -- NOT on a conflict replay, which
     * re-issues the write from the same iovecs.
     */
    evpl_rpc2_encoding_take_read_chunk(req->encoding, NULL, NULL);

    /* One stable wait-die priority for the life of this op, reused across every
     * retry so the op can't starve. */
    req->txn_ts      = chimera_vfs_txn_alloc_ts(thread->vfs_thread);
    req->txn_attempt = 0;

    chimera_nfs3_write_begin_attempt(req);
} /* chimera_nfs3_write */
