// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <fcntl.h>
#include <string.h>

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"

/* See nfs3_proc_write.c: bound on transaction conflict replays. */

static void chimera_nfs3_create_begin_attempt(
    struct nfs_request *req);

static void
chimera_nfs3_create_retry(struct nfs_request *req)
{
    if (++req->txn_attempt > CHIMERA_NFS3_TXN_MAX_RETRIES) {
        struct chimera_server_nfs_thread *thread = req->thread;
        struct CREATE3res                 res;
        int                               rc;

        res.status = NFS3ERR_JUKEBOX;
        chimera_nfs3_set_wcc_data(&res.resfail.dir_wcc, NULL, NULL);
        rc = thread->shared->nfs_v3.send_reply_NFSPROC3_CREATE(thread->evpl, NULL,
                                                               &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
        return;
    }
    chimera_nfs3_create_begin_attempt(req);
} /* chimera_nfs3_create_retry */

/* Send the reply the terminal step already built into req->res_create. */
static void
chimera_nfs3_create_send(struct nfs_request *req)
{
    struct chimera_server_nfs_thread *thread = req->thread;
    int                               rc;

    rc = thread->shared->nfs_v3.send_reply_NFSPROC3_CREATE(thread->evpl, NULL,
                                                           &req->res_create,
                                                           req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
    nfs_request_free(thread, req);
} /* chimera_nfs3_create_send */

/* EndTransaction(ABORT) completion: conflict -> replay, logical error -> send
 * the (already-built) error reply. */
static void
chimera_nfs3_create_aborted(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs_request *req = private_data;

    (void) error_code;

    if (req->txn_op_status == CHIMERA_VFS_ETXN_CONFLICT) {
        chimera_nfs3_create_retry(req);
    } else {
        chimera_nfs3_create_send(req);
    }
} /* chimera_nfs3_create_aborted */

/* EndTransaction(COMMIT) completion: the durable point.  A commit-time conflict
 * (cairn) replays; otherwise send the built reply. */
static void
chimera_nfs3_create_committed(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs_request *req = private_data;

    if (error_code == CHIMERA_VFS_ETXN_CONFLICT) {
        chimera_nfs3_create_retry(req);
        return;
    }
    if (error_code != CHIMERA_VFS_OK) {
        req->res_create.status = chimera_vfs_error_to_nfsstat3(error_code);
    }
    chimera_nfs3_create_send(req);
} /* chimera_nfs3_create_committed */

/*
 * Single terminal step for every CREATE outcome.  Copies the result into
 * req->res_create (so the VFS attr/handle pointers need not survive), releases
 * the handles opened this attempt, then resolves the transaction:
 *   - conflict  -> abort + replay from the top
 *   - success   -> commit (durable) then reply
 *   - logical error -> abort then reply
 */
static void
chimera_nfs3_create_terminate(
    struct nfs_request             *req,
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr)
{
    struct chimera_server_nfs_thread *thread = req->thread;
    struct CREATE3res                *res    = &req->res_create;
    int                               rc;

    if (error_code == CHIMERA_VFS_ETXN_CONFLICT) {
        if (handle) {
            chimera_vfs_release(thread->vfs_thread, handle);
        }
        if (req->handle) {
            chimera_vfs_release(thread->vfs_thread, req->handle);
            req->handle = NULL;
        }
        req->txn_op_status = error_code;
        chimera_vfs_end_transaction(thread->vfs_thread, &req->cred, req->txn,
                                    CHIMERA_VFS_TXN_ABORT,
                                    chimera_nfs3_create_aborted, req);
        return;
    }

    res->status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res->status == NFS3_OK) {
        if (attr->va_set_mask & CHIMERA_VFS_ATTR_FH) {
            res->resok.obj.handle_follows = 1;
            rc                            = xdr_dbuf_opaque_copy(&res->resok.obj.handle.data,
                                                                 handle->fh,
                                                                 handle->fh_len,
                                                                 req->encoding->dbuf);
            chimera_nfs_abort_if(rc, "Failed to copy opaque");
        } else {
            res->resok.obj.handle_follows = 0;
        }
        chimera_nfs3_set_post_op_attr(&res->resok.obj_attributes, attr);
        chimera_nfs3_set_wcc_data(&res->resok.dir_wcc, dir_pre_attr, dir_post_attr);
    } else {
        chimera_nfs3_set_wcc_data(&res->resfail.dir_wcc, dir_pre_attr, dir_post_attr);
    }

    /* Handle fhs are now copied into the reply; drop the open refs. */
    if (handle) {
        chimera_vfs_release(thread->vfs_thread, handle);
    }
    if (req->handle) {
        chimera_vfs_release(thread->vfs_thread, req->handle);
        req->handle = NULL;
    }

    if (error_code == CHIMERA_VFS_OK) {
        chimera_vfs_end_transaction(thread->vfs_thread, &req->cred, req->txn,
                                    CHIMERA_VFS_TXN_COMMIT_SYNC,
                                    chimera_nfs3_create_committed, req);
    } else {
        /* Nothing durable to keep; abort, then send the error reply. */
        req->txn_op_status = error_code;
        chimera_vfs_end_transaction(thread->vfs_thread, &req->cred, req->txn,
                                    CHIMERA_VFS_TXN_ABORT,
                                    chimera_nfs3_create_aborted, req);
    }
} /* chimera_nfs3_create_terminate */

static void
chimera_nfs3_create_exclusive_verify(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data)
{
    struct nfs_request *req  = private_data;
    struct CREATE3args *args = req->args_create;
    uint32_t            verf_atime, verf_mtime;

    (void) set_attr;

    if (error_code == CHIMERA_VFS_ETXN_CONFLICT) {
        chimera_nfs3_create_terminate(req, error_code, NULL, NULL, NULL, NULL);
        return;
    }

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs3_create_terminate(req, CHIMERA_VFS_EEXIST, NULL, NULL, NULL, NULL);
        return;
    }

    memcpy(&verf_atime, args->how.verf, sizeof(verf_atime));
    memcpy(&verf_mtime, args->how.verf + sizeof(verf_atime), sizeof(verf_mtime));

    if (attr->va_atime.tv_sec == verf_atime &&
        attr->va_mtime.tv_sec == verf_mtime) {
        chimera_nfs3_create_terminate(req, CHIMERA_VFS_OK,
                                      handle, attr, dir_pre_attr, dir_post_attr);
    } else {
        chimera_vfs_release(req->thread->vfs_thread, handle);
        chimera_nfs3_create_terminate(req, CHIMERA_VFS_EEXIST, NULL, NULL, NULL, NULL);
    }
} /* chimera_nfs3_create_exclusive_verify */

static void
chimera_nfs3_create_open_at_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct CREATE3args               *args   = req->args_create;

    if (error_code == CHIMERA_VFS_EEXIST &&
        args->how.mode == EXCLUSIVE) {
        set_attr->va_set_mask = 0;
        chimera_vfs_open_at(thread->vfs_thread, &req->cred, req->txn,
                            req->handle,
                            args->where.name.str,
                            args->where.name.len,
                            CHIMERA_VFS_OPEN_INFERRED,
                            set_attr,
                            CHIMERA_NFS3_ATTR_MASK | CHIMERA_VFS_ATTR_FH,
                            0,
                            0,
                            chimera_nfs3_create_exclusive_verify,
                            req);
        return;
    }

    chimera_nfs3_create_terminate(req, error_code, handle, attr,
                                  dir_pre_attr, dir_post_attr);
} /* chimera_nfs3_create_open_at_complete */

static void
chimera_nfs3_create_open_at_parent_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *parent_handle,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct CREATE3args               *args   = req->args_create;
    struct chimera_vfs_attrs         *attr;
    unsigned int                      flags;

    if (error_code != CHIMERA_VFS_OK) {
        /* Parent open failed (or wait-die conflict): no handle held yet. */
        chimera_nfs3_create_terminate(req, error_code, NULL, NULL, NULL, NULL);
        return;
    }

    req->handle = parent_handle;

    attr = xdr_dbuf_alloc_space(sizeof(*attr), req->encoding->dbuf);
    chimera_nfs_abort_if(attr == NULL, "Failed to allocate space");

    attr->va_req_mask = 0;
    attr->va_set_mask = 0;
    flags             = CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_INFERRED;

    switch (args->how.mode) {
        case UNCHECKED:
            chimera_nfs3_sattr3_to_va(attr, &args->how.obj_attributes);
            break;
        case GUARDED:
            chimera_nfs3_sattr3_to_va(attr, &args->how.obj_attributes);
            flags |= CHIMERA_VFS_OPEN_EXCLUSIVE;
            break;
        case EXCLUSIVE:
            flags            |= CHIMERA_VFS_OPEN_EXCLUSIVE;
            attr->va_set_mask = CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME;
            memcpy(&attr->va_atime.tv_sec, args->how.verf, 4);
            attr->va_atime.tv_nsec = 0;
            memcpy(&attr->va_mtime.tv_sec, args->how.verf + 4, 4);
            attr->va_mtime.tv_nsec = 0;
            break;
    } /* switch */

    chimera_vfs_open_at(thread->vfs_thread, &req->cred, req->txn,
                        parent_handle,
                        args->where.name.str,
                        args->where.name.len,
                        flags,
                        attr,
                        CHIMERA_NFS3_ATTR_MASK | CHIMERA_VFS_ATTR_FH,
                        CHIMERA_NFS3_ATTR_WCC_MASK,
                        CHIMERA_NFS3_ATTR_MASK,
                        chimera_nfs3_create_open_at_complete,
                        req);
} /* chimera_nfs3_create_open_at_parent_complete */

static void
chimera_nfs3_create_began(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_transaction *txn,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct CREATE3args               *args   = req->args_create;

    if (error_code != CHIMERA_VFS_OK) {
        if (error_code == CHIMERA_VFS_ETXN_CONFLICT) {
            chimera_nfs3_create_retry(req);
        } else {
            struct CREATE3res res;
            int               rc;
            res.status = chimera_vfs_error_to_nfsstat3(error_code);
            chimera_nfs3_set_wcc_data(&res.resfail.dir_wcc, NULL, NULL);
            rc = thread->shared->nfs_v3.send_reply_NFSPROC3_CREATE(thread->evpl, NULL,
                                                                   &res, req->encoding);
            chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
            nfs_request_free(thread, req);
        }
        return;
    }

    req->txn = txn;     /* NULL for a non-transactional backend (autocommit) */

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred, req->txn,
                        args->where.dir.data.data,
                        args->where.dir.data.len,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_nfs3_create_open_at_parent_complete,
                        req);
} /* chimera_nfs3_create_began */

static void
chimera_nfs3_create_begin_attempt(struct nfs_request *req)
{
    struct chimera_server_nfs_thread *thread = req->thread;
    struct CREATE3args               *args   = req->args_create;

    chimera_vfs_begin_transaction(thread->vfs_thread, &req->cred,
                                  args->where.dir.data.data,
                                  args->where.dir.data.len,
                                  CHIMERA_VFS_TXN_WRITE,
                                  req->txn_ts,
                                  chimera_nfs3_create_began,
                                  req);
} /* chimera_nfs3_create_begin_attempt */

void
chimera_nfs3_create(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct CREATE3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred(&req->cred, cred);

    nfs3_dump_create(req, args);

    req->args_create = args;
    req->handle      = NULL;

    req->txn_ts      = chimera_vfs_txn_alloc_ts(thread->vfs_thread);
    req->txn_attempt = 0;

    chimera_nfs3_create_begin_attempt(req);
} /* chimera_nfs3_create */
