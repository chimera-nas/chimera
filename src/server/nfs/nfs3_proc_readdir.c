// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "nfs_internal.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"

static int
chimera_nfs3_readdir_callback(
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg)
{
    struct nfs_request             *req  = arg;
    struct READDIR3args            *args = req->args_readdir;
    struct entry3                  *entry;
    struct nfs_nfs3_readdir_cursor *cursor;
    int                             dbuf_before = req->encoding->dbuf->used, dbuf_cur;
    int                             rc;

    cursor = &req->readdir3_cursor;

    entry = xdr_dbuf_alloc_space(sizeof(*entry), req->encoding->dbuf);
    chimera_nfs_abort_if(entry == NULL, "Failed to allocate space");

    entry->fileid    = inum;
    entry->cookie    = cookie;
    entry->nextentry = NULL;

    rc = xdr_dbuf_alloc_string(&entry->name, name, namelen, req->encoding->dbuf);
    chimera_nfs_abort_if(rc, "Failed to allocate string");

    dbuf_cur = req->encoding->dbuf->used - dbuf_before;

    if (cursor->count + dbuf_cur > args->count) {
        return -1;
    }

    cursor->count += dbuf_cur;

    if (cursor->entries) {
        cursor->last->nextentry = entry;
        cursor->last            = entry;
    } else {
        cursor->entries = entry;
        cursor->last    = entry;
    }

    return 0;
} /* chimera_nfs3_readdir_callback */

static void
chimera_nfs3_readdir_reply(struct nfs_request *req)
{
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    if (req->txn_op_status != CHIMERA_VFS_OK) {
        req->res_readdir.status = chimera_vfs_error_to_nfsstat3(req->txn_op_status);
    }

    if (req->handle) {
        chimera_vfs_release(thread->vfs_thread, req->handle);
    }

    rc = shared->nfs_v3.send_reply_NFSPROC3_READDIR(thread->evpl, NULL,
                                                    &req->res_readdir, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_readdir_reply */

static void
chimera_nfs3_readdir_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        cookie,
    uint64_t                        verifier,
    uint32_t                        eof,
    struct chimera_vfs_attrs       *dir_attr,
    void                           *private_data)
{
    struct nfs_request             *req    = private_data;
    struct READDIR3res             *res    = &req->res_readdir;
    struct nfs_nfs3_readdir_cursor *cursor = &req->readdir3_cursor;

    if (error_code == CHIMERA_VFS_OK) {
        res->status = NFS3_OK;
        chimera_nfs3_set_post_op_attr(&res->resok.dir_attributes, dir_attr);
        res->resok.reply.eof     = !!eof;
        res->resok.reply.entries = cursor->entries;
        memcpy(res->resok.cookieverf, &verifier, sizeof(res->resok.cookieverf));
    } else {
        /* The encoder serializes resfail.dir_attributes on error; initialize it
         * so a recycled request slot does not leak stale attribute bytes. */
        chimera_nfs3_set_post_op_attr(&res->resfail.dir_attributes, dir_attr);
    }

    chimera_nfs3_txn_finish(req, error_code);
} /* chimera_nfs3_readdir_complete */

static void
chimera_nfs3_readdir_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request  *req  = private_data;
    struct READDIR3args *args = req->args_readdir;
    uint64_t             cookieverf;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs3_txn_finish(req, error_code);
        return;
    }

    req->handle = handle;

    memcpy(&cookieverf, args->cookieverf, sizeof(cookieverf));
    chimera_vfs_readdir(req->thread->vfs_thread, &req->cred, req->txn,
                        handle,
                        0,
                        CHIMERA_NFS3_ATTR_MASK,
                        args->cookie,
                        cookieverf,
                        CHIMERA_VFS_READDIR_EMIT_DOT,
                        NULL, 0,    /* no search-pattern filter */
                        chimera_nfs3_readdir_callback,
                        chimera_nfs3_readdir_complete,
                        req);
} /* chimera_nfs3_readdir_open_callback */

static void
chimera_nfs3_readdir_start(struct nfs_request *req)
{
    struct nfs_nfs3_readdir_cursor *cursor = &req->readdir3_cursor;

    /* Reset the accumulator each attempt so a conflict replay does not duplicate
     * entries. */
    cursor->count   = 256; /* reserve space for non-entry serialization */
    cursor->entries = NULL;
    cursor->last    = NULL;

    req->res_readdir.resok.reply.entries = NULL;

    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred, req->txn,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_nfs3_readdir_open_callback,
                        req);
} /* chimera_nfs3_readdir_start */

void
chimera_nfs3_readdir(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct READDIR3args       *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;
    struct READDIR3res               *res;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred_req(req, cred);

    nfs3_dump_readdir(req, args);

    req->args_readdir = args;

    /* Decode the directory handle up front so the request export/squash and
     * routing hint are set before the transaction begins.  The per-attempt
     * cursor/reply reset lives in chimera_nfs3_readdir_start so a conflict
     * replay cannot duplicate entries. */
    res         = &req->res_readdir;
    res->status = chimera_nfs3_decode_fh(req, args->dir.data.data, args->dir.data.len);
    if (res->status != NFS3_OK) {
        int      rc;
        nfsstat3 fh_status = res->status;
        memset(res, 0, sizeof(*res));
        res->status = fh_status;
        rc          = thread->shared->nfs_v3.send_reply_NFSPROC3_READDIR(evpl, NULL, res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
        return;
    }

    chimera_nfs3_txn_run(req, req->fh, req->fhlen,
                         CHIMERA_VFS_TXN_READ,
                         chimera_nfs3_readdir_start, chimera_nfs3_readdir_reply);
} /* chimera_nfs3_readdir */
