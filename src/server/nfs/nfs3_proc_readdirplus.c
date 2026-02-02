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
chimera_nfs3_readdirplus_callback(
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg)
{
    struct nfs_request                 *req  = arg;
    struct READDIRPLUS3args            *args = req->args_readdirplus;
    struct entryplus3                  *entry;
    struct nfs_nfs3_readdirplus_cursor *cursor;
    int                                 dbuf_before = req->encoding->dbuf->used, dbuf_cur;
    int                                 rc;

    cursor = &req->readdirplus3_cursor;

    entry = xdr_dbuf_alloc_space(sizeof(*entry), req->encoding->dbuf);
    chimera_nfs_abort_if(entry == NULL, "Failed to allocate space");

    entry->cookie    = cookie;
    entry->fileid    = inum;
    entry->nextentry = NULL;

    rc = xdr_dbuf_alloc_string(&entry->name, name, namelen, req->encoding->dbuf);
    chimera_nfs_abort_if(rc, "Failed to allocate string");

    chimera_nfs3_set_post_op_attr(&entry->name_attributes, attrs);

    if (attrs->va_set_mask & CHIMERA_VFS_ATTR_FH) {
        entry->name_handle.handle_follows = 1;

        rc = xdr_dbuf_opaque_copy(&entry->name_handle.handle.data,
                                  attrs->va_fh,
                                  attrs->va_fh_len,
                                  req->encoding->dbuf);
        chimera_nfs_abort_if(rc, "Failed to copy opaque");

    } else {
        entry->name_handle.handle_follows = 0;
    }

    dbuf_cur = req->encoding->dbuf->used - dbuf_before;

    if (cursor->count + dbuf_cur > args->maxcount) {
        chimera_nfs_debug("readdirplus: cursor->count + dbuf_cur > args->maxcount (%d + %d > %d)", cursor->count,
                          dbuf_cur, args->maxcount);
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
chimera_nfs3_readdirplus_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        cookie,
    uint64_t                        verifier,
    uint32_t                        eof,
    struct chimera_vfs_attrs       *dir_attr,
    void                           *private_data)
{
    struct nfs_request                 *req    = private_data;
    struct chimera_server_nfs_shared   *shared = req->thread->shared;
    struct evpl                        *evpl   = req->thread->evpl;
    struct READDIRPLUS3res             *res    = &req->res_readdirplus;
    struct nfs_nfs3_readdirplus_cursor *cursor = &req->readdirplus3_cursor;
    int                                 rc;

    res->status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res->status == NFS3_OK) {
        chimera_nfs3_set_post_op_attr(&res->resok.dir_attributes, dir_attr);
        res->resok.reply.eof     = !!eof;
        res->resok.reply.entries = cursor->entries;
        memcpy(res->resok.cookieverf,
               &verifier,
               sizeof(res->resok.cookieverf));
    }

    rc = shared->nfs_v3.send_reply_NFSPROC3_READDIRPLUS(evpl, NULL, res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    chimera_vfs_release(req->thread->vfs_thread, req->handle);

    nfs_request_free(req->thread, req);
} /* chimera_nfs3_readdirplus_complete */ /* chimera_nfs3_readdirplus_complete */

static void
chimera_nfs3_readdirplus_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct READDIRPLUS3args          *args   = req->args_readdirplus;
    struct READDIRPLUS3res           *res    = &req->res_readdirplus;
    int                               rc;

    if (error_code == CHIMERA_VFS_OK) {
        req->handle = handle;

        uint64_t cookieverf;
        memcpy(&cookieverf, args->cookieverf, sizeof(cookieverf));
        chimera_vfs_readdir(thread->vfs_thread, &req->cred,
                            handle,
                            CHIMERA_NFS3_ATTR_MASK | CHIMERA_VFS_ATTR_FH,
                            CHIMERA_NFS3_ATTR_MASK,
                            args->cookie,
                            cookieverf,
                            CHIMERA_VFS_READDIR_EMIT_DOT,
                            chimera_nfs3_readdirplus_callback,
                            chimera_nfs3_readdirplus_complete,
                            req);

    } else {
        res->status = chimera_vfs_error_to_nfsstat3(error_code);
        rc          = shared->nfs_v3.send_reply_NFSPROC3_READDIRPLUS(evpl, NULL, res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
    }
} /* chimera_nfs3_readdir_open_callback */

void
chimera_nfs3_readdirplus(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct READDIRPLUS3args   *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread   *thread = private_data;
    struct nfs_request                 *req;
    struct READDIRPLUS3res             *res;
    struct nfs_nfs3_readdirplus_cursor *cursor;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred(&req->cred, cred);

    nfs3_dump_readdirplus(req, args);

    req->args_readdirplus = args;

    res = &req->res_readdirplus;

    res->resok.reply.entries = NULL;

    cursor = &req->readdirplus3_cursor;

    cursor->count   = 256; /* reserve some space for non-entry serialization */
    cursor->entries = NULL;
    cursor->last    = NULL;

    chimera_vfs_open(thread->vfs_thread, &req->cred,
                     args->dir.data.data,
                     args->dir.data.len,
                     CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                     chimera_nfs3_readdirplus_open_callback,
                     req);
} /* chimera_nfs3_readdirplus */
