// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "nfs_internal.h"
#include "vfs/vfs_internal_procs.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"
#include "nfs3_trace.h"
#include "nfs3_compound.h"

static void
chimera_nfs3_lookup_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs3_compound                 *ctx = private_data;

    if (nfs3_compound_retry(ctx)) {
        return;
    }
    struct nfs_request                   *req      = ctx->req;
    const struct chimera_vfs_compound_op *op       = nfs3_compound_result(ctx);
    const struct chimera_vfs_attrs       *attr     = &op->attr;
    const struct chimera_vfs_attrs       *dir_attr = &op->dir_post_attr;

    struct chimera_server_nfs_thread     *thread = req->thread;
    struct chimera_server_nfs_shared     *shared = thread->shared;
    struct evpl                          *evpl   = thread->evpl;
    struct LOOKUP3res                     res;
    int                                   rc;

    res.status = nfs3_compound_status(ctx);

    if (res.status == NFS3_OK) {

        uint8_t wire[CHIMERA_NFS_FH_MAX];
        int     wirelen;

        chimera_nfs_abort_if(!(attr->va_set_mask & CHIMERA_VFS_ATTR_FH),
                             "NFS3 lookup: no file handle was returned");

        /* The looked-up child lives in the same export as the directory; wrap
         * its handle with the request's export id (and sign it) for the wire. */
        chimera_nfs_fh_encode(req, attr->va_fh, attr->va_fh_len, wire, &wirelen);

        rc = xdr_dbuf_opaque_copy(&res.resok.object.data,
                                  wire,
                                  wirelen,
                                  req->encoding->dbuf);
        chimera_nfs_abort_if(rc, "Failed to copy opaque");

        chimera_nfs3_set_post_op_attr(&res.resok.obj_attributes, attr);
        chimera_nfs3_set_post_op_attr(&res.resok.dir_attributes, dir_attr);
    } else {
        chimera_nfs3_set_post_op_attr(&res.resfail.dir_attributes, dir_attr);
    }


    rc = shared->nfs_v3.send_reply_NFSPROC3_LOOKUP(evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");


    nfs3_compound_free(ctx);
    nfs_request_free(thread, req);
} /* chimera_nfs3_lookup_complete */

void
chimera_nfs3_lookup(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct LOOKUP3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_request               *req;
    struct LOOKUP3res                 res;
    int                               rc;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred_req(req, cred);

    nfs3_dump_lookup(req, args);
    nfs3_trace_lookup(req, args);

    req->args_lookup = args;

    res.status = chimera_nfs3_decode_fh(req, args->what.dir.data.data, args->what.dir.data.len);
    if (res.status != NFS3_OK) {
        nfsstat3 fh_status = res.status;
        memset(&res, 0, sizeof(res));
        res.status = fh_status;
        rc         = shared->nfs_v3.send_reply_NFSPROC3_LOOKUP(evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
        return;
    }

    struct nfs3_compound        *ctx = nfs3_compound_alloc(req, CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH |
                                                           CHIMERA_VFS_OPEN_DIRECTORY);
    struct chimera_vfs_compound *compound = ctx->compound;
    ctx->result = chimera_vfs_compound_add_lookup(compound, args->what.name.str, args->what.name.len,
                                                  CHIMERA_NFS3_ATTR_MASK | CHIMERA_VFS_ATTR_FH, 0);
    chimera_vfs_compound_set_result_masks(compound, ctx->result, CHIMERA_NFS3_ATTR_MASK | CHIMERA_VFS_ATTR_FH, 0,
                                          CHIMERA_NFS3_ATTR_MASK);
    chimera_vfs_compound_submit(compound, chimera_nfs3_lookup_complete, ctx);
} /* chimera_nfs3_lookup */
