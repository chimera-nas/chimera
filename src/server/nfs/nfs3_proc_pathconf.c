// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdint.h>
#include <string.h>

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs_internal_procs.h"
#include "nfs3_compound.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"
#include "nfs3_trace.h"

/* PATHCONF's limits are server constants, independent of the object, but the
 * handle is still validated the way every other file-handle-bearing procedure
 * validates it: decoded (a bogus handle is NFS3ERR_BADHANDLE) then resolved
 * with a GETATTR (a stale handle is NFS3ERR_NOENT).  memfs's open_fh is lazy
 * and does not resolve the inode, so the GETATTR is what actually catches a
 * freed object; its attributes fill obj_attributes, which RFC 1813 3.3.20
 * defines as the post-operation attributes of the argument object. */
/*
 * PUTFH, OPEN, GETATTR.  The open the handler used to make by hand belongs to
 * the sequence now, and goes with it -- nothing here releases a handle.
 */
static void
chimera_nfs3_pathconf_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs3_compound                 *ctx = private_data;

    if (nfs3_compound_retry(ctx)) {
        return;
    }
    struct nfs_request                   *req = ctx->req;
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_attrs              result_attr;
    const struct chimera_vfs_attrs       *attr;
    struct chimera_server_nfs_thread     *thread = req->thread;
    struct chimera_server_nfs_shared     *shared = thread->shared;
    struct evpl                          *evpl   = thread->evpl;
    struct PATHCONF3res                   res;
    int                                   rc;
    enum chimera_vfs_error                status;

    status = chimera_vfs_compound_status(compound);

    memset(&result_attr, 0, sizeof(result_attr));

    /* Taken out before the free: a freed sequence is recycled and reset. */
    if (status == CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(compound,
                                     chimera_vfs_compound_num_ops(compound) - 1);
        result_attr = op->attr;
    }

    nfs3_compound_free(ctx);

    attr = status == CHIMERA_VFS_OK ? &result_attr : NULL;

    res.status = chimera_vfs_error_to_nfsstat3(status);

    if (res.status == NFS3_OK) {
        chimera_nfs3_set_post_op_attr(&res.resok.obj_attributes, attr);

        res.resok.case_insensitive = 0;
        res.resok.case_preserving  = 1;
        res.resok.no_trunc         = 1;
        res.resok.linkmax          = UINT32_MAX;
        res.resok.name_max         = 255;
        res.resok.chown_restricted = 1;   /* POSIX: chown is superuser-only */
    } else {
        chimera_nfs3_set_post_op_attr(&res.resfail.obj_attributes, attr);
    }


    rc = shared->nfs_v3.send_reply_NFSPROC3_PATHCONF(evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_pathconf_sequence_complete */


void
chimera_nfs3_pathconf(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct PATHCONF3args      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_request               *req;
    struct chimera_vfs_compound      *compound;
    struct PATHCONF3res               res;
    int                               rc;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred_req(req, cred);

    nfs3_dump_pathconf(req, args);
    nfs3_trace_pathconf(req, args);

    res.status = chimera_nfs3_decode_fh(req, args->object.data.data, args->object.data.len);
    if (res.status != NFS3_OK) {
        nfsstat3 fh_status = res.status;
        memset(&res, 0, sizeof(res));
        res.status = fh_status;
        rc         = shared->nfs_v3.send_reply_NFSPROC3_PATHCONF(evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
        return;
    }

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);
    struct nfs3_compound *ctx = calloc(1, sizeof(*ctx));
    chimera_nfs_abort_if(!ctx, "NFS3 compound context allocation failed");
    ctx->req      = req;
    ctx->compound = compound;

    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_open_current(compound, CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_PATH, 0);
    chimera_vfs_compound_add_getattr(compound, CHIMERA_NFS3_ATTR_MASK);

    chimera_vfs_compound_submit(compound,
                                chimera_nfs3_pathconf_sequence_complete, ctx);
} /* chimera_nfs3_pathconf */
