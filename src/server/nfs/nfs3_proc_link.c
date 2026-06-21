// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs_procs.h"
#include "nfs3_dump.h"
static void
chimera_nfs3_link_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *r_attr,
    struct chimera_vfs_attrs *r_dir_pre_attr,
    struct chimera_vfs_attrs *r_dir_post_attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct LINK3res                   res;
    int                               rc;

    res.status = chimera_vfs_error_to_nfsstat3(
        error_code);

    if (res.status == NFS3_OK) {
        chimera_nfs3_set_post_op_attr(&res.resok.file_attributes, r_attr);
        chimera_nfs3_set_wcc_data(&res.resok.linkdir_wcc, r_dir_pre_attr, r_dir_post_attr);
    } else {
        chimera_nfs3_set_post_op_attr(&res.resfail.file_attributes, r_attr);
        chimera_nfs3_set_wcc_data(&res.resfail.linkdir_wcc, r_dir_pre_attr, r_dir_post_attr);
    }

    rc = shared->nfs_v3.send_reply_NFSPROC3_LINK(evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_mkdir_complete */

void
chimera_nfs3_link(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct LINK3args          *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_request               *req;
    struct LINK3res                   res;
    uint16_t                          linkdir_export_id;
    int                               rc;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred_req(req, cred);

    nfs3_dump_link(req, args);

    /* Decode the existing-file handle (sets the request export + squash) and
     * the target-directory handle (authenticated into req->saved_fh).  The
     * target handle must outlive this (async) call, so it lives in the request
     * rather than on the stack (saved_fh is unused by NFSv3). */
    res.status = chimera_nfs3_decode_fh(req, args->file.data.data, args->file.data.len);
    if (res.status != NFS3_OK ||
        chimera_nfs_fh_unwrap(args->link.dir.data.data, args->link.dir.data.len,
                              &linkdir_export_id, req->saved_fh, &req->saved_fhlen,
                              shared->fh_key, shared->fh_sign) != CHIMERA_NFS_FH_OK) {
        nfsstat3 fh_status = res.status != NFS3_OK ? res.status : NFS3ERR_BADHANDLE;
        memset(&res, 0, sizeof(res));
        res.status = fh_status;
        rc         = shared->nfs_v3.send_reply_NFSPROC3_LINK(evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
        return;
    }

    chimera_vfs_link_at(thread->vfs_thread,
                        &req->cred,
                        req->fh,
                        req->fhlen,
                        req->saved_fh,
                        req->saved_fhlen,
                        args->link.name.str,
                        args->link.name.len,
                        0,
                        CHIMERA_NFS3_ATTR_MASK,
                        CHIMERA_NFS3_ATTR_WCC_MASK | CHIMERA_VFS_ATTR_ATOMIC,
                        CHIMERA_NFS3_ATTR_MASK,
                        NULL,
                        NULL,
                        chimera_nfs3_link_complete,
                        req);
} /* chimera_nfs3_link */
