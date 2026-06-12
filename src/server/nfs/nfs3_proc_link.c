// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs_procs.h"
#include "nfs3_dump.h"

static void
chimera_nfs3_link_reply(struct nfs_request *req)
{
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    if (req->txn_op_status != CHIMERA_VFS_OK) {
        req->res_link.status = chimera_vfs_error_to_nfsstat3(req->txn_op_status);
        chimera_nfs3_set_post_op_attr(&req->res_link.resfail.file_attributes, NULL);
        chimera_nfs3_set_wcc_data(&req->res_link.resfail.linkdir_wcc, NULL, NULL);
    }

    rc = shared->nfs_v3.send_reply_NFSPROC3_LINK(thread->evpl, NULL,
                                                 &req->res_link, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_link_reply */

static void
chimera_nfs3_link_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *r_attr,
    struct chimera_vfs_attrs *r_dir_pre_attr,
    struct chimera_vfs_attrs *r_dir_post_attr,
    void                     *private_data)
{
    struct nfs_request *req = private_data;

    if (error_code == CHIMERA_VFS_OK) {
        req->res_link.status = NFS3_OK;
        chimera_nfs3_set_post_op_attr(&req->res_link.resok.file_attributes, r_attr);
        chimera_nfs3_set_wcc_data(&req->res_link.resok.linkdir_wcc, r_dir_pre_attr, r_dir_post_attr);
    }

    chimera_nfs3_txn_finish(req, error_code);
} /* chimera_nfs3_link_complete */

static void
chimera_nfs3_link_start(struct nfs_request *req)
{
    struct LINK3args *args = req->args_link;

    chimera_vfs_link_at(req->thread->vfs_thread,
                        &req->cred, req->txn,
                        args->file.data.data,
                        args->file.data.len,
                        args->link.dir.data.data,
                        args->link.dir.data.len,
                        args->link.name.str,
                        args->link.name.len,
                        0,
                        CHIMERA_NFS3_ATTR_MASK,
                        CHIMERA_NFS3_ATTR_WCC_MASK | CHIMERA_VFS_ATTR_ATOMIC,
                        CHIMERA_NFS3_ATTR_MASK,
                        chimera_nfs3_link_complete,
                        req);
} /* chimera_nfs3_link_start */

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
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred(&req->cred, cred);

    nfs3_dump_link(req, args);

    req->args_link = args;

    chimera_nfs3_txn_run(req, args->file.data.data, args->file.data.len,
                         CHIMERA_VFS_TXN_WRITE,
                         chimera_nfs3_link_start, chimera_nfs3_link_reply);
} /* chimera_nfs3_link */
