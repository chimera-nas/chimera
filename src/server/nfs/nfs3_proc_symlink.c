// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"
#include "nfs3_trace.h"

static void
chimera_nfs3_symlink_complete(
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
    struct SYMLINK3res                res;
    int                               rc;

    res.status = chimera_vfs_error_to_nfsstat3(
        error_code);

    if (res.status == NFS3_OK) {
        if (r_attr->va_set_mask & CHIMERA_VFS_ATTR_FH) {
            uint8_t wire[CHIMERA_NFS_FH_MAX];
            int     wirelen;

            chimera_nfs_fh_encode(req, r_attr->va_fh, r_attr->va_fh_len, wire, &wirelen);
            res.resok.obj.handle_follows = 1;
            rc                           = xdr_dbuf_opaque_copy(&res.resok.obj.handle.data,
                                                                wire,
                                                                wirelen,
                                                                req->encoding->dbuf);
            chimera_nfs_abort_if(rc, "Failed to copy opaque");
        } else {
            res.resok.obj.handle_follows = 0;
        }

        chimera_nfs3_set_post_op_attr(&res.resok.obj_attributes, r_attr);
        chimera_nfs3_set_wcc_data(&res.resok.dir_wcc, r_dir_pre_attr, r_dir_post_attr);
    } else {
        chimera_nfs3_set_wcc_data(&res.resfail.dir_wcc, r_dir_pre_attr, r_dir_post_attr);
    }

    /* The parent open belonged to the sequence and went with it. */

    rc = shared->nfs_v3.send_reply_NFSPROC3_SYMLINK(evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_mkdir_complete */


/*
 * PUTFH, OPEN, CREATE.  The new symlink becomes current, so its handle and
 * attributes come back on the CREATE itself.
 */
static void
chimera_nfs3_symlink_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request                   *req = private_data;
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_attrs              attr, pre_attr, post_attr;
    enum chimera_vfs_error                status;

    status = chimera_vfs_compound_status(compound);

    memset(&attr, 0, sizeof(attr));

    op = chimera_vfs_compound_op(compound,
                                 chimera_vfs_compound_num_ops(compound) - 1);

    /* dir_wcc rides on both arms. */
    pre_attr  = op->dir_pre_attr;
    post_attr = op->dir_post_attr;

    if (status == CHIMERA_VFS_OK) {
        attr = op->attr;
    }

    chimera_vfs_compound_free(compound);

    chimera_nfs3_symlink_complete(status, &attr, &pre_attr, &post_attr, req);
} /* chimera_nfs3_symlink_sequence_complete */

void
chimera_nfs3_symlink(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct SYMLINK3args       *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_request               *req;
    struct chimera_vfs_compound      *compound;
    struct chimera_vfs_attrs         *attr;
    struct SYMLINK3res                res;
    int                               rc;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred_req(req, cred);

    nfs3_dump_symlink(req, args);
    nfs3_trace_symlink(req, args);
    req->args_symlink = args;

    res.status = chimera_nfs3_decode_fh(req, args->where.dir.data.data, args->where.dir.data.len);
    if (res.status == NFS3_OK) {
        res.status = chimera_nfs3_check_rofs(req, req->export_id);
    }
    if (res.status != NFS3_OK) {
        nfsstat3 fh_status = res.status;
        memset(&res, 0, sizeof(res));
        res.status = fh_status;
        rc         = shared->nfs_v3.send_reply_NFSPROC3_SYMLINK(evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
        return;
    }

    attr = xdr_dbuf_alloc_space(sizeof(*attr), req->encoding->dbuf);
    chimera_nfs_abort_if(attr == NULL, "Failed to allocate space");

    chimera_nfs3_sattr3_to_va(attr, &args->symlink.symlink_attributes);

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_PATH |
                                          CHIMERA_VFS_OPEN_DIRECTORY, 0);
    chimera_vfs_compound_add_create(compound,
                                    CHIMERA_VFS_COMPOUND_CREATE_SYMLINK,
                                    args->where.name.str,
                                    args->where.name.len,
                                    args->symlink.symlink_data.str,
                                    (int) args->symlink.symlink_data.len,
                                    attr,
                                    CHIMERA_VFS_ATTR_FH |
                                    CHIMERA_NFS3_ATTR_MASK,
                                    CHIMERA_NFS3_ATTR_WCC_MASK,
                                    CHIMERA_NFS3_ATTR_MASK);

    chimera_vfs_compound_submit(compound,
                                chimera_nfs3_symlink_sequence_complete, req);
} /* chimera_nfs3_symlink */
