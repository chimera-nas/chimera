// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"


struct chimera_nfs4_symlink_ctx {
    struct chimera_nfs_thread        *thread;
    struct chimera_nfs_client_server *server;
};

static void
chimera_nfs4_symlink_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request      *request = private_data;
    struct chimera_nfs4_symlink_ctx *ctx     = request->plugin_data;
    struct nfs_resop4               *create_res;
    struct nfs_resop4               *getfh_res;
    struct nfs_resop4               *getattr_res;
    xdr_opaque                      *remote_fh;

    if (unlikely(status)) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    if (res->status != NFS4_OK) {
        request->status = chimera_nfs4_status_to_errno(res->status);
        request->complete(request);
        return;
    }

    /* Check SEQUENCE result */
    if (res->num_resarray < 1 || res->resarray[0].opsequence.sr_status != NFS4_OK) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    /* Check PUTFH result */
    if (res->num_resarray < 2 || res->resarray[1].opputfh.status != NFS4_OK) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    /* The parent's pre-mutation snapshot (op 2).  Taken before the CREATE
     * status check so it is recorded even when the create itself failed. */
    chimera_nfs4_unmarshall_dir_attr(res, 2, &request->symlink_at.r_dir_pre_attr);

    /* Check CREATE result */
    if (res->num_resarray < 4) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }
    create_res = &res->resarray[3];
    if (create_res->opcreate.status != NFS4_OK) {
        request->status = chimera_nfs4_status_to_errno(create_res->opcreate.status);
        request->complete(request);
        return;
    }

    /* Check GETFH result */
    if (res->num_resarray < 5) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }
    getfh_res = &res->resarray[4];
    if (getfh_res->opgetfh.status != NFS4_OK) {
        request->status = chimera_nfs4_status_to_errno(getfh_res->opgetfh.status);
        request->complete(request);
        return;
    }

    remote_fh = &getfh_res->opgetfh.resok4.object;

    /* Build local file handle from server index + remote FH */
    if (chimera_nfs4_unmarshall_fh(remote_fh, ctx->server->index, request->fh, &request->symlink_at.r_attr) != 0) {
        request->status = CHIMERA_VFS_EOVERFLOW;
        request->complete(request);
        return;
    }

    /* Get GETATTR result */
    if (res->num_resarray >= 6) {
        getattr_res = &res->resarray[5];
        if (getattr_res->opgetattr.status == NFS4_OK) {
            chimera_nfs4_unmarshall_fattr(&getattr_res->opgetattr.resok4.obj_attributes,
                                          &request->symlink_at.r_attr);
        }
    }

    /* The parent's post-mutation snapshot (op 7, after the PUTFH that put the
     * current filehandle back on the parent). */
    chimera_nfs4_unmarshall_dir_attr(res, 7, &request->symlink_at.r_dir_post_attr);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs4_symlink_callback */

void
chimera_nfs4_symlink_at(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh,
                                                                                                  request->fh_len);
    struct chimera_nfs_client_server        *server;
    struct chimera_nfs4_client_session      *session;
    struct chimera_nfs4_symlink_ctx         *ctx;
    struct COMPOUND4args                     args;
    struct nfs_argop4                        argarray[8];
    uint32_t                                 attr_request[2];
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                 *dir_fh;
    int                                      dir_fhlen;

    ctx = request->plugin_data;

    if (!server_thread) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    server  = server_thread->server;
    session = server->nfs4_session;

    if (!session) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    ctx->thread = thread;
    ctx->server = server;

    /* Map directory FH */
    chimera_nfs4_map_fh(request->fh, request->fh_len, &dir_fh, &dir_fhlen);

    /* Build compound: SEQUENCE + PUTFH(dir) + CREATE(NF4LNK) + GETFH + GETATTR */
    memset(&args, 0, sizeof(args));
    args.tag.len      = 0;
    args.minorversion = 1;
    args.argarray     = argarray;
    args.num_argarray = 8;

    /* Op 0: SEQUENCE */
    argarray[0].argop = OP_SEQUENCE;

    /* Op 1: PUTFH - set current FH to directory */
    chimera_nfs4_putfh_op(&argarray[1], dir_fh, dir_fhlen);

    /* Op 2: GETATTR - the parent's change attribute before the mutation */
    chimera_nfs4_dir_getattr_op(&argarray[2], attr_request);

    /* Op 3: CREATE - create symbolic link */
    argarray[3].argop                               = OP_CREATE;
    argarray[3].opcreate.objtype.type               = NF4LNK;
    argarray[3].opcreate.objtype.linkdata.data      = (uint8_t *) request->symlink_at.target;
    argarray[3].opcreate.objtype.linkdata.len       = request->symlink_at.targetlen;
    argarray[3].opcreate.objname.data               = (uint8_t *) request->symlink_at.name;
    argarray[3].opcreate.objname.len                = request->symlink_at.namelen;
    argarray[3].opcreate.createattrs.num_attrmask   = 0;
    argarray[3].opcreate.createattrs.attrmask       = NULL;
    argarray[3].opcreate.createattrs.attr_vals.data = NULL;
    argarray[3].opcreate.createattrs.attr_vals.len  = 0;

    /* Op 4: GETFH - get file handle of created symlink */
    argarray[4].argop = OP_GETFH;

    /* Op 5: GETATTR - get attributes of created symlink */
    argarray[5].argop = OP_GETATTR;
    chimera_nfs4_attr_request_stat(attr_request);
    argarray[5].opgetattr.attr_request     = attr_request;
    argarray[5].opgetattr.num_attr_request = 2;

    /* Op 6: PUTFH - CREATE left the current filehandle on the new object, so
     * put it back on the parent before snapshotting it again. */
    chimera_nfs4_putfh_op(&argarray[6], dir_fh, dir_fhlen);

    /* Op 7: GETATTR - the parent's change attribute after the mutation */
    chimera_nfs4_dir_getattr_op(&argarray[7], attr_request);

    chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    chimera_nfs4_compound_call(
        thread,
        shared,
        server_thread,
        request,
        &args,
        &rpc2_cred,
        0, 0, NULL, 0, 0,
        chimera_nfs4_symlink_callback,
        request,
        chimera_nfs4_dispatch, private_data);
} /* chimera_nfs4_symlink_at */
