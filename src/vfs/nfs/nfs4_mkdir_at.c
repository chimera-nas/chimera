// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"

struct chimera_nfs4_mkdir_ctx {
    struct chimera_nfs_thread        *thread;
    struct chimera_nfs_client_server *server;
    uint32_t                          attr_mask[2];
    uint8_t                           attr_vals[128];
};

static void
chimera_nfs4_mkdir_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct chimera_nfs4_mkdir_ctx *ctx     = request->plugin_data;
    struct nfs_resop4             *create_res;
    struct nfs_resop4             *getfh_res;
    struct nfs_resop4             *getattr_res;
    xdr_opaque                    *remote_fh;

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

    /* Check CREATE result */
    if (res->num_resarray < 3) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }
    create_res = &res->resarray[2];
    if (create_res->opcreate.status != NFS4_OK) {
        request->status = chimera_nfs4_status_to_errno(create_res->opcreate.status);
        request->complete(request);
        return;
    }

    /* Check GETFH result */
    if (res->num_resarray < 4) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }
    getfh_res = &res->resarray[3];
    if (getfh_res->opgetfh.status != NFS4_OK) {
        request->status = chimera_nfs4_status_to_errno(getfh_res->opgetfh.status);
        request->complete(request);
        return;
    }

    remote_fh = &getfh_res->opgetfh.resok4.object;

    /* Build local file handle from server index + remote FH */
    chimera_nfs4_unmarshall_fh(remote_fh, ctx->server->index, request->fh, &request->mkdir_at.r_attr);

    /* Get GETATTR result */
    if (res->num_resarray >= 5) {
        getattr_res = &res->resarray[4];
        if (getattr_res->opgetattr.status == NFS4_OK) {
            chimera_nfs4_unmarshall_fattr(&getattr_res->opgetattr.resok4.obj_attributes,
                                          &request->mkdir_at.r_attr);
        }
    }

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs4_mkdir_callback */

void
chimera_nfs4_mkdir_at(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh,
                                                                                                  request->fh_len);
    struct chimera_nfs_client_server        *server;
    struct chimera_nfs4_client_session      *session;
    struct chimera_nfs4_mkdir_ctx           *ctx;
    struct COMPOUND4args                     args;
    struct nfs_argop4                        argarray[5];
    uint32_t                                 attr_request[2];
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                 *fh;
    int                                      fhlen;

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

    chimera_nfs4_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    /* Build compound: SEQUENCE + PUTFH + CREATE + GETFH + GETATTR */
    memset(&args, 0, sizeof(args));
    args.tag.len      = 0;
    args.minorversion = 1;
    args.argarray     = argarray;
    args.num_argarray = 5;

    /* Op 0: SEQUENCE */
    argarray[0].argop = OP_SEQUENCE;

    /* Op 1: PUTFH - set current file handle to parent directory */
    argarray[1].argop               = OP_PUTFH;
    argarray[1].opputfh.object.data = fh;
    argarray[1].opputfh.object.len  = fhlen;

    /* Op 2: CREATE - create the directory */
    argarray[2].argop                 = OP_CREATE;
    argarray[2].opcreate.objtype.type = NF4DIR;
    argarray[2].opcreate.objname.data = (uint8_t *) request->mkdir_at.name;
    argarray[2].opcreate.objname.len  = request->mkdir_at.name_len;

    {
        int attr_len     = 0;
        int num_attrmask = chimera_nfs4_marshall_createattrs(request->mkdir_at.set_attr,
                                                             ctx->attr_mask,
                                                             ctx->attr_vals,
                                                             &attr_len);
        argarray[2].opcreate.createattrs.num_attrmask   = num_attrmask;
        argarray[2].opcreate.createattrs.attrmask       = num_attrmask ? ctx->attr_mask : NULL;
        argarray[2].opcreate.createattrs.attr_vals.len  = attr_len;
        argarray[2].opcreate.createattrs.attr_vals.data = attr_len ? ctx->attr_vals : NULL;
    }

    /* Op 3: GETFH - get file handle for created directory */
    argarray[3].argop = OP_GETFH;

    /* Op 4: GETATTR - get attributes for created directory */
    argarray[4].argop = OP_GETATTR;
    attr_request[0]   = (1 << FATTR4_TYPE) | (1 << FATTR4_SIZE) | (1 << FATTR4_FILEID);
    attr_request[1]   = (1 << (FATTR4_MODE - 32)) | (1 << (FATTR4_NUMLINKS - 32)) |
        (1 << (FATTR4_TIME_ACCESS - 32)) | (1 << (FATTR4_TIME_MODIFY - 32));
    argarray[4].opgetattr.attr_request     = attr_request;
    argarray[4].opgetattr.num_attr_request = 2;

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
        chimera_nfs4_mkdir_callback,
        request,
        chimera_nfs4_dispatch, private_data);
} /* chimera_nfs4_mkdir_at */
