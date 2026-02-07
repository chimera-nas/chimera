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
    chimera_nfs4_unmarshall_fh(remote_fh, ctx->server->index, request->fh, &request->symlink.r_attr);

    /* Get GETATTR result */
    if (res->num_resarray >= 5) {
        getattr_res = &res->resarray[4];
        if (getattr_res->opgetattr.status == NFS4_OK) {
            chimera_nfs4_unmarshall_fattr(&getattr_res->opgetattr.resok4.obj_attributes,
                                          &request->symlink.r_attr);
        }
    }

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs4_symlink_callback */

void
chimera_nfs4_symlink(
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
    struct nfs_argop4                        argarray[5];
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
    args.num_argarray = 5;

    /* Op 0: SEQUENCE */
    argarray[0].argop = OP_SEQUENCE;
    memcpy(argarray[0].opsequence.sa_sessionid, session->sessionid, NFS4_SESSIONID_SIZE);
    argarray[0].opsequence.sa_sequenceid     = chimera_nfs4_get_sequenceid(session, server_thread->slot_id);
    argarray[0].opsequence.sa_slotid         = server_thread->slot_id;
    argarray[0].opsequence.sa_highest_slotid = session->max_slots - 1;
    argarray[0].opsequence.sa_cachethis      = 0;

    /* Op 1: PUTFH - set current FH to directory */
    argarray[1].argop               = OP_PUTFH;
    argarray[1].opputfh.object.data = dir_fh;
    argarray[1].opputfh.object.len  = dir_fhlen;

    /* Op 2: CREATE - create symbolic link */
    argarray[2].argop                               = OP_CREATE;
    argarray[2].opcreate.objtype.type               = NF4LNK;
    argarray[2].opcreate.objtype.linkdata.data      = (uint8_t *) request->symlink.target;
    argarray[2].opcreate.objtype.linkdata.len       = request->symlink.targetlen;
    argarray[2].opcreate.objname.data               = (uint8_t *) request->symlink.name;
    argarray[2].opcreate.objname.len                = request->symlink.namelen;
    argarray[2].opcreate.createattrs.num_attrmask   = 0;
    argarray[2].opcreate.createattrs.attrmask       = NULL;
    argarray[2].opcreate.createattrs.attr_vals.data = NULL;
    argarray[2].opcreate.createattrs.attr_vals.len  = 0;

    /* Op 3: GETFH - get file handle of created symlink */
    argarray[3].argop = OP_GETFH;

    /* Op 4: GETATTR - get attributes of created symlink */
    argarray[4].argop = OP_GETATTR;
    attr_request[0]   = (1 << FATTR4_TYPE) | (1 << FATTR4_SIZE) | (1 << FATTR4_FILEID);
    attr_request[1]   = (1 << (FATTR4_MODE - 32)) | (1 << (FATTR4_NUMLINKS - 32)) |
        (1 << (FATTR4_TIME_ACCESS - 32)) | (1 << (FATTR4_TIME_MODIFY - 32));
    argarray[4].opgetattr.attr_request     = attr_request;
    argarray[4].opgetattr.num_attr_request = 2;

    chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    shared->nfs_v4.send_call_NFSPROC4_COMPOUND(
        &shared->nfs_v4.rpc2,
        thread->evpl,
        server_thread->nfs_conn,
        &rpc2_cred,
        &args,
        0, 0, 0,
        chimera_nfs4_symlink_callback,
        request);
} /* chimera_nfs4_symlink */ /* chimera_nfs4_symlink */ /* chimera_nfs4_symlink */

