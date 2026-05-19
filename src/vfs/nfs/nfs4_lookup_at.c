// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "vfs/vfs_error.h"

enum chimera_nfs4_lookup_op_type {
    LOOKUP_OP_NORMAL,
    LOOKUP_OP_DOT,
    LOOKUP_OP_DOTDOT,
};

struct chimera_nfs4_lookup_ctx {
    struct chimera_nfs_client_server *server;
    enum chimera_nfs4_lookup_op_type op_type;
};

static void
chimera_nfs4_lookup_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request     *request = private_data;
    struct chimera_nfs4_lookup_ctx *ctx     = request->plugin_data;
    struct nfs_resop4              *getfh_res;
    struct nfs_resop4              *getattr_res;
    xdr_opaque                     *remote_fh;
    int                             getfh_idx;
    int                             getattr_idx;
    nfsstat4                        traverse_status;

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

    /*
     * For ".", the compound is SEQUENCE + PUTFH + GETFH + GETATTR (no
     * traversal op).  For ".." and normal names, there is a LOOKUPP or
     * LOOKUP at index 2 whose status must be checked.
     */
    if (ctx->op_type == LOOKUP_OP_DOT) {
        getfh_idx   = 2;
        getattr_idx = 3;
    } else {
        if (res->num_resarray < 3) {
            request->status = CHIMERA_VFS_EIO;
            request->complete(request);
            return;
        }
        if (ctx->op_type == LOOKUP_OP_DOTDOT) {
            traverse_status = res->resarray[2].oplookupp.status;
        } else {
            traverse_status = res->resarray[2].oplookup.status;
        }
        if (traverse_status != NFS4_OK) {
            request->status = chimera_nfs4_status_to_errno(traverse_status);
            request->complete(request);
            return;
        }
        getfh_idx   = 3;
        getattr_idx = 4;
    }

    /* Get GETFH result */
    if (res->num_resarray <= (uint32_t) getfh_idx) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }
    getfh_res = &res->resarray[getfh_idx];
    if (getfh_res->opgetfh.status != NFS4_OK) {
        request->status = chimera_nfs4_status_to_errno(getfh_res->opgetfh.status);
        request->complete(request);
        return;
    }

    remote_fh = &getfh_res->opgetfh.resok4.object;

    /* Build local file handle from server index + remote FH */
    chimera_nfs4_unmarshall_fh(remote_fh, ctx->server->index, request->fh, &request->lookup_at.r_attr);

    /* Get GETATTR result */
    if (res->num_resarray > (uint32_t) getattr_idx) {
        getattr_res = &res->resarray[getattr_idx];
        if (getattr_res->opgetattr.status == NFS4_OK) {
            chimera_nfs4_unmarshall_fattr(&getattr_res->opgetattr.resok4.obj_attributes,
                                          &request->lookup_at.r_attr);
        }
    }

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs4_lookup_callback */

void
chimera_nfs4_lookup_at(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh,
                                                                                                  request->fh_len);
    struct chimera_nfs_client_server        *server;
    struct chimera_nfs4_client_session      *session;
    struct COMPOUND4args                     args;
    struct nfs_argop4                        argarray[5];
    uint32_t                                 attr_request[2];
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                 *fh;
    int                                      fhlen;
    struct chimera_nfs4_lookup_ctx          *ctx;
    enum chimera_nfs4_lookup_op_type         op_type;
    int                                      getattr_idx;

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

    /*
     * NFSv4 OP_LOOKUP rejects "." and ".." with NFS4ERR_BADNAME (RFC 7530
     * 14.2.15).  For ".", drop the traversal op entirely (current FH is
     * unchanged after PUTFH).  For "..", use OP_LOOKUPP which moves the
     * current FH to the parent directory.
     */
    if (request->lookup_at.component_len == 1 &&
        request->lookup_at.component[0] == '.') {
        op_type = LOOKUP_OP_DOT;
    } else if (request->lookup_at.component_len == 2 &&
               request->lookup_at.component[0] == '.' &&
               request->lookup_at.component[1] == '.') {
        op_type = LOOKUP_OP_DOTDOT;
    } else {
        op_type = LOOKUP_OP_NORMAL;
    }

    ctx          = request->plugin_data;
    ctx->server  = server;
    ctx->op_type = op_type;

    chimera_nfs4_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    memset(&args, 0, sizeof(args));
    args.tag.len      = 0;
    args.minorversion = 1;
    args.argarray     = argarray;

    /* Op 0: SEQUENCE */
    argarray[0].argop = OP_SEQUENCE;
    memcpy(argarray[0].opsequence.sa_sessionid, session->sessionid, NFS4_SESSIONID_SIZE);
    argarray[0].opsequence.sa_sequenceid     = chimera_nfs4_get_sequenceid(session, server_thread->slot_id);
    argarray[0].opsequence.sa_slotid         = server_thread->slot_id;
    argarray[0].opsequence.sa_highest_slotid = session->max_slots - 1;
    argarray[0].opsequence.sa_cachethis      = 0;

    /* Op 1: PUTFH - set current file handle to parent directory */
    argarray[1].argop               = OP_PUTFH;
    argarray[1].opputfh.object.data = fh;
    argarray[1].opputfh.object.len  = fhlen;

    if (op_type == LOOKUP_OP_DOT) {
        /* No traversal op: GETFH at 2, GETATTR at 3 */
        argarray[2].argop = OP_GETFH;
        getattr_idx       = 3;
        args.num_argarray = 4;
    } else {
        if (op_type == LOOKUP_OP_DOTDOT) {
            /* Op 2: LOOKUPP - move current FH to parent directory */
            argarray[2].argop = OP_LOOKUPP;
        } else {
            /* Op 2: LOOKUP - lookup the component name */
            argarray[2].argop                 = OP_LOOKUP;
            argarray[2].oplookup.objname.data = (void *) request->lookup_at.component;
            argarray[2].oplookup.objname.len  = request->lookup_at.component_len;
        }

        /* Op 3: GETFH - get file handle for resolved object */
        argarray[3].argop = OP_GETFH;
        getattr_idx       = 4;
        args.num_argarray = 5;
    }

    /* GETATTR for the resolved object */
    argarray[getattr_idx].argop = OP_GETATTR;
    attr_request[0]             = (1 << FATTR4_TYPE) | (1 << FATTR4_SIZE) | (1 << FATTR4_FILEID);
    attr_request[1]             = (1 << (FATTR4_MODE - 32)) | (1 << (FATTR4_NUMLINKS - 32)) |
        (1 << (FATTR4_TIME_ACCESS - 32)) | (1 << (FATTR4_TIME_MODIFY - 32));
    argarray[getattr_idx].opgetattr.attr_request     = attr_request;
    argarray[getattr_idx].opgetattr.num_attr_request = 2;

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
        chimera_nfs4_lookup_callback,
        request);
} /* chimera_nfs4_lookup_at */
