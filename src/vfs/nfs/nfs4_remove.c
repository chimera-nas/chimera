// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"

struct chimera_nfs4_remove_ctx {
    struct chimera_nfs_thread        *thread;
    struct chimera_nfs_client_server *server;
};

static void
chimera_nfs4_remove_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct nfs_resop4          *remove_res;

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

    /* Check REMOVE result */
    if (res->num_resarray < 3) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }
    remove_res = &res->resarray[2];
    if (remove_res->opremove.status != NFS4_OK) {
        request->status = chimera_nfs4_status_to_errno(remove_res->opremove.status);
        request->complete(request);
        return;
    }

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs4_remove_callback */

void
chimera_nfs4_remove(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh,
                                                                                                  request->fh_len);
    struct chimera_nfs_client_server        *server;
    struct chimera_nfs4_client_session      *session;
    struct chimera_nfs4_remove_ctx          *ctx;
    struct COMPOUND4args                     args;
    struct nfs_argop4                        argarray[3];
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

    /* Build compound: SEQUENCE + PUTFH + REMOVE */
    memset(&args, 0, sizeof(args));
    args.tag.len      = 0;
    args.minorversion = 1;
    args.argarray     = argarray;
    args.num_argarray = 3;

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

    /* Op 2: REMOVE - remove the file or directory */
    argarray[2].argop                = OP_REMOVE;
    argarray[2].opremove.target.data = (uint8_t *) request->remove.name;
    argarray[2].opremove.target.len  = request->remove.namelen;

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
        chimera_nfs4_remove_callback,
        request);
} /* chimera_nfs4_remove */
