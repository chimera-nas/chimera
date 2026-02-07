// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"

struct chimera_nfs4_rename_ctx {
    struct chimera_nfs_thread        *thread;
    struct chimera_nfs_client_server *server;
};

static void
chimera_nfs4_rename_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct nfs_resop4          *rename_res;

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

    /* Check SEQUENCE result (index 0) */
    if (res->num_resarray < 1 || res->resarray[0].opsequence.sr_status != NFS4_OK) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    /* Check PUTFH result (index 1) - source directory */
    if (res->num_resarray < 2 || res->resarray[1].opputfh.status != NFS4_OK) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    /* Check SAVEFH result (index 2) */
    if (res->num_resarray < 3 || res->resarray[2].opsavefh.status != NFS4_OK) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    /* Check PUTFH result (index 3) - target directory */
    if (res->num_resarray < 4 || res->resarray[3].opputfh.status != NFS4_OK) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    /* Check RENAME result (index 4) */
    if (res->num_resarray < 5) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }
    rename_res = &res->resarray[4];
    if (rename_res->oprename.status != NFS4_OK) {
        request->status = chimera_nfs4_status_to_errno(rename_res->oprename.status);
        request->complete(request);
        return;
    }

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs4_rename_callback */

void
chimera_nfs4_rename(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh,
                                                                                                  request->fh_len);
    struct chimera_nfs_client_server        *server;
    struct chimera_nfs4_client_session      *session;
    struct chimera_nfs4_rename_ctx          *ctx;
    struct COMPOUND4args                     args;
    struct nfs_argop4                        argarray[5];
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                 *src_fh;
    int                                      src_fhlen;
    uint8_t                                 *dst_fh;
    int                                      dst_fhlen;

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

    /* Map source directory FH (request->fh) */
    chimera_nfs4_map_fh(request->fh, request->fh_len, &src_fh, &src_fhlen);

    /* Map target directory FH */
    chimera_nfs4_map_fh(request->rename.new_fh, request->rename.new_fhlen, &dst_fh, &dst_fhlen);

    /* Build compound: SEQUENCE + PUTFH(src) + SAVEFH + PUTFH(dst) + RENAME */
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

    /* Op 1: PUTFH - set current FH to source directory */
    argarray[1].argop               = OP_PUTFH;
    argarray[1].opputfh.object.data = src_fh;
    argarray[1].opputfh.object.len  = src_fhlen;

    /* Op 2: SAVEFH - save source directory FH */
    argarray[2].argop = OP_SAVEFH;

    /* Op 3: PUTFH - set current FH to target directory */
    argarray[3].argop               = OP_PUTFH;
    argarray[3].opputfh.object.data = dst_fh;
    argarray[3].opputfh.object.len  = dst_fhlen;

    /* Op 4: RENAME - rename from saved FH (source dir) to current FH (target dir) */
    argarray[4].argop                 = OP_RENAME;
    argarray[4].oprename.oldname.data = (uint8_t *) request->rename.name;
    argarray[4].oprename.oldname.len  = request->rename.namelen;
    argarray[4].oprename.newname.data = (uint8_t *) request->rename.new_name;
    argarray[4].oprename.newname.len  = request->rename.new_namelen;

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
        chimera_nfs4_rename_callback,
        request);
} /* chimera_nfs4_rename */
