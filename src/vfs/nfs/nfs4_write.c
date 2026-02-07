// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs4_open_state.h"
#include "vfs/vfs_error.h"

struct chimera_nfs4_write_ctx {
    struct chimera_nfs_thread        *thread;
    struct chimera_nfs_client_server *server;
    struct chimera_nfs4_open_state   *open_state;
};

static void
chimera_nfs4_write_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct chimera_nfs4_write_ctx *ctx     = request->plugin_data;
    struct nfs_resop4             *write_res;

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

    /* Check WRITE result */
    if (res->num_resarray < 3) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }
    write_res = &res->resarray[2];
    if (write_res->opwrite.status != NFS4_OK) {
        request->status = chimera_nfs4_status_to_errno(write_res->opwrite.status);
        request->complete(request);
        return;
    }

    /* Mark file as dirty if the write was not fully committed to stable storage */
    if (write_res->opwrite.resok4.committed != FILE_SYNC4 && ctx->open_state) {
        chimera_nfs4_open_state_mark_dirty(ctx->open_state);
    }

    request->write.r_sync   = write_res->opwrite.resok4.committed;
    request->write.r_length = write_res->opwrite.resok4.count;
    request->status         = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs4_write_callback */

void
chimera_nfs4_write(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh,
                                                                                                  request->fh_len);
    struct chimera_nfs_client_server        *server;
    struct chimera_nfs4_client_session      *session;
    struct chimera_nfs4_write_ctx           *ctx;
    struct chimera_nfs4_open_state          *open_state;
    struct COMPOUND4args                     args;
    struct nfs_argop4                        argarray[3];
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                 *fh;
    int                                      fhlen;

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

    ctx             = request->plugin_data;
    ctx->thread     = thread;
    ctx->server     = server;
    open_state      = (struct chimera_nfs4_open_state *) request->write.handle->vfs_private;
    ctx->open_state = open_state;

    chimera_nfs4_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    /* Build compound: SEQUENCE + PUTFH + WRITE */
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

    /* Op 1: PUTFH */
    argarray[1].argop               = OP_PUTFH;
    argarray[1].opputfh.object.data = fh;
    argarray[1].opputfh.object.len  = fhlen;

    /* Op 2: WRITE */
    argarray[2].argop = OP_WRITE;

    /* Use the stateid from the open state, or anonymous stateid if not available */
    if (open_state) {
        argarray[2].opwrite.stateid = open_state->stateid;
    } else {
        /* Anonymous stateid */
        memset(&argarray[2].opwrite.stateid, 0, sizeof(argarray[2].opwrite.stateid));
        argarray[2].opwrite.stateid.seqid = 0;
    }

    argarray[2].opwrite.offset      = request->write.offset;
    argarray[2].opwrite.stable      = request->write.sync ? FILE_SYNC4 : UNSTABLE4;
    argarray[2].opwrite.data.iov    = request->write.iov;
    argarray[2].opwrite.data.niov   = request->write.niov;
    argarray[2].opwrite.data.length = request->write.length;

    chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    shared->nfs_v4.send_call_NFSPROC4_COMPOUND(
        &shared->nfs_v4.rpc2,
        thread->evpl,
        server_thread->nfs_conn,
        &rpc2_cred,
        &args,
        1, 0, 0,
        chimera_nfs4_write_callback,
        request);
} /* chimera_nfs4_write */
