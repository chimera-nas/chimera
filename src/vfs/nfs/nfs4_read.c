// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs4_open_state.h"

struct chimera_nfs4_read_ctx {
    struct chimera_nfs_thread        *thread;
    struct chimera_nfs_client_server *server;
};

static void
chimera_nfs4_read_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct nfs_resop4          *read_res;

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

    /* Check READ result */
    if (res->num_resarray < 3) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }
    read_res = &res->resarray[2];
    if (read_res->opread.status != NFS4_OK) {
        request->status = chimera_nfs4_status_to_errno(read_res->opread.status);
        request->complete(request);
        return;
    }

    /* Extract read results */
    request->read.r_eof    = read_res->opread.resok4.eof;
    request->read.r_length = read_res->opread.resok4.data.length;
    request->read.r_niov   = read_res->opread.resok4.data.niov;
    request->read.iov      = read_res->opread.resok4.data.iov;

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs4_read_callback */

void
chimera_nfs4_read(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh,
                                                                                                  request->fh_len);
    struct chimera_nfs_client_server        *server;
    struct chimera_nfs4_client_session      *session;
    struct chimera_nfs4_read_ctx            *ctx;
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

    ctx         = request->plugin_data;
    ctx->thread = thread;
    ctx->server = server;

    open_state = (struct chimera_nfs4_open_state *) request->read.handle->vfs_private;

    chimera_nfs4_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    /* Build compound: SEQUENCE + PUTFH + READ */
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

    /* Op 2: READ */
    argarray[2].argop = OP_READ;

    /* Use the stateid from the open state, or anonymous stateid if not available */
    if (open_state) {
        argarray[2].opread.stateid = open_state->stateid;
    } else {
        /* Anonymous stateid */
        memset(&argarray[2].opread.stateid, 0, sizeof(argarray[2].opread.stateid));
        argarray[2].opread.stateid.seqid = 0;
    }

    argarray[2].opread.offset = request->read.offset;
    argarray[2].opread.count  = request->read.length;

    chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    shared->nfs_v4.send_call_NFSPROC4_COMPOUND(
        &shared->nfs_v4.rpc2,
        thread->evpl,
        server_thread->nfs_conn,
        &rpc2_cred,
        &args,
        0, request->read.length, 0,
        chimera_nfs4_read_callback,
        request);
} /* chimera_nfs4_read */
