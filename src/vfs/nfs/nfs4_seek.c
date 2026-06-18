// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs4_open_state.h"

static void
chimera_nfs4_seek_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct SEEK4res            *seek_res;

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

    /* SEQUENCE + PUTFH + SEEK */
    if (res->num_resarray < 3 ||
        res->resarray[0].opsequence.sr_status != NFS4_OK ||
        res->resarray[1].opputfh.status != NFS4_OK) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    seek_res = &res->resarray[2].opseek;

    if (seek_res->sa_status != NFS4_OK) {
        request->status = chimera_nfs4_status_to_errno(seek_res->sa_status);
        request->complete(request);
        return;
    }

    request->seek.r_eof    = seek_res->resok4.sr_eof;
    request->seek.r_offset = seek_res->resok4.sr_offset;
    request->status        = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs4_seek_callback */

void
chimera_nfs4_seek(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread =
        chimera_nfs_thread_get_server_thread(thread, request->fh, request->fh_len);
    struct chimera_nfs_client_server        *server;
    struct chimera_nfs4_client_session      *session;
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

    open_state = (struct chimera_nfs4_open_state *) request->seek.handle->vfs_private;

    chimera_nfs4_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    /* Build compound: SEQUENCE + PUTFH + SEEK.  SEEK is an NFSv4.2 operation,
     * so the compound advertises minorversion 2 (the server gates ops per
     * compound minor version; the session is not pinned to one). */
    memset(&args, 0, sizeof(args));
    args.tag.len      = 0;
    args.minorversion = 2;
    args.argarray     = argarray;
    args.num_argarray = 3;

    /* Op 0: SEQUENCE */
    argarray[0].argop = OP_SEQUENCE;

    /* Op 1: PUTFH */
    argarray[1].argop               = OP_PUTFH;
    argarray[1].opputfh.object.data = fh;
    argarray[1].opputfh.object.len  = fhlen;

    /* Op 2: SEEK */
    argarray[2].argop = OP_SEEK;
    if (open_state) {
        argarray[2].opseek.sa_stateid = open_state->stateid;
    } else {
        memset(&argarray[2].opseek.sa_stateid, 0,
               sizeof(argarray[2].opseek.sa_stateid));
    }
    argarray[2].opseek.sa_offset = request->seek.offset;
    argarray[2].opseek.sa_what   = request->seek.what; /* 0=DATA, 1=HOLE */

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
        chimera_nfs4_seek_callback,
        request,
        chimera_nfs4_dispatch, private_data);
} /* chimera_nfs4_seek */
