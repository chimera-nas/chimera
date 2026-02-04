// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "vfs/vfs_procs.h"
#include "evpl/evpl.h"

static void
chimera_nfs4_write_complete(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs_request *req  = private_data;
    struct WRITE4args  *args = req->args_write4;
    struct WRITE4res   *res  = &req->res_compound.resarray[req->index].opwrite;

    /* Release write iovecs here on the server thread, not in VFS backend.
     * The iovecs were allocated on this thread and must be released here
     * to avoid cross-thread access to non-atomic refcounts.
     */
    evpl_iovecs_release(req->thread->evpl, args->data.iov, args->data.niov);

    if (error_code == CHIMERA_VFS_OK) {
        res->status           = NFS4_OK;
        res->resok4.count     = length;
        res->resok4.committed = sync ? FILE_SYNC4 : UNSTABLE4;

        memcpy(res->resok4.writeverf,
               &req->thread->shared->nfs_verifier,
               sizeof(res->resok4.writeverf));
    } else {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
    }

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs3_write_complete */

void
chimera_nfs4_write(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct WRITE4args   *args    = &argop->opwrite;
    struct nfs4_session *session = req->session;
    struct nfs4_state   *state;

    state = nfs4_session_get_state(session, &args->stateid);

    req->args_write4 = args;

    /* Transfer ownership of write iovecs from the RPC2 message to prevent
     * msg_free from double-releasing (args->data.iov points to msg->read_chunk.iov
     * via XDR zerocopy). The iovecs will be released in the write completion
     * callback on this server thread, not in the VFS backend (which may run on
     * a different delegation thread).
     */
    evpl_rpc2_encoding_take_read_chunk(req->encoding, NULL, NULL);

    chimera_vfs_write(thread->vfs_thread, &req->cred,
                      state->nfs4_state_handle,
                      args->offset,
                      args->data.length,
                      (args->stable != UNSTABLE4),
                      0,
                      0,
                      args->data.iov,
                      args->data.niov,
                      chimera_nfs4_write_complete,
                      req);
} /* chimera_nfs4_write */
