// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "vfs/vfs_procs.h"

static void
chimera_nfs4_read_complete(
    enum chimera_vfs_error    error_code,
    uint32_t                  count,
    uint32_t                  eof,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request *req = private_data;
    struct READ4res    *res = &req->res_compound.resarray[req->index].opread;

    if (error_code == CHIMERA_VFS_OK) {
        res->status             = NFS4_OK;
        res->resok4.eof         = eof;
        res->resok4.data.length = count;
        res->resok4.data.iov    = iov;
        res->resok4.data.niov   = niov;
    } else {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        evpl_iovecs_release(req->thread->evpl, iov, niov);
    }

    chimera_nfs4_compound_complete(req, NFS4_OK);

} /* chimera_nfs3_write_complete */

void
chimera_nfs4_read(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct READ4args               *args    = &argop->opread;
    struct READ4res                *res     = &resop->opread;
    struct nfs4_session            *session = nfs4_resolve_session(
        req->session, &args->stateid,
        &thread->shared->nfs4_shared_clients);
    struct chimera_vfs_open_handle *handle;
    struct evpl_iovec              *iov;

    if (!session) {
        res->status = NFS4ERR_BAD_STATEID;
        chimera_nfs4_compound_complete(req, NFS4_OK);
        return;
    }

    if (!req->session) {
        req->session = session;
        evpl_rpc2_conn_set_private_data(req->conn, session);
    }

    handle = nfs4_session_get_open_handle(session, &args->stateid);

    if (!handle) {
        chimera_nfs4_compound_complete(req, NFS4ERR_BAD_STATEID);
        return;
    }

    iov = xdr_dbuf_alloc_space(sizeof(*iov) * 256, req->encoding->dbuf);
    chimera_nfs_abort_if(iov == NULL, "Failed to allocate space");

    chimera_vfs_read(thread->vfs_thread, &req->cred,
                     handle,
                     args->offset,
                     args->count,
                     iov,
                     256,
                     0,
                     chimera_nfs4_read_complete,
                     req);
} /* chimera_nfs4_read */
