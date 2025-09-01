// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

static void
chimera_nfs4_readlink_complete(
    enum chimera_vfs_error error_code,
    int                    targetlen,
    void                  *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct READLINK4res              *res    = &req->res_compound.resarray[req->index].opreadlink;

    if (error_code == CHIMERA_VFS_OK) {
        res->status = NFS4_OK;
    } else {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
    }

    chimera_vfs_release(thread->vfs_thread, req->handle);

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_readlink_complete */

static void
chimera_nfs4_readlink_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request  *req = private_data;
    struct READLINK4res *res = &req->res_compound.resarray[req->index].opreadlink;

    req->handle = handle;

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    xdr_dbuf_alloc_space(res->resok4.link.data, 4096, req->msg->dbuf);
    res->resok4.link.len = 4096;

    chimera_vfs_readlink(req->thread->vfs_thread,
                         handle,
                         res->resok4.link.data,
                         res->resok4.link.len,
                         chimera_nfs4_readlink_complete,
                         req);
} /* chimera_nfs4_readlink_open_callback */

void
chimera_nfs4_readlink(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    chimera_vfs_open(thread->vfs_thread,
                     req->fh,
                     req->fhlen,
                     CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                     chimera_nfs4_readlink_open_callback,
                     req);
} /* chimera_nfs4_readlink */
