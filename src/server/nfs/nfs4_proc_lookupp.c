// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

static void
chimera_nfs4_lookupp_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct nfs_request *req    = private_data;
    nfsstat4            status = chimera_nfs4_errno_to_nfsstat4(error_code);
    struct LOOKUPP4res *res    = &req->res_compound.resarray[req->index].oplookupp;

    if (error_code == CHIMERA_VFS_OK) {
        if (!(attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
            status = NFS4ERR_SERVERFAULT;
        } else {
            memcpy(req->fh, attr->va_fh, attr->va_fh_len);
            req->fhlen = attr->va_fh_len;
        }
    }

    res->status = status;
    chimera_vfs_release(req->thread->vfs_thread, req->handle);
    chimera_nfs4_compound_complete(req, status);
} /* chimera_nfs4_lookupp_complete */

static void
chimera_nfs4_lookupp_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request *req    = private_data;
    nfsstat4            status = chimera_nfs4_errno_to_nfsstat4(error_code);
    struct LOOKUPP4res *res    = &req->res_compound.resarray[req->index].oplookupp;

    if (error_code == CHIMERA_VFS_OK) {
        req->handle = handle;

        /*
         * Resolve parent via lookup_at(handle, "..").  Backends that
         * understand directory structure (memfs, cairn, demofs) handle
         * ".." natively; pass-through backends (linux, io_uring) get
         * parent attrs from fstatat(parent_fd, "..").
         */
        chimera_vfs_lookup_at(req->thread->vfs_thread, &req->cred,
                              handle,
                              "..",
                              2,
                              CHIMERA_VFS_ATTR_FH,
                              0,
                              chimera_nfs4_lookupp_complete,
                              req);
    } else {
        res->status = status;
        chimera_nfs4_compound_complete(req, status);
    }
} /* chimera_nfs4_lookupp_open_callback */

void
chimera_nfs4_lookupp(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct LOOKUPP4res *res = &resop->oplookupp;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /*
     * LOOKUPP at the namespace root returns the root again (RFC 7530
     * 14.2.16).  No need to descend into VFS for the synthetic NFS4 root.
     */
    if (fh_is_nfs4_root(req->fh, req->fhlen)) {
        res->status = NFS4_OK;
        chimera_nfs4_compound_complete(req, NFS4_OK);
        return;
    }

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_nfs4_lookupp_open_callback,
                        req);
} /* chimera_nfs4_lookupp */
