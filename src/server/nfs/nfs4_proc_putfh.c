// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

static void
chimera_nfs4_putfh_getattr_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request             *req    = private_data;
    struct PUTFH4args              *args   = &req->args_compound->argarray[req->index].opputfh;
    struct PUTFH4res               *res    = &req->res_compound.resarray[req->index].opputfh;
    struct chimera_vfs_open_handle *handle = req->handle;

    req->handle = NULL;

    if (error_code != CHIMERA_VFS_OK) {
        res->status = (error_code == CHIMERA_VFS_ENOENT ||
                       error_code == CHIMERA_VFS_ESTALE) ?
            NFS4ERR_STALE : chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_vfs_release(req->thread->vfs_thread, handle);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    chimera_vfs_release(req->thread->vfs_thread, handle);

    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_NLINK) &&
        attr->va_nlink == 0) {
        res->status = NFS4ERR_STALE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    memcpy(req->fh, args->object.data, args->object.len);
    req->fhlen = args->object.len;

    res->status = NFS4_OK;
    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_putfh_getattr_complete */

static void
chimera_nfs4_putfh_validate_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request *req = private_data;
    struct PUTFH4res   *res = &req->res_compound.resarray[req->index].opputfh;

    if (error_code != CHIMERA_VFS_OK) {
        res->status = (error_code == CHIMERA_VFS_ENOENT ||
                       error_code == CHIMERA_VFS_ESTALE) ?
            NFS4ERR_STALE : chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    req->handle = handle;
    chimera_vfs_getattr(req->thread->vfs_thread, &req->cred,
                        handle,
                        CHIMERA_VFS_ATTR_NLINK,
                        chimera_nfs4_putfh_getattr_complete,
                        req);
} /* chimera_nfs4_putfh_validate_complete */

void
chimera_nfs4_putfh(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct PUTFH4args *args = &argop->opputfh;
    struct  PUTFH4res *res  = &resop->opputfh;

    /* RFC 7530 §16.20.5: a structurally invalid handle (wrong length, unknown
     * mount) is NFS4ERR_BADHANDLE. Existence of the target is not checked here
     * — a well-formed but deleted handle surfaces NFS4ERR_STALE on the
     * operation that dereferences it.
     *
     * The synthetic NFSv4 pseudo-root handle is not a VFS mount handle (it is
     * resolved by the server's fh_is_nfs4_root fast path), so accept it
     * explicitly; the kernel client PUTFHs it during mount. */
    if (!fh_is_nfs4_root(args->object.data, args->object.len) &&
        (args->object.len > NFS4_FHSIZE ||
         !chimera_vfs_fh_is_plausible(thread->vfs_thread,
                                      args->object.data,
                                      args->object.len))) {
        res->status = NFS4ERR_BADHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    res->status = NFS4_OK;

    if (!fh_is_nfs4_root(args->object.data, args->object.len)) {
        chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                            args->object.data,
                            args->object.len,
                            CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                            chimera_nfs4_putfh_validate_complete,
                            req);
        return;
    }

    memcpy(req->fh, args->object.data, args->object.len);
    req->fhlen = args->object.len;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_putfh */
