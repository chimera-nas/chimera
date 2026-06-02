// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_attr.h"
#include "nfs4_access.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_acl.h"
#include "vfs/vfs_access.h"

static void
chimera_nfs4_access_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request *req  = private_data;
    struct ACCESS4args *args = &req->args_compound->argarray[req->index].opaccess;
    struct ACCESS4res  *res  = &req->res_compound.resarray[req->index].opaccess;
    uint32_t            meaningful, requested, granted;

    chimera_vfs_release(req->thread->vfs_thread, req->handle);

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* The server reports in `supported` exactly the requested bits it
     * evaluated, never undefined bits or bits not meaningful for the object
     * type (RFC 7530 sec 16.1.4 / RFC 8276 sec 8.4).  The xattr access bits
     * exist only in NFSv4.2, so they are meaningful only when the client
     * negotiated minorversion >= 2 AND the backend implements xattrs -- on 4.0/
     * 4.1 bit 0x40 is undefined and must be ignored. */
    meaningful = chimera_nfs4_access_meaningful(
        S_ISDIR(attr->va_mode),
        req->minorversion >= 2 &&
        chimera_nfs4_xattr_supported(req->thread->vfs_thread,
                                     req->fh, req->fhlen));

    requested = args->access & meaningful;

    /* Evaluate the canonical ACL (or mode fallback) once via the shared gate,
     * then map the granted ACE bits back to the ACCESS4_* result bits. */
    granted = chimera_vfs_access_check(attr, &req->cred,
                                       chimera_nfs4_access4_to_mask(requested));

    res->status           = NFS4_OK;
    res->resok4.supported = requested;
    res->resok4.access    = chimera_nfs4_access_from_granted(requested, granted);

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_access_complete */

static void
chimera_nfs4_access_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request *req = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs4_compound_complete(req, chimera_nfs4_errno_to_nfsstat4(error_code));
        return;
    }

    req->handle = handle;

    chimera_vfs_getattr(req->thread->vfs_thread, &req->cred, NULL,
                        handle,
                        CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL,
                        chimera_nfs4_access_complete,
                        req);
} /* chimera_nfs4_access_open_callback */

void
chimera_nfs4_access(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct ACCESS4args *args = &argop->opaccess;
    struct ACCESS4res  *res  = &resop->opaccess;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, NFS4ERR_NOFILEHANDLE);
        return;
    }

    if (fh_is_nfs4_root(req->fh, req->fhlen)) {
        /* The pseudo-root is a directory with no backing store (no xattrs):
         * report only the directory-meaningful bits the client asked for, and
         * grant them all. */
        uint32_t meaningful = chimera_nfs4_access_meaningful(1, 0);

        res->status           = NFS4_OK;
        res->resok4.supported = args->access & meaningful;
        res->resok4.access    = args->access & meaningful;
        chimera_nfs4_compound_complete(req, NFS4_OK);
        return;
    }

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred, NULL,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                        chimera_nfs4_access_open_callback,
                        req);
} /* chimera_nfs4_access */
