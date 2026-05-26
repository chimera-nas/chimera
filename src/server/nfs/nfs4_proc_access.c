// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_attr.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

static void
chimera_nfs4_access_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request *req    = private_data;
    struct ACCESS4args *args   = &req->args_compound->argarray[req->index].opaccess;
    struct ACCESS4res  *res    = &req->res_compound.resarray[req->index].opaccess;
    uint32_t            access = 0;
    uint32_t            meaningful, requested;

    chimera_vfs_release(req->thread->vfs_thread, req->handle);

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* RFC 7530 §16.1.4: ACCESS4_LOOKUP and ACCESS4_DELETE only have meaning
    * for a directory; ACCESS4_EXECUTE only for a non-directory. The server
    * reports in `supported` exactly the requested bits it evaluated, never
    * undefined bits or bits that are not meaningful for the object type. */
    meaningful = ACCESS4_READ | ACCESS4_MODIFY | ACCESS4_EXTEND;
    if (S_ISDIR(attr->va_mode)) {
        meaningful |= ACCESS4_LOOKUP | ACCESS4_DELETE;
    } else {
        meaningful |= ACCESS4_EXECUTE;
    }

    requested = args->access & meaningful;

    if ((requested & ACCESS4_READ) &&
        chimera_nfs4_cred_has_mode_access(attr, &req->cred, 1, 0, 0)) {
        access |= ACCESS4_READ;
    }

    if ((requested & ACCESS4_LOOKUP) &&
        chimera_nfs4_cred_has_mode_access(attr, &req->cred, 0, 0, 1)) {
        access |= ACCESS4_LOOKUP;
    }

    if ((requested & ACCESS4_MODIFY) &&
        chimera_nfs4_cred_has_mode_access(attr, &req->cred, 0, 1, 0)) {
        access |= ACCESS4_MODIFY;
    }

    if ((requested & ACCESS4_EXTEND) &&
        chimera_nfs4_cred_has_mode_access(attr, &req->cred, 0, 1, 0)) {
        access |= ACCESS4_EXTEND;
    }

    if ((requested & ACCESS4_DELETE) &&
        chimera_nfs4_cred_has_mode_access(attr, &req->cred, 0, 1, 0)) {
        access |= ACCESS4_DELETE;
    }

    if ((requested & ACCESS4_EXECUTE) &&
        chimera_nfs4_cred_has_mode_access(attr, &req->cred, 0, 0, 1)) {
        access |= ACCESS4_EXECUTE;
    }

    res->status           = NFS4_OK;
    res->resok4.supported = requested;
    res->resok4.access    = access;

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

    chimera_vfs_getattr(req->thread->vfs_thread, &req->cred,
                        handle,
                        CHIMERA_VFS_ATTR_MASK_STAT,
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
        /* The pseudo-root is a directory: report only the directory-meaningful
         * bits the client asked for, and grant them all. */
        uint32_t meaningful = ACCESS4_READ | ACCESS4_LOOKUP | ACCESS4_MODIFY |
            ACCESS4_EXTEND | ACCESS4_DELETE;

        res->status           = NFS4_OK;
        res->resok4.supported = args->access & meaningful;
        res->resok4.access    = args->access & meaningful;
        chimera_nfs4_compound_complete(req, NFS4_OK);
        return;
    }

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                        chimera_nfs4_access_open_callback,
                        req);
} /* chimera_nfs4_access */
