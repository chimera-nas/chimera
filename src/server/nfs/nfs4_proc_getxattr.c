// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_xattr.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

static void
chimera_nfs4_getxattr_complete(
    enum chimera_vfs_error error_code,
    uint32_t               value_len,
    void                  *private_data)
{
    struct nfs_request  *req = private_data;
    struct GETXATTR4res *res = &req->res_compound.resarray[req->index].opgetxattr;

    if (error_code == CHIMERA_VFS_OK) {
        res->gxr_status    = NFS4_OK;
        res->gxr_value.len = value_len;
        /* gxr_value.data already points at the dbuf-backed buffer. */
    } else {
        res->gxr_status = chimera_nfs4_errno_to_nfsstat4(error_code);
    }

    chimera_vfs_release(req->thread->vfs_thread, req->handle);
    chimera_nfs4_compound_complete(req, res->gxr_status);
} /* chimera_nfs4_getxattr_complete */

static void
chimera_nfs4_getxattr_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request   *req  = private_data;
    struct GETXATTR4args *args = &req->args_compound->argarray[req->index].opgetxattr;
    struct GETXATTR4res  *res  = &req->res_compound.resarray[req->index].opgetxattr;
    uint32_t              avail, maxval, namecap;
    char                 *name;
    int                   namelen, rc;

    if (error_code != CHIMERA_VFS_OK) {
        res->gxr_status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->gxr_status);
        return;
    }

    req->handle = handle;

    /* RFC 8276 carries the name without a namespace prefix; the VFS expects a
     * fully-qualified "user." name.  Stage the prefixed name in the response
     * dbuf, which outlives the async VFS op. */
    if (args->gxa_name.len == 0) {
        res->gxr_status = NFS4ERR_INVAL;
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        chimera_nfs4_compound_complete(req, res->gxr_status);
        return;
    }

    namecap = CHIMERA_NFS4_XATTR_USER_PREFIX_LEN + args->gxa_name.len;
    name    = xdr_dbuf_alloc_space(namecap, req->encoding->dbuf);
    if (!name) {
        res->gxr_status = NFS4ERR_RESOURCE;
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        chimera_nfs4_compound_complete(req, res->gxr_status);
        return;
    }

    namelen = chimera_nfs4_xattr_build_user(name, namecap,
                                            args->gxa_name.data,
                                            args->gxa_name.len);
    if (namelen < 0) {
        res->gxr_status = NFS4ERR_NAMETOOLONG;
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        chimera_nfs4_compound_complete(req, res->gxr_status);
        return;
    }

    /* Stage the value into the response dbuf, leaving headroom for the rest
     * of the compound reply. */
    avail  = req->encoding->dbuf->size - req->encoding->dbuf->used;
    maxval = avail > 8192 ? avail - 8192 : 0;
    if (maxval > 65536) {
        maxval = 65536;
    }

    rc = xdr_dbuf_alloc_opaque(&res->gxr_value, maxval, req->encoding->dbuf);
    if (rc) {
        res->gxr_status = NFS4ERR_RESOURCE;
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        chimera_nfs4_compound_complete(req, res->gxr_status);
        return;
    }

    chimera_vfs_get_xattr(req->thread->vfs_thread, &req->cred,
                          handle,
                          name,
                          namelen,
                          res->gxr_value.data,
                          maxval,
                          chimera_nfs4_getxattr_complete,
                          req);
} /* chimera_nfs4_getxattr_open_callback */

void
chimera_nfs4_getxattr(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct GETXATTR4res *res = &resop->opgetxattr;

    if (req->fhlen == 0) {
        res->gxr_status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->gxr_status);
        return;
    }

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED,
                        chimera_nfs4_getxattr_open_callback,
                        req);
} /* chimera_nfs4_getxattr */
