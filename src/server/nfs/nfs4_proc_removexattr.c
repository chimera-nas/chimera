// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "vfs/vfs_xattr_name.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

static void
chimera_nfs4_removexattr_complete(
    enum chimera_vfs_error          error_code,
    const struct chimera_vfs_attrs *pre_attr,
    const struct chimera_vfs_attrs *post_attr,
    void                           *private_data)
{
    struct nfs_request     *req = private_data;
    struct REMOVEXATTR4res *res = &req->res_compound.resarray[req->index].opremovexattr;

    if (error_code == CHIMERA_VFS_OK) {
        res->rxr_status      = NFS4_OK;
        res->rxr_info.atomic = 1;
        res->rxr_info.before = pre_attr->va_ctime.tv_sec * 1000000000ULL +
            pre_attr->va_ctime.tv_nsec;
        res->rxr_info.after = post_attr->va_ctime.tv_sec * 1000000000ULL +
            post_attr->va_ctime.tv_nsec;
    } else {
        res->rxr_status = chimera_nfs4_errno_to_nfsstat4(error_code);
    }

    chimera_vfs_release(req->thread->vfs_thread, req->handle);
    chimera_nfs4_compound_complete(req, res->rxr_status);
} /* chimera_nfs4_removexattr_complete */

static void
chimera_nfs4_removexattr_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request      *req  = private_data;
    struct REMOVEXATTR4args *args = &req->args_compound->argarray[req->index].opremovexattr;
    struct REMOVEXATTR4res  *res  = &req->res_compound.resarray[req->index].opremovexattr;
    uint32_t                 namecap;
    char                    *name;
    int                      namelen;

    if (error_code != CHIMERA_VFS_OK) {
        res->rxr_status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->rxr_status);
        return;
    }

    req->handle = handle;

    /* RFC 8276 carries the name without a namespace prefix; the VFS expects a
     * fully-qualified "user." name.  Stage the prefixed name in the response
     * dbuf, which outlives the async VFS op. */
    if (args->rxa_name.len == 0) {
        res->rxr_status = NFS4ERR_INVAL;
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        chimera_nfs4_compound_complete(req, res->rxr_status);
        return;
    }

    namecap = CHIMERA_VFS_XATTR_USER_PREFIX_LEN + args->rxa_name.len;
    name    = xdr_dbuf_alloc_space(namecap, req->encoding->dbuf);
    if (!name) {
        res->rxr_status = NFS4ERR_RESOURCE;
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        chimera_nfs4_compound_complete(req, res->rxr_status);
        return;
    }

    namelen = chimera_vfs_xattr_build_user(name, namecap,
                                           args->rxa_name.data,
                                           args->rxa_name.len);
    if (namelen < 0) {
        res->rxr_status = NFS4ERR_NAMETOOLONG;
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        chimera_nfs4_compound_complete(req, res->rxr_status);
        return;
    }

    chimera_vfs_remove_xattr(req->thread->vfs_thread, &req->cred,
                             handle,
                             name,
                             namelen,
                             chimera_nfs4_removexattr_complete,
                             req);
} /* chimera_nfs4_removexattr_open_callback */

void
chimera_nfs4_removexattr(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct REMOVEXATTR4res *res = &resop->opremovexattr;

    if (req->fhlen == 0) {
        res->rxr_status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->rxr_status);
        return;
    }

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred, NULL,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED,
                        chimera_nfs4_removexattr_open_callback,
                        req);
} /* chimera_nfs4_removexattr */
