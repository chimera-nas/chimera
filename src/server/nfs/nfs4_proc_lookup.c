// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs.h"
#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
static void
chimera_nfs4_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct nfs_request *req    = private_data;
    nfsstat4            status = chimera_nfs4_errno_to_nfsstat4(error_code);
    struct LOOKUP4res  *res    = &req->res_compound.resarray[req->index].oplookup;

    res->status = status;

    if (error_code == CHIMERA_VFS_OK) {
        if (!(attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
            res->status = NFS4ERR_SERVERFAULT;
            status      = NFS4ERR_SERVERFAULT;
        } else {
            memcpy(req->fh, attr->va_fh, attr->va_fh_len);
            req->fhlen = attr->va_fh_len;
        }
    }

    chimera_vfs_release(req->thread->vfs_thread, req->handle);
    chimera_nfs4_compound_complete(req, status);
} /* chimera_nfs4_lookup_complete */

static void
chimera_nfs4_lookup_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request *req    = private_data;
    struct LOOKUP4args *args   = &req->args_compound->argarray[req->index].oplookup;
    nfsstat4            status = chimera_nfs4_errno_to_nfsstat4(error_code);
    struct LOOKUP4res  *res    = &req->res_compound.resarray[req->index].oplookup;

    if (error_code == CHIMERA_VFS_OK) {
        req->handle = handle;

        chimera_vfs_lookup_at(req->thread->vfs_thread, &req->cred, req->compound,
                              handle,
                              args->objname.data,
                              args->objname.len,
                              CHIMERA_VFS_ATTR_FH,
                              0,
                              chimera_nfs4_lookup_complete,
                              req);
    } else {
        res->status = status;
        chimera_nfs4_compound_complete(req, status);
    }
} /* chimera_nfs4_lookup_open_callback */

static void
chimera_nfs4_lookup_resume(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    int                               at_root_export)
{
    struct LOOKUP4args       *args =
        &req->args_compound->argarray[req->index].oplookup;
    struct chimera_nfs_export sibling;

    /* At the "/" export's root, sibling exports are grafted over the real
     * directory as junctions: a name matching a sibling export enters that
     * export (shadowing any real entry of the same name), exactly as the
     * synthetic pseudo-root routes into exports. */
    if (at_root_export &&
        chimera_nfs_get_export_by_component(thread->shared,
                                            args->objname.data,
                                            args->objname.len,
                                            &sibling) == 0) {
        nfs4_root_lookup_export(thread, req, &sibling, sibling.path);
        return;
    }

    // For non-root lookups, we can just open the directory and let the VFS handle the lookup
    chimera_vfs_open_fh(thread->vfs_thread, &req->cred, req->compound,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_nfs4_lookup_open_callback,
                        req);
} /* chimera_nfs4_lookup_resume */

void
chimera_nfs4_lookup(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct LOOKUP4args *args = &argop->oplookup;
    struct LOOKUP4res  *res  = &resop->oplookup;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    res->status = chimera_nfs4_validate_name(&args->objname);

    if (res->status != NFS4_OK) {
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    if (fh_is_nfs4_root(req->fh, req->fhlen)) {
        nfs4_root_lookup(thread, req);
        return;
    }

    nfs4_root_junction_check(thread, req, chimera_nfs4_lookup_resume);
} /* chimera_nfs4_lookup */
