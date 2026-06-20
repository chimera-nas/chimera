// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs.h"
#include "evpl/evpl_rpc2.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

/* RFC 7530 §16.31: SECINFO returns the security mechanisms the server will
 * accept for the named entry in the current-filehandle directory.  Chimera
 * advertises the owning export's configured flavors (or all supported flavors
 * if the export has no explicit policy).  The name lookup also produces the
 * spec-mandated error returns: NFS4ERR_NOTDIR (cfh not a directory) and
 * NFS4ERR_NOENT (name absent). */

static void
chimera_nfs4_secinfo_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct nfs_request              *req = private_data;
    struct SECINFO4res              *res = &req->res_compound.resarray[req->index].opsecinfo;
    const struct chimera_nfs_export *export;

    chimera_vfs_release(req->thread->vfs_thread, req->handle);

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* The looked-up name lives in the current FH's export. */
    export      = chimera_nfs_get_export_by_id(req->thread->shared, req->export_id);
    res->resok4 = xdr_dbuf_alloc_space(4 * sizeof(struct secinfo4), req->encoding->dbuf);
    chimera_nfs_abort_if(res->resok4 == NULL, "Failed to allocate space");

    res->num_resok4 = chimera_nfs_fill_secinfo(res->resok4,
                                               export ? export->sec_allowed : 0);
    res->status = NFS4_OK;
    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_secinfo_complete */

static void
chimera_nfs4_secinfo_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request  *req  = private_data;
    struct SECINFO4args *args = &req->args_compound->argarray[req->index].opsecinfo;
    struct SECINFO4res  *res  = &req->res_compound.resarray[req->index].opsecinfo;

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    req->handle = handle;

    chimera_vfs_lookup_at(req->thread->vfs_thread, &req->cred,
                          handle,
                          args->name.data,
                          args->name.len,
                          0,
                          0,
                          chimera_nfs4_secinfo_complete,
                          req);
} /* chimera_nfs4_secinfo_open_callback */

void
chimera_nfs4_secinfo(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct SECINFO4args *args = &argop->opsecinfo;
    struct SECINFO4res  *res  = &resop->opsecinfo;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* Reject empty / malformed component names (NFS4ERR_INVAL, etc.) before
     * touching the VFS. */
    res->status = chimera_nfs4_validate_name(&args->name);
    if (res->status != NFS4_OK) {
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* When the current FH is the NFSv4 pseudo-fs root, the name is an export
     * name (not a VFS object).  This is the path a client takes to renegotiate
     * after NFS4ERR_WRONGSEC at the export boundary, so resolve the export
     * directly and advertise its configured flavors. */
    if (fh_is_nfs4_root(req->fh, req->fhlen)) {
        const struct chimera_nfs_export *export    = NULL;
        char                            *full_path = NULL;

        if (chimera_nfs_find_export_path(thread->shared, args->name.data,
                                         args->name.len, &full_path, &export) != 0) {
            res->status = NFS4ERR_NOENT;
            chimera_nfs4_compound_complete(req, NFS4ERR_NOENT);
            return;
        }
        free(full_path);

        res->resok4 = xdr_dbuf_alloc_space(4 * sizeof(struct secinfo4),
                                           req->encoding->dbuf);
        chimera_nfs_abort_if(res->resok4 == NULL, "Failed to allocate space");
        res->num_resok4 = chimera_nfs_fill_secinfo(res->resok4,
                                                   export ? export->sec_allowed : 0);
        res->status = NFS4_OK;
        chimera_nfs4_compound_complete(req, NFS4_OK);
        return;
    }

    /* Opening the current FH as a directory yields NFS4ERR_NOTDIR when it is
     * not one; the subsequent lookup yields NFS4ERR_NOENT for a missing name. */
    chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_nfs4_secinfo_open_callback,
                        req);
} /* chimera_nfs4_secinfo */
