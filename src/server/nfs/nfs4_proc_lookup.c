// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs.h"
#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_named_attr.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_release.h"

/* LOOKUP of a name inside a named-attribute directory resolves to the named
 * stream of that name on the base file.  OPEN_STREAM without a create is the
 * lightest way to obtain the stream's file handle, and the base it acts on is
 * the run's current object -- PATH-opened, because only the stream's identity
 * is wanted of it.  Both handles belong to the sequence and go with it: the
 * current fh is stateless until a subsequent OPEN. */
#define NFS4_ATTRDIR_LOOKUP_OP_STREAM 2

static void
chimera_nfs4_lookup_attrdir_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request                   *req = private_data;
    struct LOOKUP4res                    *res =
        &req->res_compound.resarray[req->index].oplookup;
    const struct chimera_vfs_compound_op *sop;
    enum chimera_vfs_error                error_code;

    error_code = chimera_vfs_compound_status(compound);

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_compound_free(compound);
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    sop = chimera_vfs_compound_op(compound, NFS4_ATTRDIR_LOOKUP_OP_STREAM);

    if (!sop->fh_len) {
        chimera_vfs_compound_free(compound);
        res->status = NFS4ERR_SERVERFAULT;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    memcpy(req->fh, sop->fh, sop->fh_len);
    req->fhlen = (int) sop->fh_len;

    chimera_vfs_compound_free(compound);

    res->status = NFS4_OK;
    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_lookup_attrdir_complete */

static void
chimera_nfs4_lookup_attrdir(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req)
{
    struct LOOKUP4args          *args =
        &req->args_compound->argarray[req->index].oplookup;
    struct chimera_vfs_compound *compound;
    const uint8_t               *base;
    int                          base_len;

    chimera_nfs4_attrdir_base(req->fh, req->fhlen, &base, &base_len);

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, base, base_len);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_PATH, 0);
    chimera_vfs_compound_add_open_stream(compound,
                                         (const char *) args->objname.data,
                                         (int) args->objname.len,
                                         0 /* no create: a plain lookup */,
                                         NULL, 0);

    chimera_vfs_compound_submit(compound, chimera_nfs4_lookup_attrdir_complete,
                                req);
} /* chimera_nfs4_lookup_attrdir */


/* PUTFH, OPEN_CURRENT, LOOKUP: the resolve is op 2 of the run. */
#define NFS4_LOOKUP_OP_LOOKUP 2

static void
chimera_nfs4_lookup_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request                   *req = private_data;
    struct LOOKUP4res                    *res = &req->res_compound.resarray[req->index].oplookup;
    const struct chimera_vfs_compound_op *lop;
    enum chimera_vfs_error                error_code;
    nfsstat4                              status;

    error_code = chimera_vfs_compound_status(compound);
    status     = chimera_nfs4_errno_to_nfsstat4(error_code);

    if (error_code == CHIMERA_VFS_OK) {
        lop = chimera_vfs_compound_op(compound, NFS4_LOOKUP_OP_LOOKUP);

        if (!(lop->attr.va_set_mask & CHIMERA_VFS_ATTR_FH)) {
            status = NFS4ERR_SERVERFAULT;
        } else {
            memcpy(req->fh, lop->attr.va_fh, lop->attr.va_fh_len);
            req->fhlen = lop->attr.va_fh_len;
        }
    }

    chimera_vfs_compound_free(compound);

    res->status = status;
    chimera_nfs4_compound_complete(req, status);
} /* chimera_nfs4_lookup_complete */

static void
chimera_nfs4_lookup_resume(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    int                               at_root_export)
{
    struct LOOKUP4args          *args =
        &req->args_compound->argarray[req->index].oplookup;
    struct chimera_nfs_export    sibling;
    struct chimera_vfs_compound *compound;

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

    /* For non-root lookups the directory is opened and the VFS resolves the
     * name in it -- which is the run PUTFH, OPEN_CURRENT, LOOKUP. */
    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_PATH |
                                          CHIMERA_VFS_OPEN_DIRECTORY, 0);
    chimera_vfs_compound_add_lookup(compound,
                                    (const char *) args->objname.data,
                                    (int) args->objname.len,
                                    CHIMERA_VFS_ATTR_FH, 0);

    chimera_vfs_compound_submit(compound, chimera_nfs4_lookup_complete, req);
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

    /* LOOKUP inside a named-attribute directory: open the base file and resolve
     * the named stream of this name. */
    if (chimera_nfs4_fh_is_attrdir(req->fh, req->fhlen)) {
        chimera_nfs4_lookup_attrdir(thread, req);
        return;
    }

    nfs4_root_junction_check(thread, req, chimera_nfs4_lookup_resume);
} /* chimera_nfs4_lookup */
