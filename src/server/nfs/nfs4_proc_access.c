// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_attr.h"
#include "nfs4_access.h"
#include "vfs/vfs_compound.h"
#include "vfs/sdk/vfs_acl.h"
#include "vfs/sdk/vfs_access.h"

/*
 * The ACCESS4_* bits this server will actually evaluate for `attr`/`fh`.
 *
 * The server reports in `supported` exactly the requested bits it evaluated,
 * never undefined bits or bits not meaningful for the object type (RFC 7530
 * sec 16.1.4 / RFC 8276 sec 8.4).  The xattr access bits exist only in
 * NFSv4.2, so they are meaningful only when the client negotiated
 * minorversion >= 2 AND the backend implements xattrs -- on 4.0/4.1 bit 0x40
 * is undefined and must be ignored.
 *
 * Split out from the fill because the caller needs it to build the ACE mask it
 * evaluates; the VFS-compound path passes `fh` for the object that op ran
 * against rather than req->fh.
 */
uint32_t
chimera_nfs4_access_requested(
    struct nfs_request             *req,
    const struct ACCESS4args       *args,
    const struct chimera_vfs_attrs *attr,
    const uint8_t                  *fh,
    int                             fhlen)
{
    uint32_t meaningful;

    meaningful = chimera_nfs4_access_meaningful(
        S_ISDIR(attr->va_mode),
        req->minorversion >= 2 &&
        chimera_nfs4_xattr_supported(req->thread->vfs_thread, fh, fhlen));

    return args->access & meaningful;
} /* chimera_nfs4_access_requested */

/*
 * Fill an ACCESS4 result from the evaluated request bits and the ACE bits the
 * central gate granted.  `granted` may cover more than `requested` asked for
 * (the VFS compound evaluates the client's whole request); the mapping back is
 * limited to `requested` either way.
 *
 * `attr` is the object's, and is needed for the execute rule below -- which
 * lives HERE rather than in either caller because both paths have to give the
 * same answer, and a rule applied on one of them would be the one thing this
 * conversion is not allowed to change.
 */
void
chimera_nfs4_access_fill(
    struct nfs_request             *req,
    struct ACCESS4res              *res,
    const struct chimera_vfs_attrs *attr,
    uint32_t                        requested,
    uint32_t                        granted)
{
    /* RFC 8881 18.1.4: the server SHOULD NOT set ACCESS4_EXECUTE unless an
     * execute bit is set.  A privileged caller's DAC override grants
     * ACE_EXECUTE on a file with no execute bit anywhere in its mode, which is
     * right for the OPEN that follows and wrong for the advisory answer -- so
     * withhold that ONE bit here rather than weakening the override.
     *
     * Restricted to non-directories on purpose: a directory's search
     * permission travels as ACCESS4_LOOKUP, which maps to the same ACE bit and
     * which the RFC does NOT qualify this way -- stripping it there would
     * refuse a privileged caller the traversal it really does have. */
    if (!S_ISDIR(attr->va_mode) &&
        !(attr->va_mode & (S_IXUSR | S_IXGRP | S_IXOTH))) {
        granted &= ~CHIMERA_ACE_EXECUTE;
    }

    res->status           = NFS4_OK;
    res->resok4.supported = requested;
    res->resok4.access    = chimera_nfs4_access_from_granted(requested, granted);

    /* A read-only export never grants write-class access, regardless of what
     * the ACL/mode would allow.  `supported` stays unmasked: the bits were
     * evaluated, just not granted (RFC 7530 sec 16.1). */
    if (chimera_nfs_export_id_is_ro(req->thread->shared, req->export_id)) {
        res->resok4.access &= ~(ACCESS4_MODIFY | ACCESS4_EXTEND |
                                ACCESS4_DELETE | ACCESS4_XAWRITE);
    }
} /* chimera_nfs4_access_fill */

/* PUTFH, OPEN_CURRENT, GETATTR: the stat is op 2 of the run. */
#define NFS4_ACCESS_OP_GETATTR 2

static void
chimera_nfs4_access_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request                   *req  = private_data;
    struct ACCESS4args                   *args = &req->args_compound->argarray[req->index].opaccess;
    struct ACCESS4res                    *res  = &req->res_compound.resarray[req->index].opaccess;
    const struct chimera_vfs_compound_op *gop;
    enum chimera_vfs_error                error_code;
    uint32_t                              requested, granted;

    error_code = chimera_vfs_compound_status(compound);

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_compound_free(compound);
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    gop = chimera_vfs_compound_op(compound, NFS4_ACCESS_OP_GETATTR);

    requested = chimera_nfs4_access_requested(req, args, &gop->attr,
                                              req->fh, req->fhlen);

    /* Evaluate the canonical ACL (or mode fallback) once via the shared gate,
     * then map the granted ACE bits back to the ACCESS4_* result bits.  The ACL
     * belongs to the compound, so both readings happen before the free. */
    granted = chimera_vfs_access_check(&gop->attr, &req->cred,
                                       chimera_nfs4_access4_to_mask(requested));

    chimera_nfs4_access_fill(req, res, &gop->attr, requested, granted);

    chimera_vfs_compound_free(compound);

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_access_complete */

void
chimera_nfs4_access(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct ACCESS4args          *args = &argop->opaccess;
    struct ACCESS4res           *res  = &resop->opaccess;
    struct chimera_vfs_compound *compound;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, NFS4ERR_NOFILEHANDLE);
        return;
    }

    if (fh_is_nfs4_root(req->fh, req->fhlen)) {
        /* The pseudo-root is a directory with no backing store (no xattrs):
         * report only the directory-meaningful bits the client asked for.
         * It is immutable, so the write-class bits are never granted. */
        uint32_t meaningful = chimera_nfs4_access_meaningful(1, 0);

        res->status           = NFS4_OK;
        res->resok4.supported = args->access & meaningful;
        res->resok4.access    = args->access & meaningful &
            ~(ACCESS4_MODIFY | ACCESS4_EXTEND | ACCESS4_DELETE);
        chimera_nfs4_compound_complete(req, NFS4_OK);
        return;
    }

    /* A PATH open, because the whole of this op is a stat: the object's mode and
     * its ACL, which this server evaluates itself.  Nothing is read or written
     * through the handle, and a data open of a FIFO blocks. */
    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_PATH, 0);
    chimera_vfs_compound_add_getattr(compound,
                                     CHIMERA_VFS_ATTR_MASK_STAT |
                                     CHIMERA_VFS_ATTR_ACL);

    chimera_vfs_compound_submit(compound, chimera_nfs4_access_complete, req);
} /* chimera_nfs4_access */
