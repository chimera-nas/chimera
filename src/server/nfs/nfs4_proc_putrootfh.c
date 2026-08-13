// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs.h"
#include "nfs4_procs.h"

/*
 * PUTROOTFH (and PUTPUBFH, a synonym here) install the root of the NFSv4
 * namespace as the current filehandle.
 *
 * When a "/" export is configured, the namespace root is that export's real
 * backend directory (the NFS-Ganesha Pseudo="/" model, and the moral
 * equivalent of knfsd's fsid=0 root): the export's path is resolved to its
 * backend FH so every subsequent op in the compound -- GETATTR, READDIR,
 * OPEN, CREATE -- runs against the real directory with no pseudo-fs special
 * cases.  Sibling exports remain reachable as junctions grafted over that
 * root at LOOKUP (nfs4_root_junction_check).
 *
 * With no "/" export the namespace root is the synthetic pseudo-root listing
 * the exports, exactly as before.
 */

/* PUTROOTFH and PUTPUBFH share this file's completion paths; the result
 * unions differ only in which member carries the status. */
static nfsstat4 *
chimera_nfs4_putrootfh_status(struct nfs_request *req)
{
    struct nfs_resop4 *resop = &req->res_compound.resarray[req->index];

    return resop->resop == OP_PUTPUBFH ?
           &resop->opputpubfh.status : &resop->opputrootfh.status;
} /* chimera_nfs4_putrootfh_status */

/*
 * RFC 5661 §2.6.3.1.1 forbids returning NFS4ERR_WRONGSEC from PUTROOTFH
 * itself when the next op can carry the renegotiation (SECINFO /
 * SECINFO_NO_NAME), and a 4.0 client has no way to recover from it at the
 * root at all.  Mirror knfsd's need_wrongsec_check(): peek at the next op in
 * the compound and let the FH-installing op succeed when that op handles
 * WRONGSEC itself (or replaces the FH, making the violation moot); otherwise
 * the flavor violation surfaces here, before any FH is installed.  A
 * trailing PUTROOTFH with no next op installs an FH nothing will use.
 */
static int
chimera_nfs4_next_op_handles_wrongsec(struct nfs_request *req)
{
    if ((uint32_t) (req->index + 1) >= req->args_compound->num_argarray) {
        return 1;
    }

    switch (req->args_compound->argarray[req->index + 1].argop) {
        case OP_SECINFO:
        case OP_SECINFO_NO_NAME:
        case OP_PUTFH:
        case OP_PUTROOTFH:
        case OP_PUTPUBFH:
        case OP_RESTOREFH:
            return 1;
        default:
            return 0;
    } /* switch */
} /* chimera_nfs4_next_op_handles_wrongsec */

static void
chimera_nfs4_putrootfh_fh_ready(
    enum chimera_vfs_error            error_code,
    const uint8_t                    *fh,
    uint32_t                          fh_len,
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req)
{
    nfsstat4 *status = chimera_nfs4_putrootfh_status(req);

    (void) thread;

    if (error_code != CHIMERA_VFS_OK || fh == NULL) {
        /* The "/" export exists but its backing path did not resolve
         * (misconfigured path, backend unavailable).  Fail the op rather
         * than fall back to the synthetic pseudo-root: a "successful" mount
         * of an empty fake root would silently hide the export's contents. */
        chimera_nfs_error("PUTROOTFH: failed to resolve the \"/\" export's "
                          "backing path: error %d", error_code);
        *status = NFS4ERR_SERVERFAULT;
        chimera_nfs4_compound_complete(req, *status);
        return;
    }

    memcpy(req->fh, fh, fh_len);
    req->fhlen = fh_len;

    *status = NFS4_OK;
    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_putrootfh_fh_ready */

void
chimera_nfs4_putrootfh_common(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req)
{
    struct chimera_nfs_export root_export;
    nfsstat4                 *status = chimera_nfs4_putrootfh_status(req);
    uint32_t                  fhlen;

    if (chimera_nfs_get_export_copy(thread->shared, "/", &root_export) == 0) {

        if (!chimera_nfs_export_sec_ok(&root_export, req->sec_bit) &&
            !chimera_nfs4_next_op_handles_wrongsec(req)) {
            *status = NFS4ERR_WRONGSEC;
            chimera_nfs4_compound_complete(req, *status);
            return;
        }

        /* The namespace root is inside the export: adopt its id (handles
        * minted for the client carry it) and apply its squash policy. */
        chimera_nfs_set_export(req, &root_export);

        nfs4_root_export_fh_resolve(thread, req,
                                    chimera_nfs4_putrootfh_fh_ready);
        return;
    }

    /* No "/" export -- the namespace root is the synthetic pseudo-root. */
    nfs4_root_get_fh(req->fh, &fhlen);
    req->fhlen     = fhlen;
    req->export_id = 0;          /* pseudo root: no export, no squash */
    req->cred      = req->orig_cred;

    *status = NFS4_OK;
    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_putrootfh_common */

void
chimera_nfs4_putrootfh(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    (void) argop;
    (void) resop;

    chimera_nfs4_putrootfh_common(thread, req);
} /* chimera_nfs4_putrootfh */
