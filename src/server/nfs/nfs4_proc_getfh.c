// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"

/*
 * Put `fh` on the wire as a GETFH4 result.  Shared by the per-op path below and
 * by the VFS-compound path, which hands over the handle the sequence had
 * current when the GETFH ran rather than req->fh.
 */
nfsstat4
chimera_nfs4_getfh_fill(
    struct nfs_request *req,
    struct GETFH4res   *res,
    const uint8_t      *fh,
    int                 fhlen)
{
    int            rc;
    uint8_t        wire[CHIMERA_NFS_FH_MAX];
    int            wirelen;
    const uint8_t *out;
    int            outlen;

    /* The pseudo-root handle is exempt from wrapping and returned verbatim so
     * a subsequent PUTFH recognizes it.  Every real handle is wrapped with its
     * export id (and signed) before going on the wire. */
    if (fh_is_nfs4_root(fh, fhlen)) {
        out    = fh;
        outlen = fhlen;
    } else {
        chimera_nfs_fh_encode(req, fh, fhlen, wire, &wirelen);
        out    = wire;
        outlen = wirelen;
    }

    rc = xdr_dbuf_opaque_copy(&res->resok4.object,
                              out,
                              outlen,
                              req->encoding->dbuf);

    if (rc) {
        return NFS4ERR_RESOURCE;
    }

    return NFS4_OK;
} /* chimera_nfs4_getfh_fill */

void
chimera_nfs4_getfh(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct GETFH4res *res = &resop->opgetfh;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    res->status = chimera_nfs4_getfh_fill(req, res, req->fh, req->fhlen);

    chimera_nfs4_compound_complete(req, res->status);
} /* chimera_nfs4_getfh */