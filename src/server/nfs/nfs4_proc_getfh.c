// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"

void
chimera_nfs4_getfh(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct GETFH4res *res = &resop->opgetfh;
    int               rc;
    uint8_t           wire[CHIMERA_NFS_FH_MAX];
    int               wirelen;
    const uint8_t    *out;
    int               outlen;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* The pseudo-root handle is exempt from wrapping and returned verbatim so
     * a subsequent PUTFH recognizes it.  Every real handle is wrapped with its
     * export id (and signed) before going on the wire. */
    if (fh_is_nfs4_root(req->fh, req->fhlen)) {
        out    = req->fh;
        outlen = req->fhlen;
    } else {
        chimera_nfs_fh_encode(req, req->fh, req->fhlen, wire, &wirelen);
        out    = wire;
        outlen = wirelen;
    }

    rc = xdr_dbuf_opaque_copy(&res->resok4.object,
                              out,
                              outlen,
                              req->encoding->dbuf);

    if (rc) {
        res->status = NFS4ERR_RESOURCE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    res->status = NFS4_OK;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_getfh */