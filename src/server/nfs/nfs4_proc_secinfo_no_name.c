// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs_common.h"
#include "evpl/evpl_rpc2.h"

/*
 * SECINFO_NO_NAME (RFC 8881 §18.45) reports the security flavors the server
 * accepts for the current filehandle (SECINFO_STYLE4_CURRENT_FH) or for its
 * parent (SECINFO_STYLE4_PARENT).  We advertise the owning export's configured
 * flavors (or everything chimera supports if the export has no explicit
 * policy), as RPCSEC_GSS triples for krb5/krb5i/krb5p.
 *
 * The flavors are a property of the export, so a parent inside the same export
 * has the same answer as the current filehandle and needs no ".." resolution;
 * only the namespace root, which has no parent at all, is special-cased below.
 */
void
chimera_nfs4_secinfo_no_name(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct SECINFO4res              *res   = &resop->opsecinfo_no_name;
    uint32_t                         style = argop->opsecinfo_no_name;
    const struct chimera_nfs_export *export;

    /* RFC 8881 §18.45.3: the operation reports on the current filehandle, so
     * it needs one. */
    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* secinfo_style4 names exactly two values (RFC 8881 §18.45.1).  The XDR
     * decoder is built with relaxed enum checking, so an undeclared style
     * reaches here and has to be rejected rather than silently treated as
     * CURRENT_FH. */
    if (style != SECINFO_STYLE4_CURRENT_FH && style != SECINFO_STYLE4_PARENT) {
        res->status = NFS4ERR_INVAL;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* SECINFO_STYLE4_PARENT asks about "..", which the NFSv4 namespace root
     * does not have -- the same reason LOOKUPP answers NFS4ERR_NOENT there. */
    if (style == SECINFO_STYLE4_PARENT && fh_is_nfs4_root(req->fh, req->fhlen)) {
        res->status = NFS4ERR_NOENT;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    export = chimera_nfs_get_export_by_id(thread->shared, req->export_id);

    res->resok4 = xdr_dbuf_alloc_space(4 * sizeof(struct secinfo4),
                                       req->encoding->dbuf);
    chimera_nfs_abort_if(res->resok4 == NULL, "Failed to allocate space");

    res->num_resok4 = chimera_nfs_fill_secinfo(res->resok4,
                                               export ? export->sec_allowed : 0,
                                               thread->shared->gss_enabled);
    /* RFC 8881 §18.45.3: SECINFO_NO_NAME consumes the current filehandle on
     * success, so a following op relying on it fails with NFS4ERR_NOFILEHANDLE. */
    req->fhlen  = 0;
    res->status = NFS4_OK;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_secinfo_no_name */
