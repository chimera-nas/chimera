// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs_common.h"
#include "evpl/evpl_rpc2.h"

/*
 * SECINFO_NO_NAME (RFC 8881 §18.45) reports the security flavors the server
 * accepts for the current filehandle.  We advertise the owning export's
 * configured flavors (or everything chimera supports if the export has no
 * explicit policy), as RPCSEC_GSS triples for krb5/krb5i/krb5p.
 */
void
chimera_nfs4_secinfo_no_name(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct SECINFO4res              *res = &resop->opsecinfo_no_name;
    const struct chimera_nfs_export *export;

    export = chimera_nfs_get_export_by_id(thread->shared, req->export_id);

    res->resok4 = xdr_dbuf_alloc_space(4 * sizeof(struct secinfo4),
                                       req->encoding->dbuf);
    chimera_nfs_abort_if(res->resok4 == NULL, "Failed to allocate space");

    res->num_resok4 = chimera_nfs_fill_secinfo(res->resok4,
                                               export ? export->sec_allowed : 0);
    res->status = NFS4_OK;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_secinfo_no_name */
