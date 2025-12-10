// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "evpl/evpl_rpc2.h"

void
chimera_nfs4_secinfo_no_name(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct SECINFO4res   *res = &resop->opsecinfo_no_name;
    struct evpl_rpc2_msg *msg = req->msg;

    res->status     = NFS4_OK;
    res->num_resok4 = 1;

    res->resok4 = xdr_dbuf_alloc_space(sizeof(struct secinfo4), msg->dbuf);
    chimera_nfs_abort_if(res->resok4 == NULL, "Failed to allocate space");

    res->resok4[0].flavor = RPC_GSS_SVC_NONE;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_reclaim_complete */