// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "evpl/evpl_rpc2.h"

void
chimera_nfs4_exchange_id(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct EXCHANGE_ID4args *args = &argop->opexchange_id;
    struct EXCHANGE_ID4res  *res  = &resop->opexchange_id;
    uint32_t                 client_id;
    uint64_t                 owner_major  = 42;
    uint64_t                 owner_minor  = 42;
    uint64_t                 server_scope = 42;
    struct timespec          now;
    int                      rc;

    clock_gettime(CLOCK_REALTIME, &now);

    client_id = nfs4_client_register(
        &thread->shared->nfs4_shared_clients,
        args->eia_clientowner.co_ownerid.data,
        args->eia_clientowner.co_ownerid.len,
        *(uint64_t *) args->eia_clientowner.co_verifier,
        40,
        NULL, NULL);

    res->eir_status                           = NFS4_OK;
    res->eir_resok4.eir_clientid              = client_id;
    res->eir_resok4.eir_sequenceid            = 1;
    res->eir_resok4.eir_flags                 = EXCHGID4_FLAG_USE_NON_PNFS;
    res->eir_resok4.eir_state_protect.spr_how = SP4_NONE;
    res->eir_resok4.num_eir_server_impl_id    = 1;

    res->eir_resok4.eir_server_impl_id = xdr_dbuf_alloc_space(sizeof(struct nfs_impl_id4), req->encoding->dbuf);
    chimera_nfs_abort_if(res->eir_resok4.eir_server_impl_id == NULL, "Failed to allocate space");

    rc = xdr_dbuf_opaque_copy(&res->eir_resok4.eir_server_impl_id[0].nii_domain,
                              "chimera.org", sizeof("chimera.org"), req->encoding->dbuf);

    chimera_nfs_abort_if(rc, "Failed to copy opaque");

    rc = xdr_dbuf_opaque_copy(&res->eir_resok4.eir_server_impl_id[0].nii_name,
                              "chimera", sizeof("chimera"), req->encoding->dbuf);

    chimera_nfs_abort_if(rc, "Failed to allocate string");

    res->eir_resok4.eir_server_impl_id[0].nii_date.seconds  = now.tv_sec;
    res->eir_resok4.eir_server_impl_id[0].nii_date.nseconds = now.tv_nsec;

    res->eir_resok4.eir_server_owner.so_major_id.len  = sizeof(owner_major);
    res->eir_resok4.eir_server_owner.so_major_id.data = xdr_dbuf_alloc_space(res->eir_resok4.eir_server_owner.
                                                                             so_major_id.len, req->encoding->dbuf);
    chimera_nfs_abort_if(res->eir_resok4.eir_server_owner.so_major_id.data == NULL, "Failed to allocate space");
    memcpy(res->eir_resok4.eir_server_owner.so_major_id.data, &owner_major, res->eir_resok4.eir_server_owner.so_major_id
           .len);

    res->eir_resok4.eir_server_scope.len  = sizeof(server_scope);
    res->eir_resok4.eir_server_scope.data = xdr_dbuf_alloc_space(res->eir_resok4.eir_server_scope.len, req->encoding->
                                                                 dbuf);
    chimera_nfs_abort_if(res->eir_resok4.eir_server_scope.data == NULL, "Failed to allocate space");
    memcpy(res->eir_resok4.eir_server_scope.data, &server_scope, res->eir_resok4.eir_server_scope.len);

    res->eir_resok4.eir_server_owner.so_minor_id = owner_minor;


    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_setclientid */