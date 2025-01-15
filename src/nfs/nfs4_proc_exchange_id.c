#include "nfs4_procs.h"
#include "rpc2/rpc2.h"

void
chimera_nfs4_exchange_id(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct evpl_rpc2_msg    *msg  = req->msg;
    struct EXCHANGE_ID4args *args = &argop->opexchange_id;
    struct EXCHANGE_ID4res  *res  = &resop->opexchange_id;
    uint32_t                 client_id;
    uint64_t                 owner_major  = 42;
    uint64_t                 owner_minor  = 42;
    uint64_t                 server_scope = 42;
    struct timespec          now;

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
    res->eir_resok4.eir_sequenceid            = 0;
    res->eir_resok4.eir_flags                 = 0;
    res->eir_resok4.eir_state_protect.spr_how = SP4_NONE;
    res->eir_resok4.num_eir_server_impl_id    = 1;

    xdr_dbuf_memcpy(&res->eir_resok4.eir_server_impl_id[0].nii_domain, "chimera.org", sizeof("chimera.org"), msg->dbuf);
    xdr_dbuf_memcpy(&res->eir_resok4.eir_server_impl_id[0].nii_name, "chimera", sizeof("chimera"), msg->dbuf);

    res->eir_resok4.eir_server_impl_id[0].nii_date.seconds  = now.tv_sec;
    res->eir_resok4.eir_server_impl_id[0].nii_date.nseconds = now.tv_nsec;

    xdr_dbuf_memcpy(&res->eir_resok4.eir_server_owner.so_major_id, &owner_major, sizeof(owner_major), msg->dbuf);

    xdr_dbuf_memcpy(&res->eir_resok4.eir_server_scope, &server_scope, sizeof(server_scope), msg->dbuf);

    res->eir_resok4.eir_server_owner.so_minor_id = owner_minor;


    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_setclientid */