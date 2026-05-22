// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_recovery.h"
#include "server/server.h"
#include "vfs/vfs_pnfs.h"
#include "nfs4_session.h"
#include "nfs4_state.h"
#include "evpl/evpl_rpc2.h"

/* Flag bits a client is permitted to set in eia_flags (RFC 8881 §18.35.3).
 * EXCHGID4_FLAG_CONFIRMED_R is result-only; any other bit is undefined. */
#define NFS4_EID_VALID_INPUT_FLAGS                                       \
        (EXCHGID4_FLAG_SUPP_MOVED_REFER | EXCHGID4_FLAG_SUPP_MOVED_MIGR |     \
         EXCHGID4_FLAG_SUPP_FENCE_OPS | EXCHGID4_FLAG_BIND_PRINC_STATEID |    \
         EXCHGID4_FLAG_USE_NON_PNFS | EXCHGID4_FLAG_USE_PNFS_MDS |            \
         EXCHGID4_FLAG_USE_PNFS_DS | EXCHGID4_FLAG_UPD_CONFIRMED_REC_A)

void
chimera_nfs4_exchange_id(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct EXCHANGE_ID4args       *args         = &argop->opexchange_id;
    struct EXCHANGE_ID4res        *res          = &resop->opexchange_id;
    uint64_t                       owner_major  = 42;
    uint64_t                       owner_minor  = 42;
    uint64_t                       server_scope = 42;
    struct timespec                now;
    struct nfs4_client_principal   principal;
    struct nfs4_exchange_id_result eid;
    bool                           update;
    int                            rc;

    clock_gettime(CLOCK_REALTIME, &now);

    /* RFC 8881 §18.35.3: when used outside a session (no preceding SEQUENCE),
     * EXCHANGE_ID must be the sole operation in its COMPOUND.  Inside a
     * session it may appear as an ordinary operation. */
    if (!req->seen_sequence && req->args_compound->num_argarray != 1) {
        res->eir_status = NFS4ERR_NOT_ONLY_OP;
        chimera_nfs4_compound_complete(req, res->eir_status);
        return;
    }

    /* The client impl-id is XDR array<1>; more than one element is malformed. */
    if (args->num_eia_client_impl_id > 1) {
        res->eir_status = NFS4ERR_BADXDR;
        chimera_nfs4_compound_complete(req, res->eir_status);
        return;
    }

    /* RFC 8881 §18.35.3: undefined flag bits (and the result-only
    * EXCHGID4_FLAG_CONFIRMED_R) must not be set by the client. */
    if (args->eia_flags & ~NFS4_EID_VALID_INPUT_FLAGS) {
        res->eir_status = NFS4ERR_INVAL;
        chimera_nfs4_compound_complete(req, res->eir_status);
        return;
    }

    update = (args->eia_flags & EXCHGID4_FLAG_UPD_CONFIRMED_REC_A) != 0;

    principal.flavor          = req->principal_flavor;
    principal.uid             = req->principal_uid;
    principal.gid             = req->principal_gid;
    principal.machinename     = req->principal_machinename;
    principal.machinename_len = req->principal_machinename_len;

    nfs4_client_exchange_id(
        &thread->shared->nfs4_shared_clients,
        args->eia_clientowner.co_ownerid.data,
        args->eia_clientowner.co_ownerid.len,
        *(uint64_t *) args->eia_clientowner.co_verifier,
        &principal,
        update,
        req->minorversion,
        &eid);

    /* A superseded record's state hierarchy is torn down outside the table
     * lock (see nfs4_client_exchange_id). */
    if (eid.destroy_unified) {
        nfs_client_destroy(eid.destroy_unified,
                           &thread->shared->nfs4_state_table,
                           thread->vfs_thread);
    }

    if (eid.status != NFS4_OK) {
        res->eir_status = eid.status;
        chimera_nfs4_compound_complete(req, res->eir_status);
        return;
    }

    /* Phase 5: 4.1+ EXCHANGE_ID is the moment of identity establishment;
     * stub-persist the unified client so a future stable-storage backend
     * can pick it up after a restart. */
    {
        struct nfs4_client *c  = NULL;
        struct nfs_client  *uc = NULL;
        pthread_mutex_lock(&thread->shared->nfs4_shared_clients.nfs4_ct_lock);
        HASH_FIND(nfs4_client_hh_by_id,
                  thread->shared->nfs4_shared_clients.nfs4_ct_clients_by_id,
                  &eid.clientid, sizeof(eid.clientid), c);
        if (c) {
            uc = c->unified;
        }
        pthread_mutex_unlock(&thread->shared->nfs4_shared_clients.nfs4_ct_lock);
        if (uc) {
            nfs_recovery_persist(&thread->shared->nfs4_recovery, uc);
        }
    }

    /* Advertise our pNFS role (RFC 8881 §13.1): a data server confirms
     * USE_PNFS_DS so the client will route layout I/O to it; a metadata server
     * advertises USE_PNFS_MDS; otherwise plain NFS. */
    uint32_t pnfs_flags;
    if (chimera_server_config_get_nfs_data_server(thread->shared->config)) {
        pnfs_flags = EXCHGID4_FLAG_USE_PNFS_DS;
    } else if (chimera_vfs_pnfs_enabled(thread->shared->vfs)) {
        pnfs_flags = EXCHGID4_FLAG_USE_PNFS_MDS;
    } else {
        pnfs_flags = EXCHGID4_FLAG_USE_NON_PNFS;
    }

    res->eir_status                = NFS4_OK;
    res->eir_resok4.eir_clientid   = eid.clientid;
    res->eir_resok4.eir_sequenceid = 1;
    res->eir_resok4.eir_flags      = pnfs_flags |
        (eid.confirmed ? EXCHGID4_FLAG_CONFIRMED_R : 0);
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