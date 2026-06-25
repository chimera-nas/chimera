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
         EXCHGID4_FLAG_BIND_PRINC_STATEID | EXCHGID4_FLAG_USE_NON_PNFS |      \
         EXCHGID4_FLAG_USE_PNFS_MDS | EXCHGID4_FLAG_USE_PNFS_DS |             \
         EXCHGID4_FLAG_UPD_CONFIRMED_REC_A)

/* State-management operations chimera enforces under SP4_MACH_CRED, as a
 * bitmap4 (RFC 8881 §2.10.8.3): BIND_CONN_TO_SESSION(41), EXCHANGE_ID(42),
 * CREATE_SESSION(43), DESTROY_SESSION(44), DESTROY_CLIENTID(57).  Bit op N is
 * word[N/32] bit (N%32); ops 41-57 all fall in word 1. */
#define NFS4_MACH_ENFORCE_WORD1                                                 \
        ((1u << (41 - 32)) | (1u << (42 - 32)) | (1u << (43 - 32)) |            \
         (1u << (44 - 32)) | (1u << (57 - 32)))

/*
 * Build the EXCHANGE_ID reply's state-protection result (RFC 8881 §18.35.3)
 * and return the negotiated mode (stored on the client record for per-op
 * enforcement).  SP4_MACH_CRED is honored when requested: the server echoes
 * the operations it will enforce against the machine credential.
 *
 * SP4_SSV is declined -- we fall back to SP4_NONE.  Although the wire encoding
 * could be negotiated, chimera does not actually enforce the SSV-derived
 * RPCSEC_GSS credential on the spo_must_enforce state-management operations,
 * SET_SSV does not compute the ssr_digest proof-of-knowledge, and
 * nfs4_client_mach_cred_ok never checks an SSV credential (RFC 8881
 * §2.10.8.3 / §18.47.3).  Advertising SP4_SSV would therefore be both a
 * protocol lie (claiming a capability we cannot back) and a security
 * overstatement (a client would believe its session-management operations are
 * cryptographically bound to a shared secret when they are not).  Until real
 * SSV enforcement is implemented we decline it.  Any case we cannot satisfy
 * falls back to SP4_NONE.
 */
static uint32_t
nfs4_set_state_protect(
    const struct state_protect4_a *req_sp,
    struct state_protect4_r       *res_sp,
    xdr_dbuf                      *dbuf)
{
    if (req_sp->spa_how == SP4_MACH_CRED) {
        static const uint32_t      enforce[2] = { 0, NFS4_MACH_ENFORCE_WORD1 };
        struct state_protect_ops4 *ops        = &res_sp->spr_mach_ops;

        chimera_nfs_debug("EXCHANGE_ID: negotiated SP4_MACH_CRED state protection");
        res_sp->spr_how           = SP4_MACH_CRED;
        ops->num_spo_must_enforce = 2;
        ops->spo_must_enforce     = xdr_dbuf_alloc_space(sizeof(enforce), dbuf);
        chimera_nfs_abort_if(ops->spo_must_enforce == NULL, "Failed to allocate space");
        memcpy(ops->spo_must_enforce, enforce, sizeof(enforce));
        ops->num_spo_must_allow = 0;
        ops->spo_must_allow     = NULL;
        return SP4_MACH_CRED;
    }

    /* SP4_SSV is declined: chimera cannot enforce SSV-derived credentials, so
     * it falls through to the SP4_NONE result below rather than advertising a
     * capability it does not back.  See the function comment above. */
    if (req_sp->spa_how == SP4_SSV) {
        chimera_nfs_debug(
            "EXCHANGE_ID: client requested SP4_SSV; declining (not enforced), "
            "negotiating SP4_NONE instead");
    }

    res_sp->spr_how = SP4_NONE;
    return SP4_NONE;
} /* nfs4_set_state_protect */

void
chimera_nfs4_exchange_id(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct EXCHANGE_ID4args *args = &argop->opexchange_id;
    struct EXCHANGE_ID4res  *res  = &resop->opexchange_id;
    /* NFSv4.1 server identity.  A client treats two server addresses as the same
     * server (shared state, eligible for trunking) when both the server-owner
     * major id AND the server scope match (RFC 8881 sec 2.10.5); the Linux
     * trunking-detection path keys on the major id.  Independent chimera servers
     * that do not share state -- e.g. a pNFS data server co-deployed with its
     * MDS -- must therefore differ in both, or the client coalesces them and
     * misroutes the data path.  The "nfs_server_scope" knob sets both (default
     * 42, preserving prior behavior). */
    uint64_t                       server_scope = chimera_server_config_get_nfs_server_scope(
        thread->shared->config);
    uint64_t                       owner_major = server_scope;
    uint64_t                       owner_minor = 42;
    struct timespec                now;
    struct nfs4_client_principal   principal;
    struct nfs4_exchange_id_result eid;
    bool                           update;
    int                            rc;

    clock_gettime(CLOCK_REALTIME, &now);

    /* While the persistent cold-start reconstruction is still in flight, hold
    * the client off so it cannot create a fresh record that races the
    * reconstructed one for the same owner (NFS4ERR_DELAY = retry shortly). */
    if (chimera_server_config_get_nfs4_drc(thread->shared->config) &&
        nfs_recovery_loading(&thread->shared->nfs4_recovery)) {
        res->eir_status = NFS4ERR_DELAY;
        chimera_nfs4_compound_complete(req, res->eir_status);
        return;
    }

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
                           thread->vfs_thread,
                           false);
    }

    if (eid.status != NFS4_OK) {
        res->eir_status = eid.status;
        chimera_nfs4_compound_complete(req, res->eir_status);
        return;
    }

    /* Negotiate state protection (RFC 8881 §18.35.3) and record the chosen mode
     * on the client so per-op enforcement (SP4_MACH_CRED) can consult it. */
    uint32_t sp_how = nfs4_set_state_protect(&args->eia_state_protect,
                                             &res->eir_resok4.eir_state_protect,
                                             req->encoding->dbuf);

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
            c->nfs4_client_sp_how = sp_how;
            uc                    = c->unified;
            /* EXCHANGE_ID is client liveness: renew the lease so a client
             * mid-handshake (EXCHANGE_ID -> CREATE_SESSION -> first SEQUENCE)
             * is not expired by the lease sweep before it establishes a
             * session. */
            if (uc) {
                nfs_client_touch(uc);
            }
        }
        pthread_mutex_unlock(&thread->shared->nfs4_shared_clients.nfs4_ct_lock);
        if (uc) {
            nfs_recovery_persist(thread->vfs_thread,
                                 &thread->shared->nfs4_recovery, uc);
        }
    }

    /* Advertise our pNFS role (RFC 8881 §13.1): a data server confirms
     * USE_PNFS_DS so the client will route layout I/O to it; a metadata server
     * advertises USE_PNFS_MDS; otherwise plain NFS. */
    uint32_t pnfs_flags;
    if (chimera_server_config_get_nfs_data_server(thread->shared->config)) {
        pnfs_flags = EXCHGID4_FLAG_USE_PNFS_DS;
    } else if (chimera_vfs_pnfs_feature_enabled(thread->shared->vfs)) {
        pnfs_flags = EXCHGID4_FLAG_USE_PNFS_MDS;
    } else {
        pnfs_flags = EXCHGID4_FLAG_USE_NON_PNFS;
    }

    res->eir_status                = NFS4_OK;
    res->eir_resok4.eir_clientid   = eid.clientid;
    res->eir_resok4.eir_sequenceid = 1;
    res->eir_resok4.eir_flags      = pnfs_flags |
        (eid.confirmed ? EXCHGID4_FLAG_CONFIRMED_R : 0);
    /* eir_state_protect was already filled (and the mode recorded on the
     * client) above. */
    res->eir_resok4.num_eir_server_impl_id = 1;

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
