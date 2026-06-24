// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_session.h"
#include "nfs4_state.h"

void
chimera_nfs4_test_stateid(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct TEST_STATEID4args  *args  = &argop->optest_stateid;
    struct TEST_STATEID4res   *res   = &resop->optest_stateid;
    struct TEST_STATEID4resok *resok = &res->tsr_resok4;
    struct nfs_state_table    *table = &thread->shared->nfs4_state_table;
    uint32_t                   i;

    resok->num_tsr_status_codes = args->num_ts_stateids;
    resok->tsr_status_codes     = xdr_dbuf_alloc_space(
        sizeof(nfsstat4) * args->num_ts_stateids,
        req->encoding->dbuf);

    for (i = 0; i < args->num_ts_stateids; i++) {
        nfsstat4 st = nfs_state_table_validate(table, &args->ts_stateids[i]);

        /* NFS4ERR_STALE_STATEID is a 4.0-only code and is not a valid
         * per-stateid status for TEST_STATEID (RFC 8881 §18.48.3 / §15.1.16.5);
         * a wrong-epoch or older-generation stateid is reported as
         * NFS4ERR_BAD_STATEID to a 4.1+ client. */
        if (st == NFS4ERR_STALE_STATEID) {
            st = NFS4ERR_BAD_STATEID;
        }
        resok->tsr_status_codes[i] = st;
    }

    res->tsr_status = NFS4_OK;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_test_stateid */
