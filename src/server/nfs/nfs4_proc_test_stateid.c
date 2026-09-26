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
    uint64_t                   bytes    = (uint64_t) sizeof(nfsstat4) * args->num_ts_stateids;
    uint64_t                   avail    = req->encoding->dbuf->size - req->encoding->dbuf->used;
    uint64_t                   headroom = 8192;

    /* The generated COMPOUND reply allocates 260 transport iovecs from this
     * same arena. Preserve the dispatcher/adapter's reply-space floor rather
     * than consuming it with an otherwise valid variable-length result. */
    if (headroom < 260 * sizeof(struct evpl_iovec)) {
        headroom = 260 * sizeof(struct evpl_iovec);
    }
    if (((bytes + 7) & ~UINT64_C(7)) + headroom > avail) {
        resok->num_tsr_status_codes = 0;
        resok->tsr_status_codes     = NULL;
        res->tsr_status             = NFS4ERR_REP_TOO_BIG;
        chimera_nfs4_compound_complete(req, res->tsr_status);
        return;
    }

    resok->num_tsr_status_codes = args->num_ts_stateids;
    resok->tsr_status_codes     = xdr_dbuf_alloc_space(
        sizeof(nfsstat4) * args->num_ts_stateids,
        req->encoding->dbuf);

    /* ts_stateids<> is unbounded on the wire, so a large enough array does not
     * fit the remaining response buffer.  Report that as REP_TOO_BIG (RFC 8881
     * §18.48.3; NFS4ERR_RESOURCE is not a 4.1 error) rather than writing the
     * per-stateid results through a NULL pointer. */
    if (resok->tsr_status_codes == NULL) {
        resok->num_tsr_status_codes = 0;
        res->tsr_status             = NFS4ERR_REP_TOO_BIG;
        chimera_nfs4_compound_complete(req, res->tsr_status);
        return;
    }

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
