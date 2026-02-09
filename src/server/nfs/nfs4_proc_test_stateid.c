// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"

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
    uint32_t                   i;

    resok->num_tsr_status_codes = args->num_ts_stateids;
    resok->tsr_status_codes     = xdr_dbuf_alloc_space(
        sizeof(nfsstat4) * args->num_ts_stateids,
        req->encoding->dbuf);

    for (i = 0; i < args->num_ts_stateids; i++) {
        resok->tsr_status_codes[i] = NFS4_OK;
    }

    res->tsr_status = NFS4_OK;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_test_stateid */
