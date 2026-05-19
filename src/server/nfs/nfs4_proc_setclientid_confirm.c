// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * RFC 7530 §16.34: SETCLIENTID_CONFIRM
 *
 * The client follows SETCLIENTID with SETCLIENTID_CONFIRM to confirm its
 * binding.  Server must:
 *   1. Verify the clientid matches a registered record.
 *   2. Verify the confirmation verifier matches what SETCLIENTID returned.
 *   3. Mark the unified client record `confirmed = true`.
 *
 * Phase 4 honors steps 1-3.  The setclientid_confirm verifier returned by
 * SETCLIENTID is the implicit session_id (first 8 bytes), which is what we
 * compare against here.
 */

#include <string.h>

#include "nfs4_procs.h"
#include "nfs4_recovery.h"
#include "nfs4_session.h"
#include "nfs4_state.h"

void
chimera_nfs4_setclientid_confirm(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct SETCLIENTID_CONFIRM4args *args  = &argop->opsetclientid_confirm;
    struct nfs4_client_table        *table =
        &thread->shared->nfs4_shared_clients;
    struct nfs4_client              *c  = NULL;
    struct nfs_client               *uc = NULL;
    uint8_t                          expected[NFS4_SESSIONID_SIZE];
    bool                             have_expected = false;
    nfsstat4                         status;

    /* table is only read inside the __clang_analyzer__-excluded block
     * below; consume it here so scan-build doesn't flag the init as
     * a dead store. */
    (void) table;

#ifndef __clang_analyzer__
    pthread_mutex_lock(&table->nfs4_ct_lock);

    HASH_FIND(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id,
              &args->clientid, sizeof(args->clientid), c);

    if (c) {
        uc = c->unified;

        /* The SETCLIENTID response copied the implicit session_id into the
         * setclientid_confirm verifier; recover it by walking the sessions
         * table for this clientid. */
        struct nfs4_session *s, *stmp;
        HASH_ITER(nfs4_session_hh, table->nfs4_ct_sessions, s, stmp)
        {
            if (s->nfs4_session_clientid == args->clientid) {
                memcpy(expected, s->nfs4_session_id, NFS4_SESSIONID_SIZE);
                have_expected = true;
                break;
            }
        }
    }

    pthread_mutex_unlock(&table->nfs4_ct_lock);
#endif /* ifndef __clang_analyzer__ */

    if (!c) {
        status = NFS4ERR_STALE_CLIENTID;
    } else if (!have_expected ||
               memcmp(args->setclientid_confirm, expected,
                      sizeof(args->setclientid_confirm)) != 0) {
        status = NFS4ERR_CLID_INUSE;
    } else {
        if (uc) {
            uc->confirmed = 1;
            /* Phase 5: stub-persist the confirmed client.  Future backends
             * will write a record to stable storage so a restart can
             * reclaim it within the grace window. */
            nfs_recovery_persist(&thread->shared->nfs4_recovery, uc);
        }
        status = NFS4_OK;
    }

    resop->opsetclientid_confirm.status = status;
    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_setclientid_confirm */
