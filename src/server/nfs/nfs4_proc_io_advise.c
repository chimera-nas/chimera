// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_state.h"

/*
 * IO_ADVISE (RFC 7862 15.5) conveys client I/O hints (sequential, random,
 * willneed, dontneed, ...).  The hints are purely advisory: a server is
 * permitted to honor none of them, in which case it returns an empty
 * ior_hints bitmap.  Chimera's VFS exposes no fadvise primitive, so this is a
 * validating no-op -- it confirms a file handle and a usable stateid are
 * present, then reports that no hints were applied.
 */
void
chimera_nfs4_io_advise(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct IO_ADVISE4args  *args  = &argop->opio_advise;
    struct IO_ADVISE4res   *res   = &resop->opio_advise;
    struct nfs_state_table *table = &thread->shared->nfs4_state_table;
    void                   *state_void;
    uint8_t                 state_type;
    nfsstat4                status;

    if (req->fhlen == 0) {
        res->ior_status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->ior_status);
        return;
    }

    /* NFS4.1 current-stateid substitution (RFC 8881 16.2.3.1.2). */
    chimera_nfs4_resolve_current_stateid(req, &args->iaa_stateid);

    /*
     * A special (anonymous/read-bypass) stateid carries no state-table entry
     * and is always acceptable for an advisory operation.  Otherwise validate
     * the stateid against the state table.
     */
    if (!nfs4_stateid_is_special(&args->iaa_stateid)) {
        status = nfs_state_table_acquire(table, &args->iaa_stateid, 0,
                                         &state_void, &state_type);
        if (status != NFS4_OK) {
            res->ior_status = status;
            chimera_nfs4_compound_complete(req, res->ior_status);
            return;
        }
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
    }

    /* No hints honored: return an empty ior_hints bitmap. */
    res->ior_status           = NFS4_OK;
    res->resok4.num_ior_hints = 0;
    res->resok4.ior_hints     = NULL;

    chimera_nfs4_compound_complete(req, res->ior_status);
} /* chimera_nfs4_io_advise */
