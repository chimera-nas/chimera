// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"

/* RFC 7530 §16.41: RELEASE_LOCKOWNER notifies the server that the client is
 * done with a lock-owner and any associated state may be released. Chimera
 * does not retain per-lock-owner state beyond the lifetime of held locks
 * themselves, so there is nothing to free here. */
void
chimera_nfs4_release_lockowner(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct RELEASE_LOCKOWNER4res *res = &resop->oprelease_lockowner;

    res->status = NFS4_OK;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_release_lockowner */
