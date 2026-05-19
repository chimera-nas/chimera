// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"

/* RFC 7530 §16.30: RENEW refreshes the lease for the given client. Chimera
 * does not currently enforce lease expiry, so this is a no-op that always
 * succeeds. Tests that require true lease-expiry semantics (e.g. pynfs
 * testExpired) will not behave correctly until real lease tracking lands. */
void
chimera_nfs4_renew(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct RENEW4res *res = &resop->oprenew;

    res->status = NFS4_OK;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_renew */
