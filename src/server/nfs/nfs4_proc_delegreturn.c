// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"

#include "nfs4_protocol.h"

/* Claim release and delegation teardown publish only after accepted finish. */
void
chimera_nfs4_delegreturn(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    (void) argop;
    (void) resop;
    chimera_nfs4_compound_state(thread, req);
} /* chimera_nfs4_delegreturn */

void
chimera_nfs4_delegpurge(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    (void) thread;
    resop->opdelegpurge.status = chimera_nfs4_protocol_status(req, argop);
    chimera_nfs4_compound_complete(req, resop->opdelegpurge.status);
} /* chimera_nfs4_delegpurge */
