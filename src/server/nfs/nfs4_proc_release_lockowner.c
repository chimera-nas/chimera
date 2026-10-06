// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"

/* State retirement shares the ordered, retry-private compound journal. */
void
chimera_nfs4_release_lockowner(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    (void) argop;
    (void) resop;
    chimera_nfs4_compound_state(thread, req);
} /* chimera_nfs4_release_lockowner */
