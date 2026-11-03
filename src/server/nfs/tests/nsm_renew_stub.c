// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Link stubs for unit tests that compile nfs4_lease.c straight in.  The lease
 * tick calls the NSM cold-start kickoff and renewal sweep, which live in
 * nfs_nsm.c and drag in the whole NLM server; these tests exercise neither,
 * so both are no-ops.
 */

#include "nfs_nsm.h"

void
chimera_nfs_nsm_kickoff(struct chimera_server_nfs_thread *thread)
{
} /* chimera_nfs_nsm_kickoff */

void
chimera_nfs_nsm_renew_sweep(struct chimera_server_nfs_thread *thread)
{
} /* chimera_nfs_nsm_renew_sweep */
