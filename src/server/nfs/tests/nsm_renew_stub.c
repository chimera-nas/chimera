// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Link stub for unit tests that compile nfs4_lease.c straight in.  The lease
 * tick calls the NSM renewal sweep, which lives in nfs_nsm.c and drags in the
 * whole NLM server; these tests exercise neither, so the sweep is a no-op.
 */

#include "nfs_nsm.h"

void
chimera_nfs_nsm_renew_sweep(struct chimera_server_nfs_thread *thread)
{
} /* chimera_nfs_nsm_renew_sweep */
