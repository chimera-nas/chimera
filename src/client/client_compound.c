// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_internal.h"
#include "vfs/vfs_procs.h"
#include "common/macros.h"

/*
 * Public caller-owned compound API (see client.h for the contract).  Thin
 * wrappers over the VFS compound primitives on the client thread's VFS
 * thread; the caller-owned lane is GROUPING (flags 0): no RETRYABLE opt-in,
 * so a backend conflict is rewritten by the VFS core into the retriable,
 * never-replayed ECOMPOUND_EXHAUSTED and the caller needs no replay
 * machinery.
 */

SYMBOL_EXPORT struct chimera_vfs_compound *
chimera_client_compound_begin(
    struct chimera_client_thread  *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *hint_fh,
    int                            hint_fhlen,
    enum chimera_vfs_compound_mode mode)
{
    if (!cred) {
        cred = &thread->client->cred;
    }

    return chimera_vfs_compound_begin(thread->vfs_thread,
                                      cred,
                                      hint_fh,
                                      hint_fhlen,
                                      mode,
                                      chimera_vfs_compound_alloc_ts(thread->vfs_thread),
                                      0 /* grouping lane: no RETRYABLE, no LOOSE */);
} /* chimera_client_compound_begin */

SYMBOL_EXPORT void
chimera_client_compound_end(
    struct chimera_client_thread       *thread,
    const struct chimera_vfs_cred      *cred,
    struct chimera_vfs_compound        *compound,
    enum chimera_vfs_compound_end       end_flag,
    chimera_vfs_compound_end_callback_t callback,
    void                               *private_data)
{
    if (!cred) {
        cred = &thread->client->cred;
    }

    chimera_vfs_compound_end(thread->vfs_thread,
                             cred,
                             compound,
                             end_flag,
                             callback,
                             private_data);
} /* chimera_client_compound_end */

SYMBOL_EXPORT struct chimera_vfs_compound *
chimera_client_compound_loose(struct chimera_client_thread *thread)
{
    return chimera_vfs_compound_loose(thread->vfs_thread);
} /* chimera_client_compound_loose */
