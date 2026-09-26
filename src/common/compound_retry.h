// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once

#include <stdlib.h>
#include "vfs/vfs_compound.h"

#define CHIMERA_FRONTEND_COMPOUND_RETRIES 8

/* This adapter owns only the completion policy. The frontend retains immutable
 * inputs and installs its own attempt-reset hook before submission. Each
 * accepted streaming chunk gets a fresh adapter; retry never replays previously
 * accepted chunks or re-enters the frontend's dispatch/body-consumption path.
 *
 * A finish EAGAIN certifies an aborted transaction. Ordinary operation EAGAIN
 * has successful-prefix semantics and is delivered without replay. No response,
 * application callback, ownership transfer, or shared frontend-state update is
 * allowed before the terminal callback below. Backend rollback is a prerequisite
 * for a transaction provider to reject a mutating attempt, not this adapter's job.
 */
struct chimera_frontend_compound_completion {
    chimera_vfs_compound_callback_t callback;
    void                           *private_data;
    unsigned int                    retries;
};

static void
chimera_frontend_compound_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_frontend_compound_completion *ctx = private_data;
    chimera_vfs_compound_callback_t              callback;
    void                                        *arg;

    if (chimera_vfs_compound_finish_status(compound) == CHIMERA_VFS_EAGAIN &&
        ctx->retries++ < CHIMERA_FRONTEND_COMPOUND_RETRIES &&
        chimera_vfs_compound_retry(compound)) {
        /* Retry may complete inline and free ctx and compound. */
        return;
    }
    callback = ctx->callback;
    arg      = ctx->private_data;
    free(ctx);
    callback(compound, arg);
} // chimera_frontend_compound_complete

static inline void
chimera_frontend_compound_submit(
    struct chimera_vfs_compound    *compound,
    chimera_vfs_compound_callback_t callback,
    void                           *private_data)
{
    struct chimera_frontend_compound_completion *ctx = calloc(1, sizeof(*ctx));

    if (!ctx) {
        /* Retrying is optional under memory pressure. The ordinary final
         * callback still receives the actual finish status and publishes no
         * rejected results; do not invent a successful completion. */
        chimera_vfs_compound_submit(compound, callback, private_data);
        return;
    }
    ctx->callback     = callback;
    ctx->private_data = private_data;
    chimera_vfs_compound_submit(compound, chimera_frontend_compound_complete, ctx);
} // chimera_frontend_compound_submit
