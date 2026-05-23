// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>

#include "nfs4_state.h"
#include "nfs4_stateid.h"
#include "nfs4_layout_table.h"
#include "nfs4_callback.h"
#include "nfs_common.h"
#include "nfs_internal.h"
#include "nfs4_cb.h"

#define NFS4_CB_MAX_HOLDERS 32   /* recall fan-out per file */

/* Per-holder recall context: lets the completion find and drop the holder's
 * layout if the client declines (no LAYOUTRETURN coming).  The send itself
 * rides the client's shared callback channel (nfs4_callback.c). */
struct nfs4_cb_recall_ctx {
    struct chimera_server_nfs_thread *thread;
    struct nfs_client                *client;     /* the holder */
    uint8_t                           fh[NFS4_FHSIZE];
    uint16_t                          fhlen;
};

/*
 * CB_LAYOUTRECALL completion.  NFS4_OK means the client will return the layout
 * via LAYOUTRETURN (which deregisters it and resumes the waiters).  Anything
 * else -- NOMATCHING, a callback error, or a transport failure -- means no
 * return is coming, so drop the stale layout now; its deregistration resumes
 * the waiters once the last holder for the file is gone.
 */
static void
nfs4_cb_recall_done(
    int   cb_status,
    void *arg)
{
    struct nfs4_cb_recall_ctx *ctx = arg;

    if (cb_status != NFS4_OK) {
        struct nfs_layout_state *layout =
            nfs_layout_state_find(ctx->client, ctx->fh, ctx->fhlen);

        if (layout) {
            nfs_layout_state_destroy(layout,
                                     &ctx->thread->shared->nfs4_state_table,
                                     ctx->thread->vfs_thread);
        }
    }

    free(ctx);
} /* nfs4_cb_recall_done */

/*
 * Recall `holder` from the client that holds it, over that client's shared
 * callback channel.  A client with no usable channel cannot be asked to return
 * the layout, so it is revoked locally (which deregisters it and lets the
 * recall make progress).  `holder` is pinned by the caller.
 */
static void
nfs4_cb_recall_holder(
    struct chimera_server_nfs_thread *thread,
    struct nfs_layout_state          *holder)
{
    struct nfs_client         *client = holder->client;
    struct nfs4_cb_recall_ctx *ctx;
    struct stateid4            recall_stateid;

    nfs4_stateid_encode(&recall_stateid, holder->seqid, NFS4_STATEID_TYPE_LAYOUT,
                        holder->shard, holder->slot_idx, holder->generation,
                        (uint32_t) client->client_id);

    ctx         = calloc(1, sizeof(*ctx));
    ctx->thread = thread;
    ctx->client = client;
    memcpy(ctx->fh, holder->fh, holder->fh_len);
    ctx->fhlen = holder->fh_len;

    if (!nfs4_cb_layoutrecall(thread, client, holder->fh, holder->fh_len,
                              &recall_stateid, nfs4_cb_recall_done, ctx)) {
        chimera_nfs_error("CB: holder has no callback channel; revoking layout");
        free(ctx);
        nfs_layout_state_destroy(holder, &thread->shared->nfs4_state_table,
                                 thread->vfs_thread);
    }
} /* nfs4_cb_recall_holder */

void
chimera_nfs4_cb_recall_and_wait(
    struct chimera_server_nfs_thread *thread,
    const uint8_t                    *fh,
    uint32_t                          fhlen,
    void (                           *resume )(void *arg),
    void                             *resume_arg)
{
    struct nfs_layout_recall_waiter *waiter;
    struct nfs_layout_state         *holders[NFS4_CB_MAX_HOLDERS];
    int                              n, i;

    waiter         = calloc(1, sizeof(*waiter));
    waiter->resume = resume;
    waiter->arg    = resume_arg;

    n = nfs_layout_table_recall_prepare(&thread->shared->nfs4_layout_table,
                                        fh, (uint16_t) fhlen, waiter,
                                        holders, NFS4_CB_MAX_HOLDERS);

    if (n == 0) {
        /* No layouts held for this file: nothing to recall, proceed now. */
        free(waiter);
        resume(resume_arg);
        return;
    }

    chimera_nfs_info("pNFS: recalling layout from %d holder(s) before conflicting op", n);

    /* Recall every holder.  Each one's return (or decline) deregisters it; the
     * last deregistration resumes the deferred operation. */
    for (i = 0; i < n; i++) {
        nfs4_cb_recall_holder(thread, holders[i]);
        nfs_layout_state_put(holders[i]);
    }
} /* chimera_nfs4_cb_recall_and_wait */
