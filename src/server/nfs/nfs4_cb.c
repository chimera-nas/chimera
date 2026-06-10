// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>

#include "nfs4_state.h"
#include "nfs4_stateid.h"
#include "nfs4_layout_table.h"
#include "nfs4_callback.h"
#include "nfs4_session.h"
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

/* Deferred-op resume that must run on its home thread.  The layout table fires
 * the waiter's resume on whichever thread processed the final LAYOUTRETURN, but
 * the deferred op's request and iovecs are owned by the thread that received it
 * (evpl/iovec ops are not cross-thread safe).  recall_and_wait wraps the real
 * resume in nfs4_cb_resume_bounce, which marshals it back to the origin thread
 * via cb_doorbell; the drain runs it there. */
struct nfs4_cb_resume_ctx {
    void                              (*resume)(
        void *arg);
    void                             *arg;
    struct chimera_server_nfs_thread *origin;
    struct nfs4_cb_resume_ctx        *next;
};

static void
nfs4_cb_resume_bounce(void *arg)
{
    struct nfs4_cb_resume_ctx        *ctx    = arg;
    struct chimera_server_nfs_thread *origin = ctx->origin;

    pthread_mutex_lock(&origin->cb_recall_lock);
    ctx->next               = origin->cb_resume_queue;
    origin->cb_resume_queue = ctx;
    pthread_mutex_unlock(&origin->cb_recall_lock);

    evpl_ring_doorbell(&origin->cb_doorbell);
} /* nfs4_cb_resume_bounce */

void
nfs4_cb_drain_resume_queue(struct chimera_server_nfs_thread *thread)
{
    struct nfs4_cb_resume_ctx *q;

    pthread_mutex_lock(&thread->cb_recall_lock);
    q                       = thread->cb_resume_queue;
    thread->cb_resume_queue = NULL;
    pthread_mutex_unlock(&thread->cb_recall_lock);

    while (q) {
        struct nfs4_cb_resume_ctx *ctx = q;
        q = ctx->next;
        ctx->resume(ctx->arg);
        free(ctx);
    }
} /* nfs4_cb_drain_resume_queue */

/*
 * Recall `holder` from the client that holds it, over that client's shared
 * callback channel.  A client with no usable channel cannot be asked to return
 * the layout, so it is revoked locally (which deregisters it and lets the
 * recall make progress).  `holder` is pinned by the caller.
 */
void
nfs4_cb_recall_holder(
    struct chimera_server_nfs_thread *thread,
    struct nfs_layout_state          *holder)
{
    struct nfs_client                *client   = holder->client;
    struct nfs4_cb_client            *chan     = client->cb_path.cb_client;
    struct chimera_server_nfs_thread *cb_owner =
        (chan && chan->session) ? chan->session->nfs4_session_backchannel_owner : NULL;
    struct nfs4_cb_recall_ctx        *ctx;
    struct stateid4                   recall_stateid;

    /* The CB_LAYOUTRECALL rides the session's backchannel conn, owned by one
     * thread's evpl (evpl sends are not cross-thread safe).  When a conflicting
     * op recalls from a different thread, bounce to that conn's owner -- mirrors
     * the delegation CB_RECALL path (nfs4_cb_recall_enqueue): pin the layout
     * with a ref, queue it, ring the owner's cb_doorbell.  The drain re-enters
     * this function on the owner thread (where the test below is false) and
     * sends inline.  Note: the bounce target is the *backchannel conn's* owner
     * (tracked on the session, repointed by CREATE_SESSION/BIND_CONN), NOT
     * chan->owner_thread, which is fixed at channel-open and drifts from the
     * conn.  If there is no backchannel the send below fails -> revoke. */
    if (cb_owner && thread != cb_owner) {
        nfs_layout_state_get(holder);
        pthread_mutex_lock(&cb_owner->cb_recall_lock);
        holder->recall_qnext            = cb_owner->cb_layoutrecall_queue;
        cb_owner->cb_layoutrecall_queue = holder;
        pthread_mutex_unlock(&cb_owner->cb_recall_lock);
        evpl_ring_doorbell(&cb_owner->cb_doorbell);
        return;
    }

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
    struct nfs4_cb_resume_ctx       *rctx;
    struct nfs_layout_state         *holders[NFS4_CB_MAX_HOLDERS];
    int                              n, i;

    /* The waiter's resume fires on whichever thread processes the final
     * LAYOUTRETURN, not this one; wrap it so it bounces back to this (the
     * deferred op's home) thread, where the op's request/iovecs are owned. */
    rctx         = calloc(1, sizeof(*rctx));
    rctx->resume = resume;
    rctx->arg    = resume_arg;
    rctx->origin = thread;

    waiter         = calloc(1, sizeof(*waiter));
    waiter->resume = nfs4_cb_resume_bounce;
    waiter->arg    = rctx;

    n = nfs_layout_table_recall_prepare(&thread->shared->nfs4_layout_table,
                                        fh, (uint16_t) fhlen, waiter,
                                        holders, NFS4_CB_MAX_HOLDERS);

    if (n == 0) {
        /* No layouts held for this file: nothing to recall, proceed now (we are
         * already on the deferred op's home thread). */
        free(rctx);
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
