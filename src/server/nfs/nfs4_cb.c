// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "common/thread.h"
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


/* Every recall owns a separate queue node, layout reference and client pin.
 * Concurrent recalls of one holder must never share intrusive queue linkage. */
struct nfs4_cb_layout_recall_ctx {
    struct chimera_server_nfs_thread *thread;
    struct nfs_client                *client;
    struct nfs_layout_state          *holder;
    struct nfs4_cb_layout_recall_ctx *next;
};

static void
nfs4_cb_recall_done(
    int   cb_status,
    void *arg)
{
    struct nfs4_cb_layout_recall_ctx *ctx = arg;

    if (cb_status != NFS4_OK) {
        /* Revoke the original holder only. A layout returned and regranted
        * while the callback was outstanding is a different reservation. */
        nfs_layout_state_destroy(ctx->holder,
                                 &ctx->thread->shared->nfs4_state_table,
                                 ctx->thread->vfs_thread);
    }
    nfs_layout_state_put(ctx->holder);
    nfs_client_finish_compound(ctx->client,
                               &ctx->thread->shared->nfs4_state_table,
                               ctx->thread->vfs_thread);
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

    evpl_mutex_lock(&origin->cb_recall_lock);
    ctx->next               = origin->cb_resume_queue;
    origin->cb_resume_queue = ctx;
    evpl_mutex_unlock(&origin->cb_recall_lock);

    evpl_ring_doorbell(&origin->cb_doorbell);
} /* nfs4_cb_resume_bounce */

void
nfs4_cb_drain_resume_queue(struct chimera_server_nfs_thread *thread)
{
    struct nfs4_cb_resume_ctx *q;

    evpl_mutex_lock(&thread->cb_recall_lock);
    q                       = thread->cb_resume_queue;
    thread->cb_resume_queue = NULL;
    evpl_mutex_unlock(&thread->cb_recall_lock);

    while (q) {
        struct nfs4_cb_resume_ctx *ctx = q;
        q = ctx->next;
        ctx->resume(ctx->arg);
        free(ctx);
    }
} /* nfs4_cb_drain_resume_queue */

static void
nfs4_cb_recall_send(struct nfs4_cb_layout_recall_ctx *ctx)
{
    struct chimera_server_nfs_thread *thread   = ctx->thread;
    struct nfs_layout_state          *holder   = ctx->holder;
    struct nfs4_cb_client            *chan     = ctx->client->cb_path.cb_client;
    struct chimera_server_nfs_thread *cb_owner =
        (chan && chan->session) ? chan->session->nfs4_session_backchannel_owner : NULL;
    struct stateid4                   recall_stateid;

    if (atomic_load_explicit(&holder->destroyed, memory_order_acquire)) {
        nfs4_cb_recall_done(NFS4_OK, ctx);
        return;
    }
    if (cb_owner && thread != cb_owner) {
        ctx->thread = cb_owner;
        evpl_mutex_lock(&cb_owner->cb_recall_lock);
        ctx->next                       = cb_owner->cb_layoutrecall_queue;
        cb_owner->cb_layoutrecall_queue = ctx;
        evpl_mutex_unlock(&cb_owner->cb_recall_lock);
        evpl_ring_doorbell(&cb_owner->cb_doorbell);
        return;
    }

    nfs4_stateid_encode(&recall_stateid, holder->seqid, NFS4_STATEID_TYPE_LAYOUT,
                        holder->shard, holder->slot_idx, holder->generation,
                        thread->shared->nfs4_state_table.epoch);
    if (!nfs4_cb_layoutrecall(thread, ctx->client, holder->fh, holder->fh_len,
                              holder->export_id, holder->layout_type,
                              &recall_stateid, nfs4_cb_recall_done, ctx)) {
        chimera_nfs_error("CB: holder has no callback channel; revoking layout");
        nfs4_cb_recall_done(-1, ctx);
    }
} /* nfs4_cb_recall_send */

void
nfs4_cb_recall_holder(
    struct chimera_server_nfs_thread *thread,
    struct nfs_layout_state          *holder)
{
    struct nfs_client                *client;
    struct nfs4_cb_layout_recall_ctx *ctx;

    /* A layout reference retains client memory. Pin callbacks even when client
     * destruction is pending behind a compound which is waiting for recall. */
    client = nfs_layout_state_reserve_client(holder);
    if (!client) {
        /* Teardown has already removed this holder (under the same lock). */
        return;
    }
    ctx = calloc(1, sizeof(*ctx));
    if (!ctx) {
        nfs_layout_state_destroy(holder, &thread->shared->nfs4_state_table,
                                 thread->vfs_thread);
        nfs_client_finish_compound(client, &thread->shared->nfs4_state_table,
                                   thread->vfs_thread);
        return;
    }
    nfs_layout_state_get(holder);
    ctx->thread = thread;
    ctx->client = client;
    ctx->holder = holder;
    nfs4_cb_recall_send(ctx);
} /* nfs4_cb_recall_holder */

void
nfs4_cb_drain_layoutrecall_queue(struct chimera_server_nfs_thread *thread)
{
    struct nfs4_cb_layout_recall_ctx *q;

    evpl_mutex_lock(&thread->cb_recall_lock);
    q                             = thread->cb_layoutrecall_queue;
    thread->cb_layoutrecall_queue = NULL;
    evpl_mutex_unlock(&thread->cb_recall_lock);
    while (q) {
        struct nfs4_cb_layout_recall_ctx *ctx = q;
        q         = ctx->next;
        ctx->next = NULL;
        nfs4_cb_recall_send(ctx);
    }
} /* nfs4_cb_drain_layoutrecall_queue */

void
chimera_nfs4_cb_recall_and_wait(
    struct chimera_server_nfs_thread *thread,
    const uint8_t                    *fh,
    uint32_t                          fhlen,
    void (                           *resume )(
        void *arg),
    void                             *resume_arg)
{
    struct nfs_layout_recall_waiter *waiter;
    struct nfs4_cb_resume_ctx       *rctx;
    struct nfs_layout_state        **holders;
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
                                        &holders);

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
    free(holders);
} /* chimera_nfs4_cb_recall_and_wait */
