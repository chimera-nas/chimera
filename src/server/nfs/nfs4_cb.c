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
#include "nfs4_lease.h"
#include "prometheus-c.h"

#define NFS4_CB_MAX_HOLDERS                32 /* holders recalled per batch */

/* A CB_LAYOUTRECALL the client answered NFS4ERR_DELAY, or that never got an
 * answer, is sent again after a delay that starts at the base and doubles up
 * to the cap.  The retries stop when the holder returns or is revoked, and in
 * any case at the two-lease deadline, where the lease sweep revokes it. */
#define NFS4_CB_LAYOUTRECALL_RETRY_BASE_US 250000   /* 250 ms */
#define NFS4_CB_LAYOUTRECALL_RETRY_CAP_US  5000000  /* 5 s */

static const char *const nfs4_layout_recall_cause_names[NFS4_LAYOUT_RECALL_NUM] = {
    "conflict", "unlinked", "umount",
};

static const char *const nfs4_layout_revoke_why_names[NFS4_LAYOUT_REVOKE_NUM] = {
    "deadline", "refused",
};

void
nfs4_layout_metrics_init(struct chimera_server_nfs_shared *shared)
{
    struct nfs4_layout_metrics *m = &shared->layout_metrics;
    int                         i;

    m->recall = prometheus_metrics_create_counter(
        shared->metrics, "chimera_nfs4_layout_recall",
        "Layout recalls started, by what caused them");
    for (i = 0; i < NFS4_LAYOUT_RECALL_NUM; i++) {
        m->recall_series[i] = prometheus_counter_create_series(
            m->recall, (const char *[]) { "cause" },
            (const char *[]) { nfs4_layout_recall_cause_names[i] }, 1);
        m->recalls[i] = prometheus_counter_series_create_instance(m->recall_series[i]);
    }

    m->revoke = prometheus_metrics_create_counter(
        shared->metrics, "chimera_nfs4_layout_revoke",
        "Layouts revoked by the server, by why");
    for (i = 0; i < NFS4_LAYOUT_REVOKE_NUM; i++) {
        m->revoke_series[i] = prometheus_counter_create_series(
            m->revoke, (const char *[]) { "why" },
            (const char *[]) { nfs4_layout_revoke_why_names[i] }, 1);
        m->revokes[i] = prometheus_counter_series_create_instance(m->revoke_series[i]);
    }
} /* nfs4_layout_metrics_init */

void
nfs4_layout_metrics_destroy(struct chimera_server_nfs_shared *shared)
{
    struct nfs4_layout_metrics *m = &shared->layout_metrics;
    int                         i;

    for (i = 0; i < NFS4_LAYOUT_RECALL_NUM; i++) {
        if (m->recalls[i]) {
            prometheus_counter_series_destroy_instance(m->recall_series[i], m->recalls[i]);
            prometheus_counter_destroy_series(m->recall, m->recall_series[i]);
        }
    }
    if (m->recall) {
        prometheus_counter_destroy(shared->metrics, m->recall);
    }
    for (i = 0; i < NFS4_LAYOUT_REVOKE_NUM; i++) {
        if (m->revokes[i]) {
            prometheus_counter_series_destroy_instance(m->revoke_series[i], m->revokes[i]);
            prometheus_counter_destroy_series(m->revoke, m->revoke_series[i]);
        }
    }
    if (m->revoke) {
        prometheus_counter_destroy(shared->metrics, m->revoke);
    }
} /* nfs4_layout_metrics_destroy */

static void
nfs4_layout_recall_count(
    struct chimera_server_nfs_shared *shared,
    enum nfs4_layout_recall_cause     cause)
{
    if (shared->layout_metrics.recalls[cause]) {
        prometheus_counter_increment(shared->layout_metrics.recalls[cause]);
    }
} /* nfs4_layout_recall_count */

static void
nfs4_layout_revoke_count(
    struct chimera_server_nfs_shared *shared,
    enum nfs4_layout_revoke_why       why)
{
    if (shared->layout_metrics.revokes[why]) {
        prometheus_counter_increment(shared->layout_metrics.revokes[why]);
    }
} /* nfs4_layout_revoke_count */

/* Per-holder recall context: lets the completion find the holder it recalled,
 * to revoke it if the client refuses (no LAYOUTRETURN coming) or to ask again.
 * The send itself rides the client's shared callback channel
 * (nfs4_callback.c). */
struct nfs4_cb_recall_ctx {
    struct chimera_server_nfs_thread *thread;
    struct nfs_layout_state          *holder;     /* layout ref held */
};

/* A pending re-send of the recall to `holder`; holds a layout ref and a client
 * ref until the timer fires. */
struct nfs4_cb_layout_retry {
    struct evpl_timer                 timer;
    struct chimera_server_nfs_thread *thread;
    struct nfs_layout_state          *holder;
    struct nfs4_cb_layout_retry      *prev, *next; /* thread->cb_layout_retries */
};

static void
nfs4_cb_layout_retry_unlink(struct nfs4_cb_layout_retry *r)
{
    if (r->prev) {
        r->prev->next = r->next;
    } else {
        r->thread->cb_layout_retries = r->next;
    }
    if (r->next) {
        r->next->prev = r->prev;
    }
} /* nfs4_cb_layout_retry_unlink */

static void
nfs4_cb_layout_retry_fire(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct nfs4_cb_layout_retry *r =
        (struct nfs4_cb_layout_retry *) ((char *) timer -
                                         offsetof(struct nfs4_cb_layout_retry, timer));
    struct nfs_layout_state     *holder = r->holder;

    (void) evpl;

    nfs4_cb_layout_retry_unlink(r);

    /* A holder that returned or was revoked meanwhile needs no recall. */
    if (!atomic_load_explicit(&holder->destroyed, memory_order_acquire)) {
        nfs4_cb_recall_holder(r->thread, holder);
    }

    nfs_client_put(holder->client);
    nfs_layout_state_put(holder);
    free(r);
} /* nfs4_cb_layout_retry_fire */

static void
nfs4_cb_layout_retry_arm(
    struct chimera_server_nfs_thread *thread,
    struct nfs_layout_state          *holder)
{
    struct nfs4_cb_layout_retry *r        = calloc(1, sizeof(*r));
    uint64_t                     delay_us = NFS4_CB_LAYOUTRECALL_RETRY_BASE_US;
    uint32_t                     n        = holder->recall_retries++;

    chimera_nfs_abort_if(r == NULL, "layout recall retry alloc failed");

    while (n-- > 0 && delay_us < NFS4_CB_LAYOUTRECALL_RETRY_CAP_US) {
        delay_us *= 2;
    }
    if (delay_us > NFS4_CB_LAYOUTRECALL_RETRY_CAP_US) {
        delay_us = NFS4_CB_LAYOUTRECALL_RETRY_CAP_US;
    }

    r->thread = thread;
    r->holder = holder;
    r->next   = thread->cb_layout_retries;
    if (r->next) {
        r->next->prev = r;
    }
    thread->cb_layout_retries = r;
    nfs_layout_state_get(holder);
    nfs_client_get(holder->client);
    evpl_add_oneshot_timer(thread->evpl, &r->timer, nfs4_cb_layout_retry_fire,
                           delay_us);
} /* nfs4_cb_layout_retry_arm */

/* Drop the re-sends still waiting when the thread goes: the recall deadline
 * and the client's own teardown decide those holders' fate, not a timer on a
 * dead thread. */
void
nfs4_cb_layout_retries_destroy(struct chimera_server_nfs_thread *thread)
{
    struct nfs4_cb_layout_retry *r = thread->cb_layout_retries;
    struct nfs4_cb_layout_retry *next;

    thread->cb_layout_retries = NULL;

    while (r != NULL) {
        next = r->next;
        evpl_remove_timer(thread->evpl, &r->timer);
        nfs_client_put(r->holder->client);
        nfs_layout_state_put(r->holder);
        free(r);
        r = next;
    }
} /* nfs4_cb_layout_retries_destroy */

/*
 * CB_LAYOUTRECALL completion (RFC 8881 20.3).
 *
 * NFS4_OK means the client will return the layout via LAYOUTRETURN (which
 * deregisters it and resumes the waiters).  NFS4ERR_DELAY says to ask again,
 * and a transport failure (cb_status < 0) says nothing about what the client
 * holds, so both are retried with a growing delay; the two-lease deadline
 * revokes the layout if the client never returns it.  Anything else -- chiefly
 * NFS4ERR_NOMATCHING_LAYOUT, the client has nothing to return, but also a real
 * refusal -- means no return is coming, so drop the stale layout now; its
 * deregistration resumes the waiters once the last holder for the file is
 * gone.
 */
static void
nfs4_cb_recall_done(
    int   cb_status,
    void *arg)
{
    struct nfs4_cb_recall_ctx *ctx    = arg;
    struct nfs_layout_state   *holder = ctx->holder;

    if (cb_status != NFS4_OK &&
        !atomic_load_explicit(&holder->destroyed, memory_order_acquire)) {
        if (cb_status < 0 || cb_status == NFS4ERR_DELAY) {
            nfs4_cb_layout_retry_arm(ctx->thread, holder);
        } else {
            nfs4_layout_revoke_count(ctx->thread->shared, NFS4_LAYOUT_REVOKE_REFUSED);
            nfs_layout_state_destroy(holder,
                                     &ctx->thread->shared->nfs4_state_table,
                                     ctx->thread->vfs_thread);
        }
    }

    nfs_layout_state_put(holder);
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

/*
 * Recall `holder` from the client that holds it, over that client's shared
 * callback channel.  A client with no usable channel cannot be asked to return
 * the layout, but a lost backchannel is often temporary (the client learns of
 * it through SEQ4_STATUS_CB_PATH_DOWN and rebuilds it), so the holder is not
 * revoked here: it stays a recalled holder and the two-lease deadline
 * (nfs4_cb_revoke_overdue_layouts) revokes it if it is still there then.  The
 * recall is not re-sent if a path reappears before the deadline.  `holder` is
 * pinned by the caller.
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
     * conn.  If there is no backchannel the send below fails and the
     * holder waits for the recall deadline. */
    if (cb_owner && thread != cb_owner) {
        nfs_layout_state_get(holder);
        evpl_mutex_lock(&cb_owner->cb_recall_lock);
        holder->recall_qnext            = cb_owner->cb_layoutrecall_queue;
        cb_owner->cb_layoutrecall_queue = holder;
        evpl_mutex_unlock(&cb_owner->cb_recall_lock);
        evpl_ring_doorbell(&cb_owner->cb_doorbell);
        return;
    }

    /* Called with no thread (the VFS close thread's recall) and no backchannel
     * owner to bounce to: nothing can be sent, and the holder waits for the
     * deadline like any other with no channel. */
    if (!thread) {
        chimera_nfs_error("CB: holder has no callback channel; "
                          "its layout is revoked at the recall deadline");
        return;
    }

    nfs4_stateid_encode(&recall_stateid, holder->seqid, NFS4_STATEID_TYPE_LAYOUT,
                        holder->shard, holder->slot_idx, holder->generation,
                        (uint32_t) client->client_id);

    ctx         = calloc(1, sizeof(*ctx));
    ctx->thread = thread;
    ctx->holder = holder;
    nfs_layout_state_get(holder);

    if (!nfs4_cb_layoutrecall(thread, client, holder->fh, holder->fh_len,
                              holder->export_id, holder->layout_type,
                              &recall_stateid, nfs4_cb_recall_done, ctx)) {
        chimera_nfs_error("CB: holder has no callback channel; "
                          "its layout is revoked at the recall deadline");
        nfs_layout_state_put(holder);
        free(ctx);
    }
} /* nfs4_cb_recall_holder */

/*
 * Start a recall of every layout held on fh and park `waiter` behind it, a
 * batch of holders at a time.  Returns -1 when the file has no layouts (the
 * waiter is not parked), else how many holders were recalled.  The waiter is
 * parked, so LAYOUTGET is refused and no holder joins; each batch holds only
 * holders no earlier batch (or overlapping recall) took, so a short batch means
 * all have been asked.  A holder's return, decline or revoke deregisters it and
 * the last one fires the waiter -- never inside this loop.  `thread` is the
 * caller's NFS thread, or NULL off the NFS threads (the VFS close thread), in
 * which case every send is bounced to the holder's backchannel owner.
 */
static int
nfs4_cb_recall_batches(
    struct chimera_server_nfs_thread *thread,
    struct nfs_layout_table          *table,
    const uint8_t                    *fh,
    uint32_t                          fhlen,
    struct nfs_layout_recall_waiter  *waiter)
{
    struct nfs_layout_state *holders[NFS4_CB_MAX_HOLDERS];
    int                      n, i, total = 0;

    n = nfs_layout_table_recall_prepare(table, fh, (uint16_t) fhlen, waiter,
                                        holders, NFS4_CB_MAX_HOLDERS);
    if (n < 0) {
        return -1;
    }

    while (n > 0) {
        for (i = 0; i < n; i++) {
            nfs4_cb_recall_holder(thread, holders[i]);
            nfs_layout_state_put(holders[i]);
        }
        total += n;
        if (n < NFS4_CB_MAX_HOLDERS) {
            break;
        }
        n = nfs_layout_table_recall_prepare(table, fh, (uint16_t) fhlen, NULL,
                                            holders, NFS4_CB_MAX_HOLDERS);
    }

    return total;
} /* nfs4_cb_recall_batches */

void
chimera_nfs4_cb_recall_and_wait(
    struct chimera_server_nfs_thread *thread,
    const uint8_t                    *fh,
    uint32_t                          fhlen,
    void (                           *resume )(
        void *arg),
    void                             *resume_arg)
{
    struct nfs_layout_table         *table = &thread->shared->nfs4_layout_table;
    struct nfs_layout_recall_waiter *waiter;
    struct nfs4_cb_resume_ctx       *rctx;
    int                              total;

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

    total = nfs4_cb_recall_batches(thread, table, fh, fhlen, waiter);

    if (total < 0) {
        /* No layouts held for this file: nothing to recall, proceed now (we are
         * already on the deferred op's home thread). */
        free(rctx);
        free(waiter);
        resume(resume_arg);
        return;
    }

    if (total > 0) {
        nfs4_layout_recall_count(thread->shared, NFS4_LAYOUT_RECALL_CONFLICT);
        chimera_nfs_info("pNFS: recalled layout from %d holder(s) before conflicting op",
                         total);
    } else {
        chimera_nfs_info("pNFS: joined a layout recall already in progress");
    }
} /* chimera_nfs4_cb_recall_and_wait */

static void
nfs4_cb_detached_resume(void *arg)
{
    (void) arg;
} /* nfs4_cb_detached_resume */

void
nfs4_layout_vfs_recall(
    const uint8_t                 *fh,
    uint16_t                       fh_len,
    enum chimera_vfs_recall_reason reason,
    void                          *arg)
{
    struct nfs_layout_table          *table  = arg;
    struct chimera_server_nfs_shared *shared = table->owner;
    struct nfs_layout_recall_waiter  *waiter;
    int                               total;

    /* The waiter does nothing when the last holder leaves.  It is parked only
     * so the table knows a recall is active on the file: LAYOUTGET is refused
     * with RECALLCONFLICT and the overdue sweep considers the holders. */
    waiter         = calloc(1, sizeof(*waiter));
    waiter->resume = nfs4_cb_detached_resume;

    total = nfs4_cb_recall_batches(NULL, table, fh, fh_len, waiter);

    if (total < 0) {
        /* The layouts went before the callback ran. */
        free(waiter);
        return;
    }

    if (shared) {
        nfs4_layout_recall_count(shared, reason == CHIMERA_VFS_RECALL_UMOUNT ?
                                 NFS4_LAYOUT_RECALL_UMOUNT : NFS4_LAYOUT_RECALL_UNLINKED);
    }

    if (total > 0) {
        chimera_nfs_info("pNFS: file %s, recalled layout from %d holder(s)",
                         reason == CHIMERA_VFS_RECALL_UMOUNT ? "is being unmounted" :
                         "unlinked and no longer open", total);
    } else {
        chimera_nfs_info("pNFS: joined a layout recall already in progress");
    }
} /* nfs4_layout_vfs_recall */

/* A waiter parked by chimera_nfs4_cb_recall_and_wait for `resume_arg`.  The
 * resume_arg of a parked waiter is alive until its resume runs, so the pointer
 * cannot be reused by another waiter meanwhile. */
static bool
nfs4_cb_waiter_is(
    const struct nfs_layout_recall_waiter *w,
    const void                            *resume_arg)
{
    const struct nfs4_cb_resume_ctx *rctx = w->arg;

    return w->resume == nfs4_cb_resume_bounce && rctx->arg == resume_arg;
} /* nfs4_cb_waiter_is */

bool
chimera_nfs4_cb_recall_cancel(
    struct chimera_server_nfs_thread *thread,
    const uint8_t                    *fh,
    uint32_t                          fhlen,
    void                             *resume_arg)
{
    struct nfs_layout_recall_waiter *w;

    w = nfs_layout_table_take_waiter(&thread->shared->nfs4_layout_table,
                                     fh, (uint16_t) fhlen,
                                     nfs4_cb_waiter_is, resume_arg);
    if (!w) {
        return false;
    }
    free(w->arg);
    free(w);
    return true;
} /* chimera_nfs4_cb_recall_cancel */

/*
 * Revoke every recalled layout still held two lease periods after its recall
 * (RFC 8881 12.5.5.2).  Only the layout goes -- the same local revoke a
 * declined or undeliverable recall gets -- so the client keeps its lease and
 * falls back to I/O through the metadata server.  Its deregistration
 * releases whatever waits on the file's recall: a SETATTR truncate or a pNFS
 * deferred delete.  Called from each thread's lease sweep; two threads may
 * collect the same holder, and the second destroy is a no-op.
 */
void
nfs4_cb_revoke_overdue_layouts(struct chimera_server_nfs_thread *thread)
{
    struct nfs_layout_state *overdue[NFS4_CB_MAX_HOLDERS];
    uint64_t                 deadline_s = (uint64_t) thread->shared->nfs_lease_time_s * 2;
    uint64_t                 max_age_ns = deadline_s * 1000000000ULL;
    int                      n, i;

    do {
        n = nfs_layout_table_collect_overdue(&thread->shared->nfs4_layout_table,
                                             nfs_lease_now_ns(), max_age_ns,
                                             overdue, NFS4_CB_MAX_HOLDERS);
        for (i = 0; i < n; i++) {
            chimera_nfs_error("pNFS: layout recall unanswered for %u s (two lease periods); "
                              "revoking that layout", (unsigned) deadline_s);
            nfs4_layout_revoke_count(thread->shared, NFS4_LAYOUT_REVOKE_DEADLINE);
            nfs_layout_state_destroy(overdue[i], &thread->shared->nfs4_state_table,
                                     thread->vfs_thread);
            nfs_client_put(overdue[i]->client);
            nfs_layout_state_put(overdue[i]);
        }
    } while (n == NFS4_CB_MAX_HOLDERS);
} /* nfs4_cb_revoke_overdue_layouts */
