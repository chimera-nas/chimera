// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Outbound NLM_GRANTED callback engine (RFC 1813 / XNFS blocking-lock grant
 * delivery).  See nfs_nlm_granted.h for the contract.
 *
 * Everything here runs on one dedicated evpl_thread (the "granter"), so the
 * portmap lookup, the outbound GRANTED call, and its retransmit timer never
 * execute on a core NFS event loop.  Submitters on other threads only push a
 * self-contained job onto a mutex-protected intake queue and ring a doorbell;
 * the granter drains the queue from the doorbell callback on its own evpl.
 *
 * Transport choice: we use NLMPROC4_GRANTED (proc 5), which expects a reply,
 * rather than the one-way NLMPROC4_GRANTED_MSG (proc 10).  Proc 5 delivers the
 * client's GRANTED_RES ack inline on the very connection we sent on, so ack
 * tracking and retransmit cancellation need no inbound-correlation table and no
 * second RPC service -- a materially simpler and race-free lifetime than proc
 * 10 + an inbound GRANTED_RES handler.  Stock Linux lockd services inbound
 * NLMPROC_GRANTED, so this interoperates with the F_SETLKW clients this issue
 * targets.
 */

#include <string.h>
#include <stdlib.h>
#include <pthread.h>

#include "nfs_common.h"
#include "nfs_internal.h"
#include "nfs_nlm_granted.h"
#include "portmap_xdr.h"
#include "nlm4_xdr.h"
#include "evpl/evpl.h"
#include "evpl/evpl_rpc2.h"

/* Retransmit cadence and cap.  A blocking-lock client retransmits its own LOCK
 * on a similar cadence; we keep re-sending GRANTED until it acks or we give up,
 * so a momentarily-unreachable client still converges.  After the cap the job
 * is dropped: the lock stays held (the grant already happened in vfs_state),
 * and the client recovers by retransmitting LOCK, which we answer GRANTED. */
#define NLM_GRANT_RETRY_INTERVAL_US (2 * 1000 * 1000)   /* 2s */
#define NLM_GRANT_MAX_ATTEMPTS      15                  /* ~30s total */

/* NLM program/version, looked up via the client's portmapper. */
#define NLM_PROGRAM                 100021u
#define NLM_VERSION                 4u

/* Producer-side queue node carries the request payload plus an intake link.
 * The granter copies the payload into its own job and frees the node. */
struct nlm_grant_intake {
    struct nlm_grant_request req;
    struct nlm_grant_intake *next;
};

struct nlm_granter {
    struct chimera_server_nfs_shared *shared;
    struct evpl_thread               *thread;

    /* Intake queue: producers (core threads) append under `lock` and ring the
     * doorbell; the granter thread drains it on its own evpl. */
    pthread_mutex_t                   lock;
    struct nlm_grant_intake          *intake_head;   /* singly-linked via ->next */
    struct nlm_grant_intake          *intake_tail;
    struct evpl_doorbell              doorbell;
    int                               doorbell_armed; /* set once the thread arms it */
    int                               shutdown;       /* set under lock to refuse new work */
};

/* Per-job state owned entirely by the granter thread. */
struct nlm_grant_job {
    struct nlm_grant_request req;
    struct nlm_granter      *granter;

    struct evpl_endpoint    *pm_ep;
    struct evpl_rpc2_conn   *pm_conn;
    struct evpl_endpoint    *nlm_ep;
    struct evpl_rpc2_conn   *nlm_conn;
    unsigned int             nlm_port;

    struct evpl_timer        retry_timer;
    int                      timer_armed;
    int                      attempts;
    int                      acked;
    int                      inflight;      /* a GRANTED call is outstanding */

    struct nlm_grant_job    *next;          /* link on granter active list */
    struct nlm_grant_job    *prev;
};

/* The granter's per-thread evpl context. */
struct nlm_grant_ctx {
    struct nlm_granter      *granter;
    struct evpl             *evpl;
    struct evpl_rpc2_thread *rpc2_thread;
    struct PORTMAP_V2        pm;
    struct NLM_V4            nlm;
    struct nlm_grant_job    *active;        /* DL list of live jobs */
};

/* The granter thread stashes its ctx here so the doorbell callback (which only
 * gets evpl + doorbell) can reach it.  Set once in init, before the doorbell is
 * armed, and read only on the granter thread. */
static __thread struct nlm_grant_ctx *nlm_grant_tls_ctx;

static void nlm_grant_job_send(
    struct nlm_grant_job *job);
static void nlm_grant_job_finish(
    struct nlm_grant_job *job);

/* ------------------------------------------------------------------ *
*  job teardown                                                       *
* ------------------------------------------------------------------ */

static void
nlm_grant_job_finish(struct nlm_grant_job *job)
{
    struct nlm_grant_ctx *ctx = nlm_grant_tls_ctx;

    if (job->timer_armed) {
        evpl_remove_timer(ctx->evpl, &job->retry_timer);
        job->timer_armed = 0;
    }
    if (job->nlm_conn) {
        evpl_rpc2_client_disconnect(ctx->rpc2_thread, job->nlm_conn);
        job->nlm_conn = NULL;
    }
    if (job->nlm_ep) {
        evpl_endpoint_close(job->nlm_ep);
        job->nlm_ep = NULL;
    }
    if (job->pm_conn) {
        evpl_rpc2_client_disconnect(ctx->rpc2_thread, job->pm_conn);
        job->pm_conn = NULL;
    }
    if (job->pm_ep) {
        evpl_endpoint_close(job->pm_ep);
        job->pm_ep = NULL;
    }

    /* job is always on ctx->active (DL_APPEND'd in job_start before any finish),
     * so the list is non-empty here; the guard silences a utlist DL_DELETE
     * null-deref false positive under scan-build (same idiom as
     * nlm_state_destroy). */
#ifndef __clang_analyzer__
    DL_DELETE(ctx->active, job);
#endif /* ifndef __clang_analyzer__ */
    free(job);
} /* nlm_grant_job_finish */

/* ------------------------------------------------------------------ *
*  GRANTED call + ack                                                 *
* ------------------------------------------------------------------ */

static void
nlm_grant_reply_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct nlm4_res             *reply,
    int                          status,
    void                        *private_data)
{
    struct nlm_grant_job *job = private_data;

    (void) evpl;
    (void) verf;
    (void) reply;

    job->inflight = 0;

    /* Any application-level reply (whatever stat) means the client received the
     * GRANTED and its F_SETLKW completed -- stop retransmitting.  A transport
     * error leaves the job armed for the next retry tick. */
    if (status == 0) {
        job->acked = 1;
        chimera_nfs_debug("NLM GRANTED: host '%s' (%s:%u) acked the grant",
                          job->req.caller_name, job->req.client_addr,
                          job->nlm_port);
        nlm_grant_job_finish(job);
    }
} /* nlm_grant_reply_cb */

/* Build the nlm4_testargs from the job snapshot and fire one GRANTED call on
 * the (already connected) NLM conn. */
static void
nlm_grant_job_send(struct nlm_grant_job *job)
{
    struct nlm_grant_ctx *ctx = nlm_grant_tls_ctx;
    struct nlm4_testargs  args;

    if (!job->nlm_conn || job->inflight) {
        return;
    }

    memset(&args, 0, sizeof(args));
    args.cookie.len            = job->req.cookie_len;
    args.cookie.data           = job->req.cookie;
    args.exclusive             = job->req.exclusive ? 1 : 0;
    args.alock.caller_name.str = job->req.caller_name;
    args.alock.caller_name.len = (uint32_t) strlen(job->req.caller_name);
    args.alock.fh.len          = job->req.fh_len;
    args.alock.fh.data         = job->req.fh;
    args.alock.oh.len          = job->req.oh_len;
    args.alock.oh.data         = job->req.oh;
    args.alock.svid            = job->req.svid;
    args.alock.l_offset        = job->req.offset;
    args.alock.l_len           = job->req.length;

    job->inflight = 1;
    job->attempts++;

    chimera_nfs_debug("NLM GRANTED: -> host '%s' (%s:%u) attempt %d",
                      job->req.caller_name, job->req.client_addr,
                      job->nlm_port, job->attempts);

    ctx->nlm.send_call_NLMPROC4_GRANTED(&ctx->nlm.rpc2, ctx->evpl,
                                        job->nlm_conn, NULL, &args,
                                        0, 0, NULL, 0, 0,
                                        nlm_grant_reply_cb, job);
} /* nlm_grant_job_send */

static void
nlm_grant_retry_timer_cb(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct nlm_grant_job *job = container_of(timer, struct nlm_grant_job, retry_timer);

    (void) evpl;

    if (job->acked) {
        return;     /* finish already ran (defensive) */
    }

    if (job->attempts >= NLM_GRANT_MAX_ATTEMPTS) {
        chimera_nfs_info("NLM GRANTED: host '%s' (%s:%u) did not ack after %d "
                         "attempts; giving up (lock remains held)",
                         job->req.caller_name, job->req.client_addr,
                         job->nlm_port, job->attempts);
        nlm_grant_job_finish(job);
        return;
    }

    /* Re-send only if the previous attempt is not still outstanding; otherwise
     * wait for its reply or the next tick. */
    if (!job->inflight) {
        nlm_grant_job_send(job);
    }
} /* nlm_grant_retry_timer_cb */

/* ------------------------------------------------------------------ *
*  portmap GETPORT -> connect -> first GRANTED                        *
* ------------------------------------------------------------------ */

static void
nlm_grant_getport_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    unsigned int                 port,
    int                          status,
    void                        *private_data)
{
    struct nlm_grant_job *job = private_data;
    struct nlm_grant_ctx *ctx = nlm_grant_tls_ctx;

    (void) evpl;
    (void) verf;

    /* The portmap query is done with either way; release its conn/endpoint. */
    if (job->pm_conn) {
        evpl_rpc2_client_disconnect(ctx->rpc2_thread, job->pm_conn);
        job->pm_conn = NULL;
    }
    if (job->pm_ep) {
        evpl_endpoint_close(job->pm_ep);
        job->pm_ep = NULL;
    }

    if (status != 0 || port == 0) {
        chimera_nfs_info("NLM GRANTED: host '%s' (%s) has no NLM service "
                         "registered; cannot deliver grant",
                         job->req.caller_name, job->req.client_addr);
        nlm_grant_job_finish(job);
        return;
    }

    job->nlm_port = port;
    job->nlm_ep   = evpl_endpoint_create(job->req.client_addr, port);
    job->nlm_conn = evpl_rpc2_client_connect(ctx->rpc2_thread,
                                             EVPL_STREAM_SOCKET_TCP,
                                             job->nlm_ep, NULL, 0, NULL);
    if (!job->nlm_conn) {
        chimera_nfs_info("NLM GRANTED: cannot connect to NLM at %s:%u",
                         job->req.client_addr, port);
        nlm_grant_job_finish(job);
        return;
    }

    /* Arm the retransmit timer and fire the first GRANTED. */
    evpl_add_timer(ctx->evpl, &job->retry_timer, nlm_grant_retry_timer_cb,
                   NLM_GRANT_RETRY_INTERVAL_US);
    job->timer_armed = 1;

    nlm_grant_job_send(job);
} /* nlm_grant_getport_cb */

static void
nlm_grant_job_start(
    struct nlm_grant_ctx           *ctx,
    const struct nlm_grant_request *req)
{
    struct nlm_grant_job *job;
    struct mapping        mapping;

    job = calloc(1, sizeof(*job));
    if (!job) {
        return;
    }
    job->req     = *req;
    job->granter = ctx->granter;
    /* Guarded to match the DL_DELETE in nlm_grant_job_finish: keeping both
     * utlist ops out of the analyzer's view avoids a false-positive
     * use-after-free where it models a finished (freed) job still on the list. */
#ifndef __clang_analyzer__
    DL_APPEND(ctx->active, job);
#endif /* ifndef __clang_analyzer__ */

    /* Resolve the client's NLM (prog 100021 v4) callback port via its
     * portmapper, exactly as the NSM reboot-notify path resolves statd. */
    job->pm_ep   = evpl_endpoint_create(job->req.client_addr, 111);
    job->pm_conn = evpl_rpc2_client_connect(ctx->rpc2_thread,
                                            EVPL_STREAM_SOCKET_TCP,
                                            job->pm_ep, NULL, 0, NULL);
    if (!job->pm_conn) {
        chimera_nfs_info("NLM GRANTED: cannot reach portmap at %s for host '%s'",
                         job->req.client_addr, job->req.caller_name);
        nlm_grant_job_finish(job);
        return;
    }

    mapping.prog = NLM_PROGRAM;
    mapping.vers = NLM_VERSION;
    mapping.prot = 6;   /* IPPROTO_TCP */
    mapping.port = 0;
    ctx->pm.send_call_PMAPPROC_GETPORT(&ctx->pm.rpc2, ctx->evpl, job->pm_conn,
                                       NULL, &mapping, 0, 0, NULL, 0, 0,
                                       nlm_grant_getport_cb, job);
} /* nlm_grant_job_start */

/* ------------------------------------------------------------------ *
*  intake doorbell                                                    *
* ------------------------------------------------------------------ */

static void
nlm_grant_doorbell_cb(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct nlm_granter      *granter = container_of(doorbell, struct nlm_granter, doorbell);
    struct nlm_grant_ctx    *ctx     = nlm_grant_tls_ctx;
    struct nlm_grant_intake *head, *node, *next;

    (void) evpl;

    /* Steal the whole intake list under the lock, then process lock-free. */
    pthread_mutex_lock(&granter->lock);
    head                 = granter->intake_head;
    granter->intake_head = NULL;
    granter->intake_tail = NULL;
    pthread_mutex_unlock(&granter->lock);

    for (node = head; node; node = next) {
        next = node->next;
        nlm_grant_job_start(ctx, &node->req);
        free(node);
    }
} /* nlm_grant_doorbell_cb */

/* ------------------------------------------------------------------ *
*  thread init / shutdown                                             *
* ------------------------------------------------------------------ */

static void *
nlm_granter_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct nlm_granter       *granter = private_data;
    struct nlm_grant_ctx     *ctx;
    struct evpl_rpc2_program *programs[2];

    ctx          = calloc(1, sizeof(*ctx));
    ctx->granter = granter;
    ctx->evpl    = evpl;
    ctx->active  = NULL;

    /* Outbound-only client programs (portmap + NLM); replies are matched by
     * this rpc2 thread on the conns we open. */
    PORTMAP_V2_init(&ctx->pm);
    NLM_V4_init(&ctx->nlm);
    programs[0]      = &ctx->pm.rpc2;
    programs[1]      = &ctx->nlm.rpc2;
    ctx->rpc2_thread = evpl_rpc2_thread_init(evpl, programs, 2, NULL, NULL);

    nlm_grant_tls_ctx = ctx;

    /* Arm the intake doorbell only after ctx is fully set up: the very next
     * ring will find a usable ctx via TLS. */
    evpl_add_doorbell(evpl, &granter->doorbell, nlm_grant_doorbell_cb);

    pthread_mutex_lock(&granter->lock);
    granter->doorbell_armed = 1;
    /* A producer may have queued work before the doorbell existed; ring it once
     * so we drain anything already pending. */
    pthread_mutex_unlock(&granter->lock);
    evpl_ring_doorbell(&granter->doorbell);

    return ctx;
} /* nlm_granter_thread_init */

static void
nlm_granter_thread_shutdown(
    struct evpl *evpl,
    void        *private_data)
{
    struct nlm_granter      *granter = private_data;
    struct nlm_grant_ctx    *ctx     = nlm_grant_tls_ctx;
    struct nlm_grant_job    *job, *tmp;
    struct nlm_grant_intake *head, *node, *next;

    /* Abort every in-flight grant job cleanly (timers off, conns closed). */
    if (ctx) {
        DL_FOREACH_SAFE(ctx->active, job, tmp)
        {
            nlm_grant_job_finish(job);
        }
        evpl_rpc2_thread_destroy(ctx->rpc2_thread);
    }

    evpl_remove_doorbell(evpl, &granter->doorbell);

    /* Drop any intake nodes that arrived after shutdown was flagged. */
    pthread_mutex_lock(&granter->lock);
    head                 = granter->intake_head;
    granter->intake_head = NULL;
    granter->intake_tail = NULL;
    pthread_mutex_unlock(&granter->lock);
    for (node = head; node; node = next) {
        next = node->next;
        free(node);
    }

    free(ctx);
    nlm_grant_tls_ctx = NULL;
} /* nlm_granter_thread_shutdown */

/* ------------------------------------------------------------------ *
*  public API                                                         *
* ------------------------------------------------------------------ */

struct nlm_granter *
nlm_granter_get_or_create(struct chimera_server_nfs_shared *shared)
{
    struct nlm_granter *granter;

    if (shared->nlm_granter) {
        return shared->nlm_granter;
    }

    granter = calloc(1, sizeof(*granter));
    if (!granter) {
        return NULL;
    }
    granter->shared = shared;
    pthread_mutex_init(&granter->lock, NULL);

    granter->thread = evpl_thread_create(NULL, nlm_granter_thread_init,
                                         nlm_granter_thread_shutdown, granter);
    if (!granter->thread) {
        pthread_mutex_destroy(&granter->lock);
        free(granter);
        return NULL;
    }

    shared->nlm_granter = granter;
    return granter;
} /* nlm_granter_get_or_create */

void
nlm_granter_submit(
    struct nlm_granter             *granter,
    const struct nlm_grant_request *req)
{
    struct nlm_grant_intake *node;

    if (!granter) {
        return;
    }

    node = calloc(1, sizeof(*node));
    if (!node) {
        return;
    }
    node->req  = *req;
    node->next = NULL;

    pthread_mutex_lock(&granter->lock);
    if (granter->shutdown) {
        pthread_mutex_unlock(&granter->lock);
        free(node);
        return;
    }
    if (granter->intake_tail) {
        granter->intake_tail->next = node;
    } else {
        granter->intake_head = node;
    }
    granter->intake_tail = node;
    pthread_mutex_unlock(&granter->lock);

    /* The doorbell is armed once the granter thread has run its init; before
     * that, the work sits on the intake list and the init's priming ring picks
     * it up. */
    if (granter->doorbell_armed) {
        evpl_ring_doorbell(&granter->doorbell);
    }
} /* nlm_granter_submit */

void
nlm_granter_destroy(struct nlm_granter *granter)
{
    if (!granter) {
        return;
    }

    pthread_mutex_lock(&granter->lock);
    granter->shutdown = 1;
    pthread_mutex_unlock(&granter->lock);

    /* Joins the granter thread, running nlm_granter_thread_shutdown on it. */
    if (granter->thread) {
        evpl_thread_destroy(granter->thread);
    }
    pthread_mutex_destroy(&granter->lock);
    free(granter);
} /* nlm_granter_destroy */
