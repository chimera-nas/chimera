// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Drive the granter on our event loop so connection loss and shutdown have
 * a deterministic ordering. Use real RPC2/inproc callbacks, not mocked frees. */
#include "../nfs_nlm_granted.c"
#include "common/mbt_watchdog.h"

#define CHECK(cond) \
        do { \
            if (!(cond)) { \
                fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
                abort(); \
            } \
        } while (0)

enum chimera_tcp_flavor
nfs_callback_tcp_flavor(void)
{
    return CHIMERA_TCP_FLAVOR_INPROC;
} /* nfs_callback_tcp_flavor */

static struct nlm_grant_ctx *
start(
    struct evpl        *evpl,
    struct nlm_granter *granter)
{
    memset(granter, 0, sizeof(*granter));
    evpl_mutex_init(&granter->lock, NULL);
    return nlm_granter_thread_init(evpl, granter);
} /* start */

static void
stop(
    struct evpl          *evpl,
    struct nlm_grant_ctx *ctx)
{
    struct nlm_granter *granter = ctx->granter;

    nlm_granter_thread_shutdown(evpl, ctx);
    evpl_mutex_destroy(&granter->lock);
} /* stop */

static struct nlm_grant_job *
start_grant(struct nlm_grant_ctx *ctx)
{
    struct nlm_grant_job *job = calloc(1, sizeof(*job));

    CHECK(job != NULL);
    job->granter = ctx->granter;
    strcpy(job->req.client_addr, "127.0.0.1");
    strcpy(job->req.caller_name, "grant-disconnect-test");
    DL_APPEND(ctx->active, job);
    /* The portmapper has returned a port whose NLM service is unavailable. */
    nlm_grant_getport_cb(ctx->evpl, NULL, 32803, 0, job);
    CHECK(job->inflight);
    return job;
} /* start_grant */

static void
test_lost_connection(struct evpl *evpl)
{
    struct nlm_granter    granter;
    struct nlm_grant_ctx *ctx = start(evpl, &granter);
    struct nlm_grant_job *job = start_grant(ctx);

    while (job->inflight) {
        evpl_continue(evpl);
    }
    /* Before the fix this disconnects the RPC2 connection already freed by
     * the refusal event: the exact ASan use-after-free seen in the queue. */
    nlm_grant_job_finish(job);
    CHECK(ctx->active == NULL);
    stop(evpl, ctx);
} /* test_lost_connection */

static int acknowledgements;

static void
grant_received(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_testargs      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct NLM_V4  *nlm   = private_data;
    struct nlm4_res reply = { 0 };

    (void) conn;
    (void) cred;
    reply.cookie = args->cookie;
    reply.stat   = NLM4_GRANTED;
    acknowledgements++;
    CHECK(nlm->send_reply_NLMPROC4_GRANTED(evpl, NULL, &reply, encoding) == 0);
} /* grant_received */

static void
test_retry_and_ack(struct evpl *evpl)
{
    struct nlm_granter        granter;
    struct nlm_grant_ctx     *ctx        = start(evpl, &granter);
    struct nlm_grant_job     *job        = start_grant(ctx);
    struct evpl_rpc2_program *programs[] = { &ctx->nlm.rpc2 };
    struct evpl_rpc2_server  *server;

    while (job->inflight) {
        evpl_continue(evpl);
    }
    CHECK(job->nlm_conn == NULL);
    CHECK(job->attempts == 1);

    /* The client returns before the retry: reconnect and deliver the grant. */
    ctx->nlm.recv_call_NLMPROC4_GRANTED = grant_received;
    server                              = evpl_rpc2_server_init(programs, 1);
    CHECK(evpl_rpc2_server_start(server, EVPL_STREAM_INPROC, job->nlm_ep) == 0);
    evpl_rpc2_server_attach(ctx->rpc2_thread, server, &ctx->nlm);
    nlm_grant_retry_timer_cb(evpl, &job->retry_timer);
    CHECK(job->attempts == 2);
    while (ctx->active) {
        evpl_continue(evpl);
    }
    CHECK(acknowledgements == 1);
    evpl_rpc2_server_stop(server);
    evpl_rpc2_server_detach(ctx->rpc2_thread, server);
    stop(evpl, ctx);
    evpl_rpc2_server_destroy(server);
} /* test_retry_and_ack */

static void
test_shutdown_pending(
    struct evpl *evpl,
    int          portmap)
{
    struct nlm_granter       granter;
    struct nlm_grant_ctx    *ctx = start(evpl, &granter);
    struct nlm_grant_request req = { 0 };

    if (portmap) {
        strcpy(req.client_addr, "127.0.0.1");
        nlm_grant_job_start(ctx, &req);
    } else {
        start_grant(ctx);
    }
    CHECK(ctx->active != NULL);
    /* Shutdown closes the connection before its pending reply callback runs.
     * Both GETPORT and GRANTED callbacks must still have a live job. */
    stop(evpl, ctx);
} /* test_shutdown_pending */

static void
test_portmap_refusal(struct evpl *evpl)
{
    struct nlm_granter       granter;
    struct nlm_grant_ctx    *ctx = start(evpl, &granter);
    struct nlm_grant_request req = { 0 };

    strcpy(req.client_addr, "127.0.0.1");
    nlm_grant_job_start(ctx, &req);
    while (ctx->active) {
        evpl_continue(evpl);
    }
    stop(evpl, ctx);
} /* test_portmap_refusal */

int
main(void)
{
    struct evpl *evpl = evpl_create(NULL);

    mbt_watchdog_arm(15);
    test_lost_connection(evpl);
    test_retry_and_ack(evpl);
    test_shutdown_pending(evpl, 0);
    test_shutdown_pending(evpl, 1);
    test_portmap_refusal(evpl);
    mbt_watchdog_disarm();
    evpl_destroy(evpl);
    puts("PASS: NLM grant disconnect, reconnect, acknowledgement and shutdown");
    return 0;
} /* main */
