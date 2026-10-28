// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* A GRANTED callback whose client cannot be reached must be logged and
 * dropped: a refused connect must not abort the server and a dead address
 * must cost one bounded probe, never libevpl's multi-minute connect.  A job
 * the retry timer ends, by a drop or by giving up, must be freed safely from
 * inside that timer's own callback. */
static int test_portmap_port;
#define NFS_PEER_PORTMAP_PORT        test_portmap_port
#define NLM_GRANT_CONNECT_TIMEOUT_MS 300
#define NLM_GRANT_RETRY_INTERVAL_US  50000
#include "../nfs_nlm_granted.c"
#include "common/mbt_watchdog.h"
#include <time.h>
#ifndef _WIN32
#include <netinet/in.h>
#include <arpa/inet.h>
#endif /* ifndef _WIN32 */
#include "common/socket.h"

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
    return CHIMERA_TCP_FLAVOR_PLAIN;
} /* nfs_callback_tcp_flavor */

/* Bind a loopback listener, report its port and return the socket. */
static chimera_socket_t
listen_on(int *port)
{
    struct sockaddr_in sin;
    socklen_t          len = sizeof(sin);
    chimera_socket_t   fd;

    fd = socket(AF_INET, SOCK_STREAM, 0);
    CHECK(fd != CHIMERA_INVALID_SOCKET);
    memset(&sin, 0, sizeof(sin));
    sin.sin_family      = AF_INET;
    sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    CHECK(bind(fd, (struct sockaddr *) &sin, sizeof(sin)) == 0);
    CHECK(listen(fd, 8) == 0);
    CHECK(getsockname(fd, (struct sockaddr *) &sin, &len) == 0);
    *port = ntohs(sin.sin_port);
    return fd;
} /* listen_on */

static int
closed_port(void)
{
    int              port;
    chimera_socket_t fd = listen_on(&port);

    chimera_socket_close(fd);
    return port;
} /* closed_port */

static long
now_ms(void)
{
#ifdef _WIN32
    return (long) GetTickCount64();
#else  /* ifdef _WIN32 */
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long) (ts.tv_sec * 1000 + ts.tv_nsec / 1000000);
#endif /* ifdef _WIN32 */
} /* now_ms */

static void
tick_cb(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    (void) evpl;
    (void) timer;
} /* tick_cb */

/* Pump the loop until the granter holds no job (limit_ms > 0, failing past
 * it) or for a fixed ms (limit_ms == 0).  A periodic tick keeps
 * evpl_continue from blocking once nothing else is pending. */
static void
pump(
    struct evpl          *evpl,
    struct nlm_grant_ctx *ctx,
    long                  limit_ms,
    long                  ms)
{
    struct evpl_timer tick;
    long              t0 = now_ms();

    evpl_add_timer(evpl, &tick, tick_cb, 10000);
    if (limit_ms > 0) {
        while (ctx->active) {
            CHECK(now_ms() - t0 < limit_ms);
            evpl_continue(evpl);
        }
    } else {
        while (now_ms() - t0 < ms) {
            evpl_continue(evpl);
        }
    }
    evpl_remove_timer(evpl, &tick);
} /* pump */

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
new_job(
    struct nlm_grant_ctx *ctx,
    const char           *addr)
{
    struct nlm_grant_job *job = calloc(1, sizeof(*job));

    CHECK(job != NULL);
    job->granter = ctx->granter;
    strcpy(job->req.client_addr, addr);
    strcpy(job->req.caller_name, "grant-unreachable-test");
    DL_APPEND(ctx->active, job);
    return job;
} /* new_job */

static void
test_portmap_refused(struct evpl *evpl)
{
    struct nlm_granter       granter;
    struct nlm_grant_ctx    *ctx = start(evpl, &granter);
    struct nlm_grant_request req = { 0 };

    test_portmap_port = closed_port();
    strcpy(req.client_addr, "127.0.0.1");
    strcpy(req.caller_name, "refused");
    nlm_grant_job_start(ctx, &req);
    CHECK(ctx->active == NULL);
    stop(evpl, ctx);
} /* test_portmap_refused */

static void
test_nlm_port_refused(struct evpl *evpl)
{
    struct nlm_granter    granter;
    struct nlm_grant_ctx *ctx = start(evpl, &granter);
    struct nlm_grant_job *job = new_job(ctx, "127.0.0.1");

    nlm_grant_getport_cb(ctx->evpl, NULL, closed_port(), 0, job);
    CHECK(ctx->active == NULL);
    stop(evpl, ctx);
} /* test_nlm_port_refused */

static void
test_dead_address(struct evpl *evpl)
{
    struct nlm_granter    granter;
    struct nlm_grant_ctx *ctx = start(evpl, &granter);
    struct nlm_grant_job *job = new_job(ctx, "192.0.2.1");
    long                  t0  = now_ms();

    nlm_grant_getport_cb(ctx->evpl, NULL, 32803, 0, job);
    CHECK(ctx->active == NULL);
    CHECK(now_ms() - t0 < 2000);
    stop(evpl, ctx);
} /* test_dead_address */

static void
test_retry_drops_lost_peer(struct evpl *evpl)
{
    struct nlm_granter    granter;
    struct nlm_grant_ctx *ctx = start(evpl, &granter);
    struct nlm_grant_job *job = new_job(ctx, "127.0.0.1");
    chimera_socket_t      lockd;
    int                   port;

    /* The client's lockd completes the handshake but never answers. */
    lockd = listen_on(&port);
    nlm_grant_getport_cb(ctx->evpl, NULL, port, 0, job);
    CHECK(job->nlm_conn != NULL);
    CHECK(job->inflight);

    /* Then it goes away with the GRANTED unanswered.  The connection is
     * lost, and the only way the job can end is a retry tick that finds
     * the port refused and drops it from inside the timer callback. */
    chimera_socket_close(lockd);
    pump(evpl, ctx, 3000, 0);

    /* A timer left armed on the freed job would fire during these ticks. */
    pump(evpl, ctx, 0, 5 * NLM_GRANT_RETRY_INTERVAL_US / 1000);
    stop(evpl, ctx);
} /* test_retry_drops_lost_peer */

static void
test_give_up_after_max_attempts(struct evpl *evpl)
{
    struct nlm_granter        granter;
    struct nlm_grant_ctx     *ctx = start(evpl, &granter);
    struct nlm_grant_job     *job = new_job(ctx, "127.0.0.1");
    struct NLM_V4             lockd;
    struct evpl_rpc2_program *programs[1];
    struct evpl_rpc2_server  *server;
    struct evpl_endpoint     *ep;
    int                       port = closed_port();
    long                      t0;

    /* A lockd with no GRANTED handler answers every call PROC_UNAVAIL, so
     * the granter re-sends on every tick until it gives up. */
    NLM_V4_init(&lockd);
    programs[0] = &lockd.rpc2;
    server      = evpl_rpc2_server_init(programs, 1);
    ep          = evpl_endpoint_create("127.0.0.1", port);
    CHECK(evpl_rpc2_server_start(server, EVPL_STREAM_SOCKET_TCP, ep) == 0);
    evpl_rpc2_server_attach(ctx->rpc2_thread, server, &lockd);

    t0 = now_ms();
    nlm_grant_getport_cb(ctx->evpl, NULL, port, 0, job);
    CHECK(job->inflight);
    pump(evpl, ctx, 5000, 0);
    /* Every attempt after the first waited for its own tick. */
    CHECK(now_ms() - t0 >= (NLM_GRANT_MAX_ATTEMPTS - 1) *
          (NLM_GRANT_RETRY_INTERVAL_US / 1000));

    /* A timer left armed on the freed job would fire during these ticks. */
    pump(evpl, ctx, 0, 5 * NLM_GRANT_RETRY_INTERVAL_US / 1000);

    evpl_rpc2_server_stop(server);
    evpl_rpc2_server_detach(ctx->rpc2_thread, server);
    stop(evpl, ctx);
    evpl_rpc2_server_destroy(server);
} /* test_give_up_after_max_attempts */

int
main(void)
{
    struct evpl *evpl;

#ifdef _WIN32
    WSADATA      wsa;

    CHECK(WSAStartup(MAKEWORD(2, 2), &wsa) == 0);
#endif /* ifdef _WIN32 */
    evpl = evpl_create(NULL);
    mbt_watchdog_arm(15);
    test_portmap_refused(evpl);
    test_nlm_port_refused(evpl);
    test_dead_address(evpl);
    test_retry_drops_lost_peer(evpl);
    test_give_up_after_max_attempts(evpl);
    mbt_watchdog_disarm();
    evpl_destroy(evpl);
    puts("PASS: NLM grant drops unreachable peers and ends retried jobs safely");
    return 0;
} /* main */
