// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* The reboot notify must skip a monitored host whose portmapper or statd
 * cannot be reached: a refused connect must not abort the server, a dead
 * address must cost one bounded probe, never libevpl's multi-minute connect,
 * and a peer that closes the connection mid-call must not leave the worker
 * touching a connection rpc2 already freed.  The worker must also be
 * started without parking the spawning thread. */
#include "common/tcp_flavor.h"
static int                     test_portmap_port;
static enum chimera_tcp_flavor test_flavor = CHIMERA_TCP_FLAVOR_PLAIN;
#define NFS_PEER_PORTMAP_PORT         test_portmap_port
#define NSM_NOTIFY_CONNECT_TIMEOUT_MS 300
#define NSM_NOTIFY_TIMEOUT_SECS       1
#include "../nfs_nsm_notify.c"
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
    return test_flavor;
} /* nfs_callback_tcp_flavor */

static int notified;
static int statd_port;

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

/* A peer service that accepts each connection and closes it at once, as
 * an rpcbind denied by TCP wrappers does.  It runs on its own thread so it
 * answers while nsm_notify_one pumps the loop. */
struct closer {
    chimera_socket_t     fd;
    int                  port;
    _Atomic int          stop;
    evpl_native_thread_t thread;
};

static void *
closer_main(void *arg)
{
    struct closer   *c = arg;
    chimera_socket_t s;

    while (!atomic_load(&c->stop)) {
        s = accept(c->fd, NULL, NULL);
        if (s == CHIMERA_INVALID_SOCKET) {
            break;
        }
        chimera_socket_close(s);
    }
    return NULL;
} /* closer_main */

static void
closer_start(struct closer *c)
{
    c->fd = listen_on(&c->port);
    atomic_store(&c->stop, 0);
    CHECK(evpl_native_thread_create(&c->thread, NULL, closer_main, c) == 0);
} /* closer_start */

static void
closer_stop(struct closer *c)
{
    struct sockaddr_in sin;
    chimera_socket_t   s;

    /* Wake the blocked accept with one last connection. */
    atomic_store(&c->stop, 1);
    s = socket(AF_INET, SOCK_STREAM, 0);
    CHECK(s != CHIMERA_INVALID_SOCKET);
    memset(&sin, 0, sizeof(sin));
    sin.sin_family      = AF_INET;
    sin.sin_port        = htons((uint16_t) c->port);
    sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    CHECK(connect(s, (struct sockaddr *) &sin, sizeof(sin)) == 0);
    chimera_socket_close(s);
    CHECK(evpl_native_thread_join(c->thread, NULL) == 0);
    chimera_socket_close(c->fd);
} /* closer_stop */

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

struct fixture {
    struct PORTMAP_V2         pm;
    struct SM_INTER_V1        sm;
    struct evpl_rpc2_program *programs[2];
    struct evpl_rpc2_thread  *rpc2_thread;
    struct nsm_rpc_state     *keep;
};

static void
fixture_init(
    struct fixture *f,
    struct evpl    *evpl)
{
    PORTMAP_V2_init(&f->pm);
    SM_INTER_V1_init(&f->sm);
    f->programs[0] = &f->pm.rpc2;
    f->programs[1] = &f->sm.rpc2;
    f->keep        = NULL;
    f->rpc2_thread = nsm_rpc2_thread_init(evpl, f->programs);
    notified       = 0;
} /* fixture_init */

static void
fixture_fini(struct fixture *f)
{
    struct nsm_rpc_state *rs;

    evpl_rpc2_thread_destroy(f->rpc2_thread);
    while (f->keep) {
        rs      = f->keep;
        f->keep = rs->next;
        free(rs);
    }
} /* fixture_fini */

/* Notify one host at addr; returns nsm_notify_one's verdict. */
static int
notify_target(
    struct evpl *evpl,
    const char  *addr)
{
    struct fixture           f;
    struct nsm_notify_target t = { { 0 }, { 0 } };
    int                      told;

    strcpy(t.host, "nsm-test-host");
    strcpy(t.addr, addr);
    fixture_init(&f, evpl);
    told = nsm_notify_one(evpl, f.rpc2_thread, &f.pm, &f.sm, &t, "chimera-test", 3, &f.keep);
    fixture_fini(&f);
    return told;
} /* notify_target */

static void
test_portmap_refused(struct evpl *evpl)
{
    long t0 = now_ms();

    test_portmap_port = closed_port();
    CHECK(notify_target(evpl, "127.0.0.1") == 0);
    CHECK(notified == 0);
    CHECK(now_ms() - t0 < 2000);
} /* test_portmap_refused */

static void
test_dead_host_skipped(struct evpl *evpl)
{
    long t0 = now_ms();

    test_portmap_port = 111;
    CHECK(notify_target(evpl, "192.0.2.1") == 0);
    CHECK(notified == 0);
    CHECK(now_ms() - t0 < 2000);
} /* test_dead_host_skipped */

static void
getport_received(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct mapping            *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct PORTMAP_V2 *pm = private_data;

    (void) conn;
    (void) cred;
    (void) args;
    CHECK(pm->send_reply_PMAPPROC_GETPORT(evpl, NULL, (uint32_t) statd_port,
                                          encoding) == 0);
} /* getport_received */

static void
notify_received(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct stat_chge          *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct SM_INTER_V1 *sm = private_data;

    (void) conn;
    (void) cred;
    CHECK(args->state == 3);
    notified++;
    CHECK(sm->send_reply_SM_NOTIFY(evpl, NULL, encoding) == 0);
} /* notify_received */

static void
test_live_host_told(struct evpl *evpl)
{
    struct fixture            f;
    struct nsm_notify_target  t = { { 0 }, { 0 } };
    struct evpl_rpc2_program *pm_progs[1];
    struct evpl_rpc2_program *sm_progs[1];
    struct evpl_rpc2_server  *pm_server, *sm_server;
    struct evpl_endpoint     *pm_ep, *sm_ep;

    test_portmap_port = closed_port();
    statd_port        = closed_port();
    strcpy(t.host, "nsm-test-host");
    strcpy(t.addr, "127.0.0.1");
    fixture_init(&f, evpl);

    f.pm.recv_call_PMAPPROC_GETPORT = getport_received;
    f.sm.recv_call_SM_NOTIFY        = notify_received;
    pm_progs[0]                     = &f.pm.rpc2;
    sm_progs[0]                     = &f.sm.rpc2;
    pm_server                       = evpl_rpc2_server_init(pm_progs, 1);
    sm_server                       = evpl_rpc2_server_init(sm_progs, 1);
    pm_ep                           = evpl_endpoint_create("127.0.0.1", test_portmap_port);
    sm_ep                           = evpl_endpoint_create("127.0.0.1", statd_port);
    CHECK(evpl_rpc2_server_start(pm_server, EVPL_STREAM_SOCKET_TCP, pm_ep) == 0);
    CHECK(evpl_rpc2_server_start(sm_server, EVPL_STREAM_SOCKET_TCP, sm_ep) == 0);
    evpl_rpc2_server_attach(f.rpc2_thread, pm_server, &f.pm);
    evpl_rpc2_server_attach(f.rpc2_thread, sm_server, &f.sm);

    CHECK(nsm_notify_one(evpl, f.rpc2_thread, &f.pm, &f.sm, &t, "chimera-test",
                         3, &f.keep) == 1);
    CHECK(notified == 1);

    evpl_rpc2_server_stop(pm_server);
    evpl_rpc2_server_stop(sm_server);
    evpl_rpc2_server_detach(f.rpc2_thread, pm_server);
    evpl_rpc2_server_detach(f.rpc2_thread, sm_server);
    fixture_fini(&f);
    evpl_rpc2_server_destroy(pm_server);
    evpl_rpc2_server_destroy(sm_server);
} /* test_live_host_told */

static void
test_portmap_closes(struct evpl *evpl)
{
    struct closer pm;

    /* The GETPORT call is completed with CONN_LOST and rpc2 frees the
     * connection before the worker would disconnect it. */
    closer_start(&pm);
    test_portmap_port = pm.port;
    CHECK(notify_target(evpl, "127.0.0.1") == 0);
    CHECK(notified == 0);
    closer_stop(&pm);
} /* test_portmap_closes */

static void
test_statd_closes(struct evpl *evpl)
{
    struct fixture            f;
    struct nsm_notify_target  t = { { 0 }, { 0 } };
    struct evpl_rpc2_program *pm_progs[1];
    struct evpl_rpc2_server  *pm_server;
    struct evpl_endpoint     *pm_ep;
    struct closer             statd;

    /* The portmapper answers; the statd it names closes on SM_NOTIFY. */
    closer_start(&statd);
    statd_port        = statd.port;
    test_portmap_port = closed_port();
    strcpy(t.host, "nsm-test-host");
    strcpy(t.addr, "127.0.0.1");
    fixture_init(&f, evpl);

    f.pm.recv_call_PMAPPROC_GETPORT = getport_received;
    pm_progs[0]                     = &f.pm.rpc2;
    pm_server                       = evpl_rpc2_server_init(pm_progs, 1);
    pm_ep                           = evpl_endpoint_create("127.0.0.1", test_portmap_port);
    CHECK(evpl_rpc2_server_start(pm_server, EVPL_STREAM_SOCKET_TCP, pm_ep) == 0);
    evpl_rpc2_server_attach(f.rpc2_thread, pm_server, &f.pm);

    /* The call completes with CONN_LOST: not counted as told. */
    CHECK(nsm_notify_one(evpl, f.rpc2_thread, &f.pm, &f.sm, &t, "chimera-test",
                         3, &f.keep) == 0);
    CHECK(notified == 0);

    evpl_rpc2_server_stop(pm_server);
    evpl_rpc2_server_detach(f.rpc2_thread, pm_server);
    fixture_fini(&f);
    evpl_rpc2_server_destroy(pm_server);
    closer_stop(&statd);
} /* test_statd_closes */

static void
test_inproc_skips_probe(void)
{
    long t0 = now_ms();

    test_flavor = CHIMERA_TCP_FLAVOR_INPROC;
    CHECK(nsm_peer_reachable("192.0.2.1", 111) == 1);
    CHECK(now_ms() - t0 < 100);
    test_flavor = CHIMERA_TCP_FLAVOR_PLAIN;
} /* test_inproc_skips_probe */

static struct nsm_notify_job *
new_job(uint32_t state)
{
    struct nsm_notify_job *job = calloc(1, sizeof(*job));

    CHECK(job != NULL);
    job->targets = calloc(1, sizeof(*job->targets));
    CHECK(job->targets != NULL);
    job->count = 1;
    job->state = state;
    strcpy(job->targets[0].host, "nsm-test-host");
    strcpy(job->targets[0].addr, "127.0.0.1");
    strcpy(job->my_name, "chimera-test");
    return job;
} /* new_job */

static void
test_spawn_returns_at_once(void)
{
    struct nsm_state nsm;
    chimera_socket_t listener;
    long             t0;

    /* The kernel completes the handshake but nobody answers GETPORT, so the
     * worker spends NSM_NOTIFY_TIMEOUT_SECS pumping inside its init. */
    memset(&nsm, 0, sizeof(nsm));
    evpl_mutex_init(&nsm.mutex, NULL);
    listener = listen_on(&test_portmap_port);

    t0 = now_ms();
    nsm_notify_submit(&nsm, new_job(3));
    CHECK(now_ms() - t0 < 500);
    evpl_thread_destroy(nsm.notify_thread);
    chimera_socket_close(listener);
    evpl_mutex_destroy(&nsm.mutex);
} /* test_spawn_returns_at_once */

/* A flood of submits must neither park the caller nor grow the thread count:
 * at most one worker notifies and one waits, and a submit that finds a worker
 * waiting replaces that worker's job.  Three submits therefore run two
 * serialized notifies (about 2 s), not three. */
static void
test_submit_coalesces(void)
{
    struct nsm_state nsm;
    chimera_socket_t listener;
    long             t0, t1;

    memset(&nsm, 0, sizeof(nsm));
    evpl_mutex_init(&nsm.mutex, NULL);
    listener = listen_on(&test_portmap_port);

    t1 = now_ms();
    t0 = t1;
    nsm_notify_submit(&nsm, new_job(3));
    CHECK(now_ms() - t0 < 500);

    /* Wait for the first worker to start notifying; a submit before that
     * would just replace its job. */
    while (nsm.notify_queued != NULL) {
        CHECK(now_ms() - t1 < 1000);
        usleep(1000);
    }

    t0 = now_ms();
    nsm_notify_submit(&nsm, new_job(5));
    CHECK(now_ms() - t0 < 500);
    CHECK(nsm.notify_queued != NULL);
    CHECK(nsm.notify_queued->state == 5);

    t0 = now_ms();
    nsm_notify_submit(&nsm, new_job(7));
    CHECK(now_ms() - t0 < 500);
    CHECK(nsm.notify_queued != NULL);
    CHECK(nsm.notify_queued->state == 7);

    evpl_thread_destroy(nsm.notify_thread);
    t1 = now_ms() - t1;
    CHECK(t1 >= 1800);
    CHECK(t1 < 2700);
    CHECK(nsm.notify_queued == NULL);
    chimera_socket_close(listener);
    evpl_mutex_destroy(&nsm.mutex);
} /* test_submit_coalesces */

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
    test_dead_host_skipped(evpl);
    test_live_host_told(evpl);
    test_portmap_closes(evpl);
    test_statd_closes(evpl);
    test_inproc_skips_probe();
    test_spawn_returns_at_once();
    test_submit_coalesces();
    mbt_watchdog_disarm();
    evpl_destroy(evpl);
    puts("PASS: NSM notify skips unreachable peers and spawns without waiting");
    return 0;
} /* main */
