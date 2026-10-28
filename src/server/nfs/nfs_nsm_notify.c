// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <time.h>
#include <stdlib.h>
#include <string.h>

#include "nfs_common.h"
#include "nfs_internal.h"
#include "nfs_nsm_notify.h"
#include "nfs_peer_probe.h"
#include "portmap_xdr.h"
#include "sm_inter_xdr.h"
#include "common/tcp_flavor.h"
#include "common/thread_name.h"
#include "evpl/evpl.h"
#include "evpl/evpl_rpc2.h"

/* ------------------------------------------------------------------ *
*  reboot-notify client (runs on a dedicated short-lived evpl thread) *
* ------------------------------------------------------------------ */

#ifndef NSM_NOTIFY_TIMEOUT_SECS
#define NSM_NOTIFY_TIMEOUT_SECS       5
#endif /* ifndef NSM_NOTIFY_TIMEOUT_SECS */

/* Bound on the reachability probe made before each connect: libevpl aborts
* the process on a refused connect and blocks for minutes on a dead one. */
#ifndef NSM_NOTIFY_CONNECT_TIMEOUT_MS
#define NSM_NOTIFY_CONNECT_TIMEOUT_MS 3000
#endif /* ifndef NSM_NOTIFY_CONNECT_TIMEOUT_MS */

/* Reply state for one RPC.  A call still pending after a disconnect normally
 * completes in the pump that follows it; if outstanding kernel operations hold
 * the close back it completes from inside evpl_rpc2_thread_destroy instead, so
 * the state is kept on a list owned by the worker and freed only after that
 * returns.  conn is the call's connection until rpc2 frees it: a peer that
 * closes mid-call makes rpc2 complete the call with CONN_LOST and free the
 * connection inside the pump, and the worker must not disconnect it then. */
struct nsm_rpc_state {
    struct nsm_rpc_state  *next;
    struct evpl_rpc2_conn *conn;
    unsigned int           port;
    int                    status;
    int                    done;
};

static struct nsm_rpc_state *
nsm_rpc_state_new(struct nsm_rpc_state **keep)
{
    struct nsm_rpc_state *rs = calloc(1, sizeof(*rs));

    if (rs) {
        rs->next = *keep;
        *keep = rs;
    }
    return rs;
} /* nsm_rpc_state_new */

static void
nsm_getport_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    unsigned int                 port,
    int                          status,
    void                        *private_data)
{
    struct nsm_rpc_state *g = private_data;

    g->status = status;
    g->port   = (status == 0) ? port : 0;
    g->done   = 1;
} /* nsm_getport_cb */

static void
nsm_notify_reply_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    int                          status,
    void                        *private_data)
{
    struct nsm_rpc_state *rs = private_data;

    rs->status = status;
    rs->done   = 1;
} /* nsm_notify_reply_cb */

/* rpc2 has freed conn: forget it so the worker does not disconnect it. */
static void
nsm_rpc_notify(
    struct evpl_rpc2_thread *thread,
    struct evpl_rpc2_conn   *conn,
    struct evpl_rpc2_notify *notify,
    void                    *private_data)
{
    struct nsm_rpc_state *rs = evpl_rpc2_conn_get_private_data(conn);

    (void) thread;
    (void) private_data;

    if (notify->notify_type == EVPL_RPC2_NOTIFY_DISCONNECTED && rs &&
        rs->conn == conn) {
        rs->conn = NULL;
    }
} /* nsm_rpc_notify */

/* The worker's outbound-only rpc2 thread (programs: portmap, statd);
 * replies are matched on the conns it opens. */
static struct evpl_rpc2_thread *
nsm_rpc2_thread_init(
    struct evpl               *evpl,
    struct evpl_rpc2_program **programs)
{
    return evpl_rpc2_thread_init(evpl, programs, 2, nsm_rpc_notify, NULL);
} /* nsm_rpc2_thread_init */

static int64_t
nsm_now_ms(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t) ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
} /* nsm_now_ms */

/* evpl_continue blocks until an event or a timer is due, so a peer that
 * accepts a connection and never answers would wedge the pump past its
 * deadline.  A periodic tick bounds each wait. */
#define NSM_PUMP_TICK_US 100000

static void
nsm_pump_tick_cb(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    (void) evpl;
    (void) timer;
} /* nsm_pump_tick_cb */

/* True once shutdown has asked the workers to give up (stop may be NULL). */
static int
nsm_stopping(_Atomic int *stop)
{
    return stop && atomic_load(stop);
} /* nsm_stopping */

/* Pump the dedicated evpl until *flag is set, the timeout elapses or
 * shutdown asks the worker to stop, so an unreachable host can neither
 * wedge the notify loop nor hold up the server's shutdown for long. */
static void
nsm_pump_until(
    struct evpl *evpl,
    const int   *flag,
    _Atomic int *stop)
{
    struct evpl_timer tick;
    int64_t           deadline = nsm_now_ms() +
        (int64_t) NSM_NOTIFY_TIMEOUT_SECS * 1000;

    evpl_add_timer(evpl, &tick, nsm_pump_tick_cb, NSM_PUMP_TICK_US);

    while (!*flag && nsm_now_ms() < deadline && !nsm_stopping(stop)) {
        evpl_continue(evpl);
    }

    evpl_remove_timer(evpl, &tick);
} /* nsm_pump_until */

static int
nsm_peer_reachable(
    const char *addr,
    int         port)
{
    if (nfs_callback_tcp_flavor() == CHIMERA_TCP_FLAVOR_INPROC) {
        return 1;
    }
    return nfs_peer_reachable(addr, port, NSM_NOTIFY_CONNECT_TIMEOUT_MS);
} /* nsm_peer_reachable */

/* Open the connection for rs's call and tag it with rs; NULL on failure. */
static struct evpl_rpc2_conn *
nsm_rpc_connect(
    struct evpl_rpc2_thread *rpc2_thread,
    struct evpl_endpoint    *ep,
    struct nsm_rpc_state    *rs)
{
    rs->conn = evpl_rpc2_client_connect(rpc2_thread,
                                        chimera_tcp_flavor_to_protocol(nfs_callback_tcp_flavor()), ep,
                                        NULL, 0, NULL);
    if (rs->conn) {
        evpl_rpc2_conn_set_private_data(rs->conn, rs);
    }
    return rs->conn;
} /* nsm_rpc_connect */

/* Wait for rs's call, then close its connection unless rpc2 already freed
 * it.  A call still pending after a timeout completes with an error as the
 * connection goes away; give that callback its chance to run now. */
static void
nsm_rpc_wait(
    struct evpl             *evpl,
    struct evpl_rpc2_thread *rpc2_thread,
    struct nsm_rpc_state    *rs,
    _Atomic int             *stop)
{
    nsm_pump_until(evpl, &rs->done, stop);
    if (rs->conn) {
        evpl_rpc2_client_disconnect(rpc2_thread, rs->conn);
    }
    nsm_pump_until(evpl, &rs->done, stop);
} /* nsm_rpc_wait */

/* Tell one monitored host's statd that we restarted.  Returns 1 when its
 * statd answered SM_NOTIFY, 0 when the host was skipped.  stop (may be
 * NULL) is the shutdown flag: once set, waits end at the next tick. */
static int
nsm_notify_one(
    struct evpl                    *evpl,
    struct evpl_rpc2_thread        *rpc2_thread,
    struct PORTMAP_V2              *pm,
    struct SM_INTER_V1             *sm,
    const struct nsm_notify_target *t,
    const char                     *my_name,
    uint32_t                        state,
    struct nsm_rpc_state          **keep,
    _Atomic int                    *stop)
{
    struct evpl_endpoint *ep;
    struct mapping        mapping;
    struct nsm_rpc_state *g;
    struct nsm_rpc_state *d;
    struct stat_chge      arg;

    g = nsm_rpc_state_new(keep);
    if (!g) {
        return 0;
    }

    /* Resolve the peer's statd port via its portmapper. */
    if (!nsm_peer_reachable(t->addr, NFS_PEER_PORTMAP_PORT)) {
        chimera_nfs_info("NSM notify: host '%s' (%s:%u) is unreachable; "
                         "skipping", t->host, t->addr,
                         (unsigned) NFS_PEER_PORTMAP_PORT);
        return 0;
    }

    ep = chimera_tcp_flavor_endpoint_create(nfs_callback_tcp_flavor(), t->addr, NFS_PEER_PORTMAP_PORT);
    if (!nsm_rpc_connect(rpc2_thread, ep, g)) {
        evpl_endpoint_close(ep);
        chimera_nfs_info("NSM notify: cannot reach portmap at %s:%u for "
                         "host '%s'", t->addr,
                         (unsigned) NFS_PEER_PORTMAP_PORT, t->host);
        return 0;
    }

    mapping.prog = 100024;
    mapping.vers = 1;
    mapping.prot = 6;
    mapping.port = 0;
    pm->send_call_PMAPPROC_GETPORT(&pm->rpc2, evpl, g->conn, NULL, &mapping,
                                   0, 0, NULL, 0, 0, nsm_getport_cb, g);
    nsm_rpc_wait(evpl, rpc2_thread, g, stop);
    evpl_endpoint_close(ep);

    if (nsm_stopping(stop)) {
        return 0;
    }

    if (!g->done || g->status != 0) {
        chimera_nfs_info("NSM notify: host '%s' (%s:%u) is unreachable (no "
                         "answer to GETPORT); skipping", t->host, t->addr,
                         (unsigned) NFS_PEER_PORTMAP_PORT);
        return 0;
    }

    if (g->port == 0) {
        chimera_nfs_info("NSM notify: host '%s' (%s) has no statd registered; "
                         "skipping", t->host, t->addr);
        return 0;
    }

    /* Tell the peer's statd that we (mon_name) restarted with `state`. */
    if (!nsm_peer_reachable(t->addr, (int) g->port)) {
        chimera_nfs_info("NSM notify: host '%s' (%s:%u) is unreachable; "
                         "skipping", t->host, t->addr, g->port);
        return 0;
    }

    d = nsm_rpc_state_new(keep);
    if (!d) {
        return 0;
    }

    ep = chimera_tcp_flavor_endpoint_create(nfs_callback_tcp_flavor(), t->addr, g->port);
    if (!nsm_rpc_connect(rpc2_thread, ep, d)) {
        evpl_endpoint_close(ep);
        chimera_nfs_info("NSM notify: cannot reach statd at %s:%u for "
                         "host '%s'", t->addr, g->port, t->host);
        return 0;
    }

    arg.mon_name.str = (char *) my_name;
    arg.mon_name.len = strlen(my_name);
    arg.state        = (int) state;
    sm->send_call_SM_NOTIFY(&sm->rpc2, evpl, d->conn, NULL, &arg,
                            0, 0, NULL, 0, 0, nsm_notify_reply_cb, d);
    nsm_rpc_wait(evpl, rpc2_thread, d, stop);
    evpl_endpoint_close(ep);

    if (!d->done || d->status != 0) {
        chimera_nfs_info("NSM notify: host '%s' (%s:%u) did not answer "
                         "SM_NOTIFY; skipping", t->host, t->addr, g->port);
        return 0;
    }

    chimera_nfs_info("NSM notify: told host '%s' (%s:%u) we restarted (state %u)",
                     t->host, t->addr, g->port, state);
    return 1;
} /* nsm_notify_one */

static void *
nsm_notify_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct nsm_notify_job    *job = private_data;
    struct evpl_rpc2_thread  *rpc2_thread;
    struct PORTMAP_V2         pm;
    struct SM_INTER_V1        sm;
    struct evpl_rpc2_program *programs[2];
    struct nsm_rpc_state     *keep = NULL, *rs;
    uint32_t                  i;

    /* Notifies run one worker at a time, in the order they were submitted. */
    if (job->predecessor) {
        evpl_thread_destroy(job->predecessor);
        job->predecessor = NULL;
    }

    /* From here the job is ours: a later submit starts a new worker instead of
     * rewriting this one.  Until now it could be replaced while we waited. */
    evpl_mutex_lock(&job->nsm->mutex);
    job->nsm->notify_queued = NULL;
    evpl_mutex_unlock(&job->nsm->mutex);

    PORTMAP_V2_init(&pm);
    SM_INTER_V1_init(&sm);
    programs[0] = &pm.rpc2;
    programs[1] = &sm.rpc2;
    rpc2_thread = nsm_rpc2_thread_init(evpl, programs);

    chimera_nfs_info("NSM: notifying %u monitored host(s) of restart (state %u)",
                     job->count, job->state);

    /* All work is synchronous (each call self-pumps the dedicated evpl), so it
     * completes here before the framework's own idle loop starts.  The
     * submitter does not wait for it (evpl_thread_create_async).  Shutdown
     * sets notify_stop before it joins: then no further host is started. */
    for (i = 0; i < job->count; i++) {
        if (nsm_stopping(&job->nsm->notify_stop)) {
            chimera_nfs_info("NSM: shutting down; %u monitored host(s) not "
                             "notified", job->count - i);
            break;
        }
        nsm_notify_one(evpl, rpc2_thread, &pm, &sm, &job->targets[i],
                       job->my_name, job->state, &keep,
                       &job->nsm->notify_stop);
    }

    evpl_rpc2_thread_destroy(rpc2_thread);
    while (keep) {
        rs   = keep;
        keep = rs->next;
        free(rs);
    }
    return job;
} /* nsm_notify_thread_init */

static void
nsm_notify_thread_shutdown(
    struct evpl *evpl,
    void        *private_data)
{
    struct nsm_notify_job *job = private_data;

    (void) evpl;
    free(job->targets);
    free(job);
} /* nsm_notify_thread_shutdown */

void
nsm_notify_submit(
    struct nsm_state      *nsm,
    struct nsm_notify_job *job)
{
    struct nsm_notify_job *queued;

    evpl_mutex_lock(&nsm->mutex);
    queued = nsm->notify_queued;
    if (queued) {
        /* The waiting worker has not read its job yet: give it the newer
         * monitor snapshot and start nothing.  Keep the higher state: two
         * server threads can bump the state and submit in either order,
         * and the notify must carry the newest one. */
        free(queued->targets);
        queued->targets = job->targets;
        queued->count   = job->count;
        if (job->state > queued->state) {
            queued->state = job->state;
        }
        memcpy(queued->my_name, job->my_name, sizeof(queued->my_name));
        free(job);
        evpl_mutex_unlock(&nsm->mutex);
        return;
    }

    job->predecessor   = nsm->notify_thread;
    job->nsm           = nsm;
    nsm->notify_queued = job;
    nsm->notify_thread = evpl_thread_create_async(chimera_thread_config("chm_nsm_notify"),
                                                  nsm_notify_thread_init,
                                                  nsm_notify_thread_shutdown, job);
    evpl_mutex_unlock(&nsm->mutex);
} /* nsm_notify_submit */
