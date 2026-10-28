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
 * returns. */
struct nsm_rpc_state {
    struct nsm_rpc_state *next;
    unsigned int          port;
    int                   done;
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

    g->port = (status == 0) ? port : 0;
    g->done = 1;
} /* nsm_getport_cb */

static void
nsm_notify_reply_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    int                          status,
    void                        *private_data)
{
    struct nsm_rpc_state *rs = private_data;

    (void) status;
    rs->done = 1;
} /* nsm_notify_reply_cb */

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

/* Pump the dedicated evpl until *flag is set or the timeout elapses, so an
 * unreachable host cannot wedge the notify loop forever. */
static void
nsm_pump_until(
    struct evpl *evpl,
    const int   *flag)
{
    struct evpl_timer tick;
    int64_t           deadline = nsm_now_ms() +
        (int64_t) NSM_NOTIFY_TIMEOUT_SECS * 1000;

    evpl_add_timer(evpl, &tick, nsm_pump_tick_cb, NSM_PUMP_TICK_US);

    while (!*flag && nsm_now_ms() < deadline) {
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

static void
nsm_notify_one(
    struct evpl                    *evpl,
    struct evpl_rpc2_thread        *rpc2_thread,
    struct PORTMAP_V2              *pm,
    struct SM_INTER_V1             *sm,
    const struct nsm_notify_target *t,
    const char                     *my_name,
    uint32_t                        state,
    struct nsm_rpc_state          **keep)
{
    struct evpl_endpoint  *ep;
    struct evpl_rpc2_conn *conn;
    struct mapping         mapping;
    struct nsm_rpc_state  *g;
    struct nsm_rpc_state  *d;
    struct stat_chge       arg;

    g = nsm_rpc_state_new(keep);
    if (!g) {
        return;
    }

    /* Resolve the peer's statd port via its portmapper. */
    if (!nsm_peer_reachable(t->addr, NFS_PEER_PORTMAP_PORT)) {
        chimera_nfs_info("NSM notify: host '%s' (%s:%u) is unreachable; "
                         "skipping", t->host, t->addr,
                         (unsigned) NFS_PEER_PORTMAP_PORT);
        return;
    }

    ep   = chimera_tcp_flavor_endpoint_create(nfs_callback_tcp_flavor(), t->addr, NFS_PEER_PORTMAP_PORT);
    conn = evpl_rpc2_client_connect(rpc2_thread,
                                    chimera_tcp_flavor_to_protocol(nfs_callback_tcp_flavor()), ep,
                                    NULL, 0, NULL);
    if (!conn) {
        evpl_endpoint_close(ep);
        chimera_nfs_info("NSM notify: cannot reach portmap at %s", t->addr);
        return;
    }

    mapping.prog = 100024;
    mapping.vers = 1;
    mapping.prot = 6;
    mapping.port = 0;
    pm->send_call_PMAPPROC_GETPORT(&pm->rpc2, evpl, conn, NULL, &mapping,
                                   0, 0, NULL, 0, 0, nsm_getport_cb, g);
    nsm_pump_until(evpl, &g->done);
    evpl_rpc2_client_disconnect(rpc2_thread, conn);
    /* A call still pending after a timeout completes with an error as the
     * connection goes away; give that callback its chance to run now. */
    nsm_pump_until(evpl, &g->done);
    evpl_endpoint_close(ep);

    if (!g->done || g->port == 0) {
        chimera_nfs_info("NSM notify: host '%s' (%s) has no statd registered; "
                         "skipping", t->host, t->addr);
        return;
    }

    /* Tell the peer's statd that we (mon_name) restarted with `state`. */
    if (!nsm_peer_reachable(t->addr, (int) g->port)) {
        chimera_nfs_info("NSM notify: host '%s' (%s:%u) is unreachable; "
                         "skipping", t->host, t->addr, g->port);
        return;
    }

    ep   = chimera_tcp_flavor_endpoint_create(nfs_callback_tcp_flavor(), t->addr, g->port);
    conn = evpl_rpc2_client_connect(rpc2_thread,
                                    chimera_tcp_flavor_to_protocol(nfs_callback_tcp_flavor()), ep,
                                    NULL, 0, NULL);
    if (!conn) {
        evpl_endpoint_close(ep);
        return;
    }

    d = nsm_rpc_state_new(keep);
    if (!d) {
        evpl_rpc2_client_disconnect(rpc2_thread, conn);
        evpl_endpoint_close(ep);
        return;
    }

    arg.mon_name.str = (char *) my_name;
    arg.mon_name.len = strlen(my_name);
    arg.state        = (int) state;
    sm->send_call_SM_NOTIFY(&sm->rpc2, evpl, conn, NULL, &arg,
                            0, 0, NULL, 0, 0, nsm_notify_reply_cb, d);
    nsm_pump_until(evpl, &d->done);
    evpl_rpc2_client_disconnect(rpc2_thread, conn);
    nsm_pump_until(evpl, &d->done);
    evpl_endpoint_close(ep);

    chimera_nfs_info("NSM notify: told host '%s' (%s:%u) we restarted (state %u)",
                     t->host, t->addr, g->port, state);
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

    /* Outbound-only client programs; replies are matched by this rpc2 thread. */
    PORTMAP_V2_init(&pm);
    SM_INTER_V1_init(&sm);
    programs[0] = &pm.rpc2;
    programs[1] = &sm.rpc2;
    rpc2_thread = evpl_rpc2_thread_init(evpl, programs, 2, NULL, NULL);

    chimera_nfs_info("NSM: notifying %u monitored host(s) of restart (state %u)",
                     job->count, job->state);

    /* All work is synchronous (each call self-pumps the dedicated evpl), so it
     * completes here before the framework's own idle loop starts.  The
     * submitter does not wait for it (evpl_thread_create_async). */
    for (i = 0; i < job->count; i++) {
        nsm_notify_one(evpl, rpc2_thread, &pm, &sm, &job->targets[i],
                       job->my_name, job->state, &keep);
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
         * snapshot and start nothing. */
        free(queued->targets);
        queued->targets = job->targets;
        queued->count   = job->count;
        queued->state   = job->state;
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
