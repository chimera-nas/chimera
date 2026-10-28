// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <time.h>
#include <stdlib.h>
#include <string.h>

#include "nfs_common.h"
#include "nfs_internal.h"
#include "nfs_nsm_notify.h"
#include "portmap_xdr.h"
#include "sm_inter_xdr.h"
#include "common/tcp_flavor.h"
#include "common/thread_name.h"
#include "evpl/evpl.h"
#include "evpl/evpl_rpc2.h"

/* ------------------------------------------------------------------ *
*  reboot-notify client (runs on a dedicated short-lived evpl thread) *
* ------------------------------------------------------------------ */

#define NSM_NOTIFY_TIMEOUT_SECS 5

struct nsm_getport_result {
    unsigned int port;
    int          done;
};

static void
nsm_getport_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    unsigned int                 port,
    int                          status,
    void                        *private_data)
{
    struct nsm_getport_result *g = private_data;

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
    int *done = private_data;

    (void) status;
    *done = 1;
} /* nsm_notify_reply_cb */

/* Pump the dedicated evpl until *flag is set or the timeout elapses, so an
 * unreachable host cannot wedge the notify loop forever. */
static void
nsm_pump_until(
    struct evpl *evpl,
    const int   *flag)
{
    time_t deadline = time(NULL) + NSM_NOTIFY_TIMEOUT_SECS;

    while (!*flag && time(NULL) < deadline) {
        evpl_continue(evpl);
    }
} /* nsm_pump_until */

static void
nsm_notify_one(
    struct evpl                    *evpl,
    struct evpl_rpc2_thread        *rpc2_thread,
    struct PORTMAP_V2              *pm,
    struct SM_INTER_V1             *sm,
    const struct nsm_notify_target *t,
    const char                     *my_name,
    uint32_t                        state)
{
    struct evpl_endpoint     *ep;
    struct evpl_rpc2_conn    *conn;
    struct mapping            mapping;
    struct nsm_getport_result g = { 0 };
    struct stat_chge          arg;
    int                       done = 0;

    /* Resolve the peer's statd port via its portmapper. */
    ep   = chimera_tcp_flavor_endpoint_create(nfs_callback_tcp_flavor(), t->addr, 111);
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
                                   0, 0, NULL, 0, 0, nsm_getport_cb, &g);
    nsm_pump_until(evpl, &g.done);
    evpl_rpc2_client_disconnect(rpc2_thread, conn);
    evpl_endpoint_close(ep);

    if (!g.done || g.port == 0) {
        chimera_nfs_info("NSM notify: host '%s' (%s) has no statd registered; "
                         "skipping", t->host, t->addr);
        return;
    }

    /* Tell the peer's statd that we (mon_name) restarted with `state`. */
    ep   = chimera_tcp_flavor_endpoint_create(nfs_callback_tcp_flavor(), t->addr, g.port);
    conn = evpl_rpc2_client_connect(rpc2_thread,
                                    chimera_tcp_flavor_to_protocol(nfs_callback_tcp_flavor()), ep,
                                    NULL, 0, NULL);
    if (!conn) {
        evpl_endpoint_close(ep);
        return;
    }

    arg.mon_name.str = (char *) my_name;
    arg.mon_name.len = strlen(my_name);
    arg.state        = (int) state;
    sm->send_call_SM_NOTIFY(&sm->rpc2, evpl, conn, NULL, &arg,
                            0, 0, NULL, 0, 0, nsm_notify_reply_cb, &done);
    nsm_pump_until(evpl, &done);
    evpl_rpc2_client_disconnect(rpc2_thread, conn);
    evpl_endpoint_close(ep);

    chimera_nfs_info("NSM notify: told host '%s' (%s:%u) we restarted (state %u)",
                     t->host, t->addr, g.port, state);
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
    uint32_t                  i;

    /* Outbound-only client programs; replies are matched by this rpc2 thread. */
    PORTMAP_V2_init(&pm);
    SM_INTER_V1_init(&sm);
    programs[0] = &pm.rpc2;
    programs[1] = &sm.rpc2;
    rpc2_thread = evpl_rpc2_thread_init(evpl, programs, 2, NULL, NULL);

    chimera_nfs_info("NSM: notifying %u monitored host(s) of restart (state %u)",
                     job->count, job->state);

    /* All work is synchronous (each call self-pumps the dedicated evpl), so it
     * completes here before the framework's own idle loop starts. */
    for (i = 0; i < job->count; i++) {
        nsm_notify_one(evpl, rpc2_thread, &pm, &sm, &job->targets[i],
                       job->my_name, job->state);
    }

    evpl_rpc2_thread_destroy(rpc2_thread);
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

struct evpl_thread *
nsm_notify_spawn(struct nsm_notify_job *job)
{
    return evpl_thread_create(chimera_thread_config("chm_nsm_notify"),
                              nsm_notify_thread_init,
                              nsm_notify_thread_shutdown, job);
} /* nsm_notify_spawn */
