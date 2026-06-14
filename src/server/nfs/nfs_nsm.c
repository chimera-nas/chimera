// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <pthread.h>
#include <time.h>

#include "nfs_common.h"
#include "nfs_internal.h"
#include "nfs_nsm.h"
#include "nfs_nsm_state.h"
#include "nfs_nlm_state.h"
#include "nfs_kv_keys.h"
#include "portmap_xdr.h"
#include "vfs/vfs_procs.h"
#include "evpl/evpl.h"
#include "evpl/evpl_rpc2.h"

/* ------------------------------------------------------------------ *
*  KV persistence (fire-and-forget, heap ctx freed in the callback)   *
* ------------------------------------------------------------------ */

struct nsm_kv_ctx {
    uint8_t  key[CHIMERA_KV_HDR_LEN + SM_MAXSTRLEN];
    uint32_t key_len;
    uint8_t  value[NSM_MON_VALUE_MAX > NSM_STATE_VALUE_LEN ?
                   NSM_MON_VALUE_MAX : NSM_STATE_VALUE_LEN];
    uint32_t value_len;
};

static void
nsm_kv_done(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    (void) error_code;
    free(private_data);
} /* nsm_kv_done */

static void
nsm_state_persist(
    struct chimera_vfs_thread *vfs_thread,
    uint32_t                   state_number)
{
    struct nsm_kv_ctx *ctx = malloc(sizeof(*ctx));

    ctx->key_len   = nfs_kv_nsm_state_key(ctx->key);
    ctx->value_len = nsm_state_value_serialize(ctx->value, sizeof(ctx->value),
                                               state_number);
    chimera_vfs_put_key(vfs_thread, ctx->key, ctx->key_len,
                        ctx->value, ctx->value_len, nsm_kv_done, ctx);
} /* nsm_state_persist */

void
nsm_monitor(
    struct chimera_server_nfs_thread *thread,
    const char                       *host,
    const char                       *peer_addr)
{
    struct nsm_state  *nsm = &thread->shared->nsm_state;
    struct nsm_kv_ctx *ctx;
    size_t             host_len;
    int                changed;

    pthread_mutex_lock(&nsm->mutex);
    changed = nsm_monitor_set(nsm, host, peer_addr);
    pthread_mutex_unlock(&nsm->mutex);

    /* Only persist on first sight or an address change, so a lock-heavy client
     * does not hammer the KV store. */
    if (!changed || nsm->persistence_disabled) {
        return;
    }

    host_len = strnlen(host, SM_MAXSTRLEN);

    ctx          = malloc(sizeof(*ctx));
    ctx->key_len = nfs_kv_nsm_monitor_key(ctx->key, (const uint8_t *) host,
                                          host_len);
    ctx->value_len = nsm_monitor_value_serialize(ctx->value, sizeof(ctx->value),
                                                 peer_addr);
    chimera_vfs_put_key(thread->vfs_thread, ctx->key, ctx->key_len,
                        ctx->value, ctx->value_len, nsm_kv_done, ctx);
} /* nsm_monitor */

void
nsm_unmonitor(
    struct chimera_server_nfs_thread *thread,
    const char                       *host)
{
    struct nsm_state  *nsm = &thread->shared->nsm_state;
    struct nsm_kv_ctx *ctx;
    size_t             host_len;
    int                removed;

    pthread_mutex_lock(&nsm->mutex);
    removed = nsm_monitor_remove(nsm, host);
    pthread_mutex_unlock(&nsm->mutex);

    if (!removed || nsm->persistence_disabled) {
        return;
    }

    host_len     = strnlen(host, SM_MAXSTRLEN);
    ctx          = malloc(sizeof(*ctx));
    ctx->key_len = nfs_kv_nsm_monitor_key(ctx->key, (const uint8_t *) host,
                                          host_len);
    chimera_vfs_delete_key(thread->vfs_thread, ctx->key, ctx->key_len,
                           nsm_kv_done, ctx);
} /* nsm_unmonitor */

/* ------------------------------------------------------------------ *
*  reboot-notify client (runs on a dedicated short-lived evpl thread) *
* ------------------------------------------------------------------ */

#define NSM_NOTIFY_TIMEOUT_SECS 5

struct nsm_notify_job {
    struct nsm_notify_target *targets;
    uint32_t                  count;
    uint32_t                  state;
    char                      my_name[SM_MAXSTRLEN + 1];
};

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
    ep   = evpl_endpoint_create(t->addr, 111);
    conn = evpl_rpc2_client_connect(rpc2_thread, EVPL_STREAM_SOCKET_TCP, ep,
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
    ep   = evpl_endpoint_create(t->addr, g.port);
    conn = evpl_rpc2_client_connect(rpc2_thread, EVPL_STREAM_SOCKET_TCP, ep,
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

/* Snapshot the monitor list and, if non-empty, spawn the reboot-notify worker.
 * A previously-spawned (idle, work-already-done) worker is reaped first. */
static void
nsm_spawn_notify(
    struct chimera_server_nfs_thread *thread,
    uint32_t                          state)
{
    struct nsm_state         *nsm = &thread->shared->nsm_state;
    struct nsm_notify_job    *job;
    struct nsm_notify_target *targets;
    uint32_t                  count;
    struct evpl_thread       *old;

    pthread_mutex_lock(&nsm->mutex);
    count = nsm_monitors_snapshot(nsm, &targets);
    pthread_mutex_unlock(&nsm->mutex);

    if (count == 0) {
        return;
    }

    job          = malloc(sizeof(*job));
    job->targets = targets;
    job->count   = count;
    job->state   = state;
    snprintf(job->my_name, sizeof(job->my_name), "%s", nsm->my_name);

    pthread_mutex_lock(&nsm->mutex);
    old                = nsm->notify_thread;
    nsm->notify_thread = evpl_thread_create(NULL, nsm_notify_thread_init,
                                            nsm_notify_thread_shutdown, job);
    pthread_mutex_unlock(&nsm->mutex);

    if (old) {
        evpl_thread_destroy(old);
    }
} /* nsm_spawn_notify */

/* ------------------------------------------------------------------ *
*  cold-start load (state bump + monitor reload) -> reboot notify     *
* ------------------------------------------------------------------ */

struct nsm_load_ctx {
    struct chimera_server_nfs_thread *thread;
    uint32_t                          state;
    /* Keys handed to the async KV layer must outlive the call -- keep them in
     * this heap ctx, not on the stack. */
    uint8_t                           skey[CHIMERA_KV_NSM_STATE_KEY_LEN];
    uint8_t                           start[CHIMERA_KV_HDR_LEN];
};

static int
nsm_monitor_scan_cb(
    const void *key,
    uint32_t    key_len,
    const void *value,
    uint32_t    value_len,
    void       *private_data)
{
    struct nsm_load_ctx *ctx = private_data;
    struct nsm_state    *nsm = &ctx->thread->shared->nsm_state;
    const uint8_t       *k   = key;
    char                 host[SM_MAXSTRLEN + 1];
    char                 addr[CHIMERA_NSM_ADDR_MAX];
    uint32_t             host_len;

    /* Keys arrive in order; stop once we leave the monitor type band. */
    if (key_len < CHIMERA_KV_HDR_LEN ||
        memcmp(k, ctx->start, CHIMERA_KV_HDR_LEN) != 0) {
        return 1;
    }

    host_len = key_len - CHIMERA_KV_HDR_LEN;
    if (host_len == 0 || host_len > SM_MAXSTRLEN) {
        return 0;  /* skip a malformed key */
    }
    memcpy(host, k + CHIMERA_KV_HDR_LEN, host_len);
    host[host_len] = '\0';

    if (nsm_monitor_value_parse(value, value_len, addr, sizeof(addr)) != 0) {
        return 0;  /* skip a corrupt value */
    }

    pthread_mutex_lock(&nsm->mutex);
    nsm_monitor_set(nsm, host, addr);
    pthread_mutex_unlock(&nsm->mutex);
    return 0;
} /* nsm_monitor_scan_cb */

static void
nsm_monitor_scan_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nsm_load_ctx              *ctx    = private_data;
    struct chimera_server_nfs_thread *thread = ctx->thread;
    struct nsm_state                 *nsm    = &thread->shared->nsm_state;
    uint32_t                          count;

    (void) error_code;

    pthread_mutex_lock(&nsm->mutex);
    count = HASH_COUNT(nsm->monitors);
    pthread_mutex_unlock(&nsm->mutex);

    if (count == 0) {
        /* Nothing to reclaim -- end the forced grace window early so fresh
         * locks are accepted immediately (matches the no-clients case). */
        pthread_mutex_lock(&thread->shared->nlm_state.mutex);
        nlm_state_end_grace(&thread->shared->nlm_state);
        pthread_mutex_unlock(&thread->shared->nlm_state.mutex);
        chimera_nfs_info("NSM cold-start: no monitored hosts; NLM grace ended early");
    } else {
        chimera_nfs_info("NSM cold-start: %u monitored host(s); notifying and "
                         "holding NLM grace", count);
        nsm_spawn_notify(thread, ctx->state);
    }

    atomic_store(&nsm->load_state, NSM_LOAD_READY);
    free(ctx);
} /* nsm_monitor_scan_complete */

static void
nsm_state_load_cb(
    enum chimera_vfs_error error_code,
    const void            *value,
    uint32_t               value_len,
    void                  *private_data)
{
    struct nsm_load_ctx              *ctx    = private_data;
    struct chimera_server_nfs_thread *thread = ctx->thread;
    struct nsm_state                 *nsm    = &thread->shared->nsm_state;
    uint32_t                          prev   = 0;
    uint32_t                          newstate;

    if (error_code == CHIMERA_VFS_OK && value &&
        nsm_state_value_parse(value, value_len, &prev) == 0) {
        /* Smallest odd number strictly greater than prev (odd == "we are up"
         * after a reboot -- the classic NSM state-number convention). */
        newstate = nsm_next_state(prev);
    } else {
        newstate = 1;
    }

    pthread_mutex_lock(&nsm->mutex);
    nsm->state_number = newstate;
    pthread_mutex_unlock(&nsm->mutex);
    ctx->state = newstate;

    chimera_nfs_info("NSM cold-start: state number %u -> %u", prev, newstate);

    nsm_state_persist(thread->vfs_thread, newstate);

    /* Open-ended scan of the monitor band; nsm_monitor_scan_cb stops when the
     * 3-byte type header changes (the same pattern recovery/DRC use). */
    nfs_kv_type_prefix(ctx->start, CHIMERA_KV_TYPE_NSM_MONITOR);
    chimera_vfs_search_keys(thread->vfs_thread,
                            ctx->start, CHIMERA_KV_HDR_LEN,
                            NULL, 0, 0,
                            nsm_monitor_scan_cb,
                            nsm_monitor_scan_complete, ctx);
} /* nsm_state_load_cb */

void
chimera_nfs_nsm_kickoff(struct chimera_server_nfs_thread *thread)
{
    struct nsm_state    *nsm = &thread->shared->nsm_state;
    struct nsm_load_ctx *ctx;
    int                  expected = NSM_LOAD_IDLE;
    uint32_t             klen;

    if (nsm->persistence_disabled) {
        return;  /* non-persistent: monitor list was lost, nothing to notify */
    }
    if (atomic_load(&nsm->load_state) != NSM_LOAD_IDLE) {
        return;
    }
    if (!atomic_compare_exchange_strong(&nsm->load_state, &expected,
                                        NSM_LOAD_RUNNING)) {
        return;
    }

    /* We own the load.  Read the persisted state number first; the rest of the
     * chain (bump+persist, monitor scan, notify) runs as async callbacks on
     * this thread's vfs_thread/evpl. */
    ctx         = malloc(sizeof(*ctx));
    ctx->thread = thread;
    ctx->state  = 0;
    klen        = nfs_kv_nsm_state_key(ctx->skey);
    chimera_vfs_get_key(thread->vfs_thread, ctx->skey, klen,
                        nsm_state_load_cb, ctx);
} /* chimera_nfs_nsm_kickoff */

void
chimera_nfs_sm_null(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    chimera_nfs_debug("NSM NULL");

    rc = shared->nsm_v1.send_reply_SM_NULL(evpl, NULL, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NSM NULL reply");
} /* chimera_nfs_sm_null */

void
chimera_nfs_sm_stat(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct sm_name            *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct sm_stat_res                res;
    int                               rc;

    /* We do not actively probe peers; report that any host is monitorable. */
    res.res_stat = STAT_SUCC;
    res.state    = (int) nsm_state_current(&shared->nsm_state);

    rc = shared->nsm_v1.send_reply_SM_STAT(evpl, NULL, &res, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NSM STAT reply");
} /* chimera_nfs_sm_stat */

void
chimera_nfs_sm_mon(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct mon                *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct sm_stat_res                res;
    int                               rc;

    /* The authoritative monitor list is built from NLM lock grants (Phase 2),
     * so SM_MON from a peer is accepted but not recorded here. */
    res.res_stat = STAT_SUCC;
    res.state    = (int) nsm_state_current(&shared->nsm_state);

    rc = shared->nsm_v1.send_reply_SM_MON(evpl, NULL, &res, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NSM MON reply");
} /* chimera_nfs_sm_mon */

void
chimera_nfs_sm_unmon(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct mon_id             *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct sm_stat                    res;
    int                               rc;

    res.state = (int) nsm_state_current(&shared->nsm_state);

    rc = shared->nsm_v1.send_reply_SM_UNMON(evpl, NULL, &res, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NSM UNMON reply");
} /* chimera_nfs_sm_unmon */

void
chimera_nfs_sm_unmon_all(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct my_id              *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct sm_stat                    res;
    int                               rc;

    res.state = (int) nsm_state_current(&shared->nsm_state);

    rc = shared->nsm_v1.send_reply_SM_UNMON_ALL(evpl, NULL, &res, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NSM UNMON_ALL reply");
} /* chimera_nfs_sm_unmon_all */

void
chimera_nfs_sm_simu_crash(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nsm_state                 *nsm    = &shared->nsm_state;
    uint32_t                          newstate;
    int                               rc;

    /* Simulate a crash: bump our state number (keeping it odd), persist it, and
     * tell every monitored host so they reclaim. */
    pthread_mutex_lock(&nsm->mutex);
    nsm->state_number += (nsm->state_number & 1) ? 2 : 1;
    newstate           = nsm->state_number;
    pthread_mutex_unlock(&nsm->mutex);

    chimera_nfs_info("NSM SIMU_CRASH: bumped state to %u, notifying monitored hosts",
                     newstate);

    if (!nsm->persistence_disabled) {
        nsm_state_persist(thread->vfs_thread, newstate);
    }
    nsm_spawn_notify(thread, newstate);

    rc = shared->nsm_v1.send_reply_SM_SIMU_CRASH(evpl, NULL, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NSM SIMU_CRASH reply");
} /* chimera_nfs_sm_simu_crash */

/* Strip the ephemeral port from a connection's peer address into a C-string,
 * matching how nfs_nlm.c records a monitor's reach-address. */
static void
nsm_conn_peer_addr(
    struct evpl_rpc2_conn *conn,
    char                  *out,
    int                    size)
{
    char  *p;
    size_t n;

    evpl_rpc2_conn_get_remote_address(conn, out, size);

    if (out[0] == '[') {
        p = strchr(out, ']');
        n = p ? (size_t) (p - (out + 1)) : strlen(out);
        memmove(out, out + 1, n);
        out[n] = '\0';
    } else {
        p = strrchr(out, ':');
        if (p) {
            *p = '\0';
        }
    }
} /* nsm_conn_peer_addr */

/* Release every NLM lock held by `host` (the NLMPROC4_FREE_ALL sequence).
 * Returns 1 if a matching client was found.  Takes nlm_state.mutex internally
 * and no other lock, so it is safe to call after dropping nsm_state.mutex. */
static int
nsm_release_host_locks(
    struct chimera_server_nfs_thread *thread,
    const char                       *host)
{
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nlm_client                *client;
    struct chimera_vfs_cred           anon_cred;
    int                               found = 0;

    pthread_mutex_lock(&shared->nlm_state.mutex);
    HASH_FIND_STR(shared->nlm_state.clients, host, client);
    if (client) {
        found = 1;
        chimera_vfs_cred_init_anonymous(&anon_cred,
                                        CHIMERA_VFS_ANON_UID,
                                        CHIMERA_VFS_ANON_GID);
        nlm_client_release_all_locks(&shared->nlm_state, client,
                                     thread->vfs_thread,
                                     thread->vfs->vfs_state,
                                     &anon_cred);
    }
    pthread_mutex_unlock(&shared->nlm_state.mutex);
    return found;
} /* nsm_release_host_locks */

#define NSM_NOTIFY_FALLBACK_MAX 32

void
chimera_nfs_sm_notify(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct stat_chge          *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    char                              host[SM_MAXSTRLEN + 1];
    size_t                            hn_len;
    int                               rc;

    /* NUL-terminate the wire name for use as the NLM client hash key.  A peer's
     * statd sends mon_name == the caller_name it used for its NLM locks, so the
     * direct lookup mirrors NLMPROC4_FREE_ALL. */
    hn_len = args->mon_name.len < SM_MAXSTRLEN ? args->mon_name.len : SM_MAXSTRLEN;
    memcpy(host, args->mon_name.str, hn_len);
    host[hn_len] = '\0';

    chimera_nfs_info("NSM SM_NOTIFY: host '%s' restarted (state %d); releasing its locks",
                     host, args->state);

    if (nsm_release_host_locks(thread, host)) {
        nsm_unmonitor(thread, host);
    } else {
        /* Fallback: the peer's statd may present a mon_name that differs from
         * the caller_name it used for NLM.  Match monitored hosts by the
         * notify's source IP instead.  Collect matches under nsm_state.mutex,
         * then release outside it (nsm before nlm lock ordering). */
        char                src[80];
        char                matched[NSM_NOTIFY_FALLBACK_MAX][SM_MAXSTRLEN + 1];
        struct nsm_monitor *mon, *tmp;
        int                 n = 0, truncated = 0, i;

        nsm_conn_peer_addr(conn, src, sizeof(src));

        pthread_mutex_lock(&shared->nsm_state.mutex);
        HASH_ITER(hh, shared->nsm_state.monitors, mon, tmp)
        {
            if (strcmp(mon->addr, src) != 0) {
                continue;
            }
            if (n < NSM_NOTIFY_FALLBACK_MAX) {
                snprintf(matched[n++], SM_MAXSTRLEN + 1, "%s", mon->host);
            } else {
                truncated = 1;
            }
        }
        pthread_mutex_unlock(&shared->nsm_state.mutex);

        if (truncated) {
            chimera_nfs_info("NSM SM_NOTIFY: >%d monitored hosts at %s; releasing "
                             "only the first %d", NSM_NOTIFY_FALLBACK_MAX, src,
                             NSM_NOTIFY_FALLBACK_MAX);
        }
        for (i = 0; i < n; i++) {
            chimera_nfs_info("NSM SM_NOTIFY: matched host '%s' by source IP %s; "
                             "releasing its locks", matched[i], src);
            nsm_release_host_locks(thread, matched[i]);
            nsm_unmonitor(thread, matched[i]);
        }
    }

    /* SM_NOTIFY is a void procedure (no reply body), but the RPC layer still
     * expects an accepted-reply ack. */
    rc = shared->nsm_v1.send_reply_SM_NOTIFY(evpl, NULL, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NSM NOTIFY reply");
} /* chimera_nfs_sm_notify */
