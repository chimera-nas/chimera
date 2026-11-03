// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <stdlib.h>
#include <stdatomic.h>
#include "common/thread.h"

#include "nfs_common.h"
#include "nfs_internal.h"
#include "nfs_nsm.h"
#include "nfs_nsm_state.h"
#include "nfs_nsm_notify.h"
#include "nfs_nlm_state.h"
#include "nfs_kv_keys.h"
#include "vfs/vfs_procs.h"
#include "evpl/evpl.h"
#include "evpl/evpl_rpc2.h"

/* ------------------------------------------------------------------ *
*  KV persistence (fire-and-forget, heap ctx freed in the callback)   *
* ------------------------------------------------------------------ */

struct nsm_kv_ctx {
    uint8_t  key[CHIMERA_KV_NSM_MONITOR_KEY_MAX];
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

/* Bound on the number of monitor entries an IP- or caller-keyed sweep will
 * collect on the stack in one pass (SM_NOTIFY fallback / SM_UNMON_ALL). */
#define NSM_NOTIFY_FALLBACK_MAX 32

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
    struct nsm_state   *nsm = &thread->shared->nsm_state;
    struct nsm_monitor *mon;
    struct nsm_kv_ctx  *ctx;
    uint64_t            now_s = nsm_wall_now_s();
    uint64_t            prev_stamp;
    size_t              host_len;
    int                 changed, renew;

    evpl_mutex_lock(&nsm->mutex);
    HASH_FIND_STR(nsm->monitors, host, mon);
    prev_stamp = mon ? mon->stamp_s : 0;
    changed    = nsm_monitor_set(nsm, host, peer_addr, now_s);

    /* Renewal: rewrite the record once its stamp is half the expiry old, so a
     * client that keeps taking locks never ages out of the store.  A stamp in
     * the future (the clock stepped back) is rewritten too. */
    renew = prev_stamp > now_s ||
        nsm_monitor_renew_due(prev_stamp, now_s, nsm->monitor_expiry_s);

    if (mon && !changed && !renew) {
        /* Nothing is written, so the store keeps the record stamped
        * prev_stamp: leave the entry on that stamp too, or the renewal would
        * age from the last lock grant instead of the last write. */
        mon->stamp_s = prev_stamp;
    }
    evpl_mutex_unlock(&nsm->mutex);

    /* Persist only on first sight, an address change or a renewal, so a
     * lock-heavy client does not hammer the KV store. */
    if ((!changed && !renew) || nsm->persistence_disabled) {
        return;
    }

    host_len = strnlen(host, SM_MAXSTRLEN);

    ctx          = malloc(sizeof(*ctx));
    ctx->key_len = nfs_kv_nsm_monitor_key(ctx->key, nsm->node_id,
                                          (const uint8_t *) host, host_len);
    ctx->value_len = nsm_monitor_value_serialize(ctx->value, sizeof(ctx->value),
                                                 peer_addr, now_s);
    chimera_vfs_put_key(thread->vfs_thread, ctx->key, ctx->key_len,
                        ctx->value, ctx->value_len, nsm_kv_done, ctx);
} /* nsm_monitor */

void
chimera_nfs_nsm_renew_sweep(struct chimera_server_nfs_thread *thread)
{
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nsm_state                 *nsm    = &shared->nsm_state;
    struct nsm_notify_target         *due    = NULL;
    uint64_t                          now_s  = nsm_wall_now_s();
    uint64_t                          next;
    uint32_t                          interval_s, n, i;

    if (nsm->persistence_disabled) {
        return;
    }

    /* One thread per interval claims the sweep; the lease tick fires on every
     * server thread. */
    interval_s = nsm->monitor_expiry_s / 4;
    if (interval_s < 1) {
        interval_s = 1;
    }
    next = atomic_load_explicit(&nsm->next_renew_s, memory_order_relaxed);
    if (now_s < next ||
        !atomic_compare_exchange_strong(&nsm->next_renew_s, &next,
                                        now_s + interval_s)) {
        return;
    }

    evpl_mutex_lock(&nsm->mutex);
    n = nsm_monitors_renew_due(nsm, now_s, &due);
    evpl_mutex_unlock(&nsm->mutex);

    for (i = 0; i < n; i++) {
        struct nsm_notify_target *t = &due[i];
        struct nlm_client        *client;
        struct nsm_monitor       *mon;
        struct nsm_kv_ctx        *ctx;
        char                      addr[CHIMERA_NSM_ADDR_MAX];
        int                       alive;

        /* nsm_state.mutex is not held here: nsm before nlm, never both. */
        evpl_mutex_lock(&shared->nlm_state.mutex);
        HASH_FIND_STR(shared->nlm_state.clients, t->host, client);
        alive = client && (client->locks != NULL || client->conn_count > 0);
        evpl_mutex_unlock(&shared->nlm_state.mutex);

        if (!alive) {
            continue;
        }

        evpl_mutex_lock(&nsm->mutex);
        HASH_FIND_STR(nsm->monitors, t->host, mon);
        if (!mon) {
            evpl_mutex_unlock(&nsm->mutex);
            continue;
        }
        mon->stamp_s = now_s;
        snprintf(addr, sizeof(addr), "%s", mon->addr);
        evpl_mutex_unlock(&nsm->mutex);

        ctx          = malloc(sizeof(*ctx));
        ctx->key_len = nfs_kv_nsm_monitor_key(ctx->key, nsm->node_id,
                                              (const uint8_t *) t->host,
                                              strnlen(t->host, SM_MAXSTRLEN));
        ctx->value_len = nsm_monitor_value_serialize(ctx->value,
                                                     sizeof(ctx->value),
                                                     addr, now_s);
        chimera_vfs_put_key(thread->vfs_thread, ctx->key, ctx->key_len,
                            ctx->value, ctx->value_len, nsm_kv_done, ctx);
    }
    free(due);
} /* chimera_nfs_nsm_renew_sweep */

void
nsm_unmonitor(
    struct chimera_server_nfs_thread *thread,
    const char                       *host)
{
    struct nsm_state  *nsm = &thread->shared->nsm_state;
    struct nsm_kv_ctx *ctx;
    size_t             host_len;
    int                removed;

    evpl_mutex_lock(&nsm->mutex);
    removed = nsm_monitor_remove(nsm, host);
    evpl_mutex_unlock(&nsm->mutex);

    if (!removed || nsm->persistence_disabled) {
        return;
    }

    host_len     = strnlen(host, SM_MAXSTRLEN);
    ctx          = malloc(sizeof(*ctx));
    ctx->key_len = nfs_kv_nsm_monitor_key(ctx->key, nsm->node_id,
                                          (const uint8_t *) host, host_len);
    chimera_vfs_delete_key(thread->vfs_thread, ctx->key, ctx->key_len,
                           nsm_kv_done, ctx);
} /* nsm_unmonitor */

/* Cold-start notify finished: end NLM grace early if every host answered. */
static void
nsm_cold_notify_done(struct nsm_notify_job *job)
{
    struct chimera_server_nfs_shared *shared = job->done_arg;

    if (job->answered == job->count) {
        evpl_mutex_lock(&shared->nlm_state.mutex);
        nlm_state_settle_grace(&shared->nlm_state, NLM_GRACE_SETTLE_SECS);
        evpl_mutex_unlock(&shared->nlm_state.mutex);
        chimera_nfs_info("NSM cold-start: all %u monitored host(s) answered; "
                         "NLM grace ends in %d s",
                         job->count, NLM_GRACE_SETTLE_SECS);
    } else {
        chimera_nfs_info("NSM cold-start: %u of %u monitored host(s) answered; "
                         "holding NLM grace for the full window",
                         job->answered, job->count);
    }
} /* nsm_cold_notify_done */

/* Snapshot the monitor list and, if non-empty, spawn the reboot-notify worker.
 * The worker is not waited for here.  At most one worker notifies and one
 * waits behind it (it joins the notifying one before it starts): a snapshot
 * taken while a worker is already waiting replaces that worker's job, so a
 * flood of SM_SIMU_CRASH calls cannot grow the thread count, and no server
 * thread ever blocks on a worker. */
static void
nsm_spawn_notify(
    struct chimera_server_nfs_thread *thread,
    uint32_t                          state,
    int                               cold_start)
{
    struct nsm_state         *nsm = &thread->shared->nsm_state;
    struct nsm_notify_job    *job;
    struct nsm_notify_target *targets;
    uint32_t                  count;

    evpl_mutex_lock(&nsm->mutex);
    count = nsm_monitors_snapshot(nsm, &targets);
    evpl_mutex_unlock(&nsm->mutex);

    if (count == 0) {
        return;
    }

    job              = calloc(1, sizeof(*job));
    job->predecessor = NULL;
    job->nsm         = nsm;
    job->targets     = targets;
    job->count       = count;
    job->state       = state;
    snprintf(job->my_name, sizeof(job->my_name), "%s", nsm->my_name);
    if (cold_start) {
        job->done     = nsm_cold_notify_done;
        job->done_arg = thread->shared;
    }

    nsm_notify_submit(nsm, job);
} /* nsm_spawn_notify */

/* ------------------------------------------------------------------ *
*  cold-start load (state bump + monitor reload) -> reboot notify     *
* ------------------------------------------------------------------ */

struct nsm_load_ctx {
    struct chimera_server_nfs_thread *thread;
    uint32_t                          state;
    uint64_t                          ref_s; /* expiry reference (nsm_scan_ref_s) */
    /* Keys handed to the async KV layer must outlive the call -- keep them in
     * this heap ctx, not on the stack. */
    uint8_t                           skey[CHIMERA_KV_NSM_STATE_KEY_LEN];
    uint8_t                           start[CHIMERA_KV_PREFIX_LEN];
    /* Expired records found by the scan, deleted once it completes. */
    struct {
        uint8_t  key[CHIMERA_KV_NSM_MONITOR_KEY_MAX];
        uint32_t key_len;
    } *expired;
    uint32_t                          nexpired;
    uint32_t                          expired_cap;
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
    char                 host[SM_MAXSTRLEN + 1];
    char                 addr[CHIMERA_NSM_ADDR_MAX];
    uint64_t             stamp;

    switch (nsm_monitor_record_classify(key, key_len, value, value_len,
                                        nsm->node_id, ctx->ref_s,
                                        nsm->monitor_expiry_s,
                                        host, addr, &stamp)) {
        case NSM_RECORD_END:
            /* Keys arrive in order: this one is past this node's band. */
            return 1;
        case NSM_RECORD_SKIP:
            return 0;  /* malformed, or written before stamps existed */
        case NSM_RECORD_EXPIRED:
            /* Not renewed within the expiry: the client is gone.  Deleting
            * during the scan would mutate the band being iterated, so
            * remember the key (at most CHIMERA_KV_NSM_MONITOR_KEY_MAX bytes
            * for an EXPIRED record) and delete once the scan completes. */
            if (key_len > sizeof(ctx->expired[0].key)) {
                return 0;  /* cannot happen (see classify); never overrun */
            }
            if (ctx->nexpired == ctx->expired_cap) {
                ctx->expired_cap = ctx->expired_cap ? ctx->expired_cap * 2 : 8;
                ctx->expired     = realloc(ctx->expired,
                                           ctx->expired_cap * sizeof(*ctx->expired));
                chimera_nfs_abort_if(ctx->expired == NULL,
                                     "NSM expired-monitor list realloc failed");
            }
            memcpy(ctx->expired[ctx->nexpired].key, key, key_len);
            ctx->expired[ctx->nexpired].key_len = key_len;
            ctx->nexpired++;
            return 0;
        case NSM_RECORD_LOAD:
            break;
    } /* switch */

    /* Reload with the record's own stamp, so the renewal ages it from when it
     * was last written. */
    evpl_mutex_lock(&nsm->mutex);
    nsm_monitor_set(nsm, host, addr, stamp);
    evpl_mutex_unlock(&nsm->mutex);
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
    int                               scan_ok = error_code == CHIMERA_VFS_OK;

    /* A failed scan may have stopped part-way, so the monitor table can be
     * missing hosts that are owed a notify: the grace window must not be
     * ended or shortened on its account. */
    if (!scan_ok) {
        chimera_nfs_error("NSM cold-start: monitor scan failed (error %d); "
                          "holding NLM grace for the full window", error_code);
    }

    /* The scan is over, so the band can be mutated: drop the expired records
     * (fire-and-forget, heap ctx freed in nsm_kv_done). */
    for (uint32_t i = 0; i < ctx->nexpired; i++) {
        struct nsm_kv_ctx *kv = malloc(sizeof(*kv));

        memcpy(kv->key, ctx->expired[i].key, ctx->expired[i].key_len);
        kv->key_len = ctx->expired[i].key_len;
        chimera_vfs_delete_key(thread->vfs_thread, kv->key, kv->key_len,
                               nsm_kv_done, kv);
    }
    if (ctx->nexpired > 0) {
        chimera_nfs_info("NSM cold-start: purged %u expired monitor record(s)",
                         ctx->nexpired);
    }
    free(ctx->expired);

    evpl_mutex_lock(&nsm->mutex);
    count = HASH_COUNT(nsm->monitors);
    evpl_mutex_unlock(&nsm->mutex);

    if (count == 0 && scan_ok) {
        /* Nothing to reclaim -- end the forced grace window early so fresh
         * locks are accepted immediately (matches the no-clients case). */
        evpl_mutex_lock(&thread->shared->nlm_state.mutex);
        nlm_state_end_grace(&thread->shared->nlm_state);
        evpl_mutex_unlock(&thread->shared->nlm_state.mutex);
        chimera_nfs_info("NSM cold-start: no monitored hosts; NLM grace ended early");
    } else if (count > 0) {
        /* The hosts that were loaded are owed the notify either way; only a
         * complete scan may settle the grace once they all answered. */
        chimera_nfs_info("NSM cold-start: %u monitored host(s); notifying and "
                         "holding NLM grace", count);
        nsm_spawn_notify(thread, ctx->state, scan_ok);
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
    uint64_t                          prev_alive_s, now_s;

    if (error_code == CHIMERA_VFS_OK && value &&
        nsm_state_value_parse(value, value_len, &prev) == 0) {
        /* Smallest odd number strictly greater than prev (odd == "we are up"
         * after a reboot -- the classic NSM state-number convention). */
        newstate = nsm_next_state(prev);
    } else {
        newstate = 1;
    }

    evpl_mutex_lock(&nsm->mutex);
    nsm->state_number = newstate;
    evpl_mutex_unlock(&nsm->mutex);
    ctx->state = newstate;

    chimera_nfs_info("NSM cold-start: state number %u -> %u", prev, newstate);

    nsm_state_persist(thread->vfs_thread, newstate);

    /* Open-ended scan from this node's monitor band; nsm_monitor_scan_cb stops
     * at the first key past the 5-byte [type,node] prefix (the same pattern
     * the NFSv4 recovery reload uses), so only the clients this node served
     * are reloaded and notified.  Records are aged against the time this
     * node was last known alive (the NFSv4 recovery cold start, which the
     * kickoff waited for, read it), so the downtime does not count. */
    prev_alive_s = nfs_recovery_prev_alive_s(&thread->shared->nfs4_recovery);
    now_s        = nsm_wall_now_s();
    ctx->ref_s   = nsm_scan_ref_s(prev_alive_s, now_s);
    chimera_nfs_info("NSM cold-start: ageing monitor records against %s (%llu)",
                     ctx->ref_s == prev_alive_s ? "last-alive" : "now",
                     (unsigned long long) ctx->ref_s);
    nfs_kv_node_prefix(ctx->start, CHIMERA_KV_TYPE_NSM_MONITOR, nsm->node_id);
    chimera_vfs_search_keys(thread->vfs_thread,
                            ctx->start, CHIMERA_KV_PREFIX_LEN,
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
    /* The scan ages each record against the previous instance's last
     * heartbeat, which the NFSv4 recovery cold start reads in nfs_server_start
     * -- after the server threads are up.  Until that load is READY, leave the
     * run to a later lease tick. */
    if (nfs_recovery_loading(&thread->shared->nfs4_recovery)) {
        return;
    }
    if (!atomic_compare_exchange_strong(&nsm->load_state, &expected,
                                        NSM_LOAD_RUNNING)) {
        return;
    }

    /* We own the load.  Read the persisted state number first; the rest of the
     * chain (bump+persist, monitor scan, notify) runs as async callbacks on
     * this thread's vfs_thread/evpl. */
    ctx              = malloc(sizeof(*ctx));
    ctx->thread      = thread;
    ctx->state       = 0;
    ctx->ref_s       = 0;
    ctx->expired     = NULL;
    ctx->nexpired    = 0;
    ctx->expired_cap = 0;
    klen             = nfs_kv_nsm_state_key(ctx->skey);
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
    char                              host[SM_MAXSTRLEN + 1];
    size_t                            hn_len;
    int                               rc;

    /* SM_UNMON removes monitoring of the single named host (mon_id.mon_name),
     * the same key space used by SM_MON / SM_NOTIFY and the NLM caller_name.
     * Drop it symmetrically to the add path (nsm_monitor): unlink under the
     * lock and delete the persisted record. */
    hn_len = args->mon_name.len < SM_MAXSTRLEN ? args->mon_name.len : SM_MAXSTRLEN;
    memcpy(host, args->mon_name.str, hn_len);
    host[hn_len] = '\0';

    chimera_nfs_info("NSM SM_UNMON: stop monitoring host '%s'", host);

    nsm_unmonitor(thread, host);

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
    struct nsm_state                 *nsm    = &shared->nsm_state;
    struct sm_stat                    res;
    char                              caller[SM_MAXSTRLEN + 1];
    size_t                            cn_len;
    char                              matched[NSM_NOTIFY_FALLBACK_MAX][SM_MAXSTRLEN + 1];
    struct nsm_monitor               *mon, *tmp;
    int                               n = 0, truncated = 0, i, rc;

    /* SM_UNMON_ALL removes every monitor relationship the calling host
     * (my_id.my_name) established.  chimera's monitor table is keyed by the
     * monitored-host name (== the peer's NLM caller_name), which for a host
     * monitoring itself is the caller's own name; remove the entry matching the
     * caller plus any other entry the caller reaches at the same key.  Collect
     * matches under the lock, then drop each symmetrically to the add path. */
    cn_len = args->my_name.len < SM_MAXSTRLEN ? args->my_name.len : SM_MAXSTRLEN;
    memcpy(caller, args->my_name.str, cn_len);
    caller[cn_len] = '\0';

    chimera_nfs_info("NSM SM_UNMON_ALL: drop all monitors for caller '%s' "
                     "(prog=%d vers=%d proc=%d)", caller,
                     args->my_prog, args->my_vers, args->my_proc);

    evpl_mutex_lock(&nsm->mutex);
    HASH_ITER(hh, nsm->monitors, mon, tmp)
    {
        if (strcmp(mon->host, caller) != 0) {
            continue;
        }
        if (n < NSM_NOTIFY_FALLBACK_MAX) {
            snprintf(matched[n++], SM_MAXSTRLEN + 1, "%s", mon->host);
        } else {
            truncated = 1;
        }
    }
    evpl_mutex_unlock(&nsm->mutex);

    if (truncated) {
        chimera_nfs_info("NSM SM_UNMON_ALL: >%d monitors for '%s'; removing "
                         "only the first %d", NSM_NOTIFY_FALLBACK_MAX, caller,
                         NSM_NOTIFY_FALLBACK_MAX);
    }

    for (i = 0; i < n; i++) {
        nsm_unmonitor(thread, matched[i]);
    }

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
    evpl_mutex_lock(&nsm->mutex);
    nsm->state_number += (nsm->state_number & 1) ? 2 : 1;
    newstate           = nsm->state_number;
    evpl_mutex_unlock(&nsm->mutex);

    chimera_nfs_info("NSM SIMU_CRASH: bumped state to %u, notifying monitored hosts",
                     newstate);

    if (!nsm->persistence_disabled) {
        nsm_state_persist(thread->vfs_thread, newstate);
    }
    nsm_spawn_notify(thread, newstate, 0);

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

/* True unless `host` is monitored at an address other than `src`.
 *
 * SM_NOTIFY carries no authentication, so the only correlation available is
 * the one a real statd makes: the notify has to come from the peer it claims
 * restarted.  A monitored host's reach-address was recorded from its own NLM
 * connection (nfs_nlm.c), so a notify naming it from anywhere else is a
 * spoof and is ignored.  A host with no monitor entry -- one whose locks are
 * already gone, so the release is a no-op -- has no address to compare
 * against and is left to the caller's exact-match path. */
static int
nsm_notify_src_ok(
    struct chimera_server_nfs_thread *thread,
    const char                       *host,
    const char                       *src)
{
    struct nsm_state   *nsm = &thread->shared->nsm_state;
    struct nsm_monitor *mon;
    int                 ok;

    evpl_mutex_lock(&nsm->mutex);
    HASH_FIND_STR(nsm->monitors, host, mon);
    ok = (mon == NULL) || (strcmp(mon->addr, src) == 0);
    evpl_mutex_unlock(&nsm->mutex);

    return ok;
} /* nsm_notify_src_ok */

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

    /* Locate the client under the lock, then release outside it: release_all
    * takes state->mutex itself and must not be called with it held (it pumps
    * the VFS pending queue, which can re-enter the NLM acquire callback). */
    evpl_mutex_lock(&shared->nlm_state.mutex);
    HASH_FIND_STR(shared->nlm_state.clients, host, client);
    evpl_mutex_unlock(&shared->nlm_state.mutex);

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
    return found;
} /* nsm_release_host_locks */

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
    char                              src[80];
    size_t                            hn_len;
    int                               rc;

    /* NUL-terminate the wire name for use as the NLM client hash key.  A peer's
     * statd sends mon_name == the caller_name it used for its NLM locks, so the
     * direct lookup mirrors NLMPROC4_FREE_ALL. */
    hn_len = args->mon_name.len < SM_MAXSTRLEN ? args->mon_name.len : SM_MAXSTRLEN;
    memcpy(host, args->mon_name.str, hn_len);
    host[hn_len] = '\0';

    nsm_conn_peer_addr(conn, src, sizeof(src));

    /* A notify naming a host we monitor at a different address did not come
     * from that host: dropping its locks on an arbitrary peer's say-so is a
     * lock-release spoof, so ignore it (the reply is still sent -- SM_NOTIFY
     * has no error status). */
    if (!nsm_notify_src_ok(thread, host, src)) {
        chimera_nfs_info("NSM SM_NOTIFY for '%s' from %s ignored: not the "
                         "address that host is monitored at", host, src);
        goto reply;
    }

    chimera_nfs_info("NSM SM_NOTIFY: host '%s' restarted (state %d); releasing its locks",
                     host, args->state);

    if (nsm_release_host_locks(thread, host)) {
        nsm_unmonitor(thread, host);
    } else {
        /* Fallback: the peer's statd may present a mon_name that differs from
         * the caller_name it used for NLM.  Match monitored hosts by the
         * notify's source IP instead.  Collect matches under nsm_state.mutex,
         * then release outside it (nsm before nlm lock ordering). */
        char                matched[NSM_NOTIFY_FALLBACK_MAX][SM_MAXSTRLEN + 1];
        struct nsm_monitor *mon, *tmp;
        int                 n = 0, truncated = 0, i;

        evpl_mutex_lock(&shared->nsm_state.mutex);
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
        evpl_mutex_unlock(&shared->nsm_state.mutex);

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

 reply:

    /* SM_NOTIFY is a void procedure (no reply body), but the RPC layer still
     * expects an accepted-reply ack. */
    rc = shared->nsm_v1.send_reply_SM_NOTIFY(evpl, NULL, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NSM NOTIFY reply");
} /* chimera_nfs_sm_notify */
