// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * A retransmit that arrives while its original is still executing
 * (chimera#1375).
 *
 * The client lost the connection its non-idempotent request went out on,
 * reconnected and sent the request again under the same XID -- while the
 * server was still working on the first copy.  The duplicate-request cache
 * used to create its entry only when the original's reply was captured, so the
 * retransmit missed, executed a second time, and got the second execution's
 * answer (NOENT for a REMOVE that succeeded, EXIST for a MKDIR that did),
 * which then also replaced the cached reply.
 *
 * nfs_drc_probe drives one RPC at a time and so can never put a retransmit
 * inside that window.  This probe can: the server is told to hold every
 * request that reserves a cache identity for a fixed delay before executing it
 * (CHIMERA_NFS3_DRC_TEST_DELAY_MS), and the retransmits are sent on other
 * connections while the original is held.  Under the in-process transport
 * every connection has the same source address, so those connections are the
 * same client to the address-scoped NFSv3 cache -- exactly a client that
 * reconnected.
 *
 * Every check is decided by what the directory holds afterwards as well as by
 * the replies: a cache that answered correctly but let a duplicate execute is
 * caught by the directory, and one that kept the directory right but answered
 * a retransmit with the wrong reply is caught by the status.
 *
 * Modes:
 *   (default)       the retransmits are held, and answered when the original
 *                   completes; an original that is cancelled releases its
 *                   held retransmit unanswered.
 *   --hold-expiry   the hold is shorter than the original takes, so a held
 *                   retransmit is dropped unanswered; the client's next
 *                   retransmit, after the original is done, replays the cache.
 *   --ceiling       the cache may hold only one entry in progress, so a new
 *                   request that arrives while the original runs is answered
 *                   NFS3ERR_JUKEBOX without executing; sent again later
 *                   under a new XID, it executes once.
 *   --shutdown      the server stops -- before its client, with the client's
 *                   connections still open -- while an original is parked and
 *                   a retransmit is held on it.  Server stop closes those
 *                   connections first, and the cache must release both on the
 *                   way down; AddressSanitizer and LeakSanitizer check that
 *                   nothing is leaked or touched afterwards.
 *   --rdma          any of the above over RPC-over-RDMA framing.
 *
 * After each mode the cache's own account -- the chimera_nfs3_drc metrics the
 * server exports -- is checked against what the probe did.
 */

#include "nfs_drc_mbt_common.h"
#include "prometheus-c.h"

/* How long the server holds a reserved request before executing it. */
#define IP_DELAY_MS       800
/* --hold-expiry: a held retransmit is dropped after this long. */
#define IP_SHORT_HOLD_MS  200
#define IP_SHORT_DELAY_MS 1500

/* Bounded waits, in 10 ms ticks. */
#define IP_TICK_US        10000
#define IP_WAIT_TICKS     1000      /* 10 s: anything longer is a hang */

static int dump     = 0;
static int failures = 0;

static void
report(
    const char *name,
    int         ok,
    const char *fmt,
    ...)
{
    va_list ap;

    if (!ok) {
        failures++;
    }
    if (ok && !dump) {
        printf("ok   %s\n", name);
        return;
    }
    printf("%-4s %s: ", ok ? "ok" : "FAIL", name);
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf("\n");
} /* report */

/* ---- asynchronous NFSv3 calls ------------------------------------------- */

/*
 * One outstanding call.  The shared wrappers wait for each reply before
 * returning; the whole point here is to have several copies of one request
 * outstanding at once, so these send and return, and the probe pumps the event
 * loop until the replies it expects have arrived.
 */
struct ip_call {
    int      done;
    int      rpc_err;    /* nonzero: no reply (e.g. the connection was closed) */
    uint32_t status;
};

static void
ip_mkdir_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct MKDIR3res            *reply,
    int                          status,
    void                        *private_data)
{
    struct ip_call *call = private_data;

    call->rpc_err = status;
    if (status == 0) {
        call->status = reply->status;
    }
    call->done = 1;
} /* ip_mkdir_cb */

static void
ip_rmdir_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct RMDIR3res            *reply,
    int                          status,
    void                        *private_data)
{
    struct ip_call *call = private_data;

    call->rpc_err = status;
    if (status == 0) {
        call->status = reply->status;
    }
    call->done = 1;
} /* ip_rmdir_cb */

static void
ip_rename_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct RENAME3res           *reply,
    int                          status,
    void                        *private_data)
{
    struct ip_call *call = private_data;

    call->rpc_err = status;
    if (status == 0) {
        call->status = reply->status;
    }
    call->done = 1;
} /* ip_rename_cb */

static void
ip_mkdir(
    struct drc_ctx              *c,
    int                          conn_id,
    const struct evpl_rpc2_cred *cred,
    uint32_t                     xid,
    const char                  *name,
    struct ip_call              *call)
{
    struct evpl_rpc2_conn *conn = drc_conn(c, conn_id);
    struct MKDIR3args      args;

    memset(call, 0, sizeof(*call));
    args.where.dir.data.data = (void *) c->dir_fh.data;
    args.where.dir.data.len  = c->dir_fh.len;
    args.where.name.str      = (char *) name;
    args.where.name.len      = (uint32_t) strlen(name);
    mbt_sattr3_default(&args.attributes);
    args.attributes.mode.set_it = 1;
    args.attributes.mode.mode   = DRC_MODE;

    evpl_rpc2_conn_set_next_xid(conn, xid);
    c->env->nfs_v3.send_call_NFSPROC3_MKDIR(&c->env->nfs_v3.rpc2, c->env->evpl,
                                            conn, cred, &args, 0, 0, NULL, 0, 0,
                                            ip_mkdir_cb, call);
} /* ip_mkdir */

static void
ip_rmdir(
    struct drc_ctx              *c,
    int                          conn_id,
    const struct evpl_rpc2_cred *cred,
    uint32_t                     xid,
    const char                  *name,
    struct ip_call              *call)
{
    struct evpl_rpc2_conn *conn = drc_conn(c, conn_id);
    struct RMDIR3args      args;

    memset(call, 0, sizeof(*call));
    args.object.dir.data.data = (void *) c->dir_fh.data;
    args.object.dir.data.len  = c->dir_fh.len;
    args.object.name.str      = (char *) name;
    args.object.name.len      = (uint32_t) strlen(name);

    evpl_rpc2_conn_set_next_xid(conn, xid);
    c->env->nfs_v3.send_call_NFSPROC3_RMDIR(&c->env->nfs_v3.rpc2, c->env->evpl,
                                            conn, cred, &args, 0, 0, NULL, 0, 0,
                                            ip_rmdir_cb, call);
} /* ip_rmdir */

static void
ip_rename(
    struct drc_ctx              *c,
    int                          conn_id,
    const struct evpl_rpc2_cred *cred,
    uint32_t                     xid,
    const char                  *from,
    const char                  *to,
    struct ip_call              *call)
{
    struct evpl_rpc2_conn *conn = drc_conn(c, conn_id);
    struct RENAME3args     args;

    memset(call, 0, sizeof(*call));
    args.from.dir.data.data = (void *) c->dir_fh.data;
    args.from.dir.data.len  = c->dir_fh.len;
    args.from.name.str      = (char *) from;
    args.from.name.len      = (uint32_t) strlen(from);
    args.to.dir.data.data   = (void *) c->dir_fh.data;
    args.to.dir.data.len    = c->dir_fh.len;
    args.to.name.str        = (char *) to;
    args.to.name.len        = (uint32_t) strlen(to);

    evpl_rpc2_conn_set_next_xid(conn, xid);
    c->env->nfs_v3.send_call_NFSPROC3_RENAME(&c->env->nfs_v3.rpc2, c->env->evpl,
                                             conn, cred, &args, 0, 0, NULL, 0, 0,
                                             ip_rename_cb, call);
} /* ip_rename */

/* ---- pumping ------------------------------------------------------------ */

struct ip_ticker {
    struct evpl_timer timer;
    volatile int      ticks;
};

static void
ip_tick(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    (void) evpl;
    ((struct ip_ticker *) timer)->ticks++;
} /* ip_tick */

/* Pump until every call in calls[] has completed, or the bound runs out.
 * Returns 1 when all completed. */
static int
ip_wait(
    struct drc_ctx  *c,
    struct ip_call **calls,
    int              ncalls)
{
    struct ip_ticker t;
    int              i, pending = 1;

    t.ticks = 0;
    evpl_add_timer(c->env->evpl, &t.timer, ip_tick, IP_TICK_US);
    while (t.ticks < IP_WAIT_TICKS) {
        pending = 0;
        for (i = 0; i < ncalls; i++) {
            if (!calls[i]->done) {
                pending = 1;
            }
        }
        if (!pending) {
            break;
        }
        evpl_continue(c->env->evpl);
    }
    evpl_remove_timer(c->env->evpl, &t.timer);
    return !pending;
} /* ip_wait */

/* Pump for a fixed time, so a request just sent has reached the server (and
 * reserved, or been held on, its identity) before the next one goes out. */
static void
ip_settle(
    struct drc_ctx *c,
    int             ms)
{
    struct ip_ticker t;

    t.ticks = 0;
    evpl_add_timer(c->env->evpl, &t.timer, ip_tick, IP_TICK_US);
    while (t.ticks < (ms * 1000) / IP_TICK_US) {
        evpl_continue(c->env->evpl);
    }
    evpl_remove_timer(c->env->evpl, &t.timer);
} /* ip_settle */

/* ---- directory checks --------------------------------------------------- */

static int
ip_dir_has(
    struct drc_ctx *c,
    const char     *name)
{
    struct drc_dirsnap snap;
    int                i;

    drc_dir_snapshot(c, &snap);
    for (i = 0; i < snap.n; i++) {
        if (strcmp(snap.ents[i].name, name) == 0) {
            return 1;
        }
    }
    return 0;
} /* ip_dir_has */

static const char *
ip_err(const struct ip_call *call)
{
    /* Rotated, so one message can describe more than one call. */
    static char bufs[4][64];
    static int  next;
    char       *buf = bufs[next++ & 3];

    if (!call->done) {
        return "no reply";
    }
    if (call->rpc_err) {
        snprintf(buf, sizeof(bufs[0]), "rpc error %d", call->rpc_err);
        return buf;
    }
    snprintf(buf, sizeof(bufs[0]), "status %u", call->status);
    return buf;
} /* ip_err */

static int
ip_ok(
    const struct ip_call *call,
    uint32_t              want)
{
    return call->done && call->rpc_err == 0 && call->status == want;
} /* ip_ok */

/* ---- the cache's own account -------------------------------------------- */

/* One sample from the server's Prometheus registry: `name{label="value"}`.
 * Returns -1 when the sample is not there. */
static long long
ip_metric(
    struct drc_ctx *c,
    const char     *name,
    const char     *label,
    const char     *value)
{
    static char buf[1 << 20];
    char        want[160];
    const char *p;
    int         n;

    n = prometheus_metrics_scrape(c->env->metrics, buf, sizeof(buf) - 1);
    if (n <= 0) {
        return -1;
    }
    buf[n] = '\0';
    snprintf(want, sizeof(want), "\n%s{%s=\"%s\"} ", name, label, value);
    p = strstr(buf, want);
    return p ? atoll(p + strlen(want)) : -1;
} /* ip_metric */

static void
ip_check_event(
    struct drc_ctx *c,
    const char     *event,
    long long       want)
{
    char      name[64];
    long long got = ip_metric(c, "chimera_nfs3_drc", "event", event);

    snprintf(name, sizeof(name), "metrics/%s", event);
    report(name, got == want, "chimera_nfs3_drc{event=\"%s\"} = %lld, "
           "expected %lld", event, got, want);
} /* ip_check_event */

static void
ip_check_state(
    struct drc_ctx *c,
    const char     *state,
    long long       want)
{
    char      name[64];
    long long got = ip_metric(c, "chimera_nfs3_drc_state", "state", state);

    snprintf(name, sizeof(name), "metrics/state-%s", state);
    report(name, got == want, "chimera_nfs3_drc_state{state=\"%s\"} = %lld, "
           "expected %lld", state, got, want);
} /* ip_check_state */

/* ---- the held path (default mode) --------------------------------------- */

static void
run_held(
    struct drc_ctx              *c,
    const struct evpl_rpc2_cred *u1)
{
    /* Static: a call left unanswered on purpose (scenario 6) completes only
     * when its connection closes, after this function has returned. */
    static struct ip_call orig, re1, re2, other;
    struct ip_call       *all[4];
    int                   finished;

    /* ------------------------------------------------------------------ *
    * 1. The case #1375 reports: a retransmit on a new connection while   *
    *    the original executes.  A second execution would answer EXIST.   *
    * ------------------------------------------------------------------ */
    ip_mkdir(c, 0, u1, 1000, "a", &orig);
    ip_settle(c, 50);
    ip_mkdir(c, 1, u1, 1000, "a", &re1);
    ip_settle(c, 50);
    report("inprog/retransmit-not-answered-early", !re1.done,
           "the retransmit was answered (%s) while its original was still "
           "executing", ip_err(&re1));

    all[0]   = &orig;
    all[1]   = &re1;
    finished = ip_wait(c, all, 2);
    report("inprog/mkdir-original", finished && ip_ok(&orig, NFS3_OK),
           "original MKDIR: %s", ip_err(&orig));
    report("inprog/mkdir-retransmit-replays", finished && ip_ok(&re1, NFS3_OK),
           "retransmitted MKDIR: %s (status %u would mean it executed twice)",
           ip_err(&re1), NFS3ERR_EXIST);
    report("inprog/mkdir-directory", ip_dir_has(c, "a"),
           "the directory MKDIR created is missing");

    /* ------------------------------------------------------------------ *
    * 2. One retransmit held per connection, several connections: every   *
    *    one of them is answered with the original's reply.  A second     *
    *    execution of the RMDIR would answer NOENT.                        *
    * ------------------------------------------------------------------ */
    ip_rmdir(c, 0, u1, 1100, "a", &orig);
    ip_settle(c, 50);
    ip_rmdir(c, 1, u1, 1100, "a", &re1);
    ip_rmdir(c, 2, u1, 1100, "a", &re2);
    all[0]   = &orig;
    all[1]   = &re1;
    all[2]   = &re2;
    finished = ip_wait(c, all, 3);
    (void) finished;
    report("inprog/rmdir-original", ip_ok(&orig, NFS3_OK),
           "original RMDIR: %s", ip_err(&orig));
    report("inprog/rmdir-held-on-two-connections",
           ip_ok(&re1, NFS3_OK) && ip_ok(&re2, NFS3_OK),
           "retransmits: %s and %s (status %u would mean one executed again)",
           ip_err(&re1), ip_err(&re2), NFS3ERR_NOENT);

    /* ------------------------------------------------------------------ *
    * 3. An error is a reply like any other.  The original RMDIR fails    *
    *    (nothing there); while it runs, the name is created; the held    *
    *    retransmit must get the original's NOENT, not execute and remove *
    *    the new directory.                                               *
    * ------------------------------------------------------------------ */
    ip_rmdir(c, 0, u1, 1200, "d", &orig);
    ip_settle(c, 50);
    ip_mkdir(c, 3, u1, 1201, "d", &other);
    ip_settle(c, 50);
    ip_rmdir(c, 1, u1, 1200, "d", &re1);
    all[0]   = &orig;
    all[1]   = &other;
    all[2]   = &re1;
    finished = ip_wait(c, all, 3);
    (void) finished;
    report("inprog/error-original", ip_ok(&orig, NFS3ERR_NOENT),
           "original RMDIR of an absent name: %s", ip_err(&orig));
    report("inprog/error-replayed", ip_ok(&re1, NFS3ERR_NOENT),
           "retransmit: %s (status %u would mean it executed and removed the "
           "object created meanwhile)", ip_err(&re1), NFS3_OK);
    report("inprog/error-did-not-execute",
           ip_ok(&other, NFS3_OK) && ip_dir_has(c, "d"),
           "the directory created while the RMDIR was in progress is gone "
           "(MKDIR %s)", ip_err(&other));

    /* ------------------------------------------------------------------ *
    * 4. A held retransmit whose connection closes is released by the     *
    *    server (and must not be touched again -- AddressSanitizer        *
    *    watches); a retransmit on the next connection is held instead    *
    *    and answered.  RENAME executed twice would answer NOENT.          *
    * ------------------------------------------------------------------ */
    ip_rename(c, 0, u1, 1300, "d", "e", &orig);
    ip_settle(c, 50);
    ip_rename(c, 1, u1, 1300, "d", "e", &re1);
    ip_settle(c, 50);
    drc_reconnect(c, 1);
    report("inprog/closed-connection-call-lost", re1.done && re1.rpc_err != 0,
           "the retransmit on the closed connection: %s", ip_err(&re1));
    ip_rename(c, 1, u1, 1300, "d", "e", &re2);
    all[0]   = &orig;
    all[1]   = &re2;
    finished = ip_wait(c, all, 2);
    (void) finished;
    report("inprog/rename-original", ip_ok(&orig, NFS3_OK),
           "original RENAME: %s", ip_err(&orig));
    report("inprog/rename-held-after-reconnect", ip_ok(&re2, NFS3_OK),
           "retransmit after the held one's connection closed: %s",
           ip_err(&re2));
    report("inprog/rename-directory", ip_dir_has(c, "e") && !ip_dir_has(c, "d"),
           "the directory does not show exactly one RENAME");

    /* ------------------------------------------------------------------ *
    * 5. Once completed, the entry is an ordinary cached one.             *
    * ------------------------------------------------------------------ */
    ip_rename(c, 2, u1, 1300, "d", "e", &re1);
    all[0]   = &re1;
    finished = ip_wait(c, all, 1);
    (void) finished;
    report("inprog/later-retransmit-replays", ip_ok(&re1, NFS3_OK),
           "a retransmit after completion: %s", ip_err(&re1));

    /* ------------------------------------------------------------------ *
    * 6. An original that ends without a reply -- here its connection     *
    *    closes while it is still parked, so it is released unrun --       *
    *    abandons its entry.  The retransmit held on it is released        *
    *    unanswered (it was never going to get a cached reply), and the   *
    *    client's next retransmit is a fresh request that executes once.  *
    * ------------------------------------------------------------------ */
    ip_mkdir(c, 2, u1, 1400, "g", &orig);
    ip_settle(c, 50);
    ip_mkdir(c, 1, u1, 1400, "g", &re1);
    ip_settle(c, 50);
    drc_reconnect(c, 2);
    ip_settle(c, 100);
    report("abandon/original-lost-with-its-connection",
           orig.done && orig.rpc_err != 0, "original MKDIR: %s",
           ip_err(&orig));
    report("abandon/held-retransmit-released-unanswered", !re1.done,
           "the retransmit held on the abandoned original was answered (%s)",
           ip_err(&re1));
    report("abandon/original-never-ran", !ip_dir_has(c, "g"),
           "the cancelled MKDIR created its directory");
    ip_mkdir(c, 3, u1, 1400, "g", &re2);
    all[0]   = &re2;
    finished = ip_wait(c, all, 1);
    (void) finished;
    report("abandon/next-retransmit-executes-once",
           ip_ok(&re2, NFS3_OK) && ip_dir_has(c, "g"),
           "the next retransmit: %s", ip_err(&re2));

    /* ------------------------------------------------------------------ *
    * 7. The cache's own account of all of the above.                     *
    * ------------------------------------------------------------------ */
    ip_check_event(c, "held", 7);
    ip_check_event(c, "answered", 5);
    ip_check_event(c, "replayed", 1);
    ip_check_event(c, "drop_disconnect", 1);
    ip_check_event(c, "abandoned", 1);
    ip_check_event(c, "drop_abandoned", 1);
    ip_check_event(c, "drop_deadline", 0);
    ip_check_event(c, "drop_origin_conn", 0);
    ip_check_event(c, "jukebox", 0);
    ip_check_state(c, "in_progress", 0);
    ip_check_state(c, "held", 0);
} /* run_held */

/* ---- the expiry path (--hold-expiry) ------------------------------------ */

static void
run_hold_expiry(
    struct drc_ctx              *c,
    const struct evpl_rpc2_cred *u1)
{
    static struct ip_call orig, re1, re2;
    struct ip_call       *all[2];

    ip_mkdir(c, 0, u1, 2000, "f", &orig);
    ip_settle(c, 50);
    ip_mkdir(c, 1, u1, 2000, "f", &re1);

    all[0] = &orig;
    (void) ip_wait(c, all, 1);
    report("expiry/original", ip_ok(&orig, NFS3_OK), "original MKDIR: %s",
           ip_err(&orig));

    /* Had it still been held, it would have been answered as the original
     * completed. */
    ip_settle(c, 200);
    report("expiry/held-retransmit-dropped", !re1.done,
           "the retransmit held past its deadline was answered (%s)",
           ip_err(&re1));

    /* The client's next retransmit finds the completed entry. */
    ip_mkdir(c, 2, u1, 2000, "f", &re2);
    all[0] = &re2;
    (void) ip_wait(c, all, 1);
    report("expiry/next-retransmit-replays", ip_ok(&re2, NFS3_OK),
           "retransmit after the original completed: %s (status %u would mean "
           "it executed twice)", ip_err(&re2), NFS3ERR_EXIST);
    report("expiry/directory", ip_dir_has(c, "f"),
           "the directory MKDIR created is missing");

    /* The dropped call is the client's to finish, here by closing. */
    drc_reconnect(c, 1);
    report("expiry/dropped-call-lost-on-close", re1.done && re1.rpc_err != 0,
           "the dropped retransmit after its connection closed: %s",
           ip_err(&re1));

    ip_check_event(c, "held", 1);
    ip_check_event(c, "drop_deadline", 1);
    ip_check_event(c, "answered", 0);
    ip_check_event(c, "replayed", 1);
    ip_check_state(c, "in_progress", 0);
    ip_check_state(c, "held", 0);
} /* run_hold_expiry */

/* ---- the in-progress ceiling (--ceiling) -------------------------------- */

static void
run_ceiling(
    struct drc_ctx              *c,
    const struct evpl_rpc2_cred *u1)
{
    static struct ip_call orig, busy, again;
    struct ip_call       *all[1];

    /* The original takes the only entry the cache may have in progress. */
    ip_mkdir(c, 0, u1, 4000, "h", &orig);
    ip_settle(c, 50);

    /* A different request now finds no room.  It is refused straight away,
     * while the original is still parked, and does not run. */
    ip_mkdir(c, 1, u1, 4001, "i", &busy);
    all[0] = &busy;
    (void) ip_wait(c, all, 1);
    report("ceiling/refused-with-jukebox",
           ip_ok(&busy, NFS3ERR_JUKEBOX) && !orig.done,
           "a new request at the ceiling: %s, original %s", ip_err(&busy),
           ip_err(&orig));
    report("ceiling/refused-did-not-execute", !ip_dir_has(c, "i"),
           "the refused MKDIR created its directory");

    all[0] = &orig;
    (void) ip_wait(c, all, 1);
    report("ceiling/original", ip_ok(&orig, NFS3_OK), "original MKDIR: %s",
           ip_err(&orig));

    /* The client sends it again once there is room, under a new XID, as an
     * NFSv3 client does after NFS3ERR_JUKEBOX (RFC 1813 Section 2.6): it
     * executes -- exactly once.  The refusal is not recorded, so a delayed
     * duplicate of the refused XID itself could still execute after this;
     * that case is not covered here. */
    ip_mkdir(c, 1, u1, 4002, "i", &again);
    all[0] = &again;
    (void) ip_wait(c, all, 1);
    report("ceiling/sent-again-executes",
           ip_ok(&again, NFS3_OK) && ip_dir_has(c, "i"),
           "the request sent again under a new XID: %s (status %u would mean "
           "the refused one had run)", ip_err(&again), NFS3ERR_EXIST);

    ip_check_event(c, "jukebox", 1);
    /* The harness's own MKDIR of the test directory, the original, and the
     * request sent again. */
    ip_check_event(c, "reserved", 3);
    ip_check_event(c, "replayed", 0);
    ip_check_event(c, "held", 0);
    ip_check_state(c, "in_progress", 0);
} /* run_ceiling */

/* ---- server shutdown with work outstanding (--shutdown) ----------------- */

/* Leave an original parked and a retransmit held on it, then let main stop
 * the server without closing anything first.  The server must cancel the one
 * and release the other on the way down; the checks that matter are
 * AddressSanitizer's and LeakSanitizer's, at exit. */
static void
run_shutdown(
    struct drc_ctx              *c,
    const struct evpl_rpc2_cred *u1)
{
    static struct ip_call orig, re1;

    ip_mkdir(c, 0, u1, 3000, "s", &orig);
    ip_settle(c, 50);
    ip_mkdir(c, 1, u1, 3000, "s", &re1);
    ip_settle(c, 50);

    report("shutdown/original-still-parked", !orig.done, "original: %s",
           ip_err(&orig));
    report("shutdown/retransmit-held", !re1.done, "retransmit: %s",
           ip_err(&re1));
    ip_check_state(c, "in_progress", 1);
    ip_check_state(c, "held", 1);
} /* run_shutdown */

/*
 * Tear down server first, the reverse of mbt_env_stop.  The harness destroys
 * its client before the server, so the connections would close from the
 * client's side while the server is still running normally.  Here the server
 * stops while the client's connections are still open and its work is
 * outstanding -- the shutdown a real server goes through -- and only then is
 * the client released.  AUTH_SYS only: there are no GSS contexts to retire.
 */
static void
ip_stop_server_first(
    struct mbt_env *env,
    struct drc_ctx *c)
{
    chimera_server_destroy(env->server);

    drc_conns_close(c);
    if (env->portmap_conn) {
        evpl_rpc2_client_disconnect(env->rpc2_thread, env->portmap_conn);
    }
    if (env->nlm_conn) {
        evpl_rpc2_client_disconnect(env->rpc2_thread, env->nlm_conn);
    }
    if (env->nsm_conn) {
        evpl_rpc2_client_disconnect(env->rpc2_thread, env->nsm_conn);
    }
    evpl_rpc2_client_disconnect(env->rpc2_thread, env->nfs_conn);
    evpl_rpc2_client_disconnect(env->rpc2_thread, env->mount_conn);
    evpl_rpc2_thread_destroy(env->rpc2_thread);
    evpl_destroy(env->evpl);

    prometheus_metrics_destroy(env->metrics);
    free(env->data_buf);
    if (chimera_test_remove_tree(env->session_dir) != 0) {
        fprintf(stderr, "warning: failed to remove %s\n", env->session_dir);
    }
} /* ip_stop_server_first */

/* ------------------------------------------------------------------------- */

int
main(
    int   argc,
    char *argv[])
{
    struct mbt_env        env;
    struct mbt_env_opts   opts;
    struct drc_ctx        c;
    struct evpl_rpc2_cred u1;
    const char           *backend     = NULL;
    int                   hold_expiry = 0;
    int                   shutdown    = 0;
    int                   ceiling     = 0;
    int                   rdma        = 0;
    char                  num[16];
    int                   i;

    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--dump") == 0) {
            dump = 1;
        } else if (strcmp(argv[i], "--backend") == 0 && i + 1 < argc) {
            backend = argv[++i];
        } else if (strcmp(argv[i], "--hold-expiry") == 0) {
            hold_expiry = 1;
        } else if (strcmp(argv[i], "--ceiling") == 0) {
            ceiling = 1;
        } else if (strcmp(argv[i], "--shutdown") == 0) {
            shutdown = 1;
        } else if (strcmp(argv[i], "--rdma") == 0) {
            rdma = 1;
        }
    }

    /* Read by the server when the cache is installed, so before it starts. */
    snprintf(num, sizeof(num), "%d", hold_expiry ? IP_SHORT_DELAY_MS
                                                 : IP_DELAY_MS);
    setenv("CHIMERA_NFS3_DRC_TEST_DELAY_MS", num, 1);
    if (hold_expiry) {
        snprintf(num, sizeof(num), "%d", IP_SHORT_HOLD_MS);
        setenv("CHIMERA_NFS3_DRC_TEST_HOLD_MS", num, 1);
    }
    if (ceiling) {
        setenv("CHIMERA_NFS3_DRC_TEST_MAX_INPROG", "1", 1);
    }

    umask(0);

    memset(&opts, 0, sizeof(opts));
    opts.sec            = mbt_sec_scan_argv(argc, argv);
    opts.module         = backend;
    opts.nfs3_drc       = 1;
    opts.disable_caches = 1;
    opts.rdma           = rdma;

    mbt_env_open_opts(&env, &opts);
    mbt_env_fs_setup(&env, "fs0");

    memset(&c, 0, sizeof(c));
    c.env = &env;
    snprintf(c.mntpath, sizeof(c.mntpath), "/fs0");

    drc_cred_init(&env, &u1, 1000);
    drc_trace_setup(&c);

    if (shutdown) {
        if (opts.sec != MBT_SEC_SYS) {
            printf("--shutdown runs under AUTH_SYS only; skipping\n");
            return 77;
        }
        /* Stop the server with the work outstanding and the connections
         * open, so its thread teardown is what has to release both. */
        run_shutdown(&c, &u1);
        ip_stop_server_first(&env, &c);
    } else {
        if (hold_expiry) {
            run_hold_expiry(&c, &u1);
        } else if (ceiling) {
            run_ceiling(&c, &u1);
        } else {
            run_held(&c, &u1);
        }
        drc_conns_close(&c);
        mbt_env_fs_teardown(&env, "fs0");
        mbt_env_stop(&env);
    }

    if (failures) {
        printf("\n%d check(s) failed\n", failures);
        return 1;
    }
    printf("\nall checks passed\n");
    return 0;
} /* main */
