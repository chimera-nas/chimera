// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Regression test: NFSv4.0 open_owner / lock_owner heap-use-after-free
 * between the lease sweeper and an in-flight OPEN/LOCK.
 *
 * The production race: a worker thread resolves an open_owner for an OPEN,
 * stashes a borrowed pointer on the request (req->open_4_0_owner), and goes
 * async into the VFS.  Before the async completion runs, the 1 Hz lease
 * sweeper reaps the idle client and frees every owner via
 * nfs_client_expire_state().  The completion then writes owner->seqid into
 * freed memory (nfs4_proc_open.c:479) -- the faulting write in the captured
 * crash.  LOCK has the same shape via req->lock_4_0_lock_owner /
 * req->lock_4_0_open_owner and chimera_nfs4_lock_finish().
 *
 * This test forces that exact interleaving deterministically -- no threads or
 * VFS backend needed.  (A CHIMERA_VFS_CAP_BLOCKING backend only widens the
 * async window in production; here we simply call the sweeper's teardown while
 * still holding the borrowed reference.)  It holds the caller reference that
 * find_or_create now returns -- the same reference an in-flight request keeps
 * -- expires the client, and then dereferences the owner as the OPEN/LOCK
 * completion does.
 *
 *   Pre-fix: nfs_client_expire_state freed the owner unconditionally, so the
 *            post-expiry owner->seqid write is a heap-use-after-free that ASAN
 *            (debug builds) reports at the same site as the captured crash.
 *   Post-fix: the borrow ref keeps the owner alive across expiry; the write is
 *            safe, and the final put() frees it cleanly with no leak.
 *
 * Also covers the follow-up fixes from the review of the same bug:
 *   - an acquire-held open_state pins its owner across a sweep;
 *   - nfs_lock_state_create refuses (returns NULL) once the lock_owner is
 *     unpublished by expiry or the open_state destroyed, instead of
 *     installing lock state no teardown path would ever visit;
 *   - a fully-linked lock_state is torn down completely by the sweep
 *     (no leak, no leftover-states abort);
 *   - nfs_open_owner_find_or_adopt republishes a pinned owner the sweep
 *     unpublished, keeping 4.0 seqid/replay state on a single object.
 *
 * Exercises the public nfs4_state.h API directly, mirroring test_state_table.c
 * (no VFS, RPC, or compound dispatch in the picture).
 */

#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include "nfs4_state.h"
#include "nfs4_layout_table.h"
#include "nfs4_procs.h"

/* CHECK() always evaluates and aborts on failure (assert(3) is a no-op under
 * NDEBUG in Release builds). */
#define CHECK(cond)                                                    \
        do {                                                               \
            if (!(cond)) {                                                 \
                fprintf(stderr, "%s:%d: CHECK failed: %s\n",               \
                        __FILE__, __LINE__, #cond);                        \
                abort();                                                   \
            }                                                              \
        } while (0)

/*
 * An in-flight OPEN borrows an open_owner across an async VFS round-trip; the
 * lease sweeper reaps the client before the completion runs.  The borrowed
 * reference must keep the owner alive so the completion's owner->seqid write
 * is safe.
 */
static void
test_open_owner_borrow_survives_sweep(void)
{
    struct nfs_state_table table;
    struct nfs_client     *client;
    struct nfs_open_owner *oo;
    bool                   created;

    nfs_state_table_init(&table, 1);
    client = nfs_client_alloc(1, "client-oo", 9, 0x1234, /*minor*/ 0);

    /* OPEN resolves the owner and (in the fixed code) receives a caller ref
     * that the in-flight request keeps while parked in the VFS. */
    oo = nfs_open_owner_find_or_create(client, "owner-A", 7, &created);
    CHECK(created);

    /* The 1 Hz lease sweeper reaps the idle client mid-flight. */
    nfs_client_expire_state(client, &table, NULL);

    /* Owner is unpublished from the client's hash... */
    CHECK(HASH_COUNT(client->open_owners_by_str) == 0);

    /* ...but the borrowed reference kept the struct alive, so the OPEN's async
     * completion can safely advance the seqid (nfs4_proc_open.c:479).  Pre-fix,
     * expire_state freed oo and this is a heap-use-after-free. */
    pthread_mutex_lock(&oo->lock);
    oo->seqid = 42;
    pthread_mutex_unlock(&oo->lock);
    CHECK(oo->seqid == 42);

    /* Completion drops the borrow ref; the last reference frees oo cleanly. */
    nfs_open_owner_put(oo);

    nfs_client_destroy(client, &table, NULL, true);
    nfs_state_table_free(&table, NULL);
    printf("ok: open_owner_borrow_survives_sweep\n");
} /* test_open_owner_borrow_survives_sweep */

/* Sibling case: an in-flight LOCK borrows a lock_owner the same way. */
static void
test_lock_owner_borrow_survives_sweep(void)
{
    struct nfs_state_table table;
    struct nfs_client     *client;
    struct nfs_lock_owner *lo;
    bool                   created;

    nfs_state_table_init(&table, 1);
    client = nfs_client_alloc(2, "client-lo", 9, 0x5678, /*minor*/ 0);

    lo = nfs_lock_owner_find_or_create(client, "lockowner-A", 11, &created);
    CHECK(created);

    nfs_client_expire_state(client, &table, NULL);

    CHECK(HASH_COUNT(client->lock_owners_by_str) == 0);

    pthread_mutex_lock(&lo->lock);
    lo->seqid = 7;
    pthread_mutex_unlock(&lo->lock);
    CHECK(lo->seqid == 7);

    nfs_lock_owner_put(lo);

    nfs_client_destroy(client, &table, NULL, true);
    nfs_state_table_free(&table, NULL);
    printf("ok: lock_owner_borrow_survives_sweep\n");
} /* test_lock_owner_borrow_survives_sweep */

/*
 * Regression: ordinary idle expiry with no request borrowing an owner must
 * still free every owner (no leak, checked by ASAN's leak detector), and leave
 * the client's owner hashes empty.
 */
static void
test_idle_expiry_frees_owners(void)
{
    struct nfs_state_table table;
    struct nfs_client     *client;
    struct nfs_open_owner *oo;
    struct nfs_lock_owner *lo;
    bool                   created;

    nfs_state_table_init(&table, 1);
    client = nfs_client_alloc(3, "client-idle", 11, 0x9abc, /*minor*/ 0);

    /* No borrow held: release the find_or_create caller ref immediately, so
     * only the hash-table slot ref remains. */
    oo = nfs_open_owner_find_or_create(client, "owner-A", 7, &created);
    CHECK(created);
    nfs_open_owner_put(oo);

    lo = nfs_lock_owner_find_or_create(client, "lockowner-A", 11, &created);
    CHECK(created);
    nfs_lock_owner_put(lo);

    /* Expiry drops the slot refs -> both owners freed, hashes empty. */
    nfs_client_expire_state(client, &table, NULL);
    CHECK(HASH_COUNT(client->open_owners_by_str) == 0);
    CHECK(HASH_COUNT(client->lock_owners_by_str) == 0);

    nfs_client_destroy(client, &table, NULL, true);
    nfs_state_table_free(&table, NULL);
    printf("ok: idle_expiry_frees_owners\n");
} /* test_idle_expiry_frees_owners */

/*
 * An acquire-held open_state pins its owner across a sweep, and a LOCK whose
 * client was expired mid-flight must fail to install lock state rather than
 * orphan it: nfs_lock_state_create returns NULL once the lock_owner is
 * unpublished (the proc layer maps that to NFS4ERR_EXPIRED).
 */
static void
test_lock_state_install_after_expire_fails(void)
{
    struct nfs_state_table table;
    struct nfs_client     *client;
    struct nfs_open_owner *oo;
    struct nfs_open_state *os;
    struct nfs_lock_owner *lo;
    struct nfs_lock_state *ls;
    struct stateid4        sid, lock_sid;
    nfsstat4               status;
    void                  *acquired;
    uint8_t                acquired_type;
    bool                   created;
    uint8_t                fh[4] = { 0x01, 0x02, 0x03, 0x04 };

    nfs_state_table_init(&table, 1);
    client = nfs_client_alloc(4, "client-lk1", 10, 0x1111, /*minor*/ 0);

    oo = nfs_open_owner_find_or_create(client, "owner-A", 7, &created);
    CHECK(created);
    os = nfs_open_state_create(oo, 0, NULL, 0, fh, sizeof(fh),
                               OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE,
                               /*handle_dup*/ NULL, &table, &sid);
    CHECK(os != NULL);

    /* The in-flight LOCK holds the open stateid acquire ref across the async
     * VFS round-trip. */
    status = nfs_state_table_acquire(&table, &sid, NFS4_SLOT_TYPE_OPEN,
                                     &acquired, &acquired_type);
    CHECK(status == NFS4_OK);
    CHECK(acquired == os);

    lo = nfs_lock_owner_find_or_create(client, "lockowner-A", 11, &created);
    CHECK(created);

    /* Sweep reaps the client mid-flight: the open_state destroy is deferred
     * (we hold the acquire ref), the lock_owner is unpublished. */
    nfs_client_expire_state(client, &table, NULL);

    /* The state's pin keeps ->owner dereferenceable even though the sweep
     * dropped the owner's hash-slot ref. */
    CHECK(os->owner == oo);

    /* Installation after the sweep must refuse rather than orphan. */
    ls = nfs_lock_state_create(lo, os, NULL, &table, &lock_sid);
    CHECK(ls == NULL);

    /* Release the acquire ref: the deferred open_state cleanup runs now and
     * drops its owner pin. */
    nfs_state_table_release(&table, os, NFS4_SLOT_TYPE_OPEN, NULL);

    nfs_lock_owner_put(lo);
    nfs_open_owner_put(oo);
    nfs_client_destroy(client, &table, NULL, true);
    nfs_state_table_free(&table, NULL);
    printf("ok: lock_state_install_after_expire_fails\n");
} /* test_lock_state_install_after_expire_fails */

/* Sibling refusal: the open_state was destroyed (e.g. a racing CLOSE) while
 * the lock_owner is still published. */
static void
test_lock_state_install_after_close_fails(void)
{
    struct nfs_state_table table;
    struct nfs_client     *client;
    struct nfs_open_owner *oo;
    struct nfs_open_state *os;
    struct nfs_lock_owner *lo;
    struct nfs_lock_state *ls;
    struct stateid4        sid, lock_sid;
    nfsstat4               status;
    void                  *acquired;
    uint8_t                acquired_type;
    bool                   created;
    uint8_t                fh[4] = { 0x05, 0x06, 0x07, 0x08 };

    nfs_state_table_init(&table, 1);
    client = nfs_client_alloc(5, "client-lk2", 10, 0x2222, /*minor*/ 0);

    oo = nfs_open_owner_find_or_create(client, "owner-A", 7, &created);
    os = nfs_open_state_create(oo, 0, NULL, 0, fh, sizeof(fh),
                               OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE,
                               NULL, &table, &sid);
    status = nfs_state_table_acquire(&table, &sid, NFS4_SLOT_TYPE_OPEN,
                                     &acquired, &acquired_type);
    CHECK(status == NFS4_OK);

    lo = nfs_lock_owner_find_or_create(client, "lockowner-A", 11, &created);

    /* CLOSE destroys the open_state under the LOCK's feet (deferred while
     * the acquire ref is held). */
    nfs_open_state_destroy(os, &table, NULL);

    ls = nfs_lock_state_create(lo, os, NULL, &table, &lock_sid);
    CHECK(ls == NULL);

    nfs_state_table_release(&table, os, NFS4_SLOT_TYPE_OPEN, NULL);
    nfs_lock_owner_put(lo);
    nfs_open_owner_put(oo);
    nfs_client_destroy(client, &table, NULL, true);
    nfs_state_table_free(&table, NULL);
    printf("ok: lock_state_install_after_close_fails\n");
} /* test_lock_state_install_after_close_fails */

/*
 * A fully-linked lock_state created BEFORE the sweep must be torn down by it
 * completely: the expire walk reaches it via open_state->locks, unlinks it
 * from lock_owner->states (so the leftover-states abort check passes), and
 * frees it -- no leak, no abort.
 */
static void
test_expire_with_fully_linked_lock_state(void)
{
    struct nfs_state_table table;
    struct nfs_client     *client;
    struct nfs_open_owner *oo;
    struct nfs_open_state *os;
    struct nfs_lock_owner *lo;
    struct nfs_lock_state *ls;
    struct stateid4        sid, lock_sid;
    bool                   created;
    uint8_t                fh[4] = { 0x09, 0x0A, 0x0B, 0x0C };

    nfs_state_table_init(&table, 1);
    client = nfs_client_alloc(6, "client-lk3", 10, 0x3333, /*minor*/ 0);

    oo = nfs_open_owner_find_or_create(client, "owner-A", 7, &created);
    os = nfs_open_state_create(oo, 0, NULL, 0, fh, sizeof(fh),
                               OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE,
                               NULL, &table, &sid);
    lo = nfs_lock_owner_find_or_create(client, "lockowner-A", 11, &created);

    /* Client alive: installation succeeds and links both lists. */
    ls = nfs_lock_state_create(lo, os, NULL, &table, &lock_sid);
    CHECK(ls != NULL);
    CHECK(lo->states == ls);
    CHECK(os->locks == ls);

    /* Sweep: tears down open_state -> lock_state cascade, then asserts every
     * lock_owner has no leftover states.  Surviving this call (no abort) and
     * running ASAN-clean (no leak of ls) is the regression check. */
    nfs_client_expire_state(client, &table, NULL);
    CHECK(HASH_COUNT(client->open_owners_by_str) == 0);
    CHECK(HASH_COUNT(client->lock_owners_by_str) == 0);
    CHECK(lo->states == NULL);

    nfs_lock_owner_put(lo);
    nfs_open_owner_put(oo);
    nfs_client_destroy(client, &table, NULL, true);
    nfs_state_table_free(&table, NULL);
    printf("ok: expire_with_fully_linked_lock_state\n");
} /* test_expire_with_fully_linked_lock_state */

/*
 * OPEN owner identity across a sweep: a pinned owner that the sweep
 * unpublished is republished (adopted) by the completion path rather than
 * replaced with a fresh struct, so seqid/replay state stays on one object.
 */
static void
test_open_owner_adopt_after_sweep(void)
{
    struct nfs_state_table table;
    struct nfs_client     *client;
    struct nfs_open_owner *oo, *adopted, *found;
    bool                   created;

    nfs_state_table_init(&table, 1);
    client = nfs_client_alloc(7, "client-adopt", 12, 0x4444, /*minor*/ 0);

    oo = nfs_open_owner_find_or_create(client, "owner-A", 7, &created);
    CHECK(created);

    pthread_mutex_lock(&oo->lock);
    oo->seqid = 5;
    pthread_mutex_unlock(&oo->lock);

    /* Sweep unpublishes the owner; the request's pin keeps it alive. */
    nfs_client_expire_state(client, &table, NULL);
    CHECK(HASH_COUNT(client->open_owners_by_str) == 0);

    /* Completion resolves the owner with the pinned struct as the adopt
     * candidate: the SAME object comes back, republished. */
    adopted = nfs_open_owner_find_or_adopt(client, oo, "owner-A", 7, &created);
    CHECK(adopted == oo);
    CHECK(!created);
    CHECK(HASH_COUNT(client->open_owners_by_str) == 1);
    CHECK(oo->seqid == 5);

    /* A later lookup finds the republished object, not a fresh one. */
    found = nfs_open_owner_find_or_create(client, "owner-A", 7, &created);
    CHECK(found == oo);
    CHECK(!created);

    nfs_open_owner_put(found);
    nfs_open_owner_put(adopted);
    nfs_open_owner_put(oo);
    nfs_client_destroy(client, &table, NULL, true);
    nfs_state_table_free(&table, NULL);
    printf("ok: open_owner_adopt_after_sweep\n");
} /* test_open_owner_adopt_after_sweep */

/*
 * Adopt arbitration for the pathological double-race: the sweep unpublished
 * the pinned owner AND another OPEN already recreated the key.  find_or_adopt
 * must return the published object (so the reply cache lands where a
 * retransmit will look), not resurrect the orphan alongside it.
 */
static void
test_adopt_prefers_published_owner(void)
{
    struct nfs_state_table table;
    struct nfs_client     *client;
    struct nfs_open_owner *oo, *fresh, *adopted;
    bool                   created;

    nfs_state_table_init(&table, 1);
    client = nfs_client_alloc(8, "client-race", 11, 0x5555, /*minor*/ 0);

    oo = nfs_open_owner_find_or_create(client, "owner-A", 7, &created);
    CHECK(created);

    nfs_client_expire_state(client, &table, NULL);

    /* Another OPEN recreates the key while ours is still in flight. */
    fresh = nfs_open_owner_find_or_create(client, "owner-A", 7, &created);
    CHECK(created);
    CHECK(fresh != oo);

    /* Our completion must resolve to the published object, not readopt. */
    adopted = nfs_open_owner_find_or_adopt(client, oo, "owner-A", 7, &created);
    CHECK(adopted == fresh);
    CHECK(!created);
    CHECK(HASH_COUNT(client->open_owners_by_str) == 1);

    nfs_open_owner_put(adopted);
    nfs_open_owner_put(fresh);
    nfs_open_owner_put(oo);
    nfs_client_destroy(client, &table, NULL, true);
    nfs_state_table_free(&table, NULL);
    printf("ok: adopt_prefers_published_owner\n");
} /* test_adopt_prefers_published_owner */

/* --- Death tests: refcount abort diagnostics -------------------------- */

/* Run fn in a forked child and require it to die with SIGABRT (the
 * chimera_nfs_abort_if diagnostics in the get/put helpers). */
static void
expect_abort(void ( *fn )(void))
{
    pid_t pid;
    int   status;

    /* Don't let the child flush inherited stdio buffers into our output. */
    fflush(NULL);
    pid = fork();
    CHECK(pid >= 0);
    if (pid == 0) {
        /* Silence the fatal-log line and any sanitizer chatter.  Best
         * effort: on failure the child output is merely noisy, and the
         * child aborts momentarily regardless. */
        if (!freopen("/dev/null", "w", stderr) ||
            !freopen("/dev/null", "w", stdout)) {
            /* keep going */
        }
        fn();
        _exit(0);  /* not reached: fn must abort */
    }
    CHECK(waitpid(pid, &status, 0) == pid);
    /* The guard must have fired: chimera_nfs_abort_if -> __chimera_abort ends
     * the process.  How it ends is build-dependent: sanitizer (debug) builds
     * flush and abort() -> SIGABRT; non-sanitizer (release) builds first run
     * chimera_crash_handler, whose libunwind backtrace can itself terminate
     * the (minimal, -O3) test binary with a different fatal signal before it
     * reaches raise(SIGABRT).  Either way the child must NOT have survived to
     * _exit(0) -- that (and only that) means the guard did not fire. */
    CHECK(!(WIFEXITED(status) && WEXITSTATUS(status) == 0));
} /* expect_abort */

static void
die_open_owner_get_after_free(void)
{
    struct nfs_open_owner o = { 0 };

    atomic_init(&o.refcount, 0);
    nfs_open_owner_get(&o);
} /* die_open_owner_get_after_free */

static void
die_open_owner_put_underflow(void)
{
    struct nfs_open_owner o = { 0 };

    atomic_init(&o.refcount, 0);
    nfs_open_owner_put(&o);
} /* die_open_owner_put_underflow */

static void
die_lock_owner_get_after_free(void)
{
    struct nfs_lock_owner o = { 0 };

    atomic_init(&o.refcount, 0);
    nfs_lock_owner_get(&o);
} /* die_lock_owner_get_after_free */

static void
die_lock_owner_put_underflow(void)
{
    struct nfs_lock_owner o = { 0 };

    atomic_init(&o.refcount, 0);
    nfs_lock_owner_put(&o);
} /* die_lock_owner_put_underflow */

static void
test_refcount_abort_diagnostics(void)
{
    expect_abort(die_open_owner_get_after_free);
    expect_abort(die_open_owner_put_underflow);
    expect_abort(die_lock_owner_get_after_free);
    expect_abort(die_lock_owner_put_underflow);
    printf("ok: refcount_abort_diagnostics\n");
} /* test_refcount_abort_diagnostics */

/* --- Concurrency stress: LOCK install vs lease sweep ------------------ */

struct stress_ctx {
    struct nfs_state_table *table;
    struct nfs_client      *client;
    _Atomic int             done;
};

/* Worker: the OPEN+LOCK fast path, exactly as the proc layer drives it --
 * resolve owners with caller refs, pin the open_state by acquire before
 * handing it to nfs_lock_state_create, tolerate NULL (expired mid-flight). */
static void *
stress_worker(void *arg)
{
    struct stress_ctx *ctx   = arg;
    uint8_t            fh[4] = { 0xAB, 0xCD, 0xEF, 0x01 };

    for (int i = 0; i < 2000; i++) {
        struct nfs_open_owner *oo;
        struct nfs_open_state *os;
        struct stateid4        sid, lsid;
        void                  *acq;
        uint8_t                acq_type;

        oo = nfs_open_owner_find_or_create(ctx->client, "owner-A", 7, NULL);
        os = nfs_open_state_create(oo, 0, NULL, 0, fh, sizeof(fh),
                                   OPEN4_SHARE_ACCESS_READ,
                                   OPEN4_SHARE_DENY_NONE,
                                   NULL, ctx->table, &sid);
        if (!os) {
            /* Sweep unpublished the owner between resolve and install;
             * the OPEN would fail with NFS4ERR_EXPIRED. */
            nfs_open_owner_put(oo);
            continue;
        }

        if (nfs_state_table_acquire(ctx->table, &sid, NFS4_SLOT_TYPE_OPEN,
                                    &acq, &acq_type) == NFS4_OK) {
            struct nfs_lock_owner *lo;
            struct nfs_lock_state *ls;

            lo = nfs_lock_owner_find_or_create(ctx->client, "lockowner-A", 11,
                                               NULL);
            ls = nfs_lock_state_create(lo, os, NULL, ctx->table, &lsid);

            if (ls &&
                nfs_state_table_acquire(ctx->table, &lsid,
                                        NFS4_SLOT_TYPE_LOCK,
                                        &acq, &acq_type) == NFS4_OK) {
                /* LOCKU-style teardown; idempotent vs a racing expire. */
                nfs_lock_state_destroy(acq, ctx->table, NULL);
                nfs_state_table_release(ctx->table, acq,
                                        NFS4_SLOT_TYPE_LOCK, NULL);
            }

            nfs_lock_owner_put(lo);
            nfs_state_table_release(ctx->table, os, NFS4_SLOT_TYPE_OPEN, NULL);
        }

        nfs_open_owner_put(oo);
    }

    atomic_store(&ctx->done, 1);
    return NULL;
} /* stress_worker */

/* Sweeper: the 1 Hz lease reaper, with the sleep removed. */
static void *
stress_sweeper(void *arg)
{
    struct stress_ctx *ctx = arg;

    while (!atomic_load(&ctx->done)) {
        nfs_client_expire_state(ctx->client, ctx->table, NULL);
        sched_yield();
    }
    return NULL;
} /* stress_sweeper */

/*
 * Hammer the exact sweeper-vs-worker interleavings under ASAN:
 * a worker running the OPEN+LOCK install path while the sweeper expires the
 * client continuously.  Pre-fix this trips the lock_owner UAF (put before
 * nfs_lock_state_create), the lo->states leftover-states abort, the unlocked
 * lock_owner->states list race, or leaks orphaned lock_states (caught by
 * ASAN's leak checker at exit).  Post-fix it must run clean.
 */
static void
test_concurrent_install_vs_expire(void)
{
    struct nfs_state_table table;
    struct stress_ctx      ctx;
    pthread_t              worker, sweeper;

    nfs_state_table_init(&table, 1);
    ctx.table  = &table;
    ctx.client = nfs_client_alloc(9, "client-race2", 12, 0x6666, /*minor*/ 0);
    atomic_init(&ctx.done, 0);

    CHECK(pthread_create(&worker, NULL, stress_worker, &ctx) == 0);
    CHECK(pthread_create(&sweeper, NULL, stress_sweeper, &ctx) == 0);
    CHECK(pthread_join(worker, NULL) == 0);
    CHECK(pthread_join(sweeper, NULL) == 0);

    /* Final sweep + teardown must leave nothing behind (ASAN leak check). */
    nfs_client_expire_state(ctx.client, &table, NULL);
    CHECK(HASH_COUNT(ctx.client->open_owners_by_str) == 0);
    CHECK(HASH_COUNT(ctx.client->lock_owners_by_str) == 0);

    nfs_client_destroy(ctx.client, &table, NULL, true);
    nfs_state_table_free(&table, NULL);
    printf("ok: concurrent_install_vs_expire\n");
} /* test_concurrent_install_vs_expire */

/* A compound reservation must neither expose a usable stateid nor let a
 * competing OPEN install state on its owner before the attempt is accepted. */
static void
test_compound_reservation_abort(void)
{
    struct nfs_state_table table;
    struct nfs_client     *client;
    struct nfs_open_owner *owner;
    struct nfs_open_state *state;
    struct stateid4        sid, competing_sid;
    uint8_t                fh[]          = { 0xCA, 0xFE, 0x01 };
    void                  *acquired      = NULL;
    uint8_t                acquired_type = 0;
    bool                   created;

    nfs_state_table_init(&table, 1);
    client = nfs_client_alloc(20, "compound-abort", 14, 0x7000, 1);
    owner  = nfs_open_owner_find_or_create(client, "owner-A", 7, &created);
    CHECK(created);
    CHECK(atomic_load(&owner->refcount) == 2);

    state = nfs_open_state_reserve_compound(client, "owner-A", 7, 0, NULL, 0,
                                            OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE, &table, &sid);
    CHECK(state != NULL);
    CHECK(owner->compound_pending == state);
    CHECK(atomic_load(&client->compound_pins) == 1);
    CHECK(atomic_load(&owner->refcount) == 3);
    memcpy(state->fh, fh, sizeof(fh));
    state->fh_len = sizeof(fh);
    CHECK(nfs_open_owner_find_state(owner, fh, sizeof(fh)) == NULL);
    CHECK(nfs_state_table_validate(&table, &sid) == NFS4ERR_BAD_STATEID);
    CHECK(nfs_state_table_acquire(&table, &sid, NFS4_SLOT_TYPE_OPEN,
                                  &acquired, &acquired_type) == NFS4ERR_BAD_STATEID);

    CHECK(nfs_open_state_reserve_compound(client, "owner-A", 7, 0, NULL, 0,
                                          OPEN4_SHARE_ACCESS_WRITE, OPEN4_SHARE_DENY_NONE, &table, &competing_sid) ==
          NULL);
    CHECK(nfs_open_state_create(owner, 0, NULL, 0, fh, sizeof(fh),
                                OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE, NULL, &table, &competing_sid) == NULL);
    CHECK(atomic_load(&client->compound_pins) == 1);
    CHECK(atomic_load(&owner->refcount) == 3);

    nfs_open_state_finish_compound(state, false, &table, NULL);
    CHECK(owner->compound_pending == NULL);
    CHECK(owner->states_by_fh == NULL);
    CHECK(atomic_load(&client->compound_pins) == 0);
    CHECK(atomic_load(&owner->refcount) == 2);
    CHECK(nfs_state_table_validate(&table, &sid) != NFS4_OK);

    /* Aborting releases admission as well as the state slot. */
    state = nfs_open_state_reserve_compound(client, "owner-A", 7, 0, NULL, 0,
                                            OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE, &table, &competing_sid);
    CHECK(state != NULL);
    CHECK(nfs_state_table_validate(&table, &sid) != NFS4_OK);
    nfs_open_state_finish_compound(state, false, &table, NULL);
    nfs_open_owner_put(owner);
    nfs_client_destroy(client, &table, NULL, true);
    nfs_state_table_free(&table, NULL);
    printf("ok: compound_reservation_abort\n");
} /* test_compound_reservation_abort */

/* Acceptance publishes the reserved slot and transfers its owner reference
 * into the ordinary open-state lifetime, including an accepted OPEN prefix. */
static void
test_compound_reservation_publish(void)
{
    struct nfs_state_table table;
    struct nfs_client     *client;
    struct nfs_open_owner *owner;
    struct nfs_open_state *state;
    struct stateid4        sid, competing_sid;
    uint8_t                fh[]          = { 0xCA, 0xFE, 0x02 };
    void                  *acquired      = NULL;
    uint8_t                acquired_type = 0;
    bool                   created;

    nfs_state_table_init(&table, 1);
    client = nfs_client_alloc(21, "compound-publish", 16, 0x7001, 1);
    owner  = nfs_open_owner_find_or_create(client, "owner-A", 7, &created);
    CHECK(created);
    state = nfs_open_state_reserve_compound(client, "owner-A", 7, 0, NULL, 0,
                                            OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE, &table, &sid);
    CHECK(state != NULL);
    memcpy(state->fh, fh, sizeof(fh));
    state->fh_len = sizeof(fh);
    nfs_open_state_publish_compound(state, &table);
    CHECK(nfs_open_owner_find_state(owner, fh, sizeof(fh)) == state);
    CHECK(nfs_state_table_acquire(&table, &sid, NFS4_SLOT_TYPE_OPEN,
                                  &acquired, &acquired_type) == NFS4_OK);
    CHECK(acquired == state);
    CHECK(acquired_type == NFS4_SLOT_TYPE_OPEN);
    nfs_state_table_release(&table, acquired, acquired_type, NULL);
    nfs_open_state_finish_compound(state, true, &table, NULL);
    CHECK(owner->compound_pending == NULL);
    CHECK(atomic_load(&client->compound_pins) == 0);
    CHECK(nfs_state_table_validate(&table, &sid) == NFS4_OK);
    CHECK(atomic_load(&owner->refcount) == 3);

    /* Owners with accepted state use the established coalescing path. */
    CHECK(nfs_open_state_reserve_compound(client, "owner-A", 7, 0, NULL, 0,
                                          OPEN4_SHARE_ACCESS_WRITE, OPEN4_SHARE_DENY_NONE, &table, &competing_sid) ==
          NULL);
    nfs_open_state_destroy(state, &table, NULL);
    CHECK(nfs_state_table_validate(&table, &sid) != NFS4_OK);
    CHECK(atomic_load(&owner->refcount) == 2);
    nfs_open_owner_put(owner);
    nfs_client_destroy(client, &table, NULL, true);
    nfs_state_table_free(&table, NULL);
    printf("ok: compound_reservation_publish\n");
} /* test_compound_reservation_publish */

static void
test_compound_reservation_pins_expiry(void)
{
    struct nfs_state_table table;
    struct nfs_client     *client;
    struct nfs_open_owner *owner;
    struct nfs_open_state *state;
    struct stateid4        sid;

    nfs_state_table_init(&table, 1);
    client = nfs_client_alloc(22, "compound-expire", 15, 0x7002, 1);
    state  = nfs_open_state_reserve_compound(client, "owner-A", 7, 0, NULL, 0,
                                             OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE, &table, &sid);
    CHECK(state != NULL);
    owner = state->owner;
    nfs_open_owner_get(owner);

    nfs_client_expire_state(client, &table, NULL);
    CHECK(!client->expired);
    CHECK(HASH_COUNT(client->open_owners_by_str) == 1);
    CHECK(owner->compound_pending == state);
    CHECK(nfs_state_table_validate(&table, &sid) == NFS4ERR_BAD_STATEID);

    nfs_open_state_finish_compound(state, false, &table, NULL);
    CHECK(atomic_load(&client->compound_pins) == 0);
    nfs_client_expire_state(client, &table, NULL);
    CHECK(client->expired);
    CHECK(HASH_COUNT(client->open_owners_by_str) == 0);
    CHECK(atomic_load(&owner->refcount) == 1);
    nfs_open_owner_put(owner);
    nfs_client_destroy(client, &table, NULL, true);
    nfs_state_table_free(&table, NULL);
    printf("ok: compound_reservation_pins_expiry\n");
} /* test_compound_reservation_pins_expiry */

/* Destroy waits for every reservation, then cleans both aborted reservations
 * and accepted states without leaving slot or owner references behind. */
static void
test_compound_reservation_defers_destroy(void)
{
    for (int published = 0; published <= 1; published++) {
        struct nfs_state_table table;
        struct nfs_client     *client;
        struct nfs_open_state *states[2];
        struct nfs_open_owner *owners[2];
        struct stateid4        ids[2];
        const char            *names[] = { "owner-A", "owner-B" };

        nfs_state_table_init(&table, 1);
        client = nfs_client_alloc(23, "compound-destroy", 16, 0x7003, 1);
        for (int i = 0; i < 2; i++) {
            states[i] = nfs_open_state_reserve_compound(client, names[i], 7, 0, NULL, 0,
                                                        OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE, &table, &ids[i])
            ;
            CHECK(states[i] != NULL);
            owners[i] = states[i]->owner;
            nfs_open_owner_get(owners[i]);
            states[i]->fh[0]  = i + 1;
            states[i]->fh_len = 1;
            if (published) {
                nfs_open_state_publish_compound(states[i], &table);
            }
        }
        CHECK(atomic_load(&client->compound_pins) == 2);
        if (published) {
            /* A concurrent CLOSE may invalidate a published slot before the
             * compound drops its construction reference. Finish must perform
             * final state cleanup while still pinning the client. */
            nfs_open_state_destroy(states[0], &table, NULL);
            CHECK(atomic_load(&states[0]->destroyed));
            CHECK(atomic_load(&states[0]->refcount) == 1);
            CHECK(nfs_state_table_validate(&table, &ids[0]) != NFS4_OK);
        }
        nfs_client_destroy(client, &table, NULL, true);
        CHECK(client->compound_destroy_pending);
        CHECK(HASH_COUNT(client->open_owners_by_str) == 2);
        nfs_open_state_finish_compound(states[0], published, &table, NULL);
        CHECK(atomic_load(&client->compound_pins) == 1);
        CHECK(client->compound_destroy_pending);
        nfs_open_state_finish_compound(states[1], published, &table, NULL);
        /* The final finish has destroyed client. Only our owner borrows live. */
        for (int i = 0; i < 2; i++) {
            CHECK(owners[i]->compound_pending == NULL);
            CHECK(owners[i]->states_by_fh == NULL);
            CHECK(atomic_load(&owners[i]->refcount) == 1);
            CHECK(nfs_state_table_validate(&table, &ids[i]) != NFS4_OK);
            nfs_open_owner_put(owners[i]);
        }
        nfs_state_table_free(&table, NULL);
    }
    printf("ok: compound_reservation_defers_destroy\n");
} /* test_compound_reservation_defers_destroy */

static struct nfs_open_state *
make_close_state(
    struct nfs_state_table *table,
    struct nfs_client      *client,
    const char             *owner,
    uint8_t                 fh,
    struct stateid4        *sid)
{
    struct nfs_open_state *state = nfs_open_state_reserve_compound(
        client, owner, strlen(owner), 1, "host", 4, OPEN4_SHARE_ACCESS_READ,
        OPEN4_SHARE_DENY_READ, table, sid);

    CHECK(state);
    state->fh[0]  = fh;
    state->fh_len = 1;
    nfs_open_state_publish_compound(state, table);
    nfs_open_state_finish_compound(state, true, table, NULL);
    return state;
} /* make_close_state */

/* A request without an owner journal still pins the client through retries.
 * Expiry neither renews nor destroys it, and teardown waits for the last pin. */
static void
test_compound_client_lifetime(void)
{
    struct nfs_state_table table;
    struct nfs_client     *client;
    struct nfs_open_state *state;
    struct nfs_open_owner *owner;
    struct stateid4        sid;
    uint64_t               touched;

    nfs_state_table_init(&table, 1);
    client = nfs_client_alloc(48, "client-pin", 10, 0x7148, 0);
    state  = make_close_state(&table, client, "owner", 1, &sid);
    owner  = state->owner;
    nfs_open_owner_get(owner);
    touched = client->last_touch_ns;
    CHECK(!nfs_client_reserve_compound(NULL));
    client->expired = 1;
    CHECK(!nfs_client_reserve_compound(client));
    client->expired = 0;
    atomic_store(&client->reclaim_pending, 1);
    CHECK(!nfs_client_reserve_compound(client));
    atomic_store(&client->reclaim_pending, 0);
    CHECK(nfs_client_reserve_compound(client));
    CHECK(nfs_client_reserve_compound(client));
    CHECK(atomic_load(&client->compound_pins) == 2);
    nfs_client_expire_state(client, &table, NULL);
    CHECK(!client->expired && client->last_touch_ns == touched);
    CHECK(nfs_state_table_validate(&table, &sid) == NFS4_OK);
    nfs_client_destroy(client, &table, NULL, true);
    CHECK(client->compound_destroy_pending);
    CHECK(!nfs_client_reserve_compound(client));
    nfs_client_finish_compound(client, &table, NULL);
    CHECK(atomic_load(&client->compound_pins) == 1);
    CHECK(nfs_state_table_validate(&table, &sid) == NFS4_OK);
    nfs_client_finish_compound(client, &table, NULL);
    /* Client is now freed; the independent owner borrow remains inspectable. */
    CHECK(!owner->states_by_fh && atomic_load(&owner->refcount) == 1);
    CHECK(nfs_state_table_validate(&table, &sid) != NFS4_OK);
    nfs_open_owner_put(owner);
    nfs_state_table_free(&table, NULL);
    printf("ok: compound_client_lifetime\n");
} /* test_compound_client_lifetime */

/* Session refs are deliberately not part of this proof: the published client
 * table protects lookup until its compound pin is acquired. Once removed from
 * the table, destruction must wait for that pin even if no session survives. */
static void
test_compound_client_table_pin(void)
{
    struct nfs_state_table   states;
    struct nfs4_client_table table  = { 0 };
    struct nfs4_client       record = { 0 };
    struct nfs_client       *client, *pinned = NULL;

    nfs_state_table_init(&states, 1);
    pthread_mutex_init(&table.nfs4_ct_lock, NULL);
    client                = nfs_client_alloc(49, "table-pin", 9, 0x7149, 0);
    record.nfs4_client_id = client->client_id;
    record.unified        = client;
    CHECK(nfs4_client_reserve_compound(&table, client->client_id, &pinned) == NFS4ERR_STALE_CLIENTID);
    CHECK(!pinned);
    HASH_ADD(nfs4_client_hh_by_id, table.nfs4_ct_clients_by_id, nfs4_client_id, sizeof(record.nfs4_client_id), &record);
    CHECK(nfs4_client_reserve_compound(&table, client->client_id, &pinned) == NFS4ERR_STALE_CLIENTID);
    record.nfs4_client_confirmed = 1;
    atomic_store(&client->reclaim_pending, 1);
    CHECK(nfs4_client_reserve_compound(&table, client->client_id, &pinned) == NFS4ERR_DELAY);
    CHECK(!pinned && !atomic_load(&client->compound_pins));
    atomic_store(&client->reclaim_pending, 0);
    CHECK(nfs4_client_reserve_compound(&table, client->client_id, &pinned) == NFS4_OK);
    CHECK(pinned == client && atomic_load(&client->compound_pins) == 1);
    HASH_DELETE(nfs4_client_hh_by_id, table.nfs4_ct_clients_by_id, &record);
    nfs_client_destroy(client, &states, NULL, false);
    CHECK(client->compound_destroy_pending);
    CHECK(nfs4_client_reserve_compound(&table, record.nfs4_client_id, &pinned) == NFS4ERR_STALE_CLIENTID);
    CHECK(!pinned);
    nfs_client_finish_compound(client, &states, NULL);
    pthread_mutex_destroy(&table.nfs4_ct_lock);
    nfs_state_table_free(&states, NULL);
    printf("ok: compound_client_table_pin\n");
} /* test_compound_client_table_pin */

static nfsstat4
reserve_close(
    struct nfs_state_table *table,
    struct nfs_client      *client,
    const struct stateid4  *sid,
    struct nfs_open_state  *state,
    struct nfs_open_state **reserved,
    struct stateid4        *reply)
{
    return nfs_open_state_reserve_close_compound(table, sid, client,
                                                 state->fh, state->fh_len, 1, "host", 4, table, NULL, reserved, reply);
} /* reserve_close */

static void
test_compound_close_abort_and_accept(void)
{
    struct nfs_state_table table;
    struct nfs_client     *client;
    struct nfs_open_state *state, *reserved, *second;
    struct stateid4        sid, reply, ignored;
    void                  *acquired;
    uint8_t                type;

    nfs_state_table_init(&table, 1);
    client                = nfs_client_alloc(30, "close", 5, 0x7100, 1);
    state                 = make_close_state(&table, client, "owner", 1, &sid);
    client->last_touch_ns = 123;
    atomic_store(&client->courtesy, 1);
    CHECK(nfs_state_table_acquire_no_renew(&table, &sid, NFS4_SLOT_TYPE_OPEN,
                                           &acquired, &type) == NFS4_OK);
    CHECK(client->last_touch_ns == 123 && atomic_load(&client->courtesy));
    nfs_state_table_release(&table, acquired, type, NULL);
    CHECK(reserve_close(&table, client, &sid, state, &reserved, &reply) == NFS4_OK);
    CHECK(reserved == state && reply.seqid == sid.seqid + 1);
    CHECK(client->last_touch_ns == 123 && state->seqid == sid.seqid);
    CHECK(nfs_state_table_validate(&table, &sid) == NFS4_OK);
    CHECK(nfs_state_table_acquire(&table, &sid, NFS4_SLOT_TYPE_OPEN,
                                  &acquired, &type) == NFS4ERR_DELAY);
    CHECK(reserve_close(&table, client, &sid, state, &second, &ignored) == NFS4ERR_DELAY);
    CHECK(!second);
    nfs_client_expire_state(client, &table, NULL);
    nfs_open_state_destroy(state, &table, NULL);
    CHECK(!client->expired && !atomic_load(&state->destroyed));
    CHECK(nfs_client_check_io_denied(client, NULL, state->fh, state->fh_len,
                                     OPEN4_SHARE_ACCESS_READ) == NFS4ERR_LOCKED);
    CHECK(nfs_client_check_io_denied_except(client, NULL, state->fh, state->fh_len,
                                            OPEN4_SHARE_ACCESS_READ, &reserved, 1) == NFS4_OK);
    CHECK(nfs_client_check_share_conflict_except(client, NULL, state->fh, state->fh_len,
                                                 OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE, &reserved, 1) ==
          NFS4_OK);
    CHECK(nfs_client_check_share_conflict(client, NULL, state->fh, state->fh_len,
                                          OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE) == NFS4ERR_SHARE_DENIED);
    CHECK(nfs_client_has_open_state_for_fh(client, state->fh, state->fh_len));
    CHECK(!nfs_client_has_open_state_for_fh_except(client, state->fh, state->fh_len,
                                                   &reserved, 1));
    CHECK(nfs_client_has_open_state_for_fh_except(client, state->fh, state->fh_len,
                                                  NULL, 0));
    nfs_open_state_finish_close_compound(reserved, false, &table, NULL);
    CHECK(nfs_client_has_open_state_for_fh(client, state->fh, state->fh_len));
    CHECK(client->last_touch_ns == 123 && state->seqid == sid.seqid);
    CHECK(!state->owner->compound_close_count && !state->owner->compound_close_group && !client->compound_pins);
    CHECK(nfs_state_table_acquire(&table, &sid, NFS4_SLOT_TYPE_OPEN,
                                  &acquired, &type) == NFS4_OK);
    CHECK(client->last_touch_ns != 123 && !atomic_load(&client->courtesy));
    nfs_state_table_release(&table, acquired, type, NULL);

    CHECK(reserve_close(&table, client, &sid, state, &reserved, &reply) == NFS4_OK);
    nfs_open_state_finish_close_compound(reserved, true, &table, NULL);
    CHECK(!client->compound_pins);
    CHECK(nfs_state_table_validate(&table, &sid) != NFS4_OK);
    CHECK(nfs_state_table_validate(&table, &reply) != NFS4_OK);
    nfs_client_destroy(client, &table, NULL, true);
    nfs_state_table_free(&table, NULL);
    printf("ok: compound CLOSE abort preserves public state; accepted CLOSE removes it\n");
} /* test_compound_close_abort_and_accept */

static void
test_compound_close_validation_and_contention(void)
{
    struct nfs_state_table table;
    struct nfs_client     *client, *other;
    struct nfs_open_state *state, *reserved;
    struct stateid4        sid, bad, reply;
    struct stateid4        lock_sid;
    struct nfs_lock_owner *lock_owner;
    struct nfs_lock_state *lock_state;
    bool                   created;
    uint8_t                bad_fh = 2, type;
    void                  *acquired;

    nfs_state_table_init(&table, 1);
    client = nfs_client_alloc(31, "close", 5, 0x7101, 1);
    other  = nfs_client_alloc(32, "other", 5, 0x7102, 1);
    state  = make_close_state(&table, client, "owner", 1, &sid);
    CHECK(reserve_close(&table, other, &sid, state, &reserved, &reply) == NFS4ERR_BAD_STATEID);
    CHECK(nfs_open_state_reserve_close_compound(&table, &sid, client, &bad_fh, 1,
                                                1, "host", 4, &table, NULL, &reserved, &reply) == NFS4ERR_BAD_STATEID);
    CHECK(nfs_open_state_reserve_close_compound(&table, &sid, client, state->fh, 1,
                                                1, "else", 4, &table, NULL, &reserved, &reply) == NFS4ERR_ACCESS);
    CHECK(nfs_open_state_reserve_close_compound(&table, &sid, client, state->fh, 1,
                                                1, "host", 4, NULL, NULL, &reserved, &reply) == NFS4ERR_INVAL);
    bad = sid;
    bad.seqid++;
    CHECK(reserve_close(&table, client, &bad, state, &reserved, &reply) == NFS4ERR_BAD_STATEID);
    nfs4_stateid_encode(&bad, sid.seqid, NFS4_STATEID_TYPE_OPEN, state->shard,
                        state->slot_idx, state->generation, table.epoch + 1);
    CHECK(reserve_close(&table, client, &bad, state, &reserved, &reply) == NFS4ERR_STALE_STATEID);
    state->seqid++;
    CHECK(reserve_close(&table, client, &sid, state, &reserved, &reply) == NFS4ERR_OLD_STATEID);
    state->seqid--;
    CHECK(nfs_state_table_acquire(&table, &sid, NFS4_SLOT_TYPE_OPEN,
                                  &acquired, &type) == NFS4_OK);
    CHECK(reserve_close(&table, client, &sid, state, &reserved, &reply) == NFS4ERR_DELAY);
    nfs_state_table_release(&table, acquired, type, NULL);
    nfs_open_owner_get(state->owner);
    CHECK(reserve_close(&table, client, &sid, state, &reserved, &reply) == NFS4ERR_DELAY);
    nfs_open_owner_put(state->owner);
    lock_owner = nfs_lock_owner_find_or_create(client, "lock", 4, &created);
    CHECK(lock_owner && created);
    lock_state = nfs_lock_state_create(lock_owner, state, NULL, &table, &lock_sid);
    CHECK(lock_state);
    CHECK(reserve_close(&table, client, &sid, state, &reserved, &reply) == NFS4ERR_DELAY);
    nfs_lock_state_destroy(lock_state, &table, NULL);
    CHECK(!client->compound_pins && !state->owner->compound_close_count && !state->owner->compound_close_group);
    CHECK(reserve_close(&table, client, &sid, state, &reserved, &reply) == NFS4_OK);
    CHECK(nfs_lock_state_create(lock_owner, state, NULL, &table, &lock_sid) == NULL);
    nfs_open_state_finish_close_compound(reserved, false, &table, NULL);
    nfs_lock_owner_put(lock_owner);
    nfs_client_destroy(other, &table, NULL, true);
    nfs_client_destroy(client, &table, NULL, true);
    nfs_state_table_free(&table, NULL);
    printf("ok: compound CLOSE validates state/FH/client/principal and excludes concurrent users\n");
} /* test_compound_close_validation_and_contention */

static void
test_compound_close_multiple_reservations_destroy(void)
{
    struct nfs_state_table table;
    struct nfs_client     *client;
    struct nfs_open_state *states[2], *reserved[2];
    struct nfs_open_owner *owners[2];
    struct stateid4        sid[2], reply[2];

    nfs_state_table_init(&table, 1);
    client    = nfs_client_alloc(33, "close", 5, 0x7103, 1);
    states[0] = make_close_state(&table, client, "owner-A", 1, &sid[0]);
    states[1] = make_close_state(&table, client, "owner-B", 2, &sid[1]);
    for (int i = 0; i < 2; i++) {
        CHECK(reserve_close(&table, client, &sid[i], states[i], &reserved[i], &reply[i]) == NFS4_OK);
        owners[i] = states[i]->owner;
        nfs_open_owner_get(owners[i]);
    }
    CHECK(client->compound_pins == 2);
    nfs_client_destroy(client, &table, NULL, true);
    CHECK(client->compound_destroy_pending);
    nfs_open_state_finish_close_compound(reserved[0], false, &table, NULL);
    CHECK(client->compound_pins == 1);
    nfs_open_state_finish_close_compound(reserved[1], true, &table, NULL);
    for (int i = 0; i < 2; i++) {
        CHECK(!owners[i]->compound_close_count && !owners[i]->compound_close_group && !owners[i]->states_by_fh);
        CHECK(atomic_load(&owners[i]->refcount) == 1);
        CHECK(nfs_state_table_validate(&table, &sid[i]) != NFS4_OK);
        nfs_open_owner_put(owners[i]);
    }
    nfs_state_table_free(&table, NULL);
    printf("ok: multiple CLOSE reservations pin client through deferred destroy\n");
} /* test_compound_close_multiple_reservations_destroy */

/* A group is identified independently of its first state: accepting that
 * CLOSE may free the state while its siblings are still reserved. */
static void
test_compound_close_owner_group(void)
{
    for (unsigned int accepted = 0; accepted < 4; accepted++) {
        for (unsigned int first = 0; first < 2; first++) {
            struct nfs_state_table table;
            struct nfs_client     *client;
            struct nfs_open_owner *owner;
            struct nfs_open_state *states[3], *reserved[2], *competing;
            struct stateid4        sid[3], reply[2], ignored;
            uint8_t                groups[2], fh;

            nfs_state_table_init(&table, 1);
            client    = nfs_client_alloc(34, "close-group", 11, 0x7104, 1);
            states[0] = make_close_state(&table, client, "owner", 1, &sid[0]);
            owner     = states[0]->owner;
            for (unsigned int i = 1; i < 3; i++) {
                fh        = i + 1;
                states[i] = nfs_open_state_create(owner, 1, "host", 4, &fh, 1,
                                                  OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_WRITE,
                                                  NULL, &table, &sid[i]);
                CHECK(states[i]);
            }
            for (unsigned int i = 0; i < 2; i++) {
                CHECK(nfs_open_state_reserve_close_compound(&table, &sid[i], client,
                                                            states[i]->fh, 1, 1, "host", 4, &groups[0], NULL,
                                                            &reserved[i], &reply[i]) == NFS4_OK);
                CHECK(owner->compound_close_count == i + 1);
                CHECK(owner->compound_close_group == &groups[0]);
                CHECK(!owner->compound_pending);
                CHECK(nfs_open_state_reserve_close_compound(&table, &sid[2], client,
                                                            states[2]->fh, 1, 1, "host", 4, &groups[1], NULL,
                                                            &competing, &ignored) == NFS4ERR_DELAY);
                CHECK(!competing);
            }
            fh = 4;
            CHECK(!nfs_open_state_create(owner, 1, "host", 4, &fh, 1,
                                         OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE,
                                         NULL, &table, &ignored));
            CHECK(!nfs_open_state_reserve_compound(client, "owner", 5, 1, "host", 4,
                                                   OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE,
                                                   &table, &ignored));

            nfs_open_state_finish_close_compound(reserved[first], accepted & (1u << first), &table, NULL);
            CHECK(owner->compound_close_count == 1 && owner->compound_close_group == &groups[0]);
            CHECK(client->compound_pins == 1);
            CHECK(nfs_open_state_reserve_close_compound(&table, &sid[2], client,
                                                        states[2]->fh, 1, 1, "host", 4, &groups[1], NULL,
                                                        &competing, &ignored) == NFS4ERR_DELAY);
            nfs_open_state_finish_close_compound(reserved[1 - first], accepted & (1u << (1 - first)), &table, NULL);
            CHECK(!owner->compound_close_count && !owner->compound_close_group && !client->compound_pins);
            for (unsigned int i = 0; i < 2; i++) {
                CHECK((nfs_state_table_validate(&table, &sid[i]) == NFS4_OK) == !(accepted & (1u << i)));
                if (!(accepted & (1u << i))) {
                    CHECK(states[i]->seqid == sid[i].seqid && !atomic_load(&states[i]->compound_closing));
                }
            }
            CHECK(nfs_open_state_reserve_close_compound(&table, &sid[2], client,
                                                        states[2]->fh, 1, 1, "host", 4, &groups[1], NULL,
                                                        &competing, &ignored) == NFS4_OK);
            nfs_open_state_finish_close_compound(competing, false, &table, NULL);
            nfs_client_destroy(client, &table, NULL, true);
            nfs_state_table_free(&table, NULL);
        }
    }
    printf("ok: same-owner CLOSE group supports mixed outcomes and completion order\n");
} /* test_compound_close_owner_group */

static void
test_compound_close_owner_group_defers_destroy(void)
{
    for (unsigned int first = 0; first < 2; first++) {
        struct nfs_state_table table;
        struct nfs_client     *client;
        struct nfs_open_owner *owner;
        struct nfs_open_state *states[2], *reserved[2];
        struct stateid4        sid[2], reply[2];
        uint8_t                fh = 2;

        nfs_state_table_init(&table, 1);
        client    = nfs_client_alloc(35, "close-group-destroy", 19, 0x7105, 1);
        states[0] = make_close_state(&table, client, "owner", 1, &sid[0]);
        owner     = states[0]->owner;
        states[1] = nfs_open_state_create(owner, 1, "host", 4, &fh, 1,
                                          OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_WRITE,
                                          NULL, &table, &sid[1]);
        CHECK(states[1]);
        for (unsigned int i = 0; i < 2; i++) {
            CHECK(reserve_close(&table, client, &sid[i], states[i], &reserved[i], &reply[i]) == NFS4_OK);
        }
        nfs_open_owner_get(owner);
        nfs_client_destroy(client, &table, NULL, true);
        CHECK(client->compound_destroy_pending && client->compound_pins == 2);
        nfs_open_state_finish_close_compound(reserved[first], true, &table, NULL);
        CHECK(client->compound_pins == 1 && owner->compound_close_count == 1);
        nfs_open_state_finish_close_compound(reserved[1 - first], false, &table, NULL);
        /* Last reservation triggered deferred client destruction. */
        CHECK(!owner->compound_close_count && !owner->compound_close_group && !owner->states_by_fh);
        CHECK(atomic_load(&owner->refcount) == 1);
        CHECK(nfs_state_table_validate(&table, &sid[0]) != NFS4_OK);
        CHECK(nfs_state_table_validate(&table, &sid[1]) != NFS4_OK);
        nfs_open_owner_put(owner);
        nfs_state_table_free(&table, NULL);
    }
    printf("ok: grouped CLOSE cleanup keeps client pinned through deferred destruction\n");
} /* test_compound_close_owner_group_defers_destroy */

static void
compound_stateid(
    struct nfs_state_table *table,
    struct nfs_open_state  *state,
    struct stateid4        *sid)
{
    nfs4_stateid_encode(sid, state->seqid, NFS4_STATEID_TYPE_OPEN,
                        state->shard, state->slot_idx, state->generation, table->epoch);
} /* compound_stateid */

static nfsstat4
reserve_owner(
    struct nfs_state_table             *table,
    struct nfs_client                  *client,
    uint32_t                            candidates,
    struct nfs_open_owner_reservation **reservation)
{
    return nfs_open_owner_reserve_compound(client, "owner", 5, 1, "host", 4,
                                           table, candidates, table, NULL, reservation);
} /* reserve_owner */

static void
test_compound_owner_reservation_abort(void)
{
    struct nfs_state_table             table;
    struct nfs_client                 *client;
    struct nfs_open_state             *states[2], *closed;
    struct nfs_open_owner             *owner;
    struct nfs_open_owner_reservation *reservation, *competing;
    struct stateid4                    ids[2], candidate_id, ignored;
    struct nfs_lock_owner             *lock_owner;
    struct nfs_lock_state             *lock_state;
    void                              *acquired;
    uint8_t                            type, fh = 2;
    bool                               created;

    nfs_state_table_init(&table, 1);
    client    = nfs_client_alloc(36, "owner-reserve", 13, 0x7106, 1);
    states[0] = make_close_state(&table, client, "owner", 1, &ids[0]);
    owner     = states[0]->owner;
    states[1] = nfs_open_state_create(owner, 1, "host", 4, &fh, 1,
                                      OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE, NULL, &table, &ids[1]);
    CHECK(states[1]);
    CHECK(nfs_state_table_acquire_no_renew(&table, &ids[1], NFS4_SLOT_TYPE_OPEN, &acquired, &type) == NFS4_OK);
    CHECK(reserve_owner(&table, client, 2, &reservation) == NFS4ERR_DELAY);
    CHECK(!reservation && !owner->compound_reservation && !client->compound_pins);
    CHECK(!atomic_load(&states[0]->compound_reserved));
    nfs_state_table_release(&table, acquired, type, NULL);
    nfs_open_owner_get(owner);
    CHECK(reserve_owner(&table, client, 2, &reservation) == NFS4ERR_DELAY);
    nfs_open_owner_put(owner);
    lock_owner = nfs_lock_owner_find_or_create(client, "lock", 4, &created);
    lock_state = nfs_lock_state_create(lock_owner, states[1], NULL, &table, &ignored);
    CHECK(lock_state);
    CHECK(reserve_owner(&table, client, 2, &reservation) == NFS4ERR_DELAY);
    CHECK(!reservation && !owner->compound_reservation && !client->compound_pins);
    CHECK(!atomic_load(&states[0]->compound_reserved));
    nfs_lock_state_destroy(lock_state, &table, NULL);
    CHECK(nfs_open_owner_reserve_compound(client, "owner", 5, 1, "other", 5,
                                          &table, 2, &table, NULL, &reservation) == NFS4ERR_ACCESS);
    CHECK(reserve_owner(&table, client, NFS4_COMPOUND_OWNER_MAX_STATES + 1, &reservation) == NFS4ERR_INVAL);

    client->last_touch_ns = 123;
    atomic_store(&client->courtesy, 1);
    CHECK(reserve_owner(&table, client, 2, &reservation) == NFS4_OK);
    CHECK(reservation->num_existing == 2 && reservation->num_candidates == 2);
    CHECK(client->last_touch_ns == 123 && atomic_load(&client->courtesy));
    CHECK(client->compound_pins == 1 && owner->compound_reservation == reservation);
    for (unsigned int i = 0; i < 2; i++) {
        CHECK(nfs_state_table_validate(&table, &ids[i]) == NFS4_OK);
        CHECK(nfs_state_table_acquire_no_renew(&table, &ids[i], NFS4_SLOT_TYPE_OPEN,
                                               &acquired, &type) == NFS4ERR_DELAY);
        nfs_open_state_destroy(states[i], &table, NULL);
        CHECK(!atomic_load(&states[i]->destroyed));
    }
    CHECK(nfs_lock_state_create(lock_owner, states[0], NULL, &table, &ignored) == NULL);
    CHECK(reserve_owner(&table, client, 1, &competing) == NFS4ERR_DELAY);
    CHECK(reserve_close(&table, client, &ids[0], states[0], &closed, &ignored) == NFS4ERR_DELAY);
    CHECK(!nfs_open_state_reserve_compound(client, "owner", 5, 1, "host", 4,
                                           OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE, &table, &ignored));
    compound_stateid(&table, reservation->candidates[0], &candidate_id);
    CHECK(nfs_state_table_validate(&table, &candidate_id) == NFS4ERR_BAD_STATEID);
    nfs_client_expire_state(client, &table, NULL);
    CHECK(!client->expired);
    nfs_open_owner_finish_compound(reservation, &table, NULL);
    CHECK(!owner->compound_reservation && !client->compound_pins);
    CHECK(client->last_touch_ns == 123 && atomic_load(&client->courtesy));
    CHECK(nfs_state_table_validate(&table, &candidate_id) != NFS4_OK);
    for (unsigned int i = 0; i < 2; i++) {
        CHECK(nfs_state_table_acquire_no_renew(&table, &ids[i], NFS4_SLOT_TYPE_OPEN,
                                               &acquired, &type) == NFS4_OK);
        nfs_state_table_release(&table, acquired, type, NULL);
    }
    nfs_lock_owner_put(lock_owner);
    nfs_client_destroy(client, &table, NULL, true);
    nfs_state_table_free(&table, NULL);
    printf("ok: compound owner reservation freezes existing states and abort restores them\n");
} /* test_compound_owner_reservation_abort */

static void
test_compound_owner_reservation_apply(void)
{
    struct nfs_state_table                table;
    struct nfs_client                    *client;
    struct nfs_open_state                *state, *candidate;
    struct nfs_open_owner_reservation    *reservation;
    struct nfs_open_state_compound_update update = { 0 };
    struct stateid4                       original, updated, new_sid, unused;
    uint8_t                               fh = 2, type;
    void                                 *acquired;

    nfs_state_table_init(&table, 1);
    client = nfs_client_alloc(37, "owner-apply", 11, 0x7107, 1);
    state  = make_close_state(&table, client, "owner", 1, &original);
    CHECK(reserve_owner(&table, client, 2, &reservation) == NFS4_OK);
    update.fh           = state->fh;
    update.fh_len       = state->fh_len;
    update.share_access = OPEN4_SHARE_ACCESS_BOTH;
    update.share_deny   = state->share_deny;
    update.share_combos = state->share_combos | nfs_open_combo_bit(OPEN4_SHARE_ACCESS_WRITE, OPEN4_SHARE_DENY_NONE);
    update.seqid        = original.seqid + 2;
    /* Journal changes alone leave the public identity and rights untouched. */
    CHECK(state->seqid == original.seqid && state->share_access == OPEN4_SHARE_ACCESS_READ);
    nfs_open_owner_apply_compound(reservation, state, &update, &table, NULL);
    compound_stateid(&table, state, &updated);
    CHECK(!memcmp(original.other, updated.other, sizeof(original.other)));
    CHECK(state->seqid == original.seqid + 2 && state->share_access == OPEN4_SHARE_ACCESS_BOTH);
    CHECK(state->share_combos == update.share_combos);
    CHECK(nfs_state_table_acquire_no_renew(&table, &updated, NFS4_SLOT_TYPE_OPEN,
                                           &acquired, &type) == NFS4ERR_DELAY);
    candidate     = reservation->candidates[0];
    update.fh     = &fh;
    update.fh_len = 1;
    update.seqid  = 2;
    nfs_open_owner_apply_compound(reservation, candidate, &update, &table, NULL);
    compound_stateid(&table, candidate, &new_sid);
    compound_stateid(&table, reservation->candidates[1], &unused);
    CHECK(nfs_state_table_validate(&table, &new_sid) == NFS4_OK);
    CHECK(nfs_state_table_acquire_no_renew(&table, &new_sid, NFS4_SLOT_TYPE_OPEN,
                                           &acquired, &type) == NFS4ERR_DELAY);
    nfs_open_owner_finish_compound(reservation, &table, NULL);
    CHECK(nfs_state_table_validate(&table, &unused) != NFS4_OK);
    CHECK(nfs_state_table_acquire_no_renew(&table, &updated, NFS4_SLOT_TYPE_OPEN,
                                           &acquired, &type) == NFS4_OK);
    CHECK(nfs4_stateid_check_seqid(state->seqid, original.seqid) == NFS4ERR_OLD_STATEID);
    nfs_state_table_release(&table, acquired, type, NULL);
    CHECK(nfs_state_table_acquire_no_renew(&table, &new_sid, NFS4_SLOT_TYPE_OPEN,
                                           &acquired, &type) == NFS4_OK);
    nfs_state_table_release(&table, acquired, type, NULL);
    nfs_client_destroy(client, &table, NULL, true);
    nfs_state_table_free(&table, NULL);
    printf("ok: accepted owner journal preserves existing identity and publishes new candidates\n");
} /* test_compound_owner_reservation_apply */

static void
test_compound_owner_downgrade_only(void)
{
    struct nfs_state_table                table;
    struct nfs_client                    *client;
    struct nfs_open_state                *state;
    struct nfs_open_owner_reservation    *reservation;
    struct nfs_open_state_compound_update update = { 0 };
    struct stateid4                       original, updated;
    void                                 *acquired;
    uint8_t                               type;

    nfs_state_table_init(&table, 1);
    client              = nfs_client_alloc(39, "owner-downgrade", 15, 0x7109, 1);
    state               = make_close_state(&table, client, "owner", 1, &original);
    state->share_access = OPEN4_SHARE_ACCESS_BOTH;
    state->share_deny   = OPEN4_SHARE_DENY_WRITE;
    state->share_combos = nfs_open_combo_bit(OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE) |
        nfs_open_combo_bit(OPEN4_SHARE_ACCESS_WRITE, OPEN4_SHARE_DENY_WRITE);
    CHECK(reserve_owner(&table, client, 0, &reservation) == NFS4_OK);
    CHECK(reservation->num_candidates == 0 && reservation->num_existing == 1);
    update.fh           = state->fh;
    update.fh_len       = state->fh_len;
    update.share_access = OPEN4_SHARE_ACCESS_READ;
    update.share_deny   = OPEN4_SHARE_DENY_NONE;
    update.share_combos = nfs_open_combo_bit(update.share_access, update.share_deny);
    update.seqid        = state->seqid + 1;
    CHECK(nfs_state_table_acquire_no_renew(&table, &original, NFS4_SLOT_TYPE_OPEN,
                                           &acquired, &type) == NFS4ERR_DELAY);
    /* Abort an attempt without applying its private reduced state. */
    nfs_open_owner_finish_compound(reservation, &table, NULL);
    CHECK(state->share_access == OPEN4_SHARE_ACCESS_BOTH && state->share_deny == OPEN4_SHARE_DENY_WRITE);
    CHECK(state->seqid == original.seqid && !client->compound_pins);
    CHECK(reserve_owner(&table, client, 0, &reservation) == NFS4_OK);
    nfs_open_owner_apply_compound(reservation, state, &update, &table, NULL);
    compound_stateid(&table, state, &updated);
    CHECK(!memcmp(updated.other, original.other, sizeof(updated.other)));
    CHECK(state->share_access == OPEN4_SHARE_ACCESS_READ && state->share_deny == OPEN4_SHARE_DENY_NONE);
    CHECK(state->share_combos == update.share_combos && state->seqid == original.seqid + 1);
    nfs_open_owner_finish_compound(reservation, &table, NULL);
    CHECK(nfs_state_table_acquire_no_renew(&table, &updated, NFS4_SLOT_TYPE_OPEN,
                                           &acquired, &type) == NFS4_OK);
    CHECK(nfs4_stateid_check_seqid(state->seqid, original.seqid) == NFS4ERR_OLD_STATEID);
    nfs_state_table_release(&table, acquired, type, NULL);
    nfs_client_destroy(client, &table, NULL, true);
    nfs_state_table_free(&table, NULL);
    printf("ok: downgrade-only owner reservation aborts and retries without allocating candidates\n");
} /* test_compound_owner_downgrade_only */

static void
test_pure_test_stateid_snapshot(void)
{
    struct nfs_state_table             table;
    struct nfs_layout_table            layouts;
    struct nfs_client                 *client, *foreign;
    struct nfs_open_state             *state;
    struct nfs_lock_owner             *lock_owner;
    struct nfs_lock_state             *lock;
    struct nfs_delegation             *deleg;
    struct nfs_layout_state           *layout;
    struct nfs_open_owner_reservation *reservation;
    struct stateid4                    sid, probe, lock_sid, deleg_sid, layout_sid;
    uint32_t                           refs;
    bool                               created;

    nfs_state_table_init(&table, 1);
    memset(&layouts, 0, sizeof(layouts));
    for (int i = 0; i < NFS_LAYOUT_TABLE_SHARDS; i++) {
        pthread_mutex_init(&layouts.shards[i].lock, NULL);
    }
    client                = nfs_client_alloc(40, "snapshot", 8, 0x7110, 1);
    foreign               = nfs_client_alloc(41, "foreign", 7, 0x7111, 1);
    state                 = make_close_state(&table, client, "owner", 1, &sid);
    state->seqid          = 3;
    sid.seqid             = 3;
    client->last_touch_ns = 123;
    refs                  = atomic_load(&state->refcount);
    CHECK(nfs_state_table_test_stateid(&table, &sid, client) == NFS4_OK);
    CHECK(nfs_state_table_advise(&table, &sid, client, state->fh, state->fh_len, 1, "host", 4) == NFS4_OK);
    CHECK(nfs_state_table_advise(&table, &sid, foreign, state->fh, state->fh_len, 1, "host", 4) == NFS4ERR_BAD_STATEID);
    CHECK(nfs_state_table_advise(&table, &sid, client, (const uint8_t *) "wrong", 5,
                                 1, "host", 4) == NFS4ERR_BAD_STATEID);
    CHECK(nfs_state_table_advise(&table, &sid, client, state->fh, state->fh_len,
                                 1, "other", 5) == NFS4ERR_ACCESS);
    probe       = sid;
    probe.seqid = 2;
    CHECK(nfs_state_table_test_stateid(&table, &probe, client) == NFS4ERR_OLD_STATEID);
    CHECK(nfs_state_table_advise(&table, &probe, client, state->fh, state->fh_len, 1, "host", 4) == NFS4ERR_OLD_STATEID)
    ;
    probe.seqid = 4;
    CHECK(nfs_state_table_test_stateid(&table, &probe, client) == NFS4ERR_BAD_STATEID);
    CHECK(nfs_state_table_advise(&table, &probe, client, state->fh, state->fh_len, 1, "host", 4) == NFS4ERR_BAD_STATEID)
    ;
    probe.seqid = 0;
    CHECK(nfs_state_table_test_stateid(&table, &probe, client) == NFS4_OK);
    CHECK(nfs_state_table_advise(&table, &probe, client, state->fh, state->fh_len, 1, "host", 4) == NFS4_OK);
    CHECK(nfs_state_table_test_stateid(&table, &sid, foreign) == NFS4ERR_BAD_STATEID);
    memset(&probe, 0, sizeof(probe));
    CHECK(nfs_state_table_test_stateid(&table, &probe, client) == NFS4ERR_BAD_STATEID);
    memset(&probe, 0xff, sizeof(probe));
    CHECK(nfs_state_table_test_stateid(&table, &probe, client) == NFS4ERR_BAD_STATEID);
    nfs4_stateid_encode(&probe, 3, NFS4_STATEID_TYPE_OPEN, state->shard,
                        state->slot_idx, state->generation, table.epoch + 1);
    CHECK(nfs_state_table_test_stateid(&table, &probe, client) == NFS4ERR_BAD_STATEID);
    nfs4_stateid_encode(&probe, 3, NFS4_STATEID_TYPE_LOCK, state->shard,
                        state->slot_idx, state->generation, table.epoch);
    CHECK(nfs_state_table_test_stateid(&table, &probe, client) == NFS4ERR_BAD_STATEID);
    CHECK(atomic_load(&state->refcount) == refs && client->last_touch_ns == 123);
    CHECK(reserve_owner(&table, client, 0, &reservation) == NFS4_OK);
    refs = atomic_load(&state->refcount);
    CHECK(nfs_state_table_test_stateid(&table, &sid, client) == NFS4_OK);
    CHECK(nfs_state_table_advise(&table, &sid, client, state->fh, state->fh_len, 1, "host", 4) == NFS4_OK);
    CHECK(atomic_load(&state->refcount) == refs && client->last_touch_ns == 123);
    nfs_open_owner_finish_compound(reservation, &table, NULL);

    lock_owner = nfs_lock_owner_find_or_create(client, "lock", 4, &created);
    lock       = nfs_lock_state_create(lock_owner, state, NULL, &table, &lock_sid);
    CHECK(lock);
    nfs_lock_owner_put(lock_owner);
    deleg = nfs_delegation_create(client, OPEN_DELEGATE_READ, state->fh, state->fh_len,
                                  1, 0, &table, &deleg_sid);
    CHECK(deleg);
    layout = nfs_layout_state_create(client, state->fh, state->fh_len, 0, LAYOUTIOMODE4_READ,
                                     table.epoch, &table, &layouts, &layout_sid);
    CHECK(layout);
    CHECK(nfs_state_table_test_stateid(&table, &lock_sid, client) == NFS4_OK);
    refs = atomic_load(&state->refcount);
    uint32_t lock_refs = atomic_load(&lock->refcount);
    CHECK(nfs_state_table_advise(&table, &lock_sid, client, state->fh, state->fh_len, 1, "host", 4) == NFS4_OK);
    CHECK(nfs_state_table_advise(&table, &lock_sid, client, (const uint8_t *) "wrong", 5,
                                 1, "host", 4) == NFS4ERR_BAD_STATEID);
    CHECK(nfs_state_table_advise(&table, &lock_sid, client, state->fh, state->fh_len,
                                 1, "other", 5) == NFS4ERR_ACCESS);
    CHECK(nfs_state_table_advise(&table, &lock_sid, foreign, state->fh, state->fh_len,
                                 1, "host", 4) == NFS4ERR_BAD_STATEID);
    CHECK(atomic_load(&state->refcount) == refs && atomic_load(&lock->refcount) == lock_refs &&
          client->last_touch_ns == 123);
    /* A CLOSE marks the parent destroyed before cascading through lock slots.
     * The lock's parent pin protects memory but cannot authorize a dead OPEN. */
    atomic_store(&state->destroyed, 1);
    CHECK(nfs_state_table_advise(&table, &lock_sid, client, state->fh, state->fh_len, 1, "host", 4) ==
          NFS4ERR_BAD_STATEID);
    atomic_store(&state->destroyed, 0);
    CHECK(nfs_state_table_test_stateid(&table, &deleg_sid, client) == NFS4_OK);
    CHECK(nfs_state_table_test_stateid(&table, &layout_sid, client) == NFS4_OK);
    CHECK(nfs_state_table_advise(&table, &deleg_sid, client, state->fh, state->fh_len, 1, "host", 4) ==
          NFS4ERR_BAD_STATEID);
    CHECK(nfs_state_table_advise(&table, &layout_sid, client, state->fh, state->fh_len, 1, "host", 4) ==
          NFS4ERR_BAD_STATEID);
    lock->seqid++;
    layout->seqid++;
    CHECK(nfs_state_table_test_stateid(&table, &lock_sid, client) == NFS4ERR_OLD_STATEID);
    CHECK(nfs_state_table_test_stateid(&table, &layout_sid, client) == NFS4ERR_OLD_STATEID);
    atomic_store(&deleg->revoked, 1);
    CHECK(nfs_state_table_test_stateid(&table, &deleg_sid, client) == NFS4ERR_DELEG_REVOKED);
    CHECK(nfs_state_table_test_stateid(&table, &deleg_sid, foreign) == NFS4ERR_BAD_STATEID);
    atomic_store(&client->reclaim_pending, 1);
    CHECK(nfs_state_table_test_stateid(&table, &sid, client) == NFS4ERR_EXPIRED);
    CHECK(nfs_state_table_test_stateid(&table, &layout_sid, client) == NFS4ERR_EXPIRED);
    CHECK(client->last_touch_ns == 123);
    atomic_store(&client->reclaim_pending, 0);
    nfs_client_destroy(client, &table, NULL, true);
    nfs_client_destroy(foreign, &table, NULL, true);
    for (int i = 0; i < NFS_LAYOUT_TABLE_SHARDS; i++) {
        CHECK(!layouts.shards[i].by_fh);
        pthread_mutex_destroy(&layouts.shards[i].lock);
    }
    nfs_state_table_free(&table, NULL);
    printf("ok: TEST_STATEID snapshots type/client/sequence/revocation without borrowing or renewal\n");
} /* test_pure_test_stateid_snapshot */

static void
test_compound_owner_close_reopen_destroy(void)
{
    struct nfs_state_table                table;
    struct nfs_client                    *client;
    struct nfs_open_owner                *owner;
    struct nfs_open_state                *old, *replacement;
    struct nfs_open_owner_reservation    *reservation;
    struct nfs_open_state_compound_update update = { 0 };
    struct stateid4                       old_sid, new_sid;
    uint8_t                               fh = 1;

    nfs_state_table_init(&table, 1);
    client = nfs_client_alloc(38, "owner-reopen", 12, 0x7108, 1);
    old    = make_close_state(&table, client, "owner", fh, &old_sid);
    CHECK(reserve_owner(&table, client, 1, &reservation) == NFS4_OK);
    owner = reservation->owner;
    nfs_open_owner_get(owner);
    replacement = reservation->candidates[0];
    nfs_open_owner_close_compound(reservation, old, old_sid.seqid + 1, &table, NULL);
    CHECK(nfs_state_table_validate(&table, &old_sid) != NFS4_OK);
    CHECK(atomic_load(&old->refcount) == 1);
    update.fh           = &fh;
    update.fh_len       = 1;
    update.share_access = OPEN4_SHARE_ACCESS_WRITE;
    update.share_combos = nfs_open_combo_bit(OPEN4_SHARE_ACCESS_WRITE, OPEN4_SHARE_DENY_NONE);
    update.seqid        = 1;
    nfs_open_owner_apply_compound(reservation, replacement, &update, &table, NULL);
    compound_stateid(&table, replacement, &new_sid);
    CHECK(memcmp(old_sid.other, new_sid.other, sizeof(old_sid.other)));
    CHECK(nfs_open_owner_find_state(owner, &fh, 1) == replacement);
    nfs_client_destroy(client, &table, NULL, true);
    CHECK(client->compound_pins == 1 && client->compound_destroy_pending);
    nfs_open_owner_finish_compound(reservation, &table, NULL);
    CHECK(!owner->compound_reservation && !owner->states_by_fh);
    CHECK(atomic_load(&owner->refcount) == 1);
    CHECK(nfs_state_table_validate(&table, &new_sid) != NFS4_OK);
    nfs_open_owner_put(owner);
    nfs_state_table_free(&table, NULL);
    printf("ok: owner journal CLOSE/reopen changes identity and pins deferred client destruction\n");
} /* test_compound_owner_close_reopen_destroy */

static void
test_compound_lock_owner_reservation(void)
{
    struct nfs_state_table             table;
    struct nfs_client                 *client;
    struct nfs_open_state             *parent;
    struct nfs_lock_owner             *owner;
    struct nfs_lock_state             *lock, *candidate;
    struct nfs_open_owner_reservation *opens;
    struct nfs_lock_owner_reservation *locks;
    struct nfs_lock_range_journal     *ranges;
    struct stateid4                    parent_sid, lock_sid, candidate_sid;
    void                              *borrowed = NULL;
    uint8_t                            type;
    bool                               created;

    nfs_state_table_init(&table, 1);
    client = nfs_client_alloc(41, "lock-journal", 12, 0x7141, 1);
    parent = make_close_state(&table, client, "owner", 1, &parent_sid);
    owner  = nfs_lock_owner_find_or_create(client, "locks", 5, &created);
    lock   = nfs_lock_state_create(owner, parent, NULL, &table, &lock_sid);
    CHECK(lock);
    nfs_lock_owner_put(owner);
    CHECK(reserve_owner(&table, client, 0, &opens) == NFS4ERR_DELAY);
    CHECK(nfs_open_owner_reserve_compound_locks(client, "owner", 5, 1, "host", 4,
                                                &table, 0, &table, NULL, &opens) == NFS4_OK);
    CHECK(opens->num_locks == 1 && atomic_load(&lock->refcount) == 2);
    CHECK(nfs_state_table_acquire(&table, &lock_sid, NFS4_SLOT_TYPE_LOCK, &borrowed, &type) == NFS4ERR_DELAY);
    CHECK(nfs_lock_owner_reserve_compound(client, "locks", 5, &table, 1, &table, NULL, &locks) == NFS4_OK);
    candidate = locks->candidates[0];
    nfs4_stateid_encode(&candidate_sid, 1, NFS4_STATEID_TYPE_LOCK, candidate->shard,
                        candidate->slot_idx, candidate->generation, table.epoch);
    CHECK(nfs_state_table_validate(&table, &candidate_sid) != NFS4_OK);
    CHECK(nfs_lock_state_create(owner, parent, NULL, &table, &candidate_sid) == NULL);
    nfs_lock_owner_finish_compound(locks, &table, NULL);
    CHECK(atomic_load(&owner->compound_pins) == 1);
    CHECK(atomic_load(&lock->compound_reserved));
    nfs_open_owner_finish_compound(opens, &table, NULL);
    CHECK(!atomic_load(&owner->compound_pins) && !atomic_load(&lock->compound_reserved));
    CHECK(lock->seqid == 1 && atomic_load(&parent->refcount) == 2);
    CHECK(nfs_state_table_acquire(&table, &lock_sid, NFS4_SLOT_TYPE_LOCK, &borrowed, &type) == NFS4_OK);
    nfs_state_table_release(&table, borrowed, type, NULL);

    /* A new lock owner has no child to pre-freeze. Its candidate remains
     * unpublished until accepted apply and then pins the published parent. */
    CHECK(nfs_open_owner_reserve_compound_locks(client, "owner", 5, 1, "host", 4,
                                                &table, 0, &table, NULL, &opens) == NFS4_OK);
    CHECK(nfs_lock_owner_reserve_compound(client, "new-locks", 9, &table, 1, &table, NULL, &locks) == NFS4_OK);
    candidate = locks->candidates[0];
    ranges    = nfs_lock_range_journal_alloc(NULL, 1);
    CHECK(ranges && nfs_lock_range_journal_empty(ranges));
    nfs_lock_state_apply_compound(locks, candidate, parent, NULL, 2, ranges, &table, NULL);
    nfs4_stateid_encode(&candidate_sid, 2, NFS4_STATEID_TYPE_LOCK, candidate->shard,
                        candidate->slot_idx, candidate->generation, table.epoch);
    CHECK(candidate->on_owner_list && candidate->on_open_list);
    CHECK(atomic_load(&candidate->refcount) == 2 && atomic_load(&parent->refcount) == 4);
    CHECK(nfs_state_table_acquire(&table, &candidate_sid, NFS4_SLOT_TYPE_LOCK, &borrowed, &type) == NFS4ERR_DELAY);
    nfs_lock_range_journal_free(ranges);
    nfs_lock_owner_finish_compound(locks, &table, NULL);
    nfs_open_owner_finish_compound(opens, &table, NULL);
    CHECK(nfs_state_table_acquire(&table, &candidate_sid, NFS4_SLOT_TYPE_LOCK, &borrowed, &type) == NFS4_OK);
    nfs_state_table_release(&table, borrowed, type, NULL);
    nfs_client_destroy(client, &table, NULL, true);
    nfs_state_table_free(&table, NULL);
    printf("ok: lock journals freeze child slots, abort candidates and publish accepted identities\n");
} /* test_compound_lock_owner_reservation */

static void
test_compound_lock_range_journal(void)
{
    struct nfs_state_table                 table;
    struct nfs_client                     *client;
    struct nfs_open_state                 *parent;
    struct nfs_lock_owner                 *owner;
    struct nfs_lock_state                 *lock;
    struct nfs_open_owner_reservation     *opens;
    struct nfs_lock_owner_reservation     *locks;
    struct nfs_lock_range_journal         *ranges;
    struct stateid4                        parent_sid, lock_sid;
    struct chimera_vfs                     vfs    = { 0 };
    struct chimera_vfs_thread              thread = { .vfs = &vfs };
    struct chimera_vfs_file_state         *file;
    struct chimera_claim_owner             identity = { .proto = CHIMERA_CLAIM_PROTO_NFSV4, .client_key = 42, .owner_lo
                                                               = 1 };
    struct chimera_claim_owner             foreign = { .proto = CHIMERA_CLAIM_PROTO_SMB2, .client_key = 999, .owner_lo =
                                                           1 };
    struct chimera_vfs_claim               admitted, probe;
    const struct chimera_vfs_claim *const *view;
    struct nfs4_range_lease               *original;
    uint32_t                               count;
    uint8_t                                fh = 1;
    bool                                   created;

    vfs.vfs_state = chimera_vfs_state_init();
    file          = chimera_vfs_state_get(vfs.vfs_state, &fh, 1, 1, true);
    nfs_state_table_init(&table, 1);
    client = nfs_client_alloc(42, "range-journal", 13, 0x7142, 1);
    parent = make_close_state(&table, client, "owner", fh, &parent_sid);
    owner  = nfs_lock_owner_find_or_create(client, "locks", 5, &created);
    lock   = nfs_lock_state_create(owner, parent, NULL, &table, &lock_sid);
    CHECK(lock);
    nfs_lock_owner_put(owner);
    original = calloc(1, sizeof(*original));
    CHECK(original);
    original->file_state = chimera_vfs_state_get(vfs.vfs_state, &fh, 1, 1, false);
    chimera_vfs_claim_init_range(&original->claim, false, false, 0, 100, &identity);
    CHECK(chimera_vfs_claim_try_acquire(vfs.vfs_state, file, &original->claim, NULL) == CHIMERA_CLAIM_GRANTED);
    lock->range_leases = original;
    CHECK(nfs_open_owner_reserve_compound_locks(client, "owner", 5, 1, "host", 4,
                                                &table, 0, &table, &thread, &opens) == NFS4_OK);
    CHECK(nfs_lock_owner_reserve_compound(client, "locks", 5, &table, 0, &table, &thread, &locks) == NFS4_OK);
    ranges = nfs_lock_range_journal_alloc(lock, 8);
    CHECK(ranges);
    chimera_vfs_claim_init_range(&admitted, true, false, 20, 20, &identity);
    CHECK(chimera_vfs_claim_try_acquire(vfs.vfs_state, file, &admitted, NULL) == CHIMERA_CLAIM_GRANTED);
    CHECK(nfs_lock_range_journal_lock(ranges, 20, 20, true, &admitted));
    CHECK(nfs_lock_range_journal_unlock(ranges, 30, 5));
    view = nfs_lock_range_journal_current(ranges, &count);
    CHECK(count == 4 && view[0]->offset == 0 && view[0]->length == 20);
    CHECK(view[1]->offset == 20 && view[1]->length == 10 && (view[1]->used & CHIMERA_CLAIM_LW));
    CHECK(view[2]->offset == 35 && view[2]->length == 5);
    CHECK(view[3]->offset == 40 && view[3]->length == 60);
    CHECK(original->claim.file == file && original->claim.offset == 0 && original->claim.length == 100);
    chimera_vfs_claim_init_range(&probe, true, true, 30, 5, &foreign);
    CHECK(chimera_vfs_claim_try_acquire(vfs.vfs_state, file, &probe, NULL) == CHIMERA_CLAIM_DENIED);

    /* Finish rejection discards tentative claims and restores the immutable
     * snapshot without changing the original range or public sequence. */
    chimera_vfs_claim_release(vfs.vfs_state, file, &admitted);
    nfs_lock_range_journal_reset(ranges);
    view = nfs_lock_range_journal_current(ranges, &count);
    CHECK(count == 1 && view[0]->offset == 0 && view[0]->length == 100);
    nfs_lock_range_journal_previous(ranges, &count);
    CHECK(count == 1 && lock->seqid == 1 && lock->range_leases == original);
    CHECK(chimera_vfs_claim_try_acquire(vfs.vfs_state, file, &admitted, NULL) == CHIMERA_CLAIM_GRANTED);
    CHECK(nfs_lock_range_journal_lock(ranges, 20, 20, true, &admitted));
    CHECK(nfs_lock_range_journal_unlock(ranges, 30, 5));
    nfs_lock_state_apply_compound(locks, lock, parent, NULL, 3, ranges, &table, &thread);
    CHECK(lock->seqid == 3 && !admitted.file);
    CHECK(chimera_vfs_claim_try_acquire(vfs.vfs_state, file, &probe, NULL) == CHIMERA_CLAIM_GRANTED);
    chimera_vfs_claim_release(vfs.vfs_state, file, &probe);
    chimera_vfs_claim_init_range(&probe, false, true, 25, 1, &foreign);
    CHECK(chimera_vfs_claim_try_acquire(vfs.vfs_state, file, &probe, NULL) == CHIMERA_CLAIM_DENIED);
    chimera_vfs_claim_init_range(&probe, true, true, 45, 1, &foreign);
    CHECK(chimera_vfs_claim_try_acquire(vfs.vfs_state, file, &probe, NULL) == CHIMERA_CLAIM_DENIED);
    nfs_lock_range_journal_free(ranges);
    nfs_lock_owner_finish_compound(locks, &table, &thread);
    nfs_open_owner_finish_compound(opens, &table, &thread);

    CHECK(nfs_open_owner_reserve_compound_locks(client, "owner", 5, 1, "host", 4,
                                                &table, 0, &table, &thread, &opens) == NFS4_OK);
    CHECK(nfs_lock_owner_reserve_compound(client, "locks", 5, &table, 0, &table, &thread, &locks) == NFS4_OK);
    ranges = nfs_lock_range_journal_alloc(lock, 2);
    CHECK(ranges && nfs_lock_range_journal_unlock(ranges, 0, UINT64_MAX));
    CHECK(nfs_lock_range_journal_empty(ranges));
    CHECK(nfs_lock_range_journal_unlock(ranges, 500, 1));
    nfs_lock_state_apply_compound(locks, lock, parent, NULL, 4, ranges, &table, &thread);
    CHECK(!lock->range_leases && lock->seqid == 4);
    nfs_lock_range_journal_free(ranges);
    nfs_lock_owner_finish_compound(locks, &table, &thread);
    nfs_open_owner_finish_compound(opens, &table, &thread);
    nfs_client_destroy(client, &table, &thread, true);
    nfs_state_table_free(&table, &thread);
    chimera_vfs_state_put(vfs.vfs_state, file);
    chimera_vfs_state_destroy(vfs.vfs_state);
    printf("ok: private range conversion/split/reset and accepted publication preserve competing claims\n");
} /* test_compound_lock_range_journal */

static void
test_compound_new_parent_lock_deferred_destroy(void)
{
    struct nfs_state_table                table;
    struct nfs_client                    *client;
    struct nfs_open_owner_reservation    *opens;
    struct nfs_lock_owner_reservation    *locks;
    struct nfs_open_state                *parent;
    struct nfs_lock_state                *lock;
    struct nfs_lock_owner                *owner;
    struct nfs_lock_range_journal        *ranges;
    struct nfs_open_state_compound_update update = { 0 };
    struct stateid4                       sid;
    uint8_t                               fh = 1;

    nfs_state_table_init(&table, 1);
    client = nfs_client_alloc(43, "new-parent-lock", 15, 0x7143, 1);
    CHECK(reserve_owner(&table, client, 1, &opens) == NFS4_OK);
    CHECK(nfs_lock_owner_reserve_compound(client, "locks", 5, &table, 1, &table, NULL, &locks) == NFS4_OK);
    parent = opens->candidates[0];
    lock   = locks->candidates[0];
    ranges = nfs_lock_range_journal_alloc(NULL, 1);
    CHECK(ranges);
    nfs4_stateid_encode(&sid, 1, NFS4_STATEID_TYPE_LOCK, lock->shard, lock->slot_idx, lock->generation, table.epoch);
    CHECK(nfs_state_table_validate(&table, &sid) != NFS4_OK);
    nfs_client_destroy(client, &table, NULL, true);
    CHECK(client->compound_destroy_pending && client->compound_pins == 2);
    update.fh           = &fh;
    update.fh_len       = 1;
    update.share_access = OPEN4_SHARE_ACCESS_READ;
    update.share_combos = nfs_open_combo_bit(OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE);
    update.seqid        = 1;
    nfs_open_owner_apply_compound(opens, parent, &update, &table, NULL);
    nfs_lock_state_apply_compound(locks, lock, parent, NULL, 1, ranges, &table, NULL);
    CHECK(parent->locks == lock && atomic_load(&parent->refcount) == 3);
    owner = locks->owner;
    nfs_lock_owner_get(owner);
    nfs_lock_range_journal_free(ranges);
    nfs_lock_owner_finish_compound(locks, &table, NULL);
    CHECK(client->compound_pins == 1 && owner->states == lock);
    nfs_open_owner_finish_compound(opens, &table, NULL);
    CHECK(!owner->states && atomic_load(&owner->refcount) == 1);
    CHECK(nfs_state_table_validate(&table, &sid) != NFS4_OK);
    nfs_lock_owner_put(owner);
    nfs_state_table_free(&table, NULL);
    printf("ok: same-compound OPEN/LOCK publication pins deferred client destruction through both groups\n");
} /* test_compound_new_parent_lock_deferred_destroy */

static void
test_compound_close_child_ranges(void)
{
    struct nfs_state_table                table;
    struct nfs_client                    *client;
    struct nfs_open_state                *parent;
    struct nfs_lock_owner                *owner;
    struct nfs_lock_state                *child;
    struct nfs_open_owner_reservation    *opens;
    struct nfs_lock_owner_reservation    *existing, *fresh;
    struct nfs_lock_range_journal        *old_ranges, *new_ranges;
    struct stateid4                       parent_sid, child_sid, candidate_sid;
    struct chimera_vfs                    vfs    = { 0 };
    struct chimera_vfs_thread             thread = { .vfs = &vfs };
    struct chimera_vfs_file_state        *file, *old_retired, *new_retired;
    struct nfs4_range_lease              *original;
    struct chimera_vfs_claim              tentative, probe;
    struct chimera_claim_owner            identity = { .proto = CHIMERA_CLAIM_PROTO_NFSV4, .client_key = 44, .owner_lo =
                                                           1 };
    struct nfs_open_state_compound_update update = { 0 };
    uint8_t                               fh     = 1;
    bool                                  created;

    vfs.vfs_state = chimera_vfs_state_init();
    file          = chimera_vfs_state_get(vfs.vfs_state, &fh, 1, 1, true);
    nfs_state_table_init(&table, 1);
    client = nfs_client_alloc(44, "close-children", 14, 0x7144, 1);
    parent = make_close_state(&table, client, "owner", fh, &parent_sid);
    owner  = nfs_lock_owner_find_or_create(client, "locks", 5, &created);
    child  = nfs_lock_state_create(owner, parent, NULL, &table, &child_sid);
    CHECK(child);
    nfs_lock_owner_put(owner);
    original = calloc(1, sizeof(*original));
    CHECK(original);
    original->file_state = chimera_vfs_state_get(vfs.vfs_state, &fh, 1, 1, false);
    chimera_vfs_claim_init_range(&original->claim, true, false, 0, 10, &identity);
    CHECK(chimera_vfs_claim_try_acquire(vfs.vfs_state, file, &original->claim, NULL) == CHIMERA_CLAIM_GRANTED);
    child->range_leases = original;
    CHECK(nfs_open_owner_reserve_compound_locks(client, "owner", 5, 1, "host", 4,
                                                &table, 0, &table, &thread, &opens) == NFS4_OK);
    CHECK(nfs_lock_owner_reserve_compound(client, "locks", 5, &table, 0, &table, &thread, &existing) == NFS4_OK);
    CHECK(nfs_lock_owner_reserve_compound(client, "new", 3, &table, 1, &table, &thread, &fresh) == NFS4_OK);
    old_ranges = nfs_lock_range_journal_alloc(child, 2);
    new_ranges = nfs_lock_range_journal_alloc(NULL, 2);
    CHECK(old_ranges && new_ranges);
    identity.owner_lo++;
    chimera_vfs_claim_init_range(&tentative, true, false, 10, 10, &identity);
    CHECK(chimera_vfs_claim_try_acquire(vfs.vfs_state, file, &tentative, NULL) == CHIMERA_CLAIM_GRANTED);
    CHECK(nfs_lock_range_journal_lock(new_ranges, 10, 10, true, &tentative));
    CHECK(nfs_lock_range_journal_unlock(old_ranges, 0, UINT64_MAX));
    CHECK(nfs_lock_range_journal_unlock(new_ranges, 0, UINT64_MAX));
    /* Rejected finish: a private CLOSE does not invalidate slots or release
     * original claims. Only tentative reservations unwind for retry. */
    CHECK(!atomic_load(&child->destroyed) && !atomic_load(&parent->destroyed));
    CHECK(original->claim.file == file && tentative.file == file);
    chimera_vfs_claim_release(vfs.vfs_state, file, &tentative);
    nfs_lock_range_journal_reset(old_ranges);
    nfs_lock_range_journal_reset(new_ranges);
    CHECK(!nfs_lock_range_journal_empty(old_ranges) && nfs_lock_range_journal_empty(new_ranges));
    CHECK(chimera_vfs_claim_try_acquire(vfs.vfs_state, file, &tentative, NULL) == CHIMERA_CLAIM_GRANTED);
    CHECK(nfs_lock_range_journal_lock(new_ranges, 10, 10, true, &tentative));

    /* Accepted downgrade changes the parent grant without destroying child
     * lock state. Its existing handle/range rights remain lifetime-pinned. */
    update.fh           = &fh;
    update.fh_len       = 1;
    update.share_access = OPEN4_SHARE_ACCESS_READ;
    update.share_combos = nfs_open_combo_bit(OPEN4_SHARE_ACCESS_READ, OPEN4_SHARE_DENY_NONE);
    update.seqid        = 2;
    nfs_open_owner_apply_compound(opens, parent, &update, &table, &thread);
    CHECK(parent->locks == child && child->seqid == 1 && original->claim.file == file);
    nfs4_stateid_encode(&candidate_sid, 1, NFS4_STATEID_TYPE_LOCK, fresh->candidates[0]->shard,
                        fresh->candidates[0]->slot_idx, fresh->candidates[0]->generation, table.epoch);
    old_retired = nfs_lock_range_journal_retire(old_ranges, &thread);
    new_retired = nfs_lock_range_journal_retire(new_ranges, &thread);
    CHECK(old_retired == file && new_retired == file && !original->claim.file && !tentative.file);
    nfs_open_owner_close_compound(opens, parent, 3, &table, &thread);
    CHECK(atomic_load(&parent->destroyed) && atomic_load(&child->destroyed));
    CHECK(atomic_load(&child->refcount) == 1 && !parent->locks);
    CHECK(nfs_state_table_validate(&table, &child_sid) != NFS4_OK);
    CHECK(nfs_state_table_validate(&table, &candidate_sid) != NFS4_OK);
    chimera_vfs_claim_replacement_complete(old_retired);
    chimera_vfs_claim_replacement_complete(new_retired);
    chimera_vfs_state_put(vfs.vfs_state, old_retired);
    chimera_vfs_state_put(vfs.vfs_state, new_retired);
    identity.proto = CHIMERA_CLAIM_PROTO_SMB2;
    identity.client_key++;
    chimera_vfs_claim_init_range(&probe, true, true, 0, 20, &identity);
    CHECK(chimera_vfs_claim_try_acquire(vfs.vfs_state, file, &probe, NULL) == CHIMERA_CLAIM_GRANTED);
    chimera_vfs_claim_release(vfs.vfs_state, file, &probe);
    nfs_lock_range_journal_free(old_ranges);
    nfs_lock_range_journal_free(new_ranges);
    nfs_lock_owner_finish_compound(fresh, &table, &thread);
    nfs_lock_owner_finish_compound(existing, &table, &thread);
    nfs_open_owner_finish_compound(opens, &table, &thread);
    nfs_client_destroy(client, &table, &thread, true);
    nfs_state_table_free(&table, &thread);
    chimera_vfs_state_put(vfs.vfs_state, file);
    chimera_vfs_state_destroy(vfs.vfs_state);
    printf("ok: accepted CLOSE retires existing and private child ranges, retry preserves public state\n");
} /* test_compound_close_child_ranges */

static void
test_compound_lock_parent_closure(void)
{
    struct nfs_state_table             table;
    struct nfs_client                 *client;
    struct nfs_open_state             *a, *b;
    struct nfs_lock_owner             *owner;
    struct nfs_open_owner_reservation *ga, *gb;
    struct nfs_lock_owner_reservation *locks;
    struct stateid4                    asid, bsid, lsid, parents[2];
    uint32_t                           count;
    bool                               created;

    nfs_state_table_init(&table, 1);
    client = nfs_client_alloc(45, "parent-closure", 14, 0x7145, 1);
    a      = make_close_state(&table, client, "parent-a", 1, &asid);
    b      = make_close_state(&table, client, "parent-b", 2, &bsid);
    owner  = nfs_lock_owner_find_or_create(client, "locks", 5, &created);
    CHECK(nfs_lock_state_create(owner, a, NULL, &table, &lsid));
    CHECK(nfs_lock_state_create(owner, b, NULL, &table, &lsid));
    nfs_lock_owner_put(owner);
    CHECK(nfs_lock_owner_compound_parents(client, "locks", 5, &table, &table, parents, 1, &count) == NFS4ERR_RESOURCE);
    CHECK(nfs_lock_owner_compound_parents(client, "locks", 5, &table, &table, parents, 2, &count) == NFS4_OK && count ==
          2);
    CHECK(nfs_open_owner_reserve_compound_locks(client, "parent-a", 8, 1, "host", 4,
                                                &table, 0, &table, NULL, &ga) == NFS4_OK);
    CHECK(nfs_lock_owner_compound_parents(client, "locks", 5, &table, &table, parents, 2, &count) == NFS4_OK && count ==
          1);
    CHECK(parents[0].seqid == 0 && !memcmp(parents[0].other, bsid.other, sizeof(bsid.other)));
    CHECK(nfs_lock_owner_reserve_compound(client, "locks", 5, &table, 0, &table, NULL, &locks) == NFS4ERR_DELAY);
    CHECK(nfs_open_owner_reserve_compound_locks(client, "parent-b", 8, 1, "host", 4,
                                                &table, 0, &table, NULL, &gb) == NFS4_OK);
    CHECK(nfs_lock_owner_compound_parents(client, "locks", 5, &table, &table, parents, 2, &count) == NFS4_OK && !count);
    CHECK(nfs_lock_owner_reserve_compound(client, "locks", 5, &table, 0, &table, NULL, &locks) == NFS4_OK);
    nfs_lock_owner_finish_compound(locks, &table, NULL);
    nfs_open_owner_finish_compound(gb, &table, NULL);
    nfs_open_owner_finish_compound(ga, &table, NULL);

    /* RELEASE_LOCKOWNER unpublishes before destroying its children. A
     * constructor in that interval must not freeze those retiring children. */
    pthread_mutex_lock(&client->lock);
    HASH_DELETE(hh, client->lock_owners_by_str, owner);
    pthread_mutex_unlock(&client->lock);
    CHECK(nfs_open_owner_reserve_compound_locks(client, "parent-a", 8, 1, "host", 4,
                                                &table, 0, &table, NULL, &ga) == NFS4ERR_DELAY);
    CHECK(!a->compound_reservation && !atomic_load(&a->compound_reserved));
    while (owner->states) {
        nfs_lock_state_destroy(owner->states, &table, NULL);
    }
    nfs_lock_owner_put(owner);
    nfs_client_destroy(client, &table, NULL, true);
    nfs_state_table_free(&table, NULL);
    printf("ok: lock-owner parent discovery closes sibling reservations and rejects unpublished owners\n");
} /* test_compound_lock_parent_closure */

static void
test_v40_owner_replay_journals(void)
{
    struct nfs_state_table             table;
    struct nfs_client                 *client;
    struct nfs_open_state             *parent;
    struct nfs_lock_state             *child;
    struct nfs_lock_owner             *lock_owner;
    struct nfs_open_owner_reservation *opens;
    struct nfs_lock_owner_reservation *locks;
    struct nfs4_replay_cache           tombstone;
    struct stateid4                    parent_sid, lock_sid, close_sid;
    struct LOCK4res                    denied = { 0 }, response;
    uint8_t                            source_owner[] = "denying-owner";
    uint8_t                            reply_owner[NFS4_OPAQUE_LIMIT];
    bool                               created;

    nfs_state_table_init(&table, 1);
    client = nfs_client_alloc(46, "v40-replay", 10, 0x7146, 0);
    parent = make_close_state(&table, client, "owner", 1, &parent_sid);
    CHECK(reserve_owner(&table, client, 0, &opens) == NFS4_OK);
    CHECK(!opens->owner_replay.initial_confirmed && !opens->owner_replay.confirmed);
    /* A late legacy OPEN must not publish a consuming error or enter VFS while
     * this owner has a reserved replay snapshot, even before confirmation. */
    {
        struct nfs_request  *request  = calloc(1, sizeof(*request));
        struct nfs4_session  session  = { .client_unified = client };
        struct nfs_argop4    argument = { .argop = OP_OPEN };
        struct nfs_resop4    response = { .resop = OP_OPEN };
        struct COMPOUND4args args     = { .argarray = &argument, .num_argarray = 1 };
        uint32_t             refs     = atomic_load(&parent->owner->refcount);
        nfsstat4             status   = NFS4_OK;

        CHECK(request);
        argument.opopen.owner.clientid   = client->client_id;
        argument.opopen.owner.owner.data = "owner";
        argument.opopen.owner.owner.len  = 5;
        argument.opopen.seqid            = 1;
        request->session                 = &session;
        request->args_compound           = &args;
        request->res_compound.resarray   = &response;
        CHECK(chimera_nfs4_open_4_0_entry(NULL, request, 0, &status));
        CHECK(status == NFS4ERR_DELAY && response.opopen.status == NFS4ERR_DELAY);
        CHECK(!request->open_4_0_owner);
        CHECK(atomic_load(&parent->owner->refcount) == refs);
        CHECK(!parent->owner->replay.valid && parent->owner->seqid == 0);
        free(request);
    }
    opens->owner_replay.confirmed = true;
    close_sid                     = parent_sid;
    close_sid.seqid++;
    nfs4_owner_replay_record(&opens->owner_replay, 1, OP_OPEN_CONFIRM, NFS4_OK, &close_sid);
    CHECK(opens->owner_replay.dirty && !parent->owner->confirmed && parent->seqid == parent_sid.seqid);
    nfs4_owner_replay_reset(&opens->owner_replay);
    CHECK(!opens->owner_replay.confirmed && !opens->owner_replay.dirty && !parent->owner->confirmed);
    opens->owner_replay.confirmed = true;
    nfs4_owner_replay_record(&opens->owner_replay, 1, OP_OPEN_CONFIRM, NFS4_OK, &close_sid);
    nfs_open_owner_publish_replay(opens);
    nfs_open_owner_confirm_compound(opens, parent, close_sid.seqid);
    CHECK(parent->owner->confirmed && parent->seqid == close_sid.seqid);
    CHECK(parent->owner->replay.op == OP_OPEN_CONFIRM && parent->owner->seqid == 1);
    nfs_open_owner_finish_compound(opens, &table, NULL);
    parent_sid           = close_sid;
    parent->owner->seqid = 10;
    nfs4_replay_record(&parent->owner->replay, 10, OP_OPEN_CONFIRM, NFS4_OK, &parent_sid);
    lock_owner = nfs_lock_owner_find_or_create(client, "locks", 5, &created);
    child      = nfs_lock_state_create(lock_owner, parent, NULL, &table, &lock_sid);
    CHECK(child);
    lock_owner->seqid = 4;
    nfs4_replay_record(&lock_owner->replay, 4, OP_LOCKU, NFS4_OK, &lock_sid);
    nfs_lock_owner_put(lock_owner);
    CHECK(nfs_open_owner_reserve_compound_locks(client, "owner", 5, 1, "host", 4,
                                                &table, 0, &table, NULL, &opens) == NFS4_OK);
    CHECK(opens->owner_replay.initial_confirmed && opens->owner_replay.confirmed);
    CHECK(nfs_lock_owner_reserve_compound(client, "locks", 5, &table, 0, &table, NULL, &locks) == NFS4_OK);
    CHECK(nfs4_owner_replay_classify(&opens->owner_replay, 10, OP_OPEN_CONFIRM) == NFS4_SEQID_REPLAY);
    CHECK(nfs4_owner_replay_classify(&opens->owner_replay, 10, OP_CLOSE) == NFS4_SEQID_BAD);
    CHECK(nfs4_owner_replay_classify(&opens->owner_replay, 11, OP_OPEN_DOWNGRADE) == NFS4_SEQID_NEW);
    nfs4_owner_replay_record(&opens->owner_replay, 11, OP_OPEN_DOWNGRADE, NFS4ERR_OLD_STATEID, NULL);
    CHECK(opens->owner_replay.dirty && opens->owner_replay.seqid == 11);
    CHECK(parent->owner->seqid == 10 && parent->owner->replay.op == OP_OPEN_CONFIRM);
    nfs4_owner_replay_reset(&opens->owner_replay);
    CHECK(!opens->owner_replay.dirty && opens->owner_replay.seqid == 10);
    nfs4_owner_replay_record(&opens->owner_replay, 11, OP_OPEN_DOWNGRADE, NFS4ERR_DELAY, NULL);
    CHECK(!opens->owner_replay.dirty && opens->owner_replay.seqid == 10);
    nfs4_owner_replay_record(&opens->owner_replay, 11, OP_OPEN_DOWNGRADE, NFS4ERR_INVAL, NULL);
    CHECK(opens->owner_replay.dirty && opens->owner_replay.replay.status == NFS4ERR_INVAL);
    nfs4_owner_replay_reset(&opens->owner_replay);

    /* A denied new-owner LOCK consumes both owner sequences, but neither
     * shared owner changes until finish accepts. The denial owns its bytes. */
    denied.status                  = NFS4ERR_DENIED;
    denied.denied.offset           = 17;
    denied.denied.length           = UINT64_MAX;
    denied.denied.locktype         = WRITE_LT;
    denied.denied.owner.clientid   = 99;
    denied.denied.owner.owner.data = source_owner;
    denied.denied.owner.owner.len  = sizeof(source_owner) - 1;
    nfs4_owner_replay_record_lock(&opens->owner_replay, 11, &denied);
    nfs4_owner_replay_record_lock(&locks->owner_replay, 5, &denied);
    memset(source_owner, 'x', sizeof(source_owner));
    CHECK(parent->owner->seqid == 10 && lock_owner->seqid == 4);
    CHECK(nfs4_owner_replay_classify(&locks->owner_replay, 5, OP_LOCK) == NFS4_SEQID_REPLAY);
    CHECK(nfs4_owner_replay_classify(&locks->owner_replay, 5, OP_LOCKU) == NFS4_SEQID_BAD);
    CHECK(nfs4_replay_fill_lock(&locks->owner_replay.replay, &response, reply_owner));
    CHECK(response.status == NFS4ERR_DENIED && response.denied.offset == 17 && response.denied.length == UINT64_MAX);
    CHECK(response.denied.locktype == WRITE_LT && response.denied.owner.clientid == 99);
    CHECK(response.denied.owner.owner.data == reply_owner && !memcmp(reply_owner, "denying-owner", 13));
    nfs_open_owner_publish_replay(opens);
    nfs_lock_owner_publish_replay(locks);
    CHECK(parent->owner->seqid == 11 && lock_owner->seqid == 5);
    CHECK(parent->owner->replay.lock_denied_valid && lock_owner->replay.lock_denied_valid);
    nfs_lock_owner_finish_compound(locks, &table, NULL);
    nfs_open_owner_finish_compound(opens, &table, NULL);

    CHECK(nfs_open_owner_reserve_compound_locks(client, "owner", 5, 1, "host", 4,
                                                &table, 0, &table, NULL, &opens) == NFS4_OK);
    CHECK(nfs_lock_owner_reserve_compound(client, "locks", 5, &table, 0, &table, NULL, &locks) == NFS4_OK);
    close_sid = parent_sid;
    close_sid.seqid++;
    nfs4_owner_replay_record(&opens->owner_replay, 12, OP_CLOSE, NFS4_OK, &close_sid);
    CHECK(!opens->owner_replay.replay.lock_denied_valid);
    nfs_open_owner_publish_replay(opens);
    nfs_open_owner_close_compound(opens, parent, close_sid.seqid, &table, NULL);
    CHECK(nfs_state_table_lookup_replay(&table, &parent_sid, OP_CLOSE, 12, &tombstone) == NFS4_OK);
    CHECK(tombstone.status == NFS4_OK && !memcmp(&tombstone.stateid, &close_sid, sizeof(close_sid)));
    CHECK(nfs_state_table_lookup_replay(&table, &parent_sid, OP_LOCK, 12, &tombstone) != NFS4_OK);
    nfs_lock_owner_finish_compound(locks, &table, NULL);
    nfs_open_owner_finish_compound(opens, &table, NULL);
    nfs_client_destroy(client, &table, NULL, true);
    nfs_state_table_free(&table, NULL);
    printf("ok: v4.0 owner journals reset consuming errors and publish typed DENIED/CLOSE replay only on acceptance\n");
} /* test_v40_owner_replay_journals */

static void
test_typed_open_replay_snapshot(void)
{
    struct nfs4_replay_cache         cache = { 0 }, copied;
    struct nfs4_owner_replay_journal journal = { 0 };
    struct OPEN4res                  source = { .status = NFS4_OK }, reply;
    uint32_t                         attrset[] = { 0x102, 0x405, 0x607 }, reply_attrset[3];
    uint8_t                          fh[] = { 1, 2, 3, 4, 5 }, reply_fh[NFS4_FHSIZE];
    uint32_t                         reply_fh_len;

    source.resok4.stateid.seqid = 3;
    memset(source.resok4.stateid.other, 0x35, sizeof(source.resok4.stateid.other));
    source.resok4.rflags                     = OPEN4_RESULT_CONFIRM | OPEN4_RESULT_LOCKTYPE_POSIX;
    source.resok4.cinfo.atomic               = 1;
    source.resok4.cinfo.before               = 17;
    source.resok4.cinfo.after                = 19;
    source.resok4.num_attrset                = 3;
    source.resok4.attrset                    = attrset;
    source.resok4.delegation.delegation_type = OPEN_DELEGATE_NONE;
    CHECK(nfs4_replay_record_open(&cache, 9, &source, fh, sizeof(fh)));
    copied = cache;
    memset(&cache, 0, sizeof(cache));
    memset(attrset, 0, sizeof(attrset));
    memset(fh, 0, sizeof(fh));
    CHECK(nfs4_replay_fill_open(&copied, &reply, reply_attrset, reply_fh, &reply_fh_len, NULL));
    CHECK(reply.status == NFS4_OK && reply.resok4.stateid.seqid == 3);
    CHECK(reply.resok4.rflags == (OPEN4_RESULT_CONFIRM | OPEN4_RESULT_LOCKTYPE_POSIX));
    CHECK(reply.resok4.cinfo.atomic && reply.resok4.cinfo.before == 17 && reply.resok4.cinfo.after == 19);
    CHECK(reply.resok4.num_attrset == 3 && reply.resok4.attrset == reply_attrset);
    CHECK(reply_attrset[0] == 0x102 && reply_attrset[1] == 0x405 && reply_attrset[2] == 0x607);
    CHECK(reply_fh_len == 5 && reply_fh[0] == 1 && reply_fh[4] == 5);
    CHECK(reply.resok4.delegation.delegation_type == OPEN_DELEGATE_NONE);

    journal.initial_seqid  = 9;
    journal.initial_replay = copied;
    nfs4_owner_replay_reset(&journal);
    reply.resok4.cinfo.after = 29;
    CHECK(nfs4_owner_replay_record_open(&journal, 10, &reply, reply_fh, reply_fh_len));
    CHECK(journal.dirty && journal.replay.open.cinfo.after == 29);
    CHECK(journal.initial_replay.open.cinfo.after == 19);
    nfs4_owner_replay_reset(&journal);
    CHECK(!journal.dirty && journal.seqid == 9 && journal.replay.open.cinfo.after == 19);
    /* A malformed oversized principal must never publish a partial replay. */
    reply.resok4.delegation.delegation_type          = OPEN_DELEGATE_READ;
    reply.resok4.delegation.read.permissions.who.len = NFS4_OPAQUE_LIMIT + 1;
    CHECK(!nfs4_owner_replay_record_open(&journal, 10, &reply, reply_fh, reply_fh_len));
    CHECK(!journal.dirty && journal.seqid == 9 && journal.replay.open_valid);
    CHECK(!nfs4_replay_record_open(&cache, 10, &reply, reply_fh, reply_fh_len));
    CHECK(cache.valid && cache.op == OP_OPEN && !cache.open_valid);

    for (unsigned type = OPEN_DELEGATE_READ; type <= OPEN_DELEGATE_WRITE; type++) {
        uint8_t         who[] = "12345", replay_who[NFS4_OPAQUE_LIMIT];
        struct nfsace4 *permissions;
        struct stateid4 delegated = { .seqid = 17 };
        memset(delegated.other, 0xa5, sizeof(delegated.other));
        memset(&source.resok4.delegation, 0, sizeof(source.resok4.delegation));
        source.resok4.delegation.delegation_type = type;
        if (type == OPEN_DELEGATE_READ) {
            source.resok4.delegation.read.stateid = delegated;
            source.resok4.delegation.read.recall  = 1;
            permissions                           = &source.resok4.delegation.read.permissions;
        } else {
            source.resok4.delegation.write.stateid              = delegated;
            source.resok4.delegation.write.recall               = 1;
            source.resok4.delegation.write.space_limit.limitby  = NFS_LIMIT_SIZE;
            source.resok4.delegation.write.space_limit.filesize = UINT64_MAX;
            permissions                                         = &source.resok4.delegation.write.permissions;
        }
        permissions->type        = ACE4_ACCESS_ALLOWED_ACE_TYPE;
        permissions->flag        = 7;
        permissions->access_mask = ACE4_GENERIC_READ;
        permissions->who.data    = who;
        permissions->who.len     = sizeof(who) - 1;
        CHECK(nfs4_replay_record_open(&cache, 12, &source, reply_fh, reply_fh_len));
        copied = cache;
        memset(&cache, 0, sizeof(cache));
        memset(who, 0, sizeof(who));
        memset(&source.resok4.delegation, 0, sizeof(source.resok4.delegation));
        CHECK(nfs4_replay_fill_open(&copied, &reply, reply_attrset, reply_fh, &reply_fh_len, replay_who));
        CHECK(reply.resok4.delegation.delegation_type == type);
        if (type == OPEN_DELEGATE_READ) {
            CHECK(!memcmp(&reply.resok4.delegation.read.stateid, &delegated, sizeof(delegated)));
            CHECK(reply.resok4.delegation.read.recall);
            permissions = &reply.resok4.delegation.read.permissions;
        } else {
            CHECK(!memcmp(&reply.resok4.delegation.write.stateid, &delegated, sizeof(delegated)));
            CHECK(reply.resok4.delegation.write.recall);
            CHECK(reply.resok4.delegation.write.space_limit.limitby == NFS_LIMIT_SIZE);
            CHECK(reply.resok4.delegation.write.space_limit.filesize == UINT64_MAX);
            permissions = &reply.resok4.delegation.write.permissions;
        }
        CHECK(permissions->type == ACE4_ACCESS_ALLOWED_ACE_TYPE && permissions->flag == 7);
        CHECK(permissions->access_mask == ACE4_GENERIC_READ && permissions->who.len == 5);
        CHECK(permissions->who.data == replay_who && !memcmp(replay_who, "12345", 5));
        journal.initial_replay = copied;
        nfs4_owner_replay_reset(&journal);
        CHECK(nfs4_owner_replay_record_open(&journal, 10, &reply, reply_fh, reply_fh_len));
        memset(replay_who, 0, sizeof(replay_who));
        nfs4_owner_replay_reset(&journal);
        CHECK(nfs4_replay_fill_open(&journal.replay, &reply, reply_attrset, reply_fh, &reply_fh_len, replay_who));
        CHECK(!memcmp(replay_who, "12345", 5));
    }
    source.status = NFS4ERR_ACCESS;
    CHECK(nfs4_replay_record_open(&cache, 11, &source, NULL, 0));
    CHECK(nfs4_replay_fill_open(&cache, &reply, NULL, NULL, &reply_fh_len, NULL));
    CHECK(reply.status == NFS4ERR_ACCESS && !reply_fh_len && !cache.open_valid);
    printf("ok: typed OPEN replay owns cinfo/attrset/FH, resets attempts and owns READ/WRITE delegation snapshots\n");
} /* test_typed_open_replay_snapshot */

int
main(
    int   argc,
    char *argv[])
{
    (void) argc;
    (void) argv;
    test_open_owner_borrow_survives_sweep();
    test_lock_owner_borrow_survives_sweep();
    test_idle_expiry_frees_owners();
    test_lock_state_install_after_expire_fails();
    test_lock_state_install_after_close_fails();
    test_expire_with_fully_linked_lock_state();
    test_open_owner_adopt_after_sweep();
    test_adopt_prefers_published_owner();
    test_refcount_abort_diagnostics();
    test_concurrent_install_vs_expire();
    test_compound_reservation_abort();
    test_compound_reservation_publish();
    test_compound_reservation_pins_expiry();
    test_compound_reservation_defers_destroy();
    test_compound_client_lifetime();
    test_compound_client_table_pin();
    test_compound_close_abort_and_accept();
    test_compound_close_validation_and_contention();
    test_compound_close_multiple_reservations_destroy();
    test_compound_close_owner_group();
    test_compound_close_owner_group_defers_destroy();
    test_compound_owner_reservation_abort();
    test_compound_owner_reservation_apply();
    test_compound_owner_downgrade_only();
    test_pure_test_stateid_snapshot();
    test_compound_owner_close_reopen_destroy();
    test_compound_lock_owner_reservation();
    test_compound_lock_range_journal();
    test_compound_new_parent_lock_deferred_destroy();
    test_compound_close_child_ranges();
    test_compound_lock_parent_closure();
    test_v40_owner_replay_journals();
    test_typed_open_replay_snapshot();
    printf("PASS: all open_owner lifetime tests\n");
    return 0;
} /* main */
