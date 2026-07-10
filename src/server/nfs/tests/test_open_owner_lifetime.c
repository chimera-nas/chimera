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
    /* The chimera crash handler may terminate via raw SIGABRT or via a
     * nonzero exit (sanitizer builds); either way the child must not have
     * survived to _exit(0). */
    CHECK((WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT) ||
          (WIFEXITED(status) && WEXITSTATUS(status) != 0));
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
    printf("PASS: all open_owner lifetime tests\n");
    return 0;
} /* main */
