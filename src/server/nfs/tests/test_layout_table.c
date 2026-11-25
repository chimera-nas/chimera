// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* The server-wide layout table's recall barrier, driven directly: batched
 * holder snapshots, an overlapping recall joining without re-snapshotting,
 * and the barrier firing every waiter when the last holder leaves. */

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "nfs4_layout_table.h"
#include "nfs4_state.h"
#include "nfs4_lease.h"

#define CHECK(cond) \
        do { \
            if (!(cond)) { \
                fprintf(stderr, "%s:%d: CHECK failed: %s\n", \
                        __FILE__, __LINE__, #cond); \
                abort(); \
            } \
        } while (0)

/* nfs4_layout_table.c pins snapshotted holders with these; the real ones live
 * in nfs4_state.c, which this test does not link. */
void
nfs_layout_state_get(struct nfs_layout_state *st)
{
    atomic_fetch_add_explicit(&st->refcount, 1, memory_order_acq_rel);
} /* nfs_layout_state_get */

void
nfs_layout_state_put(struct nfs_layout_state *st)
{
    atomic_fetch_sub_explicit(&st->refcount, 1, memory_order_acq_rel);
} /* nfs_layout_state_put */

/* collect_overdue also pins the holder's client; count those refs. */
static int client_refs;

struct nfs_client *
nfs_client_get(struct nfs_client *client)
{
    if (client) {
        client_refs++;
    }
    return client;
} /* nfs_client_get */

void
nfs_client_put(struct nfs_client *client)
{
    if (client) {
        client_refs--;
    }
} /* nfs_client_put */

static int resumed;

static void
count_resume(void *arg)
{
    (void) arg;
    resumed++;
} /* count_resume */

static struct nfs_layout_state *
mk_holder(
    const uint8_t *fh,
    uint16_t       fh_len)
{
    struct nfs_layout_state *ls = calloc(1, sizeof(*ls));

    CHECK(ls != NULL);
    memcpy(ls->fh, fh, fh_len);
    ls->fh_len = fh_len;
    atomic_init(&ls->refcount, 1);
    return ls;
} /* mk_holder */

static struct nfs_layout_recall_waiter *
mk_waiter(void *arg)
{
    struct nfs_layout_recall_waiter *w = calloc(1, sizeof(*w));

    CHECK(w != NULL);
    w->resume = count_resume;
    w->arg    = arg;
    return w;
} /* mk_waiter */

static void
put_all(
    struct nfs_layout_state **snap,
    int                       n)
{
    int i;

    for (i = 0; i < n; i++) {
        nfs_layout_state_put(snap[i]);
    }
} /* put_all */

/* 40 holders: the first recall takes 32, the next batch 8, then none; an
 * overlapping recall joins with 0; both waiters fire on the last return. */
static void
test_batches(void)
{
    struct nfs_layout_table          table;
    struct nfs_layout_state         *h[40], *snap[32];
    struct nfs_layout_recall_waiter *w1, *w2;
    const uint8_t                    fh[] = { 9, 8, 7, 6 };
    int                              i, n;

    nfs_layout_table_init(&table);
    resumed = 0;

    w1 = mk_waiter(NULL);
    CHECK(nfs_layout_table_recall_prepare(&table, fh, 4, w1, snap, 32) == -1);

    for (i = 0; i < 40; i++) {
        h[i] = mk_holder(fh, 4);
        nfs_layout_table_register(&table, h[i]);
    }

    n = nfs_layout_table_recall_prepare(&table, fh, 4, w1, snap, 32);
    CHECK(n == 32);
    put_all(snap, n);
    n = nfs_layout_table_recall_prepare(&table, fh, 4, NULL, snap, 32);
    CHECK(n == 8);
    put_all(snap, n);
    CHECK(nfs_layout_table_recall_prepare(&table, fh, 4, NULL, snap, 32) == 0);
    for (i = 0; i < 40; i++) {
        CHECK(h[i]->recall_snapped);
    }

    w2 = mk_waiter(NULL);
    CHECK(nfs_layout_table_recall_prepare(&table, fh, 4, w2, snap, 32) == 0);
    CHECK(nfs_layout_table_recall_active(&table, fh, 4));

    for (i = 0; i < 40; i++) {
        CHECK(resumed == 0);
        nfs_layout_table_deregister(&table, h[i]);
    }
    CHECK(resumed == 2);
    CHECK(!nfs_layout_table_recall_active(&table, fh, 4));
    CHECK(nfs_layout_table_recall_prepare(&table, fh, 4, NULL, snap, 32) == -1);

    for (i = 0; i < 40; i++) {
        CHECK(atomic_load(&h[i]->refcount) == 1);
        free(h[i]);
    }
    nfs_layout_table_destroy(&table);
} /* test_batches */

/* A batch call (no waiter) must only continue a recall that is still active.
 * Once the barrier has fired and a fresh holder registers, it must not be
 * snapshotted by a stale batch loop. */
static void
test_stale_batch(void)
{
    struct nfs_layout_table          table;
    struct nfs_layout_state         *h1, *h2, *snap[32];
    struct nfs_layout_recall_waiter *w;
    const uint8_t                    fh[] = { 5, 5, 5 };
    int                              n;

    nfs_layout_table_init(&table);
    resumed = 0;

    h1 = mk_holder(fh, 3);
    nfs_layout_table_register(&table, h1);
    w = mk_waiter(NULL);
    n = nfs_layout_table_recall_prepare(&table, fh, 3, w, snap, 32);
    CHECK(n == 1);
    put_all(snap, n);
    nfs_layout_table_deregister(&table, h1);
    CHECK(resumed == 1);

    h2 = mk_holder(fh, 3);
    nfs_layout_table_register(&table, h2);
    CHECK(nfs_layout_table_recall_prepare(&table, fh, 3, NULL, snap, 32) == 0);
    CHECK(!h2->recall_snapped);
    CHECK(atomic_load(&h2->refcount) == 1);

    nfs_layout_table_deregister(&table, h2);
    free(h1);
    free(h2);
    nfs_layout_table_destroy(&table);
} /* test_stale_batch */

static bool
match_arg(
    const struct nfs_layout_recall_waiter *w,
    const void                            *key)
{
    return w->arg == key;
} /* match_arg */

/* has_holders tracks the holder list; take_waiter withdraws one parked
 * waiter, which then never fires, and finds nothing once the barrier fired. */
static void
test_take_waiter(void)
{
    struct nfs_layout_table          table;
    struct nfs_layout_state         *h, *snap[32];
    struct nfs_layout_recall_waiter *w1, *w2, *w;
    const uint8_t                    fh[] = { 1, 2, 3 };
    int                              tag1, tag2;

    nfs_layout_table_init(&table);
    resumed = 0;

    CHECK(!nfs_layout_table_has_holders(&table, fh, 3));
    h = mk_holder(fh, 3);
    nfs_layout_table_register(&table, h);
    CHECK(nfs_layout_table_has_holders(&table, fh, 3));

    w1 = mk_waiter(&tag1);
    w2 = mk_waiter(&tag2);
    CHECK(nfs_layout_table_recall_prepare(&table, fh, 3, w1, snap, 32) == 1);
    put_all(snap, 1);
    CHECK(nfs_layout_table_recall_prepare(&table, fh, 3, w2, snap, 32) == 0);

    w = nfs_layout_table_take_waiter(&table, fh, 3, match_arg, &tag1);
    CHECK(w == w1);
    free(w);
    CHECK(nfs_layout_table_take_waiter(&table, fh, 3, match_arg, &tag1) == NULL);
    CHECK(nfs_layout_table_recall_active(&table, fh, 3));

    nfs_layout_table_deregister(&table, h);
    CHECK(resumed == 1);
    CHECK(!nfs_layout_table_has_holders(&table, fh, 3));
    CHECK(nfs_layout_table_take_waiter(&table, fh, 3, match_arg, &tag2) == NULL);

    free(h);
    nfs_layout_table_destroy(&table);
} /* test_take_waiter */

/* Withdrawing the only waiter ends the recall: nothing is active, a stale
 * batch call gets nothing, and a middle waiter can be taken. */
static void
test_take_only_waiter(void)
{
    struct nfs_layout_table          table;
    struct nfs_layout_state         *h, *snap[32];
    struct nfs_layout_recall_waiter *w1, *w2, *w3, *w;
    const uint8_t                    fh[] = { 4, 4 };
    int                              t1, t2, t3;

    nfs_layout_table_init(&table);
    resumed = 0;

    h = mk_holder(fh, 2);
    nfs_layout_table_register(&table, h);
    w1 = mk_waiter(&t1);
    w2 = mk_waiter(&t2);
    w3 = mk_waiter(&t3);
    CHECK(nfs_layout_table_recall_prepare(&table, fh, 2, w1, snap, 32) == 1);
    put_all(snap, 1);
    CHECK(nfs_layout_table_recall_prepare(&table, fh, 2, w2, snap, 32) == 0);
    CHECK(nfs_layout_table_recall_prepare(&table, fh, 2, w3, snap, 32) == 0);

    /* list is w3 -> w2 -> w1: take the middle one, then the others */
    w = nfs_layout_table_take_waiter(&table, fh, 2, match_arg, &t2);
    CHECK(w == w2);
    free(w);
    w = nfs_layout_table_take_waiter(&table, fh, 2, match_arg, &t1);
    CHECK(w == w1);
    free(w);
    w = nfs_layout_table_take_waiter(&table, fh, 2, match_arg, &t3);
    CHECK(w == w3);
    free(w);

    CHECK(!nfs_layout_table_recall_active(&table, fh, 2));
    CHECK(nfs_layout_table_has_holders(&table, fh, 2));
    CHECK(nfs_layout_table_recall_prepare(&table, fh, 2, NULL, snap, 32) == 0);

    nfs_layout_table_deregister(&table, h);
    CHECK(resumed == 0);
    free(h);
    nfs_layout_table_destroy(&table);
} /* test_take_only_waiter */

/* Only holders a recall snapshotted are on the clock, and only once their
 * snapshot is old enough are they collected; removing them still fires the
 * barrier. */
static void
test_overdue(void)
{
    struct nfs_layout_table          table;
    struct nfs_layout_state         *h[3], *out[8];
    struct nfs_layout_recall_waiter *w;
    const uint8_t                    fh[] = { 4, 4, 4 };
    uint64_t                         now;
    int                              i, n;

    nfs_layout_table_init(&table);
    resumed = 0;

    client_refs = 0;
    for (i = 0; i < 2; i++) {
        h[i]         = mk_holder(fh, 3);
        h[i]->client = (struct nfs_client *) &client_refs;
        nfs_layout_table_register(&table, h[i]);
    }
    /* No recall yet: nothing is on the clock. */
    CHECK(nfs_layout_table_collect_overdue(&table, nfs_lease_now_ns(), 0, out, 8) == 0);

    w = mk_waiter(NULL);
    n = nfs_layout_table_recall_prepare(&table, fh, 3, w, out, 8);
    CHECK(n == 2);
    put_all(out, n);

    /* A holder cannot join after the snapshot: it would never be recalled. */
    h[2] = mk_holder(fh, 3);
    CHECK(nfs_layout_table_register(&table, h[2]) == NFS4ERR_RECALLCONFLICT);

    now = nfs_lease_now_ns();
    CHECK(nfs_layout_table_collect_overdue(&table, now, UINT64_MAX / 2, out, 8) == 0);
    n = nfs_layout_table_collect_overdue(&table, now + 1000000000ULL, 0, out, 8);
    CHECK(n == 2);
    CHECK(out[0] != out[1]);
    CHECK(out[0] == h[0] || out[0] == h[1]);
    CHECK(out[1] == h[0] || out[1] == h[1]);
    /* Each collected holder's client is pinned too (destroy locks it). */
    CHECK(client_refs == 2);
    for (i = 0; i < n; i++) {
        nfs_client_put(out[i]->client);
    }
    CHECK(client_refs == 0);
    put_all(out, n);

    for (i = 0; i < 2; i++) {
        CHECK(resumed == 0);
        nfs_layout_table_deregister(&table, h[i]);
    }
    CHECK(resumed == 1);

    for (i = 0; i < 3; i++) {
        free(h[i]);
    }
    nfs_layout_table_destroy(&table);
} /* test_overdue */

/* A holder is refused while a recall is outstanding on its file, and taken
 * again once that recall is over.  The refusal leaves it unlinked and the
 * recall's holders untouched; a refused advance leaves the seqid alone. */
static void
test_register_refused(void)
{
    struct nfs_layout_table          table;
    struct nfs_layout_state         *h1, *h2, *h3, *snap[32];
    struct nfs_layout_recall_waiter *w;
    const uint8_t                    fh[]  = { 6, 6, 6 };
    const uint8_t                    fh2[] = { 6, 6, 7 };
    int                              n;

    nfs_layout_table_init(&table);
    resumed = 0;

    h1        = mk_holder(fh, 3);
    h1->seqid = 5;
    CHECK(nfs_layout_table_register(&table, h1) == NFS4_OK);

    /* No recall yet: an advance goes through. */
    CHECK(nfs_layout_table_advance(&table, h1) == NFS4_OK);
    CHECK(h1->seqid == 6);

    w = mk_waiter(NULL);
    n = nfs_layout_table_recall_prepare(&table, fh, 3, w, snap, 32);
    CHECK(n == 1);
    put_all(snap, n);

    h2 = mk_holder(fh, 3);
    CHECK(nfs_layout_table_register(&table, h2) == NFS4ERR_RECALLCONFLICT);
    CHECK(h2->global_next == NULL);
    CHECK(!h2->recall_snapped);
    CHECK(nfs_layout_table_advance(&table, h1) == NFS4ERR_RECALLCONFLICT);
    CHECK(h1->seqid == 6);

    /* Another file is unaffected. */
    h3 = mk_holder(fh2, 3);
    CHECK(nfs_layout_table_register(&table, h3) == NFS4_OK);

    /* The refused holder never joined, so the barrier fires on h1 alone. */
    CHECK(resumed == 0);
    nfs_layout_table_deregister(&table, h1);
    CHECK(resumed == 1);
    CHECK(!nfs_layout_table_has_holders(&table, fh, 3));

    /* The recall is over: registering works again. */
    CHECK(nfs_layout_table_register(&table, h2) == NFS4_OK);
    CHECK(nfs_layout_table_advance(&table, h2) == NFS4_OK);

    nfs_layout_table_deregister(&table, h2);
    nfs_layout_table_deregister(&table, h3);
    free(h1);
    free(h2);
    free(h3);
    nfs_layout_table_destroy(&table);
} /* test_register_refused */

int
main(void)
{
    test_batches();
    test_stale_batch();
    test_take_waiter();
    test_take_only_waiter();
    test_overdue();
    test_register_refused();
    printf("layout table: PASS\n");
    return 0;
} /* main */
