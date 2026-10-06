// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "nfs4_state.h"
#include "nfs4_layout_table.h"

#define CHECK(cond) do { if (!(cond)) { \
                             fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #cond); abort(); \
                         } } while (0)

struct resume_ctx {
    struct nfs_layout_table *table;
    const uint8_t           *fh;
    uint16_t                 fh_len;
    int                      calls;
};

static void
returned(void *private_data)
{
    struct resume_ctx *ctx = private_data;

    CHECK(!nfs_layout_table_has_holders(ctx->table, ctx->fh, ctx->fh_len));
    /* The waiter has fired, but the asynchronous conflicting request still
     * holds its barrier until acceptance. No new layout may escape meanwhile. */
    CHECK(!nfs_layout_table_grant_begin(ctx->table, ctx->fh, ctx->fh_len));
    ctx->calls++;
} /* returned */

int
main(void)
{
    enum { HOLDERS = 40 };
    struct nfs_layout_table   layouts = { 0 };
    struct nfs_state_table    states;
    struct nfs_client        *clients[HOLDERS];
    struct nfs_layout_state **snapshot = NULL;
    struct stateid4           sid;
    const uint8_t             fh[]    = { 4, 1, 7, 9 };
    struct resume_ctx         resumed = { .table = &layouts, .fh = fh, .fh_len = sizeof(fh) };

    nfs_state_table_init(&states, 1);
    for (int i = 0; i < NFS_LAYOUT_TABLE_SHARDS; i++) {
        evpl_mutex_init(&layouts.shards[i].lock, NULL);
    }
    CHECK(!nfs_layout_table_barrier_acquire(&layouts, NULL, 0));
    CHECK(nfs_layout_table_grant_begin(&layouts, fh, sizeof(fh)));
    CHECK(!nfs_layout_table_barrier_acquire(&layouts, fh, sizeof(fh)));
    nfs_layout_table_grant_end(&layouts, fh, sizeof(fh));

    /* Exclusion applies even when no holder existed at checkpoint execution. */
    CHECK(nfs_layout_table_barrier_acquire(&layouts, fh, sizeof(fh)));
    CHECK(nfs_layout_table_barrier_acquire(&layouts, fh, sizeof(fh)));
    CHECK(!nfs_layout_table_grant_begin(&layouts, fh, sizeof(fh)));
    nfs_layout_table_barrier_release(&layouts, fh, sizeof(fh));
    CHECK(!nfs_layout_table_grant_begin(&layouts, fh, sizeof(fh)));
    nfs_layout_table_barrier_release(&layouts, fh, sizeof(fh));

    for (int i = 0; i < HOLDERS; i++) {
        char owner[32];
        snprintf(owner, sizeof(owner), "holder-%d", i);
        clients[i] = nfs_client_alloc(i + 1, owner, strlen(owner), i + 1, 1);
        CHECK(nfs_layout_table_grant_begin(&layouts, fh, sizeof(fh)));
        CHECK(nfs_layout_state_create(clients[i], fh, sizeof(fh), 1, LAYOUTIOMODE4_RW,
                                      i + 1, &states, &layouts, &sid));
        CHECK(!nfs_layout_table_barrier_acquire(&layouts, fh, sizeof(fh)));
        nfs_layout_table_grant_end(&layouts, fh, sizeof(fh));
    }
    CHECK(nfs_layout_table_barrier_acquire(&layouts, fh, sizeof(fh)));
    CHECK(nfs_layout_table_barrier_acquire(&layouts, fh, sizeof(fh)));
    struct nfs_layout_recall_waiter *waiter = calloc(1, sizeof(*waiter));
    CHECK(waiter);
    waiter->resume = returned;
    waiter->arg    = &resumed;
    int                              count = nfs_layout_table_recall_prepare(&layouts, fh, sizeof(fh), waiter, &snapshot
                                                                             );
    CHECK(count == HOLDERS && snapshot && !resumed.calls);
    for (int i = 0; i < count; i++) {
        CHECK(atomic_load(&snapshot[i]->refcount) == 2);
        CHECK(!atomic_load(&snapshot[i]->destroyed));
        for (int j = i + 1; j < count; j++) {
            CHECK(snapshot[i] != snapshot[j]);
        }
    }
    for (int i = 0; i < HOLDERS; i++) {
        nfs_client_destroy(clients[i], &states, NULL, false);
        CHECK(resumed.calls == (i == HOLDERS - 1));
        CHECK(!nfs_layout_table_grant_begin(&layouts, fh, sizeof(fh)));
    }
    for (int i = 0; i < count; i++) {
        CHECK(atomic_load(&snapshot[i]->destroyed));
        CHECK(atomic_load(&snapshot[i]->refcount) == 1);
        nfs_layout_state_put(snapshot[i]);
    }
    free(snapshot);
    nfs_layout_table_barrier_release(&layouts, fh, sizeof(fh));
    CHECK(!nfs_layout_table_grant_begin(&layouts, fh, sizeof(fh)));
    nfs_layout_table_barrier_release(&layouts, fh, sizeof(fh));
    CHECK(nfs_layout_table_grant_begin(&layouts, fh, sizeof(fh)));
    nfs_layout_table_grant_end(&layouts, fh, sizeof(fh));
    for (int i = 0; i < NFS_LAYOUT_TABLE_SHARDS; i++) {
        CHECK(!layouts.shards[i].by_fh);
        evpl_mutex_destroy(&layouts.shards[i].lock);
    }
    nfs_state_table_free(&states, NULL);
    puts("PASS: layout admission barriers, final grant exclusion, and all 40 recalled holders");
    return 0;
} /* main */
