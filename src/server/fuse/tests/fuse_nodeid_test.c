// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * The nodeid<->file-handle table underneath the FUSE server.
 *
 * The properties that matter to the kernel protocol: a handle maps to one
 * stable nodeid for as long as any lookup reference is outstanding, lookup
 * counts accumulate across repeated lookups and drain by FORGET amounts,
 * a fully-forgotten entry disappears (and a later re-lookup gets a fresh,
 * never-recycled nodeid), and the table tolerates concurrent use from many
 * threads, since multi-queue delivery spreads requests for one inode across
 * channels.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "server/fuse/fuse_node_table.h"

static int failures;

#define CHECK(cond, ...) \
        do { \
            if (!(cond)) { \
                printf("FAIL: " __VA_ARGS__); printf("\n"); failures++; \
            } else { \
                printf("ok:   " __VA_ARGS__); printf("\n"); \
            } \
        } while (0)

#define NUM_THREADS 4
#define PER_THREAD  1000

struct worker_args {
    struct chimera_fuse_node_table *table;
    int                             id;
};

static void *
worker(void *argp)
{
    struct worker_args *args = argp;
    uint8_t             fh[32];
    uint64_t            ids[PER_THREAD];
    int                 i;

    for (i = 0; i < PER_THREAD; i++) {
        /* Half the handles are shared across all threads, half private. */
        memset(fh, 0, sizeof(fh));
        if (i % 2) {
            snprintf((char *) fh, sizeof(fh), "shared-%d", i);
        } else {
            snprintf((char *) fh, sizeof(fh), "thread-%d-%d", args->id, i);
        }

        ids[i] = chimera_fuse_node_insert(args->table, fh, sizeof(fh));
    }

    for (i = 0; i < PER_THREAD; i++) {
        chimera_fuse_node_forget(args->table, ids[i], 1);
    }

    return NULL;
} /* worker */

int
main(
    int   argc,
    char *argv[])
{
    struct chimera_fuse_node_table *table;
    uint8_t                         fh_a[19], fh_b[64], fh_out[64];
    uint32_t                        fh_len;
    uint64_t                        id_a, id_a2, id_b, id_gone;
    struct worker_args              args[NUM_THREADS];
    pthread_t                       threads[NUM_THREADS];
    int                             i;

    table = chimera_fuse_node_table_create();

    memset(fh_a, 0xaa, sizeof(fh_a));
    memset(fh_b, 0xbb, sizeof(fh_b));

    /* --- basic insert / stable nodeid --- */

    id_a = chimera_fuse_node_insert(table, fh_a, sizeof(fh_a));

    CHECK(id_a >= 2, "first nodeid avoids FUSE_ROOT_ID (got %llu)",
          (unsigned long long) id_a);

    id_a2 = chimera_fuse_node_insert(table, fh_a, sizeof(fh_a));

    CHECK(id_a2 == id_a, "same handle maps to the same nodeid");

    id_b = chimera_fuse_node_insert(table, fh_b, sizeof(fh_b));

    CHECK(id_b != id_a, "distinct handles get distinct nodeids");

    /* --- get_fh round trip, length preserved --- */

    CHECK(chimera_fuse_node_get_fh(table, id_a, fh_out, &fh_len) == 0 &&
          fh_len == sizeof(fh_a) && memcmp(fh_out, fh_a, sizeof(fh_a)) == 0,
          "handle round-trips with its length");

    CHECK(chimera_fuse_node_get_fh(table, 999999, fh_out, &fh_len) != 0,
          "unknown nodeid is rejected");

    /* --- forget drains the accumulated lookup count --- */

    chimera_fuse_node_forget(table, id_a, 1);

    CHECK(chimera_fuse_node_get_fh(table, id_a, fh_out, &fh_len) == 0,
          "entry survives partial forget");

    chimera_fuse_node_forget(table, id_a, 1);

    CHECK(chimera_fuse_node_get_fh(table, id_a, fh_out, &fh_len) != 0,
          "entry retired once the count drains");

    /* --- batched forget amount --- */

    for (i = 0; i < 5; i++) {
        chimera_fuse_node_insert(table, fh_b, sizeof(fh_b));
    }

    chimera_fuse_node_forget(table, id_b, 6);

    CHECK(chimera_fuse_node_get_fh(table, id_b, fh_out, &fh_len) != 0,
          "batched forget drains accumulated lookups");

    /* --- nodeids are never recycled --- */

    id_gone = chimera_fuse_node_insert(table, fh_a, sizeof(fh_a));

    CHECK(id_gone != id_a && id_gone != id_b,
          "re-lookup after retirement gets a fresh nodeid");

    chimera_fuse_node_forget(table, id_gone, 1);

    /* --- concurrent use --- */

    for (i = 0; i < NUM_THREADS; i++) {
        args[i].table = table;
        args[i].id    = i;
        pthread_create(&threads[i], NULL, worker, &args[i]);
    }

    for (i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    /* Every worker forgot exactly what it inserted, so nothing remains. */
    CHECK(chimera_fuse_node_get_fh(table, 2, fh_out, &fh_len) != 0,
          "table is empty after concurrent insert/forget churn");

    chimera_fuse_node_table_destroy(table);

    printf("\n%s (%d failures)\n", failures ? "FAILED" : "PASSED", failures);

    return failures != 0;
} /* main */
