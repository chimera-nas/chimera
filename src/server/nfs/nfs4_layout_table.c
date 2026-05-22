// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>

#include "nfs4_layout_table.h"
#include "nfs4_state.h"
#include "nfs_internal.h"

/* FNV-1a over the file handle; the low bits pick the shard. */
static inline uint32_t
layout_shard_index(
    const uint8_t *fh,
    uint16_t       fh_len)
{
    uint32_t h = 2166136261u;
    uint16_t i;

    for (i = 0; i < fh_len; i++) {
        h = (h ^ fh[i]) * 16777619u;
    }
    return h & (NFS_LAYOUT_TABLE_SHARDS - 1);
} /* layout_shard_index */

void
nfs_layout_table_init(struct nfs_layout_table *table)
{
    int i;

    for (i = 0; i < NFS_LAYOUT_TABLE_SHARDS; i++) {
        pthread_mutex_init(&table->shards[i].lock, NULL);
        table->shards[i].by_fh = NULL;
    }
} /* nfs_layout_table_init */

void
nfs_layout_table_destroy(struct nfs_layout_table *table)
{
    int i;

    for (i = 0; i < NFS_LAYOUT_TABLE_SHARDS; i++) {
        struct nfs_layout_entry *e, *tmp;

        HASH_ITER(hh, table->shards[i].by_fh, e, tmp)
        {
            struct nfs_layout_recall_waiter *w, *wn;

            HASH_DEL(table->shards[i].by_fh, e);
            for (w = e->waiters; w; w = wn) {
                wn = w->next;
                free(w);
            }
            free(e);
        }
        pthread_mutex_destroy(&table->shards[i].lock);
    }
} /* nfs_layout_table_destroy */

void
nfs_layout_table_register(
    struct nfs_layout_table *table,
    struct nfs_layout_state *ls)
{
    struct nfs_layout_shard *shard = &table->shards[layout_shard_index(ls->fh, ls->fh_len)];
    struct nfs_layout_entry *e;

    pthread_mutex_lock(&shard->lock);

    HASH_FIND(hh, shard->by_fh, ls->fh, ls->fh_len, e);
    if (!e) {
        e = calloc(1, sizeof(*e));
        memcpy(e->fh, ls->fh, ls->fh_len);
        e->fh_len = ls->fh_len;
        HASH_ADD_KEYPTR(hh, shard->by_fh, e->fh, e->fh_len, e);
    }

    ls->global_next = e->holders;
    e->holders      = ls;

    pthread_mutex_unlock(&shard->lock);
} /* nfs_layout_table_register */

void
nfs_layout_table_deregister(
    struct nfs_layout_table *table,
    struct nfs_layout_state *ls)
{
    struct nfs_layout_shard         *shard = &table->shards[layout_shard_index(ls->fh, ls->fh_len)];
    struct nfs_layout_entry         *e;
    struct nfs_layout_recall_waiter *waiters = NULL, *w, *wn;

    pthread_mutex_lock(&shard->lock);

    HASH_FIND(hh, shard->by_fh, ls->fh, ls->fh_len, e);
    if (e) {
        struct nfs_layout_state **pp = &e->holders;

        while (*pp) {
            if (*pp == ls) {
                *pp = ls->global_next;
                break;
            }
            pp = &(*pp)->global_next;
        }
        ls->global_next = NULL;

        /* Last holder gone: the recall is complete -- detach the deferred
         * operations and drop the entry. */
        if (!e->holders) {
            waiters = e->waiters;
            HASH_DEL(shard->by_fh, e);
            free(e);
        }
    }

    pthread_mutex_unlock(&shard->lock);

    if (waiters) {
        chimera_nfs_info("pNFS: file fully returned, resuming deferred operation(s)");
    }

    for (w = waiters; w; w = wn) {
        wn = w->next;
        w->resume(w->arg);
        free(w);
    }
} /* nfs_layout_table_deregister */

int
nfs_layout_table_recall_prepare(
    struct nfs_layout_table         *table,
    const uint8_t                   *fh,
    uint16_t                         fh_len,
    struct nfs_layout_recall_waiter *waiter,
    struct nfs_layout_state        **out_holders,
    int                              max_holders)
{
    struct nfs_layout_shard *shard = &table->shards[layout_shard_index(fh, fh_len)];
    struct nfs_layout_entry *e;
    struct nfs_layout_state *ls;
    int                      n = 0;

    pthread_mutex_lock(&shard->lock);

    HASH_FIND(hh, shard->by_fh, fh, fh_len, e);
    if (!e || !e->holders) {
        pthread_mutex_unlock(&shard->lock);
        return 0;
    }

    /* Defer the caller behind this file's recall, and snapshot the current
     * holders (pinned) so the caller can recall each outside the lock. */
    waiter->next = e->waiters;
    e->waiters   = waiter;

    for (ls = e->holders; ls && n < max_holders; ls = ls->global_next) {
        nfs_layout_state_get(ls);
        out_holders[n++] = ls;
    }

    pthread_mutex_unlock(&shard->lock);
    return n;
} /* nfs_layout_table_recall_prepare */
