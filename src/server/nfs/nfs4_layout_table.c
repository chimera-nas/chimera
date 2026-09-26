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

#ifndef __clang_analyzer__
        /* uthash blows clangs mind */
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
#endif /* ifndef __clang_analyzer__ */

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

static bool
layout_admission_begin(
    struct nfs_layout_table *table,
    const uint8_t           *fh,
    uint16_t                 fh_len,
    bool                     barrier)
{
    if (!fh || !fh_len || fh_len > NFS4_FHSIZE) {
        return false;
    }
    struct nfs_layout_shard *shard = &table->shards[layout_shard_index(fh, fh_len)];
    struct nfs_layout_entry *entry;

    pthread_mutex_lock(&shard->lock);
    HASH_FIND(hh, shard->by_fh, fh, fh_len, entry);
    if (entry && (barrier ? entry->grants != 0 : (entry->barriers || entry->waiters))) {
        pthread_mutex_unlock(&shard->lock);
        return false;
    }
    if (!entry) {
        entry = calloc(1, sizeof(*entry));
        chimera_nfs_abort_if(!entry, "layout admission alloc OOM");
        memcpy(entry->fh, fh, fh_len);
        entry->fh_len = fh_len;
        HASH_ADD_KEYPTR(hh, shard->by_fh, entry->fh, entry->fh_len, entry);
    }
    if (barrier) {
        entry->barriers++;
    } else {
        entry->grants++;
    }
    pthread_mutex_unlock(&shard->lock);
    return true;
} /* layout_admission_begin */

static void
layout_admission_end(
    struct nfs_layout_table *table,
    const uint8_t           *fh,
    uint16_t                 fh_len,
    bool                     barrier)
{
    struct nfs_layout_shard *shard = &table->shards[layout_shard_index(fh, fh_len)];
    struct nfs_layout_entry *entry;

    pthread_mutex_lock(&shard->lock);
    HASH_FIND(hh, shard->by_fh, fh, fh_len, entry);
    chimera_nfs_abort_if(!entry || !(barrier ? entry->barriers : entry->grants),
                         "layout admission release without hold");
    if (barrier) {
        entry->barriers--;
    } else {
        entry->grants--;
    }
    if (!entry->holders && !entry->waiters && !entry->barriers && !entry->grants) {
        HASH_DEL(shard->by_fh, entry);
        free(entry);
    }
    pthread_mutex_unlock(&shard->lock);
} /* layout_admission_end */

bool
nfs_layout_table_barrier_acquire(
    struct nfs_layout_table *table,
    const uint8_t           *fh,
    uint16_t                 fh_len)
{
    return layout_admission_begin(table, fh, fh_len, true);
} /* nfs_layout_table_barrier_acquire */

void
nfs_layout_table_barrier_release(
    struct nfs_layout_table *table,
    const uint8_t           *fh,
    uint16_t                 fh_len)
{
    layout_admission_end(table, fh, fh_len, true);
} /* nfs_layout_table_barrier_release */

bool
nfs_layout_table_grant_begin(
    struct nfs_layout_table *table,
    const uint8_t           *fh,
    uint16_t                 fh_len)
{
    return layout_admission_begin(table, fh, fh_len, false);
} /* nfs_layout_table_grant_begin */

void
nfs_layout_table_grant_end(
    struct nfs_layout_table *table,
    const uint8_t           *fh,
    uint16_t                 fh_len)
{
    layout_admission_end(table, fh, fh_len, false);
} /* nfs_layout_table_grant_end */

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

        /* Last holder gone: resume waiters, retaining any request barrier so
         * a new LAYOUTGET cannot slip in before the conflicting op finishes. */
        if (!e->holders) {
            waiters    = e->waiters;
            e->waiters = NULL;
            if (!e->barriers && !e->grants) {
                HASH_DEL(shard->by_fh, e);
                free(e);
            }
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
    struct nfs_layout_state       ***out_holders)
{
    struct nfs_layout_shard *shard = &table->shards[layout_shard_index(fh, fh_len)];
    struct nfs_layout_entry *e;
    struct nfs_layout_state *ls;
    int                      n = 0;

    *out_holders = NULL;

    pthread_mutex_lock(&shard->lock);

    HASH_FIND(hh, shard->by_fh, fh, fh_len, e);
    if (!e || !e->holders) {
        pthread_mutex_unlock(&shard->lock);
        return 0;
    }

    /* Snapshot every holder; a fixed-size prefix would leave unnotified
     * holders keeping the waiter asleep indefinitely. */
    for (ls = e->holders; ls; ls = ls->global_next) {
        n++;
    }
    *out_holders = calloc(n, sizeof(**out_holders));
    chimera_nfs_abort_if(!*out_holders, "layout recall snapshot alloc OOM");
    n = 0;

    /* Defer the caller behind this file's recall, and snapshot the current
     * holders (pinned) so the caller can recall each outside the lock. */
    waiter->next = e->waiters;
    e->waiters   = waiter;

    for (ls = e->holders; ls; ls = ls->global_next) {
        nfs_layout_state_get(ls);
        (*out_holders)[n++] = ls;
    }

    pthread_mutex_unlock(&shard->lock);
    return n;
} /* nfs_layout_table_recall_prepare */

bool
nfs_layout_table_recall_active(
    struct nfs_layout_table *table,
    const uint8_t           *fh,
    uint16_t                 fh_len)
{
    struct nfs_layout_shard *shard = &table->shards[layout_shard_index(fh, fh_len)];
    struct nfs_layout_entry *e;
    bool                     active;

    pthread_mutex_lock(&shard->lock);

    /* Waiters cover outstanding returns; a counted barrier covers the resumed
     * conflicting operation until its request has accepted or aborted. */
    HASH_FIND(hh, shard->by_fh, fh, fh_len, e);
    active = (e && (e->waiters || e->barriers));

    pthread_mutex_unlock(&shard->lock);
    return active;
} /* nfs_layout_table_recall_active */

bool
nfs_layout_table_has_holders(
    struct nfs_layout_table *table,
    const uint8_t           *fh,
    uint16_t                 fh_len)
{
    struct nfs_layout_shard *shard = &table->shards[layout_shard_index(fh, fh_len)];
    struct nfs_layout_entry *entry;
    bool                     held;

    pthread_mutex_lock(&shard->lock);
    HASH_FIND(hh, shard->by_fh, fh, fh_len, entry);
    held = entry && entry->holders;
    pthread_mutex_unlock(&shard->lock);
    return held;
} /* nfs_layout_table_has_holders */
