// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "common/thread.h"
#include <stdlib.h>
#include <string.h>

#include "nfs4_layout_table.h"
#include "nfs4_lease.h"
#include "nfs4_state.h"
#include "nfs4_stateid.h"
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
        evpl_mutex_init(&table->shards[i].lock, NULL);
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

        evpl_mutex_destroy(&table->shards[i].lock);
    }
} /* nfs_layout_table_destroy */

nfsstat4
nfs_layout_table_register(
    struct nfs_layout_table *table,
    struct nfs_layout_state *ls)
{
    struct nfs_layout_shard *shard = &table->shards[layout_shard_index(ls->fh, ls->fh_len)];
    struct nfs_layout_entry *e;

    evpl_mutex_lock(&shard->lock);

    HASH_FIND(hh, shard->by_fh, ls->fh, ls->fh_len, e);

    /* A recall is outstanding on this file: it has already snapshotted the
     * holders it will call back, so a holder added now would be neither
     * recalled nor revoked.  Decided here, under the lock the snapshot takes,
     * so the check and the insert cannot be separated by a recall starting. */
    if (e && e->waiters) {
        evpl_mutex_unlock(&shard->lock);
        return NFS4ERR_RECALLCONFLICT;
    }

    if (!e) {
        e = calloc(1, sizeof(*e));
        memcpy(e->fh, ls->fh, ls->fh_len);
        e->fh_len = ls->fh_len;
        HASH_ADD_KEYPTR(hh, shard->by_fh, e->fh, e->fh_len, e);
    }

    ls->global_next = e->holders;
    e->holders      = ls;

    evpl_mutex_unlock(&shard->lock);
    return NFS4_OK;
} /* nfs_layout_table_register */

nfsstat4
nfs_layout_table_advance(
    struct nfs_layout_table *table,
    struct nfs_layout_state *ls)
{
    struct nfs_layout_shard *shard = &table->shards[layout_shard_index(ls->fh, ls->fh_len)];
    struct nfs_layout_entry *e;
    nfsstat4                 status = NFS4_OK;

    evpl_mutex_lock(&shard->lock);

    HASH_FIND(hh, shard->by_fh, ls->fh, ls->fh_len, e);
    if (e && e->waiters) {
        status = NFS4ERR_RECALLCONFLICT;
    } else {
        ls->seqid = nfs4_stateid_seqid_next(ls->seqid);
    }

    evpl_mutex_unlock(&shard->lock);
    return status;
} /* nfs_layout_table_advance */

void
nfs_layout_table_deregister(
    struct nfs_layout_table *table,
    struct nfs_layout_state *ls)
{
    struct nfs_layout_shard         *shard = &table->shards[layout_shard_index(ls->fh, ls->fh_len)];
    struct nfs_layout_entry         *e;
    struct nfs_layout_recall_waiter *waiters = NULL, *w, *wn;

    evpl_mutex_lock(&shard->lock);

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

    evpl_mutex_unlock(&shard->lock);

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
    int                      n      = 0;
    uint64_t                 now_ns = nfs_lease_now_ns();

    evpl_mutex_lock(&shard->lock);

    HASH_FIND(hh, shard->by_fh, fh, fh_len, e);
    if (!e || !e->holders) {
        evpl_mutex_unlock(&shard->lock);
        return -1;
    }

    /* Defer the caller behind this file's recall (or join the one already
     * active), and snapshot holders no batch has taken yet, pinned, so the
     * caller can recall each outside the lock. */
    if (waiter) {
        waiter->next = e->waiters;
        e->waiters   = waiter;
    } else if (!e->waiters) {
        /* A further batch of a recall that has since completed (every holder
         * left and the barrier fired); a holder now present was granted after
         * it and must not be recalled by a stale loop. */
        evpl_mutex_unlock(&shard->lock);
        return 0;
    }

    for (ls = e->holders; ls && n < max_holders; ls = ls->global_next) {
        if (ls->recall_snapped) {
            continue;
        }
        ls->recall_snapped    = 1;
        ls->recall_snapped_ns = now_ns;
        nfs_layout_state_get(ls);
        out_holders[n++] = ls;
    }

    evpl_mutex_unlock(&shard->lock);
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

    evpl_mutex_lock(&shard->lock);

    /* The entry carries waiters only between recall_prepare and the last
     * holder's deregistration, which frees the entry -- so a non-empty waiter
     * list is exactly "a recall is outstanding on this file". */
    HASH_FIND(hh, shard->by_fh, fh, fh_len, e);
    active = (e && e->waiters);

    evpl_mutex_unlock(&shard->lock);
    return active;
} /* nfs_layout_table_recall_active */

bool
nfs_layout_table_has_holders(
    struct nfs_layout_table *table,
    const uint8_t           *fh,
    uint16_t                 fh_len)
{
    struct nfs_layout_shard *shard = &table->shards[layout_shard_index(fh, fh_len)];
    struct nfs_layout_entry *e;
    bool                     held;

    evpl_mutex_lock(&shard->lock);
    HASH_FIND(hh, shard->by_fh, fh, fh_len, e);
    held = (e && e->holders);
    evpl_mutex_unlock(&shard->lock);
    return held;
} /* nfs_layout_table_has_holders */

struct nfs_layout_recall_waiter *
nfs_layout_table_take_waiter(
    struct nfs_layout_table *table,
    const uint8_t           *fh,
    uint16_t                 fh_len,
    bool (                  *match )(
        const struct nfs_layout_recall_waiter *w,
        const void                            *key),
    const void              *key)
{
    struct nfs_layout_shard          *shard = &table->shards[layout_shard_index(fh, fh_len)];
    struct nfs_layout_entry          *e;
    struct nfs_layout_recall_waiter **pp, *w = NULL;

    evpl_mutex_lock(&shard->lock);
    HASH_FIND(hh, shard->by_fh, fh, fh_len, e);
    if (e) {
        for (pp = &e->waiters; *pp; pp = &(*pp)->next) {
            if (match(*pp, key)) {
                w = *pp;
                *pp = w->next;
                w->next = NULL;
                break;
            }
        }
    }
    evpl_mutex_unlock(&shard->lock);
    return w;
} /* nfs_layout_table_take_waiter */

int
nfs_layout_table_collect_overdue(
    struct nfs_layout_table  *table,
    uint64_t                  now_ns,
    uint64_t                  max_age_ns,
    struct nfs_layout_state **out,
    int                       max)
{
    struct nfs_layout_entry *e, *tmp;
    struct nfs_layout_state *ls;
    int                      i, n = 0;

    for (i = 0; i < NFS_LAYOUT_TABLE_SHARDS && n < max; i++) {
        struct nfs_layout_shard *shard = &table->shards[i];

        evpl_mutex_lock(&shard->lock);
#ifndef __clang_analyzer__
        HASH_ITER(hh, shard->by_fh, e, tmp)
        {
            if (!e->waiters) {
                continue;
            }
            for (ls = e->holders; ls && n < max; ls = ls->global_next) {
                if (ls->recall_snapped &&
                    now_ns >= ls->recall_snapped_ns &&
                    now_ns - ls->recall_snapped_ns >= max_age_ns) {
                    /* The layout ref keeps only the state alive; destroy
                     * locks the client, so pin that too.  The holder is
                     * still linked here, so its client's teardown has not
                     * yet run (it deregisters every holder first). */
                    nfs_layout_state_get(ls);
                    nfs_client_get(ls->client);
                    out[n++] = ls;
                }
            }
            if (n >= max) {
                break;
            }
        }
#endif /* ifndef __clang_analyzer__ */
        evpl_mutex_unlock(&shard->lock);
    }
    return n;
} /* nfs_layout_table_collect_overdue */
