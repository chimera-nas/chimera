// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <pthread.h>
#include <stdint.h>
#include <uthash.h>

#include "nfs4_xdr.h"   /* NFS4_FHSIZE */

/*
 * Server-wide index of outstanding pNFS layouts, keyed by file handle and
 * sharded by an fh hash so that LAYOUTGET/LAYOUTRETURN and conflict lookups
 * contend only within a single shard (never a global lock).  Its purpose is to
 * answer "which clients hold a layout for this file?" so a conflicting
 * operation can recall the layout from every holder -- including holders other
 * than the client issuing the conflict -- which the per-client layout table
 * cannot do.
 *
 * Each file with >=1 outstanding layout has one entry; the entry lists the
 * per-client nfs_layout_state holders (linked via nfs_layout_state.global_next)
 * and any conflicting operations deferred until every holder has returned.
 * The deferral barrier is simply "the holder list became empty": each holder's
 * teardown (LAYOUTRETURN, recall decline, lease expiry) deregisters it, and the
 * last deregistration resumes the waiters.
 */

#define NFS_LAYOUT_TABLE_SHARDS 256   /* power of two */

struct nfs_layout_state;
struct nfs_client;
struct chimera_server_nfs_thread;

struct nfs_layout_recall_waiter {
    void                             (*resume)(
        void *arg);
    void                            *arg;
    struct nfs_layout_recall_waiter *next;
};

struct nfs_layout_entry {
    uint8_t                          fh[NFS4_FHSIZE];
    uint16_t                         fh_len;
    struct nfs_layout_state         *holders;   /* list via ls->global_next */
    struct nfs_layout_recall_waiter *waiters;   /* ops awaiting all returns  */
    UT_hash_handle                   hh;
};

struct nfs_layout_shard {
    pthread_mutex_t          lock;
    struct nfs_layout_entry *by_fh;
};

struct nfs_layout_table {
    struct nfs_layout_shard shards[NFS_LAYOUT_TABLE_SHARDS];
};

void nfs_layout_table_init(
    struct nfs_layout_table *table);

void nfs_layout_table_destroy(
    struct nfs_layout_table *table);

/* Add/remove a holder.  Called as layouts are granted (LAYOUTGET) and torn
 * down (LAYOUTRETURN / lease expiry / recall decline).  Deregistering the last
 * holder of a file resumes any operations deferred on its recall. */
void nfs_layout_table_register(
    struct nfs_layout_table *table,
    struct nfs_layout_state *ls);

void nfs_layout_table_deregister(
    struct nfs_layout_table *table,
    struct nfs_layout_state *ls);

/*
 * Begin a recall for fh: if any layouts are held, append `waiter` to the file's
 * deferred-op list and snapshot the current holders into out_holders (each
 * pinned with a layout ref the caller must release with nfs_layout_state_put).
 * Returns the number of holders snapshotted, or 0 if none are held (in which
 * case `waiter` is NOT enqueued and the caller should resume immediately).
 */
int nfs_layout_table_recall_prepare(
    struct nfs_layout_table         *table,
    const uint8_t                   *fh,
    uint16_t                         fh_len,
    struct nfs_layout_recall_waiter *waiter,
    struct nfs_layout_state        **out_holders,
    int                              max_holders);
