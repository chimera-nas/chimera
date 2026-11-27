// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "common/thread.h"
#include <stdbool.h>
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
    evpl_mutex_t             lock;
    struct nfs_layout_entry *by_fh;
};

struct nfs_layout_table {
    /* The server this table belongs to (struct chimera_server_nfs_shared), for
     * the VFS's recall callback, which is given only the table.  Set by the
     * server after init; NULL in unit tests. */
    void                   *owner;
    struct nfs_layout_shard shards[NFS_LAYOUT_TABLE_SHARDS];
};

void nfs_layout_table_init(
    struct nfs_layout_table *table);

void nfs_layout_table_destroy(
    struct nfs_layout_table *table);

/* Add/remove a holder.  Called as layouts are granted (LAYOUTGET) and torn
 * down (LAYOUTRETURN / lease expiry / recall decline).  Deregistering the last
 * holder of a file resumes any operations deferred on its recall.
 *
 * Registering refuses with NFS4ERR_RECALLCONFLICT, leaving ls unlinked, when a
 * recall is outstanding on the file: that recall has snapshotted its holders
 * already and would never call this one back.  Otherwise NFS4_OK. */
nfsstat4 nfs_layout_table_register(
    struct nfs_layout_table *table,
    struct nfs_layout_state *ls);

/* Advance ls's layout stateid seqid for a LAYOUTGET over a layout the client
 * already holds.  Refused with NFS4ERR_RECALLCONFLICT, seqid untouched, while
 * a recall is outstanding on the file, decided under the shard lock. */
nfsstat4 nfs_layout_table_advance(
    struct nfs_layout_table *table,
    struct nfs_layout_state *ls);

void nfs_layout_table_deregister(
    struct nfs_layout_table *table,
    struct nfs_layout_state *ls);

/*
 * One batch of a recall for fh.
 *
 * Returns -1 when the file has no layout holders; `waiter` is then NOT
 * enqueued and the caller resumes at once.  Otherwise, when `waiter` is
 * non-NULL it is appended to the file's deferred-op list (starting the recall,
 * or joining one already active); pass NULL to fetch a further batch of a
 * recall already started.
 *
 * Either way it snapshots into out_holders up to max_holders holders that no
 * earlier batch or overlapping recall has snapshotted, marks them
 * (recall_snapped), pins each with a layout ref the caller releases with
 * nfs_layout_state_put, and returns how many (0 when every current holder has
 * already been snapshotted, or when `waiter` is NULL and no recall is active
 * any more).  LAYOUTGET is refused while the deferred-op list
 * is non-empty, so a caller that repeats until a batch comes back shorter than
 * max_holders has recalled every holder.
 */
int nfs_layout_table_recall_prepare(
    struct nfs_layout_table         *table,
    const uint8_t                   *fh,
    uint16_t                         fh_len,
    struct nfs_layout_recall_waiter *waiter,
    struct nfs_layout_state        **out_holders,
    int                              max_holders);

/*
 * True while a recall started by nfs_layout_table_recall_prepare is still
 * outstanding for fh (i.e. the file has deferred operations waiting for every
 * holder to return).  LAYOUTGET consults this so it does not hand out a layout
 * the in-progress recall has already snapshotted past
 * (RFC 8881 §12.5.5.2 / §18.43.3: NFS4ERR_RECALLCONFLICT).
 */
bool nfs_layout_table_recall_active(
    struct nfs_layout_table *table,
    const uint8_t           *fh,
    uint16_t                 fh_len);

/* True while fh has at least one layout holder, of any layout type. */
bool nfs_layout_table_has_holders(
    struct nfs_layout_table *table,
    const uint8_t           *fh,
    uint16_t                 fh_len);

/*
 * Withdraw a parked waiter: remove from fh's deferred-op list the first
 * waiter `match` accepts and return it (the caller frees it and its arg).
 * NULL when none matches -- the barrier has already fired it, or it was never
 * parked.  A withdrawn waiter never runs.
 */
struct nfs_layout_recall_waiter *
nfs_layout_table_take_waiter(
    struct nfs_layout_table *table,
    const uint8_t           *fh,
    uint16_t                 fh_len,
    bool (                  *match )(
        const struct nfs_layout_recall_waiter *w,
        const void                            *key),
    const void              *key);

/*
 * Holders a recall snapshotted at least max_age_ns before now_ns, on files
 * whose recall is still active: pinned into out, up to max.  Each is pinned
 * twice, a layout ref and a ref on its client; release both
 * (nfs_client_put(ls->client), then nfs_layout_state_put(ls)).  The caller revokes them; a revoked holder leaves the
 * table, so calling again until fewer than max come back visits each once.
 */
int nfs_layout_table_collect_overdue(
    struct nfs_layout_table  *table,
    uint64_t                  now_ns,
    uint64_t                  max_age_ns,
    struct nfs_layout_state **out,
    int                       max);
