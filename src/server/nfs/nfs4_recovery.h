// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>

#include <uthash.h>

#include "nfs4_xdr.h"

/*
 * Server-reboot recovery + grace period.
 *
 * On startup the server loads any persistently-stored "confirmed client"
 * records from the KV store (nfs_recovery_persist writes them at confirm time)
 * and enters a grace window of `grace_time_s` during which:
 *   - OPENs with CLAIM_PREVIOUS from clients in the to_reclaim set are
 *     allowed (the reclaim path).
 *   - All other OPENs return NFS4ERR_GRACE.
 *
 * Grace ends when:
 *   - Every loaded client has issued RECLAIM_COMPLETE, OR
 *   - `grace_end_ns` is reached, whichever comes first.
 *
 * When the configured KV module is non-persistent (memkv) the load finds
 * nothing, to_reclaim is empty, and grace short-circuits to in_grace = false --
 * matching the prior in-memory-only behavior.  The async KV scan is deferred to
 * the first NFSv4 compound (see nfs_recovery_kickoff).
 */

struct nfs_recovery_record {
    char           owner_string[NFS4_OPAQUE_LIMIT];
    uint16_t       owner_len;
    uint64_t       client_id_hint; /* not authoritative; just a sticky ID */
    uint64_t       verifier;
    uint64_t       boot_id;
    bool           reclaimed;
    UT_hash_handle hh;
};

/*
 * Cold-start load progresses IDLE -> RUNNING -> READY.  The async KV scan that
 * populates to_reclaim needs a worker thread + a live event loop, neither of
 * which exists at shared init, so the load is deferred to the first NFSv4
 * compound (which atomically claims the IDLE->RUNNING transition).  While not
 * READY the grace gate treats the server as in-grace (reclaim-eligible).
 */
enum nfs_recovery_load_state {
    NFS_REC_LOAD_IDLE    = 0,
    NFS_REC_LOAD_RUNNING = 1,
    NFS_REC_LOAD_READY   = 2,
};

struct chimera_vfs;
struct chimera_vfs_thread;
struct chimera_server_nfs_thread;

struct nfs_recovery {
    pthread_mutex_t             lock;
    struct nfs_recovery_record *to_reclaim;       /* uthash by owner_string */
    uint32_t                    pending_reclaim;  /* count of !reclaimed records */
    uint64_t                    current_boot_id;
    uint64_t                    grace_end_ns;
    bool                        in_grace;
    /* Persistence wiring (see nfs4_recovery.c). */
    struct chimera_vfs         *vfs;                 /* for the cold-start load */
    uint16_t                    node_id;             /* scopes our records in a shared store */
    uint32_t                    grace_time_s;        /* captured for deferred begin */
    bool                        persistence_disabled;/* kv_module is non-persistent */
    bool                        nfs4_drc;            /* reply-cache persistence on   */
    _Atomic int                 load_state;          /* enum nfs_recovery_load_state */
};

struct nfs_client;

/*
 * Initialize recovery state at shared init.  Records `vfs` and `grace_time_s`
 * for the deferred load, detects whether the configured KV module is
 * persistent (memkv is not -> persistence_disabled + a warning), and leaves
 * load_state = IDLE.  The actual KV scan happens later in nfs_recovery_kickoff.
 */
int
nfs_recovery_load(
    struct nfs_recovery *rec,
    struct chimera_vfs  *vfs,
    uint16_t             node_id,
    uint32_t             grace_time_s,
    bool                 nfs4_drc);

void
nfs_recovery_free(
    struct nfs_recovery *rec);

/*
 * Run-once cold-start load, triggered from the first NFSv4 compound on a
 * worker thread (which owns a live vfs_thread).  Atomically claims the
 * IDLE->RUNNING transition; the loser returns immediately.  Forces the grace
 * window open, then issues async KV reads (epoch marker + recovery-record scan,
 * and -- when nfs4_drc is on -- the session + reply-cache reload) that populate
 * to_reclaim and flip load_state to READY on completion.
 */
void
nfs_recovery_kickoff(
    struct chimera_server_nfs_thread *thread);

/*
 * True while the persistent cold-start load (recovery records + DRC session
 * reconstruction) is still in flight.  EXCHANGE_ID / CREATE_SESSION return
 * NFS4ERR_DELAY in this window so a returning client cannot race ahead of the
 * reconstructed records and create a duplicate.  Always false when persistence
 * is disabled (memkv).
 */
bool
nfs_recovery_loading(
    struct nfs_recovery *rec);

/*
 * Begin the grace window.  If to_reclaim is empty (the persistence stub
 * loads nothing), short-circuits to in_grace = false so callers see no
 * behavioral change.  Otherwise stamps grace_end_ns.
 */
void
nfs_recovery_begin_grace(
    struct nfs_recovery *rec,
    uint32_t             grace_time_s);

/*
 * End the grace window early.  Idempotent.
 */
void
nfs_recovery_end_grace(
    struct nfs_recovery *rec);

bool
nfs_recovery_in_grace(
    struct nfs_recovery *rec);

/*
 * Persist a newly-confirmed client's recovery record to the KV store
 * (fire-and-forget).  No-op when persistence is disabled (memkv backend) or
 * client is NULL.  Safe to call from any NFSv4 compound (uses vfs_thread).
 */
void
nfs_recovery_persist(
    struct chimera_vfs_thread *vfs_thread,
    struct nfs_recovery       *rec,
    const struct nfs_client   *client);

/*
 * Forget a record (DESTROY_CLIENTID / lease teardown): delete it from the KV
 * store and drop any matching to_reclaim entry.  Takes the raw owner bytes so
 * it is safe to call after the owning nfs_client has been freed.
 */
void
nfs_recovery_forget(
    struct chimera_vfs_thread *vfs_thread,
    struct nfs_recovery       *rec,
    const void                *owner,
    uint16_t                   owner_len);

/*
 * Gate for OPEN during recovery:
 *   in_grace && !is_reclaim  -> NFS4ERR_GRACE
 *   !in_grace && is_reclaim  -> NFS4ERR_NO_GRACE
 *   otherwise                -> NFS4_OK
 */
nfsstat4
nfs_recovery_open_check(
    struct nfs_recovery     *rec,
    const struct nfs_client *client,
    bool                     is_reclaim);

/*
 * Mark `client` as having completed reclaim.  When the pending_reclaim
 * count drops to zero the grace window ends early.
 */
void
nfs_recovery_reclaim_complete(
    struct nfs_recovery     *rec,
    const struct nfs_client *client);

/*
 * Called from the lease sweeper at 1Hz.  Ends the grace window when
 * grace_end_ns has been reached or the to_reclaim set is fully reclaimed.
 */
void
nfs_recovery_sweep_once(
    struct nfs_recovery *rec);

/* ----------------------------------------------------------------------- *
*  Record (de)serialization -- exposed for unit tests (test_nfs_persist).  *
*  The wire layout is documented in nfs4_recovery.c.                       *
* ----------------------------------------------------------------------- */

struct nfs_client;

uint32_t
nfs_recovery_serialize(
    uint8_t                 *buf,
    uint32_t                 buf_size,
    const struct nfs_client *c);

int
nfs_recovery_deserialize(
    const uint8_t              *buf,
    uint32_t                    len,
    struct nfs_recovery_record *out);

uint32_t
nfs_recovery_epoch_serialize(
    uint8_t *buf,
    uint64_t boot_id);

int
nfs_recovery_epoch_deserialize(
    const uint8_t *buf,
    uint32_t       len,
    uint64_t      *out_boot_id);
