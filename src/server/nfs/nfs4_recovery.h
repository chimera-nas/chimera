// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "common/thread.h"
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
 * matching the prior in-memory-only behavior.  The KV scan runs synchronously
 * from the NFS protocol start hook (nfs_recovery_cold_start), before any
 * listener is bound, so no request ever races it.
 */

struct nfs_recovery_record {
    char           owner_string[NFS4_OPAQUE_LIMIT];
    uint16_t       owner_len;
    uint64_t       client_id_hint; /* not authoritative; just a sticky ID */
    uint64_t       verifier;
    uint64_t       boot_id;
    /* CLOCK_REALTIME ns of the client's last lease renewal as of the last
     * persist (see nfs_recovery_persist / the lease sweeper refresh).  0 for a
     * record written before the stamp existed, which the loader treats as
     * "unknown, keep". */
    uint64_t       last_renew_ns;
    bool           reclaimed;
    UT_hash_handle hh;
};

/*
 * Cold-start load progresses IDLE -> RUNNING -> READY.  nfs_recovery_cold_start
 * drives the whole transition synchronously from nfs_server_start, before the
 * RPC listeners exist, so RUNNING is never observable by a request.  The gates
 * that consult it (EXCHANGE_ID / CREATE_SESSION DELAY, the in-grace treatment
 * in nfs_recovery_open_check) remain as a defence against a future caller that
 * starts serving before the load.
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
    evpl_mutex_t                lock;
    struct nfs_recovery_record *to_reclaim;       /* uthash by owner_string */
    uint32_t                    pending_reclaim;  /* count of !reclaimed records */
    uint64_t                    current_boot_id;
    uint64_t                    grace_end_ns;
    bool                        in_grace;
    /* Persistence wiring (see nfs4_recovery.c). */
    struct chimera_vfs         *vfs;                 /* for the cold-start load */
    uint16_t                    node_id;             /* scopes our records in a shared store */
    uint32_t                    grace_time_s;        /* captured for deferred begin */
    uint32_t                    lease_time_s;        /* refresh + heartbeat cadence, stale test */
    /* Monotonic ns at which the next epoch heartbeat is due; claimed by CAS so
     * exactly one sweeper thread writes it per interval. */
    _Atomic uint64_t            next_heartbeat_ns;
    bool                        persistence_disabled;/* kv_module is non-persistent */
    bool                        nfs4_drc;            /* reply-cache persistence on   */
    _Atomic int                 load_state;          /* enum nfs_recovery_load_state */
    uint32_t                    stale_purged;        /* records skipped + deleted at cold start */
};

struct nfs_client;

/*
 * Initialize recovery state at shared init.  Records `vfs`, `lease_time_s`
 * (refresh cadence and stale bound) and `grace_time_s` for the cold-start load, detects whether the configured KV module is
 * persistent (memkv is not -> persistence_disabled + a warning), and leaves
 * load_state = IDLE.  The actual KV scan happens later in nfs_recovery_kickoff.
 */
int
nfs_recovery_load(
    struct nfs_recovery *rec,
    struct chimera_vfs  *vfs,
    uint16_t             node_id,
    uint32_t             lease_time_s,
    uint32_t             grace_time_s,
    bool                 nfs4_drc);

void
nfs_recovery_free(
    struct nfs_recovery *rec);

/*
 * Run-once cold-start load.  Called from nfs_server_start on the main thread
 * BEFORE the RPC listeners are bound: reads the boot epoch, bumps and rewrites
 * it, scans this node's recovery records into to_reclaim, opens the grace
 * window iff any were loaded, and flips load_state to READY -- all on a
 * private evpl loop, so it returns only when the store has answered.  A
 * non-persistent backend (memkv) settles to READY at once.
 */
void
nfs_recovery_cold_start(
    struct nfs_recovery *rec);

/*
 * True while the persistent cold-start load (recovery records + DRC session
 * reconstruction) is still in flight.  EXCHANGE_ID / CREATE_SESSION return
 * NFS4ERR_DELAY in this window; since the load completes before the listeners
 * start, that is a defensive gate rather than a path clients hit.  Always
 * false when persistence is disabled (memkv).
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
 * Stamps client->recovery_stamp_ns so the lease sweeper knows when the record
 * is due a refresh.
 */
void
nfs_recovery_persist(
    struct chimera_vfs_thread *vfs_thread,
    struct nfs_recovery       *rec,
    struct nfs_client         *client);

/*
 * Lease-sweeper cadence.  A live, confirmed client's record is re-persisted
 * once per half lease so its last_renew_ns stays within half a lease of the
 * truth; the epoch record is rewritten on the same cadence so its written_at
 * field records when this instance was last alive.  Together they let the
 * next boot tell a client that was alive at the crash from one whose lease had
 * already lapsed (nfs_recovery_record_stale).  The interval floors at 1 s.
 */
bool
nfs_recovery_persist_due(
    const struct nfs_recovery *rec,
    const struct nfs_client   *client,
    uint64_t                   now_ns);

/* Claims the current heartbeat slot; true for exactly one caller per interval. */
bool
nfs_recovery_heartbeat_due(
    struct nfs_recovery *rec,
    uint64_t             now_ns);

/* Rewrites the epoch record if a heartbeat is due.  Fire-and-forget. */
void
nfs_recovery_heartbeat(
    struct nfs_recovery       *rec,
    struct chimera_vfs_thread *vfs_thread);

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
 * Gate for the operations the grace window protects that carry no reclaim
 * flag of their own: READ, WRITE, a size-changing SETATTR and LOCKT.
 * NFS4ERR_GRACE while the window is open (or the cold-start load is still in
 * flight), NFS4_OK otherwise.
 */
nfsstat4
nfs_recovery_io_check(
    struct nfs_recovery *rec);

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

/*
 * True when the record's client had provably lost its lease before the
 * previous instance's last heartbeat (last_alive_ns, the epoch record's
 * written_at).  The persisted stamp lags the real renewal by at most half a
 * lease, so a gap above 1.5 leases means the lease had lapsed before the
 * crash.  Unstamped (legacy) records and a store with no epoch are never
 * stale.  Exposed for unit tests.
 */
bool
nfs_recovery_record_stale(
    const struct nfs_recovery        *rec,
    const struct nfs_recovery_record *r,
    uint64_t                          last_alive_ns);

uint32_t
nfs_recovery_epoch_serialize(
    uint8_t *buf,
    uint64_t boot_id);

int
nfs_recovery_epoch_deserialize(
    const uint8_t *buf,
    uint32_t       len,
    uint64_t      *out_boot_id,
    uint64_t      *out_written_ns);
