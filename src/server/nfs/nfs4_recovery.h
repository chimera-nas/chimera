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
    /* The node that owns the client: the one that last persisted the record
     * (it is carried in the value, the key being fleet-wide).  Decides who
     * reloads the record at boot and who may delete it. */
    uint16_t       owner_node;
    bool           reclaimed;
    bool           returned;  /* re-established this boot (its record was re-persisted) */
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
    evpl_mutex_t                       lock;
    struct nfs_recovery_record        *to_reclaim; /* uthash by owner_string */
    uint32_t                           pending_reclaim; /* count of !reclaimed records */
    uint64_t                           current_boot_id;
    uint64_t                           grace_end_ns;
    bool                               in_grace;
    /* Persistence wiring (see nfs4_recovery.c). */
    struct chimera_vfs                *vfs;          /* for the cold-start load */
    uint16_t                           node_id;      /* scopes our records in a shared store */
    uint32_t                           grace_time_s; /* captured for deferred begin */
    uint32_t                           lease_time_s; /* refresh + heartbeat cadence, stale test */
    /* Monotonic ns at which the next epoch heartbeat is due; claimed by CAS so
     * exactly one sweeper thread writes it per interval. */
    _Atomic uint64_t                   next_heartbeat_ns;
    bool                               persistence_disabled;/* kv_module is non-persistent */
    bool                               nfs4_drc;     /* reply-cache persistence on   */
    _Atomic int                        load_state;   /* enum nfs_recovery_load_state */
    uint32_t                           stale_purged; /* records skipped + deleted at cold start */
    uint32_t                           unreclaimed_purged; /* records deleted at grace end, cumulative */
    uint32_t                           store_errors; /* KV reads that failed during the cold start */
    uint32_t                           failover_imported; /* peer records imported, cumulative */
    uint32_t                           imports_in_flight; /* peer scans still adding to to_reclaim */
    /* Peers whose records were imported this boot, by the boot of theirs that
     * died: a peer that restarts and dies again is scanned again. */
    struct nfs_recovery_imported_peer *imported_peers;
    /* CLOCK_REALTIME ns of the previous instance's last epoch heartbeat, read
     * by the cold start (0 = none on record).  Written before load_state turns
     * READY, so a reader that saw READY sees it.  The NSM cold start ages its
     * monitor records against it (nfs_recovery_prev_alive_s). */
    uint64_t                           prev_alive_ns;
};

struct nfs_client;

/*
 * Initialize recovery state at shared init.  Records `vfs`, `lease_time_s`
 * (refresh cadence and stale bound) and `grace_time_s` for the cold-start
 * load, detects whether the configured KV module is persistent (memkv is not
 * -> persistence_disabled + a warning), and leaves load_state = IDLE.  The KV
 * scan itself runs in nfs_recovery_cold_start.
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
 * it, loads the client records this node owns from the fleet-wide band into
 * to_reclaim (moving any left in its legacy per-node band there), opens the
 * grace window iff any were loaded, and flips load_state to READY -- all on a
 * private evpl loop, so it returns only when the store has answered.  A
 * non-persistent backend (memkv) settles to READY at once.
 */
void
nfs_recovery_cold_start(
    struct nfs_recovery *rec);

/*
 * Record a KV read that failed during the cold-start load (`error` is the
 * enum chimera_vfs_error the callback received; a missing key is not an
 * error).  Logged at once and counted, so the completion line can say the
 * store failed -- a failed scan must not read as "nothing was persisted",
 * because a client owed a reclaim then meets a closed window.
 */
void
nfs_recovery_note_store_error(
    struct nfs_recovery *rec,
    const char          *what,
    int                  error);

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
 * The previous instance's last heartbeat in CLOCK_REALTIME seconds, as read by
 * the cold start: the last time this node was known to be alive.  0 when it is
 * unknown (no epoch record, a failed read, a non-persistent backend) or the
 * cold start has not finished; check nfs_recovery_loading first.
 */
uint64_t
nfs_recovery_prev_alive_s(
    const struct nfs_recovery *rec);

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
 * it is safe to call after the owning nfs_client has been freed.  The stored
 * record is deleted only while this node still owns it: a client that has
 * since moved to a peer keeps the record that peer wrote.
 */
void
nfs_recovery_forget(
    struct chimera_vfs_thread *vfs_thread,
    struct nfs_recovery       *rec,
    const void                *owner,
    uint16_t                   owner_len);

/*
 * Failover import.  `owner`'s 4.1 session is being rebuilt here (see
 * nfs4_drc_session_hydrate) and its stored record names `peer` as its owner.
 * The result, from `peer`'s epoch heartbeat:
 *   NOT_FAILOVER      `peer` is this node, or its heartbeat has moved past
 *                     `seen_ns` (the written_at observed on an earlier call).
 *   LIVENESS_PENDING  fresh but not yet seen to move: a node that has just
 *                     died looks the same.  Retry with `seen_ns` = observed_ns.
 *   STORE_ERROR       the epoch record cannot be read or decoded, or the scan
 *                     for the peer's records failed.  Retry later.
 *   FAILOVER          over 1.5 leases old, or absent: `peer` is dead, and
 *                     `owner` is owed a reclaim here (its record is in the
 *                     open window's set, not yet reclaimed).
 * On a dead peer, the grace window is opened before the scan and every record
 * it owns is loaded into to_reclaim (once per boot of that peer; a failed scan
 * is retried on the next call).  Once no import is in flight and nobody is owed
 * a reclaim, the window closes again.  Nothing is imported for the other
 * results.  Asynchronous on vfs_thread; done may run before this returns.
 */
enum nfs_recovery_failover {
    NFS_RECOVERY_NOT_FAILOVER = 0,
    NFS_RECOVERY_FAILOVER,
    NFS_RECOVERY_STORE_ERROR,
    NFS_RECOVERY_LIVENESS_PENDING,
};

void
nfs_recovery_import_peer(
    struct chimera_vfs_thread *vfs_thread,
    struct nfs_recovery       *rec,
    uint16_t                   peer,
    const void                *owner,
    uint16_t                   owner_len,
    uint64_t                   seen_ns,
    void (                    *done )(
        enum nfs_recovery_failover result,
        uint64_t                   observed_ns,
        void                      *arg),
    void                      *arg);

/*
 * Look up which node owns `owner`'s stored recovery record.  A record that
 * cannot be read or decoded is OWNER_ERROR, not OWNER_ABSENT: the caller must
 * not treat it as a client with no previous node.  Persistence off is
 * OWNER_ABSENT.  Asynchronous on vfs_thread.
 */
enum nfs_recovery_owner {
    NFS_RECOVERY_OWNER_ABSENT = 0,
    NFS_RECOVERY_OWNER_FOUND,
    NFS_RECOVERY_OWNER_ERROR,
};

void
nfs_recovery_lookup_owner(
    struct chimera_vfs_thread *vfs_thread,
    struct nfs_recovery       *rec,
    const void                *owner,
    uint16_t                   owner_len,
    void (                    *done )(
        enum nfs_recovery_owner result,
        uint16_t                node,
        void                   *arg),
    void                      *arg);

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
 * Called from the lease sweeper at 1Hz with the sweeping thread's vfs_thread.
 * Ends the grace window when grace_end_ns has been reached or the to_reclaim
 * set is fully reclaimed; on the deadline, deletes the records of clients that
 * neither reclaimed nor returned (vfs_thread may be NULL in tests: bookkeeping
 * only).
 */
void
nfs_recovery_sweep_once(
    struct nfs_recovery       *rec,
    struct chimera_vfs_thread *vfs_thread);

/*
 * Mark a reloaded record as belonging to a client that has re-established
 * itself this boot.  nfs_recovery_persist calls it; such a record is kept when
 * the window closes even if the client has not yet sent RECLAIM_COMPLETE.
 */
void
nfs_recovery_note_returned(
    struct nfs_recovery     *rec,
    const struct nfs_client *client);

/* ----------------------------------------------------------------------- *
*  Record (de)serialization -- exposed for unit tests (test_nfs_persist).  *
*  The wire layout is documented in nfs4_recovery.c.                       *
* ----------------------------------------------------------------------- */

struct nfs_client;

uint32_t
nfs_recovery_serialize(
    uint8_t                 *buf,
    uint32_t                 buf_size,
    const struct nfs_client *c,
    uint16_t                 owner_node);

/* Deserialize a recovery value.  A legacy value (NFR1/NFR2) carries no owning
 * node and comes back with owner_node = `legacy_owner`. */
int
nfs_recovery_deserialize(
    const uint8_t              *buf,
    uint32_t                    len,
    struct nfs_recovery_record *out,
    uint16_t                    legacy_owner);

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
