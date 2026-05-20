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
 * records (in this phase: zero, the persistence layer is stubbed) and
 * enters a grace window of `grace_time_s` during which:
 *   - OPENs with CLAIM_PREVIOUS from clients in the to_reclaim set are
 *     allowed (the reclaim path).
 *   - All other OPENs return NFS4ERR_GRACE.
 *
 * Grace ends when:
 *   - Every loaded client has issued RECLAIM_COMPLETE, OR
 *   - `grace_end_ns` is reached, whichever comes first.
 *
 * Since the persistence layer is a no-op stub today, to_reclaim is empty
 * at boot and nfs_recovery_begin_grace short-circuits to in_grace = false.
 * The wiring is in place for a future file-per-client or RocksDB backend
 * to plug in behind the same API without touching callers.
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

struct nfs_recovery {
    pthread_mutex_t             lock;
    struct nfs_recovery_record *to_reclaim;       /* uthash by owner_string */
    uint32_t                    pending_reclaim;  /* count of !reclaimed records */
    uint64_t                    current_boot_id;
    uint64_t                    grace_end_ns;
    bool                        in_grace;
};

struct nfs_client;

/*
 * Initialize the recovery state.  Sets current_boot_id to a fresh value;
 * future implementations will derive it from persistent storage so that a
 * server restart bumps the boot_id and triggers reclaim eligibility.
 */
int
nfs_recovery_load(
    struct nfs_recovery *rec,
    const char          *state_dir,
    uint64_t            *out_prev_boot_id);

void
nfs_recovery_free(
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

/*
 * Persist (stub) a newly-confirmed client.  Future backends will write the
 * record to stable storage so a restart can reclaim it.
 */
void
nfs_recovery_persist(
    struct nfs_recovery     *rec,
    const struct nfs_client *client);

/*
 * Forget (stub) a record when its client issues DESTROY_CLIENTID.
 */
void
nfs_recovery_forget(
    struct nfs_recovery     *rec,
    const struct nfs_client *client);

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
