// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "nfs4_recovery.h"
#include "nfs4_lease.h"
#include "nfs4_state.h"

/*
 * Generate a fresh, monotonic boot_id.  Phase 5 derives it from
 * CLOCK_REALTIME; future persistent storage will load the prior epoch
 * from disk and pick a strictly greater value.
 */
static uint64_t
nfs_recovery_fresh_boot_id(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t) ts.tv_sec * 1000000000ULL + (uint64_t) ts.tv_nsec;
} /* nfs_recovery_fresh_boot_id */

int
nfs_recovery_load(
    struct nfs_recovery *rec,
    const char          *state_dir,
    uint64_t            *out_prev_boot_id)
{
    (void) state_dir;
    (void) out_prev_boot_id;

    pthread_mutex_init(&rec->lock, NULL);
    rec->to_reclaim      = NULL;
    rec->pending_reclaim = 0;
    rec->current_boot_id = nfs_recovery_fresh_boot_id();
    rec->grace_end_ns    = 0;
    rec->in_grace        = false;
    return 0;
} /* nfs_recovery_load */

void
nfs_recovery_free(struct nfs_recovery *rec)
{
#ifndef __clang_analyzer__
    struct nfs_recovery_record *r, *tmp;

    pthread_mutex_lock(&rec->lock);
    HASH_ITER(hh, rec->to_reclaim, r, tmp)
    {
        HASH_DEL(rec->to_reclaim, r);
        free(r);
    }
    rec->pending_reclaim = 0;
    pthread_mutex_unlock(&rec->lock);
    pthread_mutex_destroy(&rec->lock);
#endif /* ifndef __clang_analyzer__ */
} /* nfs_recovery_free */

void
nfs_recovery_begin_grace(
    struct nfs_recovery *rec,
    uint32_t             grace_time_s)
{
    pthread_mutex_lock(&rec->lock);
    if (rec->to_reclaim == NULL) {
        /* No clients to reclaim -- skip the grace window entirely so
         * normal traffic is accepted immediately. */
        rec->in_grace     = false;
        rec->grace_end_ns = 0;
    } else {
        rec->in_grace     = true;
        rec->grace_end_ns = nfs_lease_now_ns() +
            (uint64_t) grace_time_s * 1000000000ULL;
    }
    pthread_mutex_unlock(&rec->lock);
} /* nfs_recovery_begin_grace */

void
nfs_recovery_end_grace(struct nfs_recovery *rec)
{
    pthread_mutex_lock(&rec->lock);
    rec->in_grace     = false;
    rec->grace_end_ns = 0;
    pthread_mutex_unlock(&rec->lock);
} /* nfs_recovery_end_grace */

bool
nfs_recovery_in_grace(struct nfs_recovery *rec)
{
    bool in_grace;

    pthread_mutex_lock(&rec->lock);
    in_grace = rec->in_grace;
    pthread_mutex_unlock(&rec->lock);

    return in_grace;
} /* nfs_recovery_in_grace */

void
nfs_recovery_persist(
    struct nfs_recovery     *rec,
    const struct nfs_client *client)
{
    /* Stub: future backends (file-per-client under state_dir or RocksDB)
     * will serialize {owner_string, verifier, boot_id, principal} here. */
    (void) rec;
    (void) client;
} /* nfs_recovery_persist */

void
nfs_recovery_forget(
    struct nfs_recovery     *rec,
    const struct nfs_client *client)
{
    /* Stub: future backends remove the persistent record. */
    (void) rec;
    (void) client;
} /* nfs_recovery_forget */

nfsstat4
nfs_recovery_open_check(
    struct nfs_recovery     *rec,
    const struct nfs_client *client,
    bool                     is_reclaim)
{
    bool in_grace;

    (void) client;  /* Phase 5 doesn't gate per-client; future use. */

    pthread_mutex_lock(&rec->lock);
    in_grace = rec->in_grace;
    pthread_mutex_unlock(&rec->lock);

    if (in_grace && !is_reclaim) {
        return NFS4ERR_GRACE;
    }
    if (!in_grace && is_reclaim) {
        return NFS4ERR_NO_GRACE;
    }
    return NFS4_OK;
} /* nfs_recovery_open_check */

void
nfs_recovery_reclaim_complete(
    struct nfs_recovery     *rec,
    const struct nfs_client *client)
{
    /* Stub-friendly: find this client's record (if any) and mark reclaimed.
     * In the no-record case we simply leave the recovery state alone. */
    (void) client;
    pthread_mutex_lock(&rec->lock);
    /* When persistence lands, look up record by owner_string here, flip
     * reclaimed=true, decrement pending_reclaim, and if 0 set in_grace=false. */
    pthread_mutex_unlock(&rec->lock);
} /* nfs_recovery_reclaim_complete */

void
nfs_recovery_sweep_once(struct nfs_recovery *rec)
{
    uint64_t now;
    bool     end_now = false;

    pthread_mutex_lock(&rec->lock);
    if (!rec->in_grace) {
        pthread_mutex_unlock(&rec->lock);
        return;
    }
    now = nfs_lease_now_ns();
    if (now >= rec->grace_end_ns || rec->pending_reclaim == 0) {
        end_now = true;
    }
    if (end_now) {
        rec->in_grace     = false;
        rec->grace_end_ns = 0;
    }
    pthread_mutex_unlock(&rec->lock);
} /* nfs_recovery_sweep_once */
