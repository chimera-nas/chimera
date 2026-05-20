// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <time.h>

#include "evpl/evpl.h"

/*
 * NFSv4 lease management.
 *
 * A client's lease records the time of the last state-touching op.  A
 * per-thread sweeper fires at 1Hz to walk the client table and mark any
 * client whose lease has expired (last_touch + lease_time < now) with the
 * `expired` flag.  RENEW (4.0) and any state op then return NFS4ERR_EXPIRED
 * for those clients.
 *
 * Phase 3 does not actively destroy expired clients' state -- destruction
 * happens at DESTROY_CLIENTID or shutdown.  This avoids racing with in-flight
 * state acquires.  Phase 5+ may add active reclamation.
 */

#define NFS4_LEASE_TIME_DEFAULT_S 90
#define NFS4_GRACE_TIME_DEFAULT_S 180

struct chimera_server_nfs_shared;
struct chimera_server_nfs_thread;

struct nfs_lease_sweeper {
    struct evpl_timer                 timer;
    struct chimera_server_nfs_thread *thread;
};

/* Monotonic time in nanoseconds, used for lease bookkeeping. */
static inline uint64_t
nfs_lease_now_ns(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);
    return (uint64_t) ts.tv_sec * 1000000000ULL + (uint64_t) ts.tv_nsec;
} /* nfs_lease_now_ns */

/*
 * Arm a periodic lease sweeper on `thread`'s evpl loop.  Each thread arms
 * its own; sweepers iterate the shared client table under the existing
 * client-table mutex, so redundant sweeps converge harmlessly.
 */
void
nfs_lease_sweeper_init(
    struct nfs_lease_sweeper         *sweeper,
    struct chimera_server_nfs_thread *thread);

void
nfs_lease_sweeper_destroy(
    struct nfs_lease_sweeper *sweeper);

/*
 * Walk the shared client table once and flip the `expired` flag on every
 * client whose lease has lapsed.  Exposed for the sweeper callback and for
 * tests.
 */
void
nfs_lease_sweep_once(
    struct chimera_server_nfs_shared *shared);
