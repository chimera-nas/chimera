// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * diskfs_test.h -- a white-box test SDK for diskfs.
 *
 * These entry points are compiled INTO the diskfs module library (they need the
 * private diskfs_internal.h / space_map.h structs) and exported for the diskfs
 * model-based-test harness and unit tests to link against.  They are the
 * mechanism the quint diskfs stress model uses to (a) assert diskfs's internal
 * on-disk-allocator and b+tree invariants directly rather than inferring them
 * through VFS behaviour, (b) steer a workload toward the deep corners
 * (fragmentation, AG exhaustion, deep trees, reclaim), and (c) simulate a crash
 * so the next mount runs the intent-log replay recovery path.
 *
 * Nothing here is part of the VFS module ABI; it is a test-only side door.  All
 * reads take the relevant allocation-group locks, so a snapshot is consistent
 * against the background intent-log / reclaim threads.
 */

#ifndef DISKFS_TEST_H
#define DISKFS_TEST_H

#include <stdint.h>

struct chimera_vfs;
struct evpl;

/* Pool-wide + aggregate space-allocator snapshot (summed across every device
 * and allocation group, under the AG locks). */
struct diskfs_test_space {
    uint64_t total_capacity;      /* raw sum of device sizes */
    uint64_t usable_capacity;     /* allocatable total (metadata excluded); constant */
    uint64_t ag_free_sum;         /* sum over AGs of ag->free_bytes */
    uint64_t dev_free_sum;        /* sum over devices of dev->free_bytes */
    uint64_t tree_free_sum;       /* sum over AGs of the free-extent tree lengths */
    uint64_t claim_bytes;         /* sum of live reservation-claim lengths */
    uint64_t largest_free_extent; /* largest single free extent anywhere */
    uint64_t total_free_extents;  /* free-extent (fragment) count across all AGs */
    uint32_t num_devices;
    uint32_t num_ags;
};

/* Fill *out with a consistent space snapshot.  Returns 0, or -1 if diskfs is
 * not the mounted module. */
int
diskfs_test_space(
    struct chimera_vfs       *vfs,
    struct diskfs_test_space *out);

/*
 * Run every always-true internal invariant of the space allocator:
 *   - each AG's free-extent tree is sorted, non-overlapping and fully coalesced
 *     (no two adjacent free extents);
 *   - each AG's free-extent tree lengths sum to ag->free_bytes;
 *   - the per-AG free totals sum to the per-device running counters;
 *   - total free never exceeds usable capacity;
 *   - every live reservation claim lies within its AG.
 * Returns 0 if consistent; on a violation returns -1 and writes a one-line
 * description into err (up to errlen bytes).  A stable snapshot is taken under
 * all AG locks, so this is safe to call while the background threads run, but
 * the cross-AG sum check is only meaningful once reclaim has quiesced (call
 * diskfs_test_await_reclaim first if a delete-heavy step just ran).
 */
int
diskfs_test_check(
    struct chimera_vfs *vfs,
    char               *err,
    int                 errlen);

/* Best-effort per-inode geometry, read from the resident inode + its home
 * block.  tree_height is the b+tree height (root level + 1); root_nitems the
 * entry count in the embedded root; nlink/size the inode's.  Returns 0 if the
 * inode (and its home block) is resident and *out was filled, -1 otherwise --
 * an evicted inode is not an error, just unavailable, so callers use this to
 * confirm depth opportunistically rather than as a hard oracle. */
struct diskfs_test_inode {
    uint64_t inum;
    uint64_t size;
    uint32_t nlink;
    uint16_t tree_height;
    uint16_t root_nitems;
};

int
diskfs_test_inode(
    struct chimera_vfs       *vfs,
    uint64_t                  inum,
    struct diskfs_test_inode *out);

/*
 * Spin (pumping evpl and yielding) until the background reclaim workers are idle
 * and the intent-log apply frontier has caught up, so all freed space has been
 * returned to the allocator -- or until max_ms elapses.  Returns 1 if it
 * quiesced, 0 if it gave up.  Reclaim runs on its own threads, so this only
 * observes their progress; evpl is the caller's loop, pumped so any completion
 * routed back to it makes progress too.
 */
int
diskfs_test_await_reclaim(
    struct chimera_vfs *vfs,
    struct evpl        *evpl,
    int                 max_ms);

/*
 * Simulate a crash: mark the mounted diskfs so the following chimera_vfs_destroy
 * tears it down WITHOUT the clean-unmount free-map persist + SM_SB_CLEAN stamp,
 * leaving the device in the on-disk state a crash leaves so the next mount runs
 * intent-log replay recovery.  This only sets a flag -- the teardown itself
 * happens inside chimera_vfs_destroy, at the point the VFS has already torn down
 * its own internal threads (whose diskfs_thread_destroy still dereferences the
 * shared state), which freeing it here would race.  Usage:
 *
 *     chimera_vfs_thread_destroy(thread);   // acked writes durable in the log
 *     diskfs_test_crash(vfs);               // mark: skip the clean finalize
 *     chimera_vfs_destroy(vfs);             // crash teardown happens here
 *     // ... re-init a fresh vfs and mount the same device, "initialize" omitted
 */
void
diskfs_test_crash(
    struct chimera_vfs *vfs);

#endif /* DISKFS_TEST_H */
