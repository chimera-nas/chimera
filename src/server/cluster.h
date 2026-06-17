// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>

struct chimera_vfs_thread;
struct chimera_cluster;

/*
 * Cluster eviction registry (WS-D control plane for the VIP + shared-backend
 * model).
 *
 * Chimera does not run its own membership/heartbeat protocol; an external
 * orchestrator (or the coherent backend) decides a node is dead and tells the
 * surviving nodes via the REST control endpoint, which calls
 * chimera_cluster_evict().  The eviction set is persisted to the shared KV so
 * every node (and a survivor that itself restarts mid-outage) observes the same
 * view, and is the authorization other workstreams consult:
 *   - WS-C cont: a node only cross-node-reclaims an SMB durable handle whose
 *     owning node (CHIMERA_SMB_PID_NODE of the persistent id) has been evicted,
 *     so it never steals a handle a live peer still holds warm.
 *   - WS-A: opening/extending the cluster grace window on an eviction.
 *   - lease-persist branch: releasing the evicted node's leases/locks.
 *
 * node_id 0 is the single-node/unclustered sentinel and is never evictable.
 */

struct chimera_cluster *
chimera_cluster_init(
    int local_node_id);

void
chimera_cluster_destroy(
    struct chimera_cluster *cluster);

/* One-time async load of the persisted eviction set from the shared KV, then
 * clear (rejoin) this node's own record -- the local node is alive by
 * definition.  Idempotent; safe to call from the first server thread online. */
void
chimera_cluster_load(
    struct chimera_cluster    *cluster,
    struct chimera_vfs_thread *vfs_thread);

/* Mark node_id evicted: set the in-memory bit and write-through to the shared
 * KV so peers observe it.  Returns 0 on success, -1 if node_id is out of range
 * (1..255) or is the local node (a live node cannot evict itself). */
int
chimera_cluster_evict(
    struct chimera_cluster    *cluster,
    struct chimera_vfs_thread *vfs_thread,
    int                        node_id);

/* Clear an eviction (a node has rejoined).  Returns 0 on success, -1 on a
 * bad node_id. */
int
chimera_cluster_rejoin(
    struct chimera_cluster    *cluster,
    struct chimera_vfs_thread *vfs_thread,
    int                        node_id);

/* Lock-free query: non-zero if node_id is currently considered evicted. */
int
chimera_cluster_node_is_evicted(
    const struct chimera_cluster *cluster,
    int                           node_id);

int
chimera_cluster_local_node_id(
    const struct chimera_cluster *cluster);

/* Snapshot the evicted node ids into out[] (up to cap entries); returns the
 * number written. */
int
chimera_cluster_evicted_list(
    const struct chimera_cluster *cluster,
    uint8_t                      *out,
    int                           cap);
