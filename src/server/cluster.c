// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>

#include "cluster.h"
#include "server_internal.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"

/*
 * Shared-KV key for an eviction record: a 3-byte header (magic, version, type)
 * followed by the 1-byte node_id.  Distinct magic (0xCB) from the NFS KV band
 * (0xC4) and the SMB "smbdh" band so the three never alias in a shared store.
 * The value is an 8-byte marker (magic + reserved); presence is what matters.
 */
#define CLUSTER_KV_MAGIC       0xCB
#define CLUSTER_KV_VERSION     0x01
#define CLUSTER_KV_TYPE_EVICT  0x01
#define CLUSTER_KV_HDR_LEN     3
#define CLUSTER_KV_KEY_LEN     (CLUSTER_KV_HDR_LEN + 1)
#define CLUSTER_KV_VALUE_MAGIC 0x5443564bU  /* 'KVCT' LE */
#define CLUSTER_KV_VALUE_LEN   8

enum cluster_load_state {
    CLUSTER_LOAD_IDLE = 0,
    CLUSTER_LOAD_RUNNING,
    CLUSTER_LOAD_READY,
};

struct chimera_cluster {
    int             local_node_id;
    _Atomic int     load_state;
    _Atomic uint8_t evicted[256];
};

struct chimera_cluster_load_ctx {
    struct chimera_cluster    *cluster;
    struct chimera_vfs_thread *vfs_thread;
    uint8_t                    start[CLUSTER_KV_HDR_LEN];
};

static inline uint32_t
cluster_evict_key(
    uint8_t *buf,
    int      node_id)
{
    buf[0] = CLUSTER_KV_MAGIC;
    buf[1] = CLUSTER_KV_VERSION;
    buf[2] = CLUSTER_KV_TYPE_EVICT;
    buf[3] = (uint8_t) node_id;
    return CLUSTER_KV_KEY_LEN;
} /* cluster_evict_key */

struct chimera_cluster *
chimera_cluster_init(int local_node_id)
{
    struct chimera_cluster *cluster = calloc(1, sizeof(*cluster));

    cluster->local_node_id = local_node_id;
    atomic_store(&cluster->load_state, CLUSTER_LOAD_IDLE);
    return cluster;
} /* chimera_cluster_init */

void
chimera_cluster_destroy(struct chimera_cluster *cluster)
{
    free(cluster);
} /* chimera_cluster_destroy */

int
chimera_cluster_node_is_evicted(
    const struct chimera_cluster *cluster,
    int                           node_id)
{
    if (node_id < 0 || node_id > 255) {
        return 0;
    }
    return atomic_load(&cluster->evicted[node_id]);
} /* chimera_cluster_node_is_evicted */

int
chimera_cluster_local_node_id(const struct chimera_cluster *cluster)
{
    return cluster->local_node_id;
} /* chimera_cluster_local_node_id */

int
chimera_cluster_evicted_list(
    const struct chimera_cluster *cluster,
    uint8_t                      *out,
    int                           cap)
{
    int n = 0;

    for (int i = 0; i < 256 && n < cap; i++) {
        if (atomic_load(&cluster->evicted[i])) {
            out[n++] = (uint8_t) i;
        }
    }
    return n;
} /* chimera_cluster_evicted_list */

static void
cluster_kv_done(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    (void) private_data;
    if (error_code != CHIMERA_VFS_OK) {
        chimera_server_error("cluster: KV update failed: %d", error_code);
    }
} /* cluster_kv_done */

int
chimera_cluster_evict(
    struct chimera_cluster    *cluster,
    struct chimera_vfs_thread *vfs_thread,
    int                        node_id)
{
    uint8_t  key[CLUSTER_KV_KEY_LEN];
    uint8_t  value[CLUSTER_KV_VALUE_LEN] = { 0 };
    uint32_t key_len;

    if (node_id < 1 || node_id > 255 || node_id == cluster->local_node_id) {
        return -1;
    }

    atomic_store(&cluster->evicted[node_id], 1);

    *(uint32_t *) value = CLUSTER_KV_VALUE_MAGIC;
    key_len             = cluster_evict_key(key, node_id);

    chimera_server_info("cluster: node %d evicted (signaled on node %d)",
                        node_id, cluster->local_node_id);

    chimera_vfs_put_key(vfs_thread, key, key_len, value, sizeof(value),
                        cluster_kv_done, NULL);
    return 0;
} /* chimera_cluster_evict */

int
chimera_cluster_rejoin(
    struct chimera_cluster    *cluster,
    struct chimera_vfs_thread *vfs_thread,
    int                        node_id)
{
    uint8_t  key[CLUSTER_KV_KEY_LEN];
    uint32_t key_len;

    if (node_id < 1 || node_id > 255) {
        return -1;
    }

    atomic_store(&cluster->evicted[node_id], 0);

    key_len = cluster_evict_key(key, node_id);
    chimera_vfs_delete_key(vfs_thread, key, key_len, cluster_kv_done, NULL);
    return 0;
} /* chimera_cluster_rejoin */

static int
cluster_load_scan_cb(
    const void *key,
    uint32_t    key_len,
    const void *value,
    uint32_t    value_len,
    void       *private_data)
{
    struct chimera_cluster_load_ctx *ctx = private_data;
    const uint8_t                   *k   = key;

    (void) value;
    (void) value_len;

    /* Keys arrive in order; stop once we walk past the eviction band. */
    if (key_len != CLUSTER_KV_KEY_LEN ||
        k[0] != CLUSTER_KV_MAGIC || k[1] != CLUSTER_KV_VERSION ||
        k[2] != CLUSTER_KV_TYPE_EVICT) {
        return 1;
    }

    atomic_store(&ctx->cluster->evicted[k[3]], 1);
    return 0;
} /* cluster_load_scan_cb */

static void
cluster_load_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_cluster_load_ctx *ctx     = private_data;
    struct chimera_cluster          *cluster = ctx->cluster;
    int                              local   = cluster->local_node_id;
    int                              n;
    uint8_t                          list[256];

    if (error_code != CHIMERA_VFS_OK) {
        chimera_server_error("cluster: eviction-set load failed: %d", error_code);
    }

    /* This node is alive: clear any stale eviction record for it (rejoin). */
    if (local >= 1 && local <= 255 && atomic_load(&cluster->evicted[local])) {
        chimera_cluster_rejoin(cluster, ctx->vfs_thread, local);
    }

    n = chimera_cluster_evicted_list(cluster, list, sizeof(list));
    if (n) {
        char buf[1024];
        int  off = 0;
        for (int i = 0; i < n && off < (int) sizeof(buf) - 8; i++) {
            off += snprintf(buf + off, sizeof(buf) - off, "%s%d",
                            i ? "," : "", list[i]);
        }
        chimera_server_info("cluster: %d node(s) evicted at load: %s", n, buf);
    }

    atomic_store(&cluster->load_state, CLUSTER_LOAD_READY);
    free(ctx);
} /* cluster_load_complete */

void
chimera_cluster_load(
    struct chimera_cluster    *cluster,
    struct chimera_vfs_thread *vfs_thread)
{
    struct chimera_cluster_load_ctx *ctx;
    int                              expected = CLUSTER_LOAD_IDLE;

    if (!atomic_compare_exchange_strong(&cluster->load_state, &expected,
                                        CLUSTER_LOAD_RUNNING)) {
        return;  /* another thread already kicked the load */
    }

    ctx             = malloc(sizeof(*ctx));
    ctx->cluster    = cluster;
    ctx->vfs_thread = vfs_thread;
    ctx->start[0]   = CLUSTER_KV_MAGIC;
    ctx->start[1]   = CLUSTER_KV_VERSION;
    ctx->start[2]   = CLUSTER_KV_TYPE_EVICT;

    chimera_vfs_search_keys(vfs_thread, ctx->start, CLUSTER_KV_HDR_LEN,
                            NULL, 0, 0,
                            cluster_load_scan_cb, cluster_load_complete, ctx);
} /* chimera_cluster_load */
