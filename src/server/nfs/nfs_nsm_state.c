// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "common/thread.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>
#ifdef _WIN32
#include "common/platform.h"
#else  /* ifdef _WIN32 */
#include <unistd.h>
#endif /* ifdef _WIN32 */

#include "nfs_nsm_state.h"
#include "nfs_kv_keys.h"
#include "nfs_internal.h"

void
nsm_state_init(
    struct nsm_state   *state,
    struct chimera_vfs *vfs,
    int                 persist,
    const char         *name,
    uint16_t            node_id,
    uint32_t            expiry_s)
{
    const char *kvname = (vfs && vfs->kv_module) ? vfs->kv_module->name : "";

    evpl_mutex_init(&state->mutex, NULL);
    state->monitors             = NULL;
    state->state_number         = 1;  /* odd == up; refined by the cold-start load */
    state->persistence_disabled = (!persist || strcmp(kvname, "memkv") == 0);
    atomic_store(&state->load_state, NSM_LOAD_IDLE);
    state->notify_thread = NULL;
    state->notify_queued = NULL;
    atomic_store(&state->notify_stop, 0);
    atomic_store(&state->next_renew_s, 0);

    state->node_id          = node_id;
    state->monitor_expiry_s = expiry_s;

    if (name && *name) {
        snprintf(state->my_name, sizeof(state->my_name), "%s", name);
    } else if (gethostname(state->my_name, sizeof(state->my_name)) != 0) {
        snprintf(state->my_name, sizeof(state->my_name), "localhost");
    }
    state->my_name[sizeof(state->my_name) - 1] = '\0';

    chimera_nfs_debug("NSM state init: my_name='%s' persistence=%s",
                      state->my_name,
                      !persist ? "disabled (nfs3_nsm_persist=false)" :
                      state->persistence_disabled ? "disabled (kv_module is non-persistent)" :
                      "enabled");
} /* nsm_state_init */

void
nsm_state_destroy(struct nsm_state *state)
{
#ifndef __clang_analyzer__

    /* HASH_DEL trips the analyzer's use-after-free checker; guard it the same
     * way the other NFS hash-table teardowns do. */
    struct nsm_monitor *mon, *tmp;

    HASH_ITER(hh, state->monitors, mon, tmp)
    {
        HASH_DEL(state->monitors, mon);
        free(mon);
    }

#endif /* ifndef __clang_analyzer__ */

    evpl_mutex_destroy(&state->mutex);
} /* nsm_state_destroy */

uint32_t
nsm_state_current(struct nsm_state *state)
{
    uint32_t n;

    evpl_mutex_lock(&state->mutex);
    n = state->state_number;
    evpl_mutex_unlock(&state->mutex);
    return n;
} /* nsm_state_current */

int
nsm_monitor_set(
    struct nsm_state *state,
    const char       *host,
    const char       *addr,
    uint64_t          stamp_s)
{
    struct nsm_monitor *mon;

    HASH_FIND_STR(state->monitors, host, mon);
    if (mon) {
        mon->stamp_s = stamp_s;
        if (strncmp(mon->addr, addr, sizeof(mon->addr)) == 0) {
            return 0;  /* unchanged */
        }
        snprintf(mon->addr, sizeof(mon->addr), "%s", addr);
        return 1;
    }

    mon = calloc(1, sizeof(*mon));
    snprintf(mon->host, sizeof(mon->host), "%s", host);
    snprintf(mon->addr, sizeof(mon->addr), "%s", addr);
    mon->stamp_s = stamp_s;
    HASH_ADD_STR(state->monitors, host, mon);
    return 1;
} /* nsm_monitor_set */

int
nsm_monitor_remove(
    struct nsm_state *state,
    const char       *host)
{
    struct nsm_monitor *mon;

    HASH_FIND_STR(state->monitors, host, mon);
    if (!mon) {
        return 0;
    }
    HASH_DEL(state->monitors, mon);
    free(mon);
    return 1;
} /* nsm_monitor_remove */

uint32_t
nsm_monitors_snapshot(
    struct nsm_state          *state,
    struct nsm_notify_target **out)
{
    struct nsm_monitor       *mon, *tmp;
    struct nsm_notify_target *arr;
    uint32_t                  count = HASH_COUNT(state->monitors);
    uint32_t                  i     = 0;

    *out = NULL;
    if (count == 0) {
        return 0;
    }

    arr = calloc(count, sizeof(*arr));
    HASH_ITER(hh, state->monitors, mon, tmp)
    {
        snprintf(arr[i].host, sizeof(arr[i].host), "%s", mon->host);
        snprintf(arr[i].addr, sizeof(arr[i].addr), "%s", mon->addr);
        i++;
    }
    *out = arr;
    return count;
} /* nsm_monitors_snapshot */

uint32_t
nsm_monitors_renew_due(
    struct nsm_state          *state,
    uint64_t                   now_s,
    struct nsm_notify_target **out)
{
    struct nsm_monitor       *mon, *tmp;
    struct nsm_notify_target *arr;
    uint32_t                  count = HASH_COUNT(state->monitors);
    uint32_t                  i     = 0;

    *out = NULL;
    if (count == 0) {
        return 0;
    }

    arr = calloc(count, sizeof(*arr));
    HASH_ITER(hh, state->monitors, mon, tmp)
    {
        if (!nsm_monitor_renew_due(mon->stamp_s, now_s,
                                   state->monitor_expiry_s)) {
            continue;
        }
        snprintf(arr[i].host, sizeof(arr[i].host), "%s", mon->host);
        snprintf(arr[i].addr, sizeof(arr[i].addr), "%s", mon->addr);
        i++;
    }
    if (i == 0) {
        free(arr);
        return 0;
    }
    *out = arr;
    return i;
} /* nsm_monitors_renew_due */

uint32_t
nsm_state_value_serialize(
    uint8_t *buf,
    uint32_t buf_size,
    uint32_t state_number)
{
    uint32_t p = 0;

    if (buf_size < NSM_STATE_VALUE_LEN) {
        return 0;
    }
    nfs_kv_put_le32(buf, &p, NSM_STATE_MAGIC);
    nfs_kv_put_le32(buf, &p, 0 /* version */);
    nfs_kv_put_le32(buf, &p, state_number);
    return p;
} /* nsm_state_value_serialize */

int
nsm_state_value_parse(
    const uint8_t *buf,
    uint32_t       len,
    uint32_t      *out_state)
{
    if (len < NSM_STATE_VALUE_LEN || nfs_kv_le32(buf) != NSM_STATE_MAGIC) {
        return -1;
    }
    *out_state = nfs_kv_le32(buf + 8);
    return 0;
} /* nsm_state_value_parse */

uint32_t
nsm_monitor_value_serialize(
    uint8_t    *buf,
    uint32_t    buf_size,
    const char *addr,
    uint64_t    stamp_s)
{
    uint32_t p        = 0;
    size_t   addr_len = strlen(addr);

    if (addr_len > CHIMERA_NSM_ADDR_MAX) {
        addr_len = CHIMERA_NSM_ADDR_MAX;
    }
    if (buf_size < 4 + 4 + 1 + addr_len + 8) {
        return 0;
    }
    nfs_kv_put_le32(buf, &p, NSM_MON_MAGIC);
    nfs_kv_put_le32(buf, &p, NSM_MON_FLAG_STAMPED);
    buf[p++] = (uint8_t) addr_len;
    memcpy(buf + p, addr, addr_len);
    p += addr_len;
    nfs_kv_put_le64(buf, &p, stamp_s);
    return p;
} /* nsm_monitor_value_serialize */

int
nsm_monitor_value_parse(
    const uint8_t *buf,
    uint32_t       len,
    char          *addr_out,
    size_t         addr_out_size,
    uint64_t      *stamp_out)
{
    uint32_t flags;
    uint8_t  addr_len;

    if (len < 9 || nfs_kv_le32(buf) != NSM_MON_MAGIC) {
        return -1;
    }
    flags    = nfs_kv_le32(buf + 4);
    addr_len = buf[8];
    if (addr_len > CHIMERA_NSM_ADDR_MAX || (uint32_t) 9 + addr_len > len ||
        addr_len + 1u > addr_out_size) {
        return -1;
    }
    if (flags & NSM_MON_FLAG_STAMPED) {
        if ((uint32_t) 9 + addr_len + 8 > len) {
            return -1;
        }
        *stamp_out = nfs_kv_le64(buf + 9 + addr_len);
    } else {
        *stamp_out = 0;  /* written before stamps existed */
    }
    memcpy(addr_out, buf + 9, addr_len);
    addr_out[addr_len] = '\0';
    return 0;
} /* nsm_monitor_value_parse */

uint64_t
nsm_wall_now_s(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t) ts.tv_sec;
} /* nsm_wall_now_s */

enum nsm_record_verdict
nsm_monitor_record_classify(
    const uint8_t *key,
    uint32_t       key_len,
    const uint8_t *value,
    uint32_t       value_len,
    uint16_t       node_id,
    uint64_t       now_s,
    uint32_t       expiry_s,
    char          *host_out,
    char          *addr_out,
    uint64_t      *stamp_out)
{
    uint8_t prefix[CHIMERA_KV_PREFIX_LEN];
    uint32_t host_len;

    /* Keys arrive in byte order: the first one outside this node's 5-byte
     * [magic,version,type,node] prefix means the band is exhausted. */
    nfs_kv_node_prefix(prefix, CHIMERA_KV_TYPE_NSM_MONITOR, node_id);
    if (key_len < CHIMERA_KV_PREFIX_LEN ||
        memcmp(key, prefix, CHIMERA_KV_PREFIX_LEN) != 0) {
        return NSM_RECORD_END;
    }

    host_len = key_len - CHIMERA_KV_PREFIX_LEN;
    if (host_len == 0 || host_len > SM_MAXSTRLEN) {
        return NSM_RECORD_SKIP;  /* malformed key */
    }

    if (nsm_monitor_value_parse(value, value_len, addr_out,
                                CHIMERA_NSM_ADDR_MAX, stamp_out) != 0) {
        return NSM_RECORD_SKIP;  /* corrupt value */
    }

    /* An unstamped value predates node-scoped keys: whichever node wrote it,
     * this key only looks like ours because the host name happens to begin
     * with our node id bytes.  Leave it alone. */
    if (*stamp_out == 0) {
        return NSM_RECORD_SKIP;
    }

    memcpy(host_out, key + CHIMERA_KV_PREFIX_LEN, host_len);
    host_out[host_len] = '\0';

    if (nsm_monitor_expired(*stamp_out, now_s, expiry_s)) {
        return NSM_RECORD_EXPIRED;
    }
    return NSM_RECORD_LOAD;
} /* nsm_monitor_record_classify */
