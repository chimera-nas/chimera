// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "nfs_nsm_state.h"
#include "nfs_kv_keys.h"
#include "nfs_internal.h"

void
nsm_state_init(
    struct nsm_state   *state,
    struct chimera_vfs *vfs)
{
    const char *kvname = (vfs && vfs->kv_module) ? vfs->kv_module->name : "";

    pthread_mutex_init(&state->mutex, NULL);
    state->monitors             = NULL;
    state->state_number         = 1;  /* odd == up; refined by the cold-start load */
    state->persistence_disabled = (strcmp(kvname, "memkv") == 0);
    atomic_store(&state->load_state, NSM_LOAD_IDLE);
    state->notify_thread = NULL;

    if (gethostname(state->my_name, sizeof(state->my_name)) != 0) {
        snprintf(state->my_name, sizeof(state->my_name), "localhost");
    }
    state->my_name[sizeof(state->my_name) - 1] = '\0';

    chimera_nfs_debug("NSM state init: my_name='%s' persistence=%s",
                      state->my_name,
                      state->persistence_disabled ? "disabled" : "enabled");
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

    pthread_mutex_destroy(&state->mutex);
} /* nsm_state_destroy */

uint32_t
nsm_state_current(struct nsm_state *state)
{
    uint32_t n;

    pthread_mutex_lock(&state->mutex);
    n = state->state_number;
    pthread_mutex_unlock(&state->mutex);
    return n;
} /* nsm_state_current */

int
nsm_monitor_set(
    struct nsm_state *state,
    const char       *host,
    const char       *addr)
{
    struct nsm_monitor *mon;

    HASH_FIND_STR(state->monitors, host, mon);
    if (mon) {
        if (strncmp(mon->addr, addr, sizeof(mon->addr)) == 0) {
            return 0;  /* unchanged */
        }
        snprintf(mon->addr, sizeof(mon->addr), "%s", addr);
        return 1;
    }

    mon = calloc(1, sizeof(*mon));
    snprintf(mon->host, sizeof(mon->host), "%s", host);
    snprintf(mon->addr, sizeof(mon->addr), "%s", addr);
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
    const char *addr)
{
    uint32_t p        = 0;
    size_t   addr_len = strlen(addr);

    if (addr_len > CHIMERA_NSM_ADDR_MAX) {
        addr_len = CHIMERA_NSM_ADDR_MAX;
    }
    if (buf_size < 4 + 4 + 1 + addr_len) {
        return 0;
    }
    nfs_kv_put_le32(buf, &p, NSM_MON_MAGIC);
    nfs_kv_put_le32(buf, &p, 0 /* flags */);
    buf[p++] = (uint8_t) addr_len;
    memcpy(buf + p, addr, addr_len);
    return p + addr_len;
} /* nsm_monitor_value_serialize */

int
nsm_monitor_value_parse(
    const uint8_t *buf,
    uint32_t       len,
    char          *addr_out,
    size_t         addr_out_size)
{
    uint8_t addr_len;

    if (len < 9 || nfs_kv_le32(buf) != NSM_MON_MAGIC) {
        return -1;
    }
    addr_len = buf[8];
    if (addr_len > CHIMERA_NSM_ADDR_MAX || (uint32_t) 9 + addr_len > len ||
        addr_len + 1u > addr_out_size) {
        return -1;
    }
    memcpy(addr_out, buf + 9, addr_len);
    addr_out[addr_len] = '\0';
    return 0;
} /* nsm_monitor_value_parse */
