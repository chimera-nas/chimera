// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * NSM/statd persistence + monitor-table unit tests.
 *
 * Validates the on-stable-storage key/value formats (nfs_kv_keys.h NSM record
 * types + nfs_nsm_state.c (de)serializers), the odd-state-number bump
 * convention, and the in-memory monitor table -- all WITHOUT a live VFS or
 * event loop.  nfs_nsm_state.c is compiled straight into this test because its
 * helpers are not SYMBOL_EXPORT'd.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "nfs_nsm_state.h"
#include "nfs_kv_keys.h"

#define CHECK(cond) do { \
            if (!(cond)) { \
                fprintf(stderr, "%s:%d: CHECK failed: %s\n", \
                        __FILE__, __LINE__, #cond); \
                abort(); \
            } \
} while (0)

static void
test_state_value_roundtrip(void)
{
    uint8_t  buf[NSM_STATE_VALUE_LEN];
    uint32_t out;

    CHECK(nsm_state_value_serialize(buf, sizeof(buf), 0x01020304u) ==
          NSM_STATE_VALUE_LEN);
    CHECK(nsm_state_value_parse(buf, NSM_STATE_VALUE_LEN, &out) == 0);
    CHECK(out == 0x01020304u);

    /* Overflow guard: too-small buffer writes nothing. */
    CHECK(nsm_state_value_serialize(buf, NSM_STATE_VALUE_LEN - 1, 7) == 0);

    /* Bad magic + short value are rejected. */
    buf[0] ^= 0xff;
    CHECK(nsm_state_value_parse(buf, NSM_STATE_VALUE_LEN, &out) == -1);
    buf[0] ^= 0xff;
    CHECK(nsm_state_value_parse(buf, NSM_STATE_VALUE_LEN - 1, &out) == -1);
} /* test_state_value_roundtrip */

static void
test_monitor_value_roundtrip(void)
{
    uint8_t  buf[NSM_MON_VALUE_MAX];
    char     addr[CHIMERA_NSM_ADDR_MAX];
    uint32_t len;

    len = nsm_monitor_value_serialize(buf, sizeof(buf), "192.168.1.50");
    CHECK(len == 4 + 4 + 1 + 12);
    CHECK(nsm_monitor_value_parse(buf, len, addr, sizeof(addr)) == 0);
    CHECK(strcmp(addr, "192.168.1.50") == 0);

    /* IPv6 literal. */
    len = nsm_monitor_value_serialize(buf, sizeof(buf), "fe80::1");
    CHECK(nsm_monitor_value_parse(buf, len, addr, sizeof(addr)) == 0);
    CHECK(strcmp(addr, "fe80::1") == 0);

    /* Bad magic + truncated value are rejected. */
    len     = nsm_monitor_value_serialize(buf, sizeof(buf), "10.0.0.1");
    buf[0] ^= 0xff;
    CHECK(nsm_monitor_value_parse(buf, len, addr, sizeof(addr)) == -1);
    buf[0] ^= 0xff;
    CHECK(nsm_monitor_value_parse(buf, len - 1, addr, sizeof(addr)) == -1);
    CHECK(nsm_monitor_value_parse(buf, 8, addr, sizeof(addr)) == -1);
} /* test_monitor_value_roundtrip */

static void
test_keys(void)
{
    uint8_t  buf[CHIMERA_KV_HDR_LEN + SM_MAXSTRLEN];
    uint32_t len;

    /* Every NSM key carries the shared 3-byte band header. */
    len = nfs_kv_nsm_state_key(buf);
    CHECK(len == CHIMERA_KV_NSM_STATE_KEY_LEN);
    CHECK(buf[0] == CHIMERA_KV_MAGIC);
    CHECK(buf[1] == CHIMERA_KV_VERSION);
    CHECK(buf[2] == CHIMERA_KV_TYPE_NSM_STATE);

    len = nfs_kv_nsm_monitor_key(buf, (const uint8_t *) "host-a", 6);
    CHECK(len == CHIMERA_KV_HDR_LEN + 6);
    CHECK(buf[2] == CHIMERA_KV_TYPE_NSM_MONITOR);
    CHECK(memcmp(buf + CHIMERA_KV_HDR_LEN, "host-a", 6) == 0);

    /* The two record types occupy distinct, adjacent bands so a search over
     * [type .. type+1) scans exactly one type. */
    CHECK(CHIMERA_KV_TYPE_NSM_MONITOR == CHIMERA_KV_TYPE_NSM_STATE + 1);
} /* test_keys */

static void
test_state_bump(void)
{
    /* Always lands on the smallest odd value strictly greater than prev. */
    CHECK(nsm_next_state(0) == 1);
    CHECK(nsm_next_state(1) == 3);
    CHECK(nsm_next_state(2) == 3);
    CHECK(nsm_next_state(3) == 5);
    CHECK(nsm_next_state(100) == 101);
    CHECK(nsm_next_state(101) == 103);
} /* test_state_bump */

static void
test_monitor_table(void)
{
    struct nsm_state          st;
    struct nsm_notify_target *snap;
    uint32_t                  n;

    nsm_state_init(&st, NULL);

    /* First sight => changed; re-set same addr => unchanged; new addr =>
     * changed. */
    CHECK(nsm_monitor_set(&st, "client1", "10.0.0.1") == 1);
    CHECK(nsm_monitor_set(&st, "client1", "10.0.0.1") == 0);
    CHECK(nsm_monitor_set(&st, "client1", "10.0.0.2") == 1);
    CHECK(nsm_monitor_set(&st, "client2", "10.0.0.3") == 1);

    n = nsm_monitors_snapshot(&st, &snap);
    CHECK(n == 2);
    CHECK(snap != NULL);
    free(snap);

    nsm_state_destroy(&st);

    /* Empty table snapshots to nothing. */
    nsm_state_init(&st, NULL);
    n = nsm_monitors_snapshot(&st, &snap);
    CHECK(n == 0);
    CHECK(snap == NULL);
    nsm_state_destroy(&st);
} /* test_monitor_table */

int
main(void)
{
    test_state_value_roundtrip();
    test_monitor_value_roundtrip();
    test_keys();
    test_state_bump();
    test_monitor_table();
    printf("test_nsm_persist: all checks passed\n");
    return 0;
} /* main */
