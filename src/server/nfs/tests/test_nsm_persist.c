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
#ifdef _WIN32
#include "common/platform.h"
#else  /* ifdef _WIN32 */
#include <unistd.h>
#endif /* ifdef _WIN32 */

#include "nfs_nsm_state.h"
#include "nfs_kv_keys.h"
#include "nfs_nlm_state.h"

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
    uint64_t stamp;

    len = nsm_monitor_value_serialize(buf, sizeof(buf), "192.168.1.50",
                                      1700000000u);
    CHECK(len == 4 + 4 + 1 + 12 + 8);
    CHECK(buf[4] & NSM_MON_FLAG_STAMPED);
    CHECK(nsm_monitor_value_parse(buf, len, addr, sizeof(addr), &stamp) == 0);
    CHECK(strcmp(addr, "192.168.1.50") == 0);
    CHECK(stamp == 1700000000u);

    /* IPv6 literal. */
    len = nsm_monitor_value_serialize(buf, sizeof(buf), "fe80::1", 1700000000u);
    CHECK(nsm_monitor_value_parse(buf, len, addr, sizeof(addr), &stamp) == 0);
    CHECK(strcmp(addr, "fe80::1") == 0);
    CHECK(stamp == 1700000000u);

    /* Overflow guard: a buffer one byte short of the stamp writes nothing. */
    CHECK(nsm_monitor_value_serialize(buf, 4 + 4 + 1 + 8 + 7, "10.0.0.1",
                                      1) == 0);

    /* Bad magic + truncated value are rejected. */
    len     = nsm_monitor_value_serialize(buf, sizeof(buf), "10.0.0.1", 1);
    buf[0] ^= 0xff;
    CHECK(nsm_monitor_value_parse(buf, len, addr, sizeof(addr), &stamp) == -1);
    buf[0] ^= 0xff;
    CHECK(nsm_monitor_value_parse(buf, len - 1, addr, sizeof(addr), &stamp) == -1);
    CHECK(nsm_monitor_value_parse(buf, 8, addr, sizeof(addr), &stamp) == -1);
} /* test_monitor_value_roundtrip */

static void
test_monitor_value_legacy(void)
{
    /* A value written by the previous build: flags 0, no stamp. */
    uint8_t  buf[NSM_MON_VALUE_MAX];
    uint32_t p = 0;
    char     addr[CHIMERA_NSM_ADDR_MAX];
    uint64_t stamp = 99;

    nfs_kv_put_le32(buf, &p, NSM_MON_MAGIC);
    nfs_kv_put_le32(buf, &p, 0);
    buf[p++] = 8;
    memcpy(buf + p, "10.0.0.1", 8);
    p += 8;
    CHECK(nsm_monitor_value_parse(buf, p, addr, sizeof(addr), &stamp) == 0);
    CHECK(strcmp(addr, "10.0.0.1") == 0);
    CHECK(stamp == 0);

    /* STAMPED flag but the stamp bytes are missing => corrupt. */
    buf[4] = NSM_MON_FLAG_STAMPED;
    CHECK(nsm_monitor_value_parse(buf, p, addr, sizeof(addr), &stamp) == -1);
} /* test_monitor_value_legacy */

static void
test_monitor_expired(void)
{
    CHECK(nsm_monitor_expired(1000, 1000 + 86400, 86400) == 0);  /* exactly the bound: fresh */
    CHECK(nsm_monitor_expired(1000, 1000 + 86401, 86400) == 1);
    CHECK(nsm_monitor_expired(0, 5000000, 86400) == 0);           /* legacy: never */
} /* test_monitor_expired */

static void
test_monitor_expired_future_stamp(void)
{
    CHECK(nsm_monitor_expired(2000, 1000, 60) == 0);
} /* test_monitor_expired_future_stamp */

static void
test_keys(void)
{
    uint8_t  buf[CHIMERA_KV_NSM_MONITOR_KEY_MAX];
    uint32_t len;

    /* Every NSM key carries the shared 3-byte band header. */
    len = nfs_kv_nsm_state_key(buf);
    CHECK(len == CHIMERA_KV_NSM_STATE_KEY_LEN);
    CHECK(buf[0] == CHIMERA_KV_MAGIC);
    CHECK(buf[1] == CHIMERA_KV_VERSION);
    CHECK(buf[2] == CHIMERA_KV_TYPE_NSM_STATE);

    /* Monitor keys are NODE-scoped: the 2-byte node id (big-endian) follows
     * the header, ahead of the host name. */
    len = nfs_kv_nsm_monitor_key(buf, 0x1234, (const uint8_t *) "host-a", 6);
    CHECK(len == CHIMERA_KV_PREFIX_LEN + 6);
    CHECK(buf[2] == CHIMERA_KV_TYPE_NSM_MONITOR);
    CHECK(buf[3] == 0x12 && buf[4] == 0x34);
    CHECK(memcmp(buf + CHIMERA_KV_PREFIX_LEN, "host-a", 6) == 0);

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
    struct nsm_monitor       *mon;
    uint32_t                  n;

    nsm_state_init(&st, NULL, 1, NULL, 1, 86400);

    /* First sight => changed; re-set same addr => unchanged (but the stamp
     * is still updated); new addr => changed. */
    CHECK(nsm_monitor_set(&st, "client1", "10.0.0.1", 1) == 1);
    CHECK(nsm_monitor_set(&st, "client1", "10.0.0.1", 5) == 0);
    HASH_FIND_STR(st.monitors, "client1", mon);
    CHECK(mon != NULL);
    CHECK(mon->stamp_s == 5);
    CHECK(nsm_monitor_set(&st, "client1", "10.0.0.2", 6) == 1);
    CHECK(mon->stamp_s == 6);
    CHECK(nsm_monitor_set(&st, "client2", "10.0.0.3", 7) == 1);

    n = nsm_monitors_snapshot(&st, &snap);
    CHECK(n == 2);
    CHECK(snap != NULL);
    free(snap);

    nsm_state_destroy(&st);

    /* Empty table snapshots to nothing. */
    nsm_state_init(&st, NULL, 1, NULL, 1, 86400);
    n = nsm_monitors_snapshot(&st, &snap);
    CHECK(n == 0);
    CHECK(snap == NULL);
    nsm_state_destroy(&st);
} /* test_monitor_table */

static void
build_record(
    uint8_t    *key,
    uint32_t   *klen,
    uint8_t    *val,
    uint32_t   *vlen,
    uint16_t    node,
    const char *host,
    uint64_t    stamp)
{
    *klen = nfs_kv_nsm_monitor_key(key, node, (const uint8_t *) host, strlen(host));
    *vlen = nsm_monitor_value_serialize(val, NSM_MON_VALUE_MAX, "10.0.0.9", stamp);
} /* build_record */

static void
test_scan_classify(void)
{
    uint8_t  key[CHIMERA_KV_NSM_MONITOR_KEY_MAX], val[NSM_MON_VALUE_MAX];
    uint32_t klen, vlen;
    char     host[SM_MAXSTRLEN + 1], addr[CHIMERA_NSM_ADDR_MAX];
    uint64_t stamp;

    build_record(key, &klen, val, &vlen, 5, "client-a", 1000);
    CHECK(nsm_monitor_record_classify(key, klen, val, vlen, 5, 1500, 86400,
                                      host, addr, &stamp) == NSM_RECORD_LOAD);
    CHECK(strcmp(host, "client-a") == 0 && strcmp(addr, "10.0.0.9") == 0 && stamp == 1000);

    /* Another node's record ends the scan. */
    CHECK(nsm_monitor_record_classify(key, klen, val, vlen, 6, 1500, 86400,
                                      host, addr, &stamp) == NSM_RECORD_END);

    /* Older than the bound: purge. */
    CHECK(nsm_monitor_record_classify(key, klen, val, vlen, 5, 1000 + 86401, 86400,
                                      host, addr, &stamp) == NSM_RECORD_EXPIRED);

    /* Empty host / short key: skip. */
    CHECK(nsm_monitor_record_classify(key, CHIMERA_KV_PREFIX_LEN, val, vlen, 5, 1500,
                                      86400, host, addr, &stamp) == NSM_RECORD_SKIP);
} /* test_scan_classify */

static void
test_scan_classify_legacy_skipped(void)
{
    /* A pre-node-prefix key whose host starts with our node bytes, carrying
     * an unstamped value: must be skipped, never loaded or purged. */
    uint8_t  key[CHIMERA_KV_NSM_MONITOR_KEY_MAX], val[NSM_MON_VALUE_MAX];
    uint32_t klen = nfs_kv_type_prefix(key, CHIMERA_KV_TYPE_NSM_MONITOR);
    uint32_t vlen = 0;
    char     host[SM_MAXSTRLEN + 1], addr[CHIMERA_NSM_ADDR_MAX];
    uint64_t stamp;

    memcpy(key + klen, "ABhost", 6);
    klen += 6;
    nfs_kv_put_le32(val, &vlen, NSM_MON_MAGIC);
    nfs_kv_put_le32(val, &vlen, 0);
    val[vlen++] = 8;
    memcpy(val + vlen, "10.0.0.1", 8);
    vlen += 8;
    CHECK(nsm_monitor_record_classify(key, klen, val, vlen, 0x4142, 1500, 86400,
                                      host, addr, &stamp) == NSM_RECORD_SKIP);
} /* test_scan_classify_legacy_skipped */

static void
test_persist_switch_off(void)
{
    struct nsm_state st;

    /* No vfs => kv name "" => persistent by name; the switch alone decides. */
    nsm_state_init(&st, NULL, 1, NULL, 1, 86400);
    CHECK(st.persistence_disabled == 0);
    nsm_state_destroy(&st);

    nsm_state_init(&st, NULL, 0, NULL, 1, 86400);
    CHECK(st.persistence_disabled == 1);
    nsm_state_destroy(&st);
} /* test_persist_switch_off */

static void
test_name_config(void)
{
    struct nsm_state st;
    char             expect[SM_MAXSTRLEN + 1];

    nsm_state_init(&st, NULL, 1, "nas.example.test", 7, 60);
    CHECK(strcmp(st.my_name, "nas.example.test") == 0);
    CHECK(st.node_id == 7);
    CHECK(st.monitor_expiry_s == 60);
    nsm_state_destroy(&st);

    /* Empty => the host name, as before. */
    gethostname(expect, sizeof(expect));
    expect[SM_MAXSTRLEN] = '\0';
    nsm_state_init(&st, NULL, 1, "", 1, 60);
    CHECK(strcmp(st.my_name, expect) == 0);
    nsm_state_destroy(&st);
} /* test_name_config */

static void
test_name_config_truncated(void)
{
    struct nsm_state st;
    char             longname[SM_MAXSTRLEN + 40];

    memset(longname, 'a', sizeof(longname) - 1);
    longname[sizeof(longname) - 1] = '\0';
    nsm_state_init(&st, NULL, 1, longname, 1, 60);
    CHECK(strlen(st.my_name) == SM_MAXSTRLEN);
    nsm_state_destroy(&st);
} /* test_name_config_truncated */

static void
test_renew_due(void)
{
    struct nsm_state          st;
    struct nsm_notify_target *due;
    uint32_t                  n;

    nsm_state_init(&st, NULL, 1, NULL, 1, 100);
    nsm_monitor_set(&st, "fresh", "10.0.0.1", 1000);
    nsm_monitor_set(&st, "stale", "10.0.0.2", 900);
    nsm_monitor_set(&st, "never", "10.0.0.3", 0);

    n = nsm_monitors_renew_due(&st, 1000 + 50, &due);
    CHECK(n == 3);
    free(due);
    n = nsm_monitors_renew_due(&st, 1000 + 49, &due);
    CHECK(n == 2);
    free(due);
    nsm_state_destroy(&st);
} /* test_renew_due */

static void
test_renew_due_rule(void)
{
    CHECK(nsm_monitor_renew_due(0, 12345, 100) == 1);
    CHECK(nsm_monitor_renew_due(1000, 1050, 100) == 1);
    CHECK(nsm_monitor_renew_due(1000, 1049, 100) == 0);
    CHECK(nsm_monitor_renew_due(2000, 1000, 100) == 0);
} /* test_renew_due_rule */

static void
test_grace_settle(void)
{
    struct nlm_state nlm;
    time_t           now;

    memset(&nlm, 0, sizeof(nlm));
    nlm_state_settle_grace(&nlm, 10);          /* not in grace: no-op */
    CHECK(nlm.in_grace == 0 && nlm.grace_end == 0);

    nlm_state_begin_grace(&nlm);
    now = time(NULL);
    CHECK(nlm.grace_end >= now + NLM_GRACE_PERIOD_SECS - 1);
    nlm_state_settle_grace(&nlm, 10);
    CHECK(nlm.in_grace == 1);
    CHECK(nlm.grace_end <= now + 11 && nlm.grace_end >= now + 9);
    nlm_state_settle_grace(&nlm, 200);         /* never pushes the deadline out */
    CHECK(nlm.grace_end <= now + 11);
} /* test_grace_settle */

int
main(void)
{
    test_state_value_roundtrip();
    test_monitor_value_roundtrip();
    test_monitor_value_legacy();
    test_monitor_expired();
    test_monitor_expired_future_stamp();
    test_keys();
    test_state_bump();
    test_monitor_table();
    test_scan_classify();
    test_scan_classify_legacy_skipped();
    test_persist_switch_off();
    test_name_config();
    test_name_config_truncated();
    test_renew_due();
    test_renew_due_rule();
    test_grace_settle();
    printf("test_nsm_persist: all checks passed\n");
    return 0;
} /* main */
