// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Unit tests for the fleet-wide FH signing keyring.
 *
 * Covers wire (de)serialization of key-slot records that get persisted under
 * KV type CHIMERA_KV_TYPE_NFS_FH_SIGNING, and the ring built when the KV store
 * does not survive a restart.  The KV-backed load is covered end to end.
 */

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#ifndef _WIN32
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#endif /* ifndef _WIN32 */

#include "common/logging.h"
#include "prometheus-c.h"
#include "evpl/evpl.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "nfs_fh_keyring.h"
#include "nfs_kv_keys.h"

#define CHECK(cond)                                                        \
        do {                                                               \
            if (!(cond)) {                                                 \
                fprintf(stderr,                                            \
                        "test_fh_keyring: FAILED at %s:%d: %s\n",          \
                        __FILE__, __LINE__, # cond);                       \
                return 1;                                                  \
            }                                                              \
        } while (0)

/* Serialize -> deserialize round-trips preserve every field, including a key
 * with byte 0 = 0x00 (to catch stray strlen-style bugs). */
static int
test_serialize_roundtrip(void)
{
    uint8_t                buf[128];
    struct nfs_fh_key_slot in;
    struct nfs_fh_key_slot out;
    uint32_t               written;

    memset(&in, 0, sizeof(in));
    in.kid        = 7;
    in.state      = NFS_FH_KEY_ACTIVE;
    in.created_ns = 0x1122334455667788ULL;
    /* Key material with a NUL in position 0 and a non-ASCII tail. */
    memcpy(in.key,
           "\x00\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff",
           16);

    written = nfs_fh_keyring_serialize(buf, sizeof(buf), &in);
    CHECK(written > 0);
    CHECK(written < sizeof(buf));

    memset(&out, 0, sizeof(out));
    CHECK(nfs_fh_keyring_deserialize(buf, written, &out) == 0);
    CHECK(out.kid == in.kid);
    CHECK(out.state == in.state);
    CHECK(out.created_ns == in.created_ns);
    CHECK(memcmp(out.key, in.key, 16) == 0);

    /* RETIRED round-trips too (fleet-interop verify path relies on this). */
    in.state = NFS_FH_KEY_RETIRED;
    written  = nfs_fh_keyring_serialize(buf, sizeof(buf), &in);
    CHECK(written > 0);
    CHECK(nfs_fh_keyring_deserialize(buf, written, &out) == 0);
    CHECK(out.state == NFS_FH_KEY_RETIRED);
    return 0;
} /* test_serialize_roundtrip */

/* The key band is fleet-scoped: keys must be a header + kid, with NO node_id.
 * A byte-order check confirms the record type nail. */
static int
test_key_encoding(void)
{
    uint8_t  buf[16];
    uint32_t len;

    len = nfs_kv_fh_signing_key(buf, 3);
    CHECK(len == CHIMERA_KV_FH_SIGNING_KEY_LEN);
    /* [hdr(3)] + kid(1); no node_id, the ring is fleet-wide. */
    CHECK(len == CHIMERA_KV_HDR_LEN + 1);
    CHECK(buf[0] == CHIMERA_KV_MAGIC);
    CHECK(buf[1] == CHIMERA_KV_VERSION);
    CHECK(buf[2] == CHIMERA_KV_TYPE_NFS_FH_SIGNING);
    CHECK(buf[3] == 3);
    return 0;
} /* test_key_encoding */

/* Refuse to persist an invalid slot: UNUSED state, sentinel kid, or too-small
 * output buffer.  Silently dropping a bogus slot is safer than emitting one
 * that later panics the loader. */
static int
test_serialize_rejects_invalid(void)
{
    uint8_t                buf[128];
    uint8_t                tiny[4];
    struct nfs_fh_key_slot in;

    memset(&in, 0, sizeof(in));
    in.kid   = 1;
    in.state = NFS_FH_KEY_UNUSED;        /* bogus for a persisted record */
    memset(in.key, 0xAB, 16);
    CHECK(nfs_fh_keyring_serialize(buf, sizeof(buf), &in) == 0);

    in.state = NFS_FH_KEY_ACTIVE;
    in.kid   = 0;                        /* reserved sentinel */
    CHECK(nfs_fh_keyring_serialize(buf, sizeof(buf), &in) == 0);

    in.kid = 1;
    CHECK(nfs_fh_keyring_serialize(tiny, sizeof(tiny), &in) == 0);
    return 0;
} /* test_serialize_rejects_invalid */

/* Corrupt every rejection path: truncated header, wrong magic, unknown state,
 * kid=0 sentinel on disk, unsupported key_len. */
static int
test_deserialize_rejects_corrupt(void)
{
    uint8_t                buf[128];
    struct nfs_fh_key_slot in;
    struct nfs_fh_key_slot out;
    uint32_t               written;

    memset(&in, 0, sizeof(in));
    in.kid        = 2;
    in.state      = NFS_FH_KEY_ACTIVE;
    in.created_ns = 42;
    memset(in.key, 0xC5, 16);

    written = nfs_fh_keyring_serialize(buf, sizeof(buf), &in);
    CHECK(written > 0);

    /* Truncate below the header. */
    CHECK(nfs_fh_keyring_deserialize(buf, 4, &out) == -1);

    /* Truncate mid-key. */
    CHECK(nfs_fh_keyring_deserialize(buf, written - 5, &out) == -1);

    /* Flip the magic. */
    buf[0] ^= 0xFF;
    CHECK(nfs_fh_keyring_deserialize(buf, written, &out) == -1);
    buf[0] ^= 0xFF;

    /* Corrupt state byte to an unknown value (offset 4 = magic(4)+state(1)). */
    buf[4] = 0x77;
    CHECK(nfs_fh_keyring_deserialize(buf, written, &out) == -1);
    buf[4] = NFS_FH_KEY_ACTIVE;

    /* kid = 0 sentinel on disk must be refused. */
    buf[5] = 0x00;
    CHECK(nfs_fh_keyring_deserialize(buf, written, &out) == -1);
    buf[5] = 2;

    /* Unsupported key_len (32 -- future format bump; not yet valid). */
    buf[6] = 32;
    CHECK(nfs_fh_keyring_deserialize(buf, written, &out) == -1);
    buf[6] = 16;

    /* Sanity: reverting all fields still parses cleanly. */
    CHECK(nfs_fh_keyring_deserialize(buf, written, &out) == 0);
    CHECK(out.kid == 2);
    return 0;
} /* test_deserialize_rejects_corrupt */

static void
ring_from_node_key(
    const uint8_t          node_key[16],
    struct nfs_fh_keyring *ring)
{
    memset(ring, 0, sizeof(*ring));
    ring->signing_kid    = 1;
    ring->slots[0].kid   = 1;
    ring->slots[0].state = NFS_FH_KEY_ACTIVE;
    nfs_fh_keyring_derive_key(node_key, ring->slots[0].key);
} /* ring_from_node_key */

/* On a non-persistent KV the ring is rebuilt from the node key at every start,
 * so a V2 handle issued before a restart must still verify after it. */
static int
test_derived_key_survives_restart(void)
{
    uint8_t                        node_key[16], other_key[16];
    uint8_t                        a[16], b[16], c[16];
    struct nfs_fh_keyring          before, after, other;
    static const uint8_t           vfs_fh[20] = { 0xde, 0xad, 0xbe, 0xef, 0x01 };
    uint8_t                        wire[CHIMERA_NFS_FH_MAX];
    uint8_t                        out[CHIMERA_VFS_FH_SIZE];
    int                            wirelen, outlen;
    uint16_t                       export_id;
    enum chimera_nfs_fh_authn_kind authn;

    memset(node_key, 0x5A, sizeof(node_key));
    memset(other_key, 0xA5, sizeof(other_key));

    nfs_fh_keyring_derive_key(node_key, a);
    nfs_fh_keyring_derive_key(node_key, b);
    nfs_fh_keyring_derive_key(other_key, c);
    CHECK(memcmp(a, b, 16) == 0);
    CHECK(memcmp(a, c, 16) != 0);
    CHECK(memcmp(a, node_key, 16) != 0);

    ring_from_node_key(node_key, &before);
    CHECK(chimera_nfs_fh_wrap_v2(wire, &wirelen, 7, vfs_fh, sizeof(vfs_fh),
                                 &before) == CHIMERA_NFS_FH_OK);

    ring_from_node_key(node_key, &after);
    CHECK(chimera_nfs_fh_unwrap_ring(wire, wirelen, &export_id, out, &outlen,
                                     &after, NULL, 1, &authn) == CHIMERA_NFS_FH_OK);
    CHECK(export_id == 7);
    CHECK(outlen == (int) sizeof(vfs_fh));
    CHECK(memcmp(out, vfs_fh, sizeof(vfs_fh)) == 0);
    CHECK(authn == CHIMERA_NFS_FH_AUTHN_V2);

    ring_from_node_key(other_key, &other);
    CHECK(chimera_nfs_fh_unwrap_ring(wire, wirelen, &export_id, out, &outlen,
                                     &other, NULL, 1, &authn) == CHIMERA_NFS_FH_BADHANDLE);
    return 0;
} /* test_derived_key_survives_restart */

/* Without a persistent KV store the load needs no KV access: it derives kid=1
 * from the node key and is READY at once, signing with it. */
static int
test_load_without_persistence(void)
{
    struct nfs_fh_keyring_state kr;
    uint8_t                     node_key[16], expect[16];

    memset(node_key, 0x3C, sizeof(node_key));
    nfs_fh_keyring_derive_key(node_key, expect);

    nfs_fh_keyring_init(&kr, 4, true);
    CHECK(!nfs_fh_keyring_ready(&kr));
    nfs_fh_keyring_load(&kr, NULL, node_key);
    CHECK(nfs_fh_keyring_ready(&kr));
    CHECK(kr.ring.signing_kid == 1);
    CHECK(memcmp(nfs_fh_keyring_find(&kr.ring, 1), expect, 16) == 0);
    nfs_fh_keyring_free(&kr);
    return 0;
} /* test_load_without_persistence */

static struct chimera_vfs *
memkv_vfs_create(struct prometheus_metrics *metrics)
{
    struct chimera_vfs_module_cfg cfg;

    memset(&cfg, 0, sizeof(cfg));
    strncpy(cfg.module_name, "memkv", sizeof(cfg.module_name) - 1);
    return chimera_vfs_init(1, 0, &cfg, 1, "memkv", 60, 1, 1, 0, metrics);
} /* memkv_vfs_create */

/* Load a ring through the KV path (persistence_disabled=false, even on
 * memkv) and wrap a handle with it. */
static int
load_and_wrap(
    struct nfs_fh_keyring_state *kr,
    uint16_t                     node_id,
    struct chimera_vfs          *vfs,
    const uint8_t                node_key[16],
    uint8_t                     *wire,
    int                         *wirelen)
{
    static const uint8_t vfs_fh[12] = { 0x44, 0x55, 0x66 };

    nfs_fh_keyring_init(kr, node_id, false);
    nfs_fh_keyring_load(kr, vfs, node_key);
    CHECK(nfs_fh_keyring_ready(kr));
    CHECK(kr->ring.signing_kid == 1);
    CHECK(chimera_nfs_fh_wrap_v2(wire, wirelen, 5, vfs_fh, sizeof(vfs_fh),
                                 &kr->ring) == CHIMERA_NFS_FH_OK);
    return 0;
} /* load_and_wrap */

static bool
verifies(
    const uint8_t               *wire,
    int                          wirelen,
    const struct nfs_fh_keyring *ring)
{
    uint8_t                        out[CHIMERA_VFS_FH_SIZE];
    int                            outlen;
    uint16_t                       export_id;
    enum chimera_nfs_fh_authn_kind authn;

    return chimera_nfs_fh_unwrap_ring(wire, wirelen, &export_id, out, &outlen,
                                      ring, NULL, 1, &authn) == CHIMERA_NFS_FH_OK;
} /* verifies */

/* Two nodes with their own KV stores and the same configured nfs_fh_key each
 * mint kid=1, and both derive the same key, so a handle minted on one
 * verifies on the other after a failover.  With different node keys they
 * do not, as without the keyring. */
static int
test_load_separate_stores_same_node_key(void)
{
    struct prometheus_metrics  *metrics;
    struct chimera_vfs         *vfs_a, *vfs_b, *vfs_c;
    struct nfs_fh_keyring_state a, b, c;
    uint8_t                     node_key[16], other_key[16], expect[16];
    uint8_t                     wire_a[CHIMERA_NFS_FH_MAX];
    uint8_t                     wire_b[CHIMERA_NFS_FH_MAX];
    uint8_t                     wire_c[CHIMERA_NFS_FH_MAX];
    int                         len_a, len_b, len_c;

    metrics = prometheus_metrics_create(NULL, NULL, 0);
    vfs_a   = memkv_vfs_create(metrics);
    vfs_b   = memkv_vfs_create(metrics);
    vfs_c   = memkv_vfs_create(metrics);
    CHECK(vfs_a != NULL && vfs_b != NULL && vfs_c != NULL);

    memset(node_key, 0x6E, sizeof(node_key));
    memset(other_key, 0x6F, sizeof(other_key));
    nfs_fh_keyring_derive_key(node_key, expect);

    CHECK(load_and_wrap(&a, 1, vfs_a, node_key, wire_a, &len_a) == 0);
    CHECK(load_and_wrap(&b, 2, vfs_b, node_key, wire_b, &len_b) == 0);
    CHECK(load_and_wrap(&c, 3, vfs_c, other_key, wire_c, &len_c) == 0);

    CHECK(memcmp(nfs_fh_keyring_find(&a.ring, 1), expect, 16) == 0);
    CHECK(memcmp(nfs_fh_keyring_find(&b.ring, 1), expect, 16) == 0);
    CHECK(verifies(wire_a, len_a, &b.ring));
    CHECK(verifies(wire_b, len_b, &a.ring));
    CHECK(!verifies(wire_a, len_a, &c.ring));
    CHECK(!verifies(wire_c, len_c, &a.ring));

    nfs_fh_keyring_free(&a);
    nfs_fh_keyring_free(&b);
    nfs_fh_keyring_free(&c);
    chimera_vfs_destroy(vfs_a);
    chimera_vfs_destroy(vfs_b);
    chimera_vfs_destroy(vfs_c);
    prometheus_metrics_destroy(metrics);
    return 0;
} /* test_load_separate_stores_same_node_key */

/* A KV store that is wiped or recreated while the node key survives mints
 * kid=1 again with the same key, so outstanding handles stay valid. */
static int
test_load_wiped_store_rederives(void)
{
    struct prometheus_metrics  *metrics;
    struct chimera_vfs         *vfs;
    struct nfs_fh_keyring_state before, after;
    uint8_t                     node_key[16];
    uint8_t                     wire[CHIMERA_NFS_FH_MAX];
    uint8_t                     wire_after[CHIMERA_NFS_FH_MAX];
    int                         len, len_after;

    metrics = prometheus_metrics_create(NULL, NULL, 0);
    memset(node_key, 0x7D, sizeof(node_key));

    vfs = memkv_vfs_create(metrics);
    CHECK(vfs != NULL);
    CHECK(load_and_wrap(&before, 1, vfs, node_key, wire, &len) == 0);
    chimera_vfs_destroy(vfs);

    vfs = memkv_vfs_create(metrics);
    CHECK(vfs != NULL);
    CHECK(load_and_wrap(&after, 1, vfs, node_key, wire_after, &len_after) == 0);
    CHECK(memcmp(nfs_fh_keyring_find(&before.ring, 1),
                 nfs_fh_keyring_find(&after.ring, 1), 16) == 0);
    CHECK(verifies(wire, len, &after.ring));

    nfs_fh_keyring_free(&before);
    nfs_fh_keyring_free(&after);
    chimera_vfs_destroy(vfs);
    prometheus_metrics_destroy(metrics);
    return 0;
} /* test_load_wiped_store_rederives */

struct put_wait {
    bool done;
    int  error;
};

static void
put_done(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct put_wait *w = private_data;

    w->error = error_code;
    w->done  = true;
} /* put_done */

/* Store `value` under the ring key for `kid`. */
static int
put_ring_record(
    struct chimera_vfs *vfs,
    uint8_t             kid,
    const void         *value,
    uint32_t            value_len)
{
    struct evpl               *evpl       = evpl_create(NULL);
    struct chimera_vfs_thread *vfs_thread = chimera_vfs_thread_init(evpl, vfs);
    struct put_wait            w          = { 0 };
    uint8_t                    key[CHIMERA_KV_FH_SIGNING_KEY_LEN];
    uint32_t                   key_len = nfs_kv_fh_signing_key(key, kid);

    chimera_vfs_put_key(vfs_thread, key, key_len, value, value_len, put_done, &w);
    while (!w.done) {
        evpl_continue(evpl);
    }
    chimera_vfs_thread_destroy(vfs_thread);
    evpl_destroy(evpl);
    CHECK(w.error == CHIMERA_VFS_OK);
    return 0;
} /* put_ring_record */

/* A record whose kid does not match its key is skipped, not trusted. */
static int
test_load_skips_mismatched_kid(void)
{
    struct prometheus_metrics  *metrics;
    struct chimera_vfs         *vfs;
    struct nfs_fh_keyring_state kr;
    struct nfs_fh_key_slot      slot;
    uint8_t                     node_key[16], expect[16], value[128];
    uint32_t                    value_len;

    metrics = prometheus_metrics_create(NULL, NULL, 0);
    vfs     = memkv_vfs_create(metrics);
    CHECK(vfs != NULL);

    memset(&slot, 0, sizeof(slot));
    slot.kid   = 2;
    slot.state = NFS_FH_KEY_ACTIVE;
    memset(slot.key, 0x99, sizeof(slot.key));
    value_len = nfs_fh_keyring_serialize(value, sizeof(value), &slot);
    CHECK(value_len > 0);
    CHECK(put_ring_record(vfs, 3, value, value_len) == 0);

    /* A kid=1 record is readable, so the ring is that alone. */
    memset(node_key, 0x4B, sizeof(node_key));
    nfs_fh_keyring_derive_key(node_key, slot.key);
    memcpy(expect, slot.key, sizeof(expect));
    slot.kid  = 1;
    value_len = nfs_fh_keyring_serialize(value, sizeof(value), &slot);
    CHECK(put_ring_record(vfs, 1, value, value_len) == 0);

    nfs_fh_keyring_init(&kr, 1, false);
    nfs_fh_keyring_load(&kr, vfs, node_key);
    CHECK(nfs_fh_keyring_ready(&kr));
    CHECK(kr.ring.signing_kid == 1);
    CHECK(nfs_fh_keyring_find(&kr.ring, 2) == NULL);
    CHECK(nfs_fh_keyring_find(&kr.ring, 3) == NULL);
    CHECK(memcmp(nfs_fh_keyring_find(&kr.ring, 1), expect, 16) == 0);

    nfs_fh_keyring_free(&kr);
    chimera_vfs_destroy(vfs);
    prometheus_metrics_destroy(metrics);
    return 0;
} /* test_load_skips_mismatched_kid */

#ifndef _WIN32
/* The ring holds records but none can be used: the load must abort rather
 * than mint a kid=1 that peers may not sign with.  Runs in a child, which
 * builds its own VFS so no threads cross the fork. */
static int
load_aborts(uint8_t kid)
{
    pid_t pid;
    int   status;

    fflush(NULL);
    pid = fork();
    CHECK(pid >= 0);
    if (pid == 0) {
        struct prometheus_metrics  *metrics = prometheus_metrics_create(NULL, NULL, 0);
        struct chimera_vfs         *vfs     = memkv_vfs_create(metrics);
        struct nfs_fh_keyring_state kr;
        uint8_t                     node_key[16];

        if (!vfs || put_ring_record(vfs, kid, "not-a-key-record", 16) != 0) {
            _exit(2);
        }
        memset(node_key, 0x21, sizeof(node_key));
        nfs_fh_keyring_init(&kr, 1, false);
        nfs_fh_keyring_load(&kr, vfs, node_key);
        _exit(0);
    }
    CHECK(waitpid(pid, &status, 0) == pid);
    CHECK(WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT);
    return 0;
} /* load_aborts */

static int
test_load_refuses_unreadable_ring(void)
{
    CHECK(load_aborts(1) == 0);
    CHECK(load_aborts(2) == 0);
    return 0;
} /* test_load_refuses_unreadable_ring */
#endif /* ifndef _WIN32 */

/* Two nodes sharing one KV store: the first mints kid=1, the second loads that
 * same key, so a handle minted on one verifies on the other. */
static int
test_load_shares_key_across_nodes(void)
{
    struct prometheus_metrics     *metrics;
    struct chimera_vfs_module_cfg  cfg;
    struct chimera_vfs            *vfs;
    struct nfs_fh_keyring_state    a, b;
    uint8_t                        node_key_a[16], node_key_b[16], expect[16];
    static const uint8_t           vfs_fh[12] = { 0x11, 0x22, 0x33 };
    uint8_t                        wire[CHIMERA_NFS_FH_MAX];
    uint8_t                        out[CHIMERA_VFS_FH_SIZE];
    int                            wirelen, outlen;
    uint16_t                       export_id;
    enum chimera_nfs_fh_authn_kind authn;

    metrics = prometheus_metrics_create(NULL, NULL, 0);
    memset(&cfg, 0, sizeof(cfg));
    strncpy(cfg.module_name, "memkv", sizeof(cfg.module_name) - 1);
    vfs = chimera_vfs_init(1, 0, &cfg, 1, "memkv", 60, 1, 1, 0, metrics);
    CHECK(vfs != NULL);

    memset(node_key_a, 0x01, sizeof(node_key_a));
    memset(node_key_b, 0x02, sizeof(node_key_b));

    /* persistence_disabled=false forces the KV path even on memkv. */
    nfs_fh_keyring_init(&a, 1, false);
    nfs_fh_keyring_load(&a, vfs, node_key_a);
    CHECK(nfs_fh_keyring_ready(&a));

    nfs_fh_keyring_init(&b, 2, false);
    nfs_fh_keyring_load(&b, vfs, node_key_b);
    CHECK(nfs_fh_keyring_ready(&b));

    CHECK(a.ring.signing_kid == 1 && b.ring.signing_kid == 1);
    CHECK(memcmp(nfs_fh_keyring_find(&a.ring, 1),
                 nfs_fh_keyring_find(&b.ring, 1), 16) == 0);
    /* The first node's candidate won, and the second adopted it. */
    nfs_fh_keyring_derive_key(node_key_a, expect);
    CHECK(memcmp(nfs_fh_keyring_find(&a.ring, 1), expect, 16) == 0);

    CHECK(chimera_nfs_fh_wrap_v2(wire, &wirelen, 9, vfs_fh, sizeof(vfs_fh),
                                 &a.ring) == CHIMERA_NFS_FH_OK);
    CHECK(chimera_nfs_fh_unwrap_ring(wire, wirelen, &export_id, out, &outlen,
                                     &b.ring, NULL, 1, &authn) == CHIMERA_NFS_FH_OK);
    CHECK(export_id == 9 && authn == CHIMERA_NFS_FH_AUTHN_V2);

    nfs_fh_keyring_free(&a);
    nfs_fh_keyring_free(&b);
    chimera_vfs_destroy(vfs);
    prometheus_metrics_destroy(metrics);
    return 0;
} /* test_load_shares_key_across_nodes */

int
main(void)
{
    chimera_log_init();

    if (test_serialize_roundtrip()) {
        return 1;
    }
    if (test_key_encoding()) {
        return 1;
    }
    if (test_serialize_rejects_invalid()) {
        return 1;
    }
    if (test_deserialize_rejects_corrupt()) {
        return 1;
    }
    if (test_derived_key_survives_restart()) {
        return 1;
    }
    if (test_load_without_persistence()) {
        return 1;
    }
    if (test_load_shares_key_across_nodes()) {
        return 1;
    }
    if (test_load_separate_stores_same_node_key()) {
        return 1;
    }
    if (test_load_wiped_store_rederives()) {
        return 1;
    }
    if (test_load_skips_mismatched_kid()) {
        return 1;
    }
#ifndef _WIN32
    if (test_load_refuses_unreadable_ring()) {
        return 1;
    }
#endif /* ifndef _WIN32 */
    printf("test_fh_keyring: all tests passed\n");
    return 0;
} /* main */
