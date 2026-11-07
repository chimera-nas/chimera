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

#include "common/logging.h"
#include "prometheus-c.h"
#include "vfs/vfs.h"
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

/* /dev/urandom fills all 16 bytes and produces non-trivially distinct output
 * across two calls (probabilistic: collision odds ~2^-128). */
static int
test_random_key(void)
{
    uint8_t a[16]    = { 0 };
    uint8_t b[16]    = { 0 };
    uint8_t zero[16] = { 0 };

    nfs_fh_keyring_random_key(a);
    nfs_fh_keyring_random_key(b);

    CHECK(memcmp(a, zero, 16) != 0);
    CHECK(memcmp(b, zero, 16) != 0);
    CHECK(memcmp(a, b, 16) != 0);
    return 0;
} /* test_random_key */

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

/* Two nodes sharing one KV store: the first mints kid=1, the second loads that
 * same key, so a handle minted on one verifies on the other. */
static int
test_load_shares_key_across_nodes(void)
{
    struct prometheus_metrics     *metrics;
    struct chimera_vfs_module_cfg  cfg;
    struct chimera_vfs            *vfs;
    struct nfs_fh_keyring_state    a, b;
    uint8_t                        node_key_a[16], node_key_b[16];
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
    if (test_random_key()) {
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
    printf("test_fh_keyring: all tests passed\n");
    return 0;
} /* main */
