// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Unit tests for the secure NFS wire file-handle wrapping (nfs_fh_wrap.h) and
 * the per-export credential squashing logic (nfs_common.h).  These are the
 * load-bearing security primitives for per-export attribution: a wire handle
 * must round-trip, a tampered/forged handle must be rejected, and squashing
 * must map identities exactly as configured.
 */

#include <stdint.h>
#include <stdio.h>
#include <string.h>

/* nfs_common.h provides the wrap/unwrap helpers, the squash helper, and the
 * export struct + option constants (via nfs.h). */
#include "nfs_common.h"

#define CHECK(cond)                                              \
        do {                                                     \
            if (!(cond)) {                                       \
                fprintf(stderr,                                  \
                        "test_fh_security: FAILED at %s:%d: %s\n", \
                        __FILE__, __LINE__, # cond);             \
                return 1;                                        \
            }                                                    \
        } while (0)

static const uint8_t test_key[16] = {
    0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
    0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff
};

/* A representative inner VFS handle (16-byte mount_id + a short fragment). */
static const uint8_t sample_vfs_fh[20] = {
    0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0x03, 0x04,
    0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
    0x2a, 0x00, 0x01, 0x07
};

/* Signed wrap -> unwrap round-trips and recovers the export id + inner handle. */
static int
test_roundtrip_signed(void)
{
    uint8_t  wire[CHIMERA_NFS_FH_MAX];
    uint8_t  out[CHIMERA_VFS_FH_SIZE];
    int      wirelen, outlen, rc;
    uint16_t export_id;

    rc = chimera_nfs_fh_wrap(wire, &wirelen, 0x1234, sample_vfs_fh,
                             sizeof(sample_vfs_fh), test_key, 1);
    CHECK(rc == CHIMERA_NFS_FH_OK);
    CHECK(wire[0] == CHIMERA_NFS_FH_TAG_SIGNED);
    CHECK(wirelen == CHIMERA_NFS_FH_HDR + (int) sizeof(sample_vfs_fh) + CHIMERA_NFS_FH_MAC);

    rc = chimera_nfs_fh_unwrap(wire, wirelen, &export_id, out, &outlen, test_key, 1);
    CHECK(rc == CHIMERA_NFS_FH_OK);
    CHECK(export_id == 0x1234);
    CHECK(outlen == (int) sizeof(sample_vfs_fh));
    CHECK(memcmp(out, sample_vfs_fh, sizeof(sample_vfs_fh)) == 0);
    return 0;
} /* test_roundtrip_signed */

/* Unsigned wrap -> unwrap round-trips and carries no MAC. */
static int
test_roundtrip_unsigned(void)
{
    uint8_t  wire[CHIMERA_NFS_FH_MAX];
    uint8_t  out[CHIMERA_VFS_FH_SIZE];
    int      wirelen, outlen, rc;
    uint16_t export_id;

    rc = chimera_nfs_fh_wrap(wire, &wirelen, 7, sample_vfs_fh,
                             sizeof(sample_vfs_fh), NULL, 0);
    CHECK(rc == CHIMERA_NFS_FH_OK);
    CHECK(wire[0] == CHIMERA_NFS_FH_TAG_PLAIN);
    CHECK(wirelen == CHIMERA_NFS_FH_HDR + (int) sizeof(sample_vfs_fh));

    rc = chimera_nfs_fh_unwrap(wire, wirelen, &export_id, out, &outlen, NULL, 0);
    CHECK(rc == CHIMERA_NFS_FH_OK);
    CHECK(export_id == 7);
    CHECK(outlen == (int) sizeof(sample_vfs_fh));
    CHECK(memcmp(out, sample_vfs_fh, sizeof(sample_vfs_fh)) == 0);
    return 0;
} /* test_roundtrip_unsigned */

/* Tampering with any byte (export id, inner handle, or MAC) must be rejected. */
static int
test_tamper_rejected(void)
{
    uint8_t  wire[CHIMERA_NFS_FH_MAX];
    uint8_t  out[CHIMERA_VFS_FH_SIZE];
    int      wirelen, outlen, rc;
    uint16_t export_id;

    rc = chimera_nfs_fh_wrap(wire, &wirelen, 0x1234, sample_vfs_fh,
                             sizeof(sample_vfs_fh), test_key, 1);
    CHECK(rc == CHIMERA_NFS_FH_OK);

    /* Flip the export id (swap to a different, more-privileged export). */
    wire[2] ^= 0xff;
    rc       = chimera_nfs_fh_unwrap(wire, wirelen, &export_id, out, &outlen, test_key, 1);
    CHECK(rc == CHIMERA_NFS_FH_BADHANDLE);
    wire[2] ^= 0xff;

    /* Flip a byte of the inner VFS handle (point at a different object). */
    wire[CHIMERA_NFS_FH_HDR] ^= 0x01;
    rc                        = chimera_nfs_fh_unwrap(wire, wirelen, &export_id, out, &outlen, test_key, 1);
    CHECK(rc == CHIMERA_NFS_FH_BADHANDLE);
    wire[CHIMERA_NFS_FH_HDR] ^= 0x01;

    /* Flip a MAC byte. */
    wire[wirelen - 1] ^= 0x80;
    rc                 = chimera_nfs_fh_unwrap(wire, wirelen, &export_id, out, &outlen, test_key, 1);
    CHECK(rc == CHIMERA_NFS_FH_BADHANDLE);
    wire[wirelen - 1] ^= 0x80;

    /* Unmodified handle still verifies (sanity that the flips were reversible). */
    rc = chimera_nfs_fh_unwrap(wire, wirelen, &export_id, out, &outlen, test_key, 1);
    CHECK(rc == CHIMERA_NFS_FH_OK);

    /* A different key must reject a handle minted under test_key. */
    {
        uint8_t other_key[16];
        memcpy(other_key, test_key, sizeof(other_key));
        other_key[0] ^= 0x01;
        rc            = chimera_nfs_fh_unwrap(wire, wirelen, &export_id, out, &outlen, other_key, 1);
        CHECK(rc == CHIMERA_NFS_FH_BADHANDLE);
    }
    return 0;
} /* test_tamper_rejected */

/* Toggling the signing flag must not silently misparse the other format. */
static int
test_format_tag_mismatch(void)
{
    uint8_t  wire[CHIMERA_NFS_FH_MAX];
    uint8_t  out[CHIMERA_VFS_FH_SIZE];
    int      wirelen, outlen, rc;
    uint16_t export_id;

    /* Signed handle parsed by an unsigned server. */
    rc = chimera_nfs_fh_wrap(wire, &wirelen, 5, sample_vfs_fh,
                             sizeof(sample_vfs_fh), test_key, 1);
    CHECK(rc == CHIMERA_NFS_FH_OK);
    rc = chimera_nfs_fh_unwrap(wire, wirelen, &export_id, out, &outlen, test_key, 0);
    CHECK(rc == CHIMERA_NFS_FH_BADHANDLE);

    /* Unsigned handle parsed by a signing server. */
    rc = chimera_nfs_fh_wrap(wire, &wirelen, 5, sample_vfs_fh,
                             sizeof(sample_vfs_fh), NULL, 0);
    CHECK(rc == CHIMERA_NFS_FH_OK);
    rc = chimera_nfs_fh_unwrap(wire, wirelen, &export_id, out, &outlen, test_key, 1);
    CHECK(rc == CHIMERA_NFS_FH_BADHANDLE);
    return 0;
} /* test_format_tag_mismatch */

/* Malformed lengths must be rejected, not read out of bounds. */
static int
test_bad_lengths(void)
{
    uint8_t  wire[CHIMERA_NFS_FH_MAX];
    uint8_t  out[CHIMERA_VFS_FH_SIZE];
    int      wirelen, outlen, rc;
    uint16_t export_id;

    rc = chimera_nfs_fh_wrap(wire, &wirelen, 1, sample_vfs_fh,
                             sizeof(sample_vfs_fh), test_key, 1);
    CHECK(rc == CHIMERA_NFS_FH_OK);

    /* Too short to even contain the header + MAC. */
    rc = chimera_nfs_fh_unwrap(wire, 2, &export_id, out, &outlen, test_key, 1);
    CHECK(rc == CHIMERA_NFS_FH_BADHANDLE);

    /* Over-long is rejected. */
    rc = chimera_nfs_fh_unwrap(wire, CHIMERA_NFS_FH_MAX + 1, &export_id, out, &outlen, test_key, 1);
    CHECK(rc == CHIMERA_NFS_FH_BADHANDLE);

    /* An over-long inner handle cannot be wrapped. */
    {
        uint8_t big[CHIMERA_VFS_FH_SIZE + 1] = { 0 };
        rc = chimera_nfs_fh_wrap(wire, &wirelen, 1, big, sizeof(big), test_key, 1);
        CHECK(rc == CHIMERA_NFS_FH_BADHANDLE);
    }
    return 0;
} /* test_bad_lengths */

/* Squash: ROOT maps uid 0 only; ALL maps everyone; NONE passes through. */
static int
test_squash(void)
{
    struct chimera_nfs_export exp;
    struct chimera_vfs_cred   cred;

    memset(&exp, 0, sizeof(exp));
    exp.anonuid = 65534;
    exp.anongid = 65534;

    /* ROOT squash: uid 0 -> anon, with supplementary groups cleared. */
    exp.squash   = CHIMERA_NFS_SQUASH_ROOT;
    cred.flavor  = CHIMERA_VFS_AUTH_UNIX;
    cred.uid     = 0;
    cred.gid     = 0;
    cred.ngids   = 2;
    cred.gids[0] = 10;
    cred.gids[1] = 20;
    chimera_nfs_squash_cred(&cred, &exp);
    CHECK(cred.uid == 65534);
    CHECK(cred.gid == 65534);
    CHECK(cred.ngids == 0);

    /* ROOT squash: non-root passes through untouched. */
    cred.flavor  = CHIMERA_VFS_AUTH_UNIX;
    cred.uid     = 1000;
    cred.gid     = 1000;
    cred.ngids   = 1;
    cred.gids[0] = 5;
    chimera_nfs_squash_cred(&cred, &exp);
    CHECK(cred.uid == 1000);
    CHECK(cred.gid == 1000);
    CHECK(cred.ngids == 1);

    /* ALL squash: every caller -> anon. */
    exp.squash  = CHIMERA_NFS_SQUASH_ALL;
    cred.flavor = CHIMERA_VFS_AUTH_UNIX;
    cred.uid    = 1000;
    cred.gid    = 1000;
    cred.ngids  = 1;
    chimera_nfs_squash_cred(&cred, &exp);
    CHECK(cred.uid == 65534);
    CHECK(cred.gid == 65534);
    CHECK(cred.ngids == 0);

    /* NONE: uid 0 preserved (no_root_squash). */
    exp.squash  = CHIMERA_NFS_SQUASH_NONE;
    cred.flavor = CHIMERA_VFS_AUTH_UNIX;
    cred.uid    = 0;
    cred.gid    = 0;
    chimera_nfs_squash_cred(&cred, &exp);
    CHECK(cred.uid == 0);
    CHECK(cred.gid == 0);

    /* Custom anon ids are honored. */
    exp.squash  = CHIMERA_NFS_SQUASH_ROOT;
    exp.anonuid = 1;
    exp.anongid = 2;
    cred.flavor = CHIMERA_VFS_AUTH_UNIX;
    cred.uid    = 0;
    cred.gid    = 0;
    chimera_nfs_squash_cred(&cred, &exp);
    CHECK(cred.uid == 1);
    CHECK(cred.gid == 2);
    return 0;
} /* test_squash */

int
main(void)
{
    if (test_roundtrip_signed()) {
        return 1;
    }
    if (test_roundtrip_unsigned()) {
        return 1;
    }
    if (test_tamper_rejected()) {
        return 1;
    }
    if (test_format_tag_mismatch()) {
        return 1;
    }
    if (test_bad_lengths()) {
        return 1;
    }
    if (test_squash()) {
        return 1;
    }
    printf("test_fh_security: all tests passed\n");
    return 0;
} /* main */
