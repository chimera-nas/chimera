// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stddef.h>

/*
 * SipHash-2-4: a fast keyed pseudo-random function suitable for short-message
 * authentication (Aumasson & Bernstein, 2012).  Used to MAC NFS wire file
 * handles so they cannot be forged or tampered by clients.
 *
 * key points to a 16-byte secret.  Returns a 64-bit tag over [in, in+inlen).
 *
 * Implemented header-inline (it is tiny and on the file-handle hot path) so it
 * needs no separate translation unit; adapted from the public-domain reference
 * by Jean-Philippe Aumasson and Daniel J. Bernstein
 * (https://github.com/veorq/SipHash).  c=2 compression rounds, d=4
 * finalization rounds, 64-bit output.
 */

#define CHIMERA_SIPHASH_ROTL(x, b) (uint64_t) (((x) << (b)) | ((x) >> (64 - (b))))

#define CHIMERA_SIPHASH_U8TO64_LE(p)          \
        (((uint64_t) ((p)[0])) |                  \
         ((uint64_t) ((p)[1]) << 8) |             \
         ((uint64_t) ((p)[2]) << 16) |            \
         ((uint64_t) ((p)[3]) << 24) |            \
         ((uint64_t) ((p)[4]) << 32) |            \
         ((uint64_t) ((p)[5]) << 40) |            \
         ((uint64_t) ((p)[6]) << 48) |            \
         ((uint64_t) ((p)[7]) << 56))

#define CHIMERA_SIPHASH_ROUND  \
        do {                       \
            v0 += v1;              \
            v1  = CHIMERA_SIPHASH_ROTL(v1, 13); \
            v1 ^= v0;              \
            v0  = CHIMERA_SIPHASH_ROTL(v0, 32); \
            v2 += v3;              \
            v3  = CHIMERA_SIPHASH_ROTL(v3, 16); \
            v3 ^= v2;              \
            v0 += v3;              \
            v3  = CHIMERA_SIPHASH_ROTL(v3, 21); \
            v3 ^= v0;              \
            v2 += v1;              \
            v1  = CHIMERA_SIPHASH_ROTL(v1, 17); \
            v1 ^= v2;              \
            v2  = CHIMERA_SIPHASH_ROTL(v2, 32); \
        } while (0)

static inline uint64_t
chimera_nfs_siphash(
    const uint8_t *key,
    const void    *in,
    size_t         inlen)
{
    const uint8_t *ni   = (const uint8_t *) in;
    uint64_t       k0   = CHIMERA_SIPHASH_U8TO64_LE(key);
    uint64_t       k1   = CHIMERA_SIPHASH_U8TO64_LE(key + 8);
    uint64_t       v0   = 0x736f6d6570736575ULL ^ k0;
    uint64_t       v1   = 0x646f72616e646f6dULL ^ k1;
    uint64_t       v2   = 0x6c7967656e657261ULL ^ k0;
    uint64_t       v3   = 0x7465646279746573ULL ^ k1;
    uint64_t       b    = (uint64_t) inlen << 56;
    const uint8_t *end  = ni + (inlen - (inlen % 8));
    const int      left = inlen % 8;
    uint64_t       m;

    for ( ; ni != end; ni += 8) {
        m   = CHIMERA_SIPHASH_U8TO64_LE(ni);
        v3 ^= m;
        CHIMERA_SIPHASH_ROUND;
        CHIMERA_SIPHASH_ROUND;
        v0 ^= m;
    }

    switch (left) {
        case 7:
            b |= ((uint64_t) ni[6]) << 48; /* fallthrough */
        case 6:
            b |= ((uint64_t) ni[5]) << 40; /* fallthrough */
        case 5:
            b |= ((uint64_t) ni[4]) << 32; /* fallthrough */
        case 4:
            b |= ((uint64_t) ni[3]) << 24; /* fallthrough */
        case 3:
            b |= ((uint64_t) ni[2]) << 16; /* fallthrough */
        case 2:
            b |= ((uint64_t) ni[1]) << 8; /* fallthrough */
        case 1:
            b |= ((uint64_t) ni[0]); /* fallthrough */
        case 0:
            break;
    } /* switch */

    v3 ^= b;
    CHIMERA_SIPHASH_ROUND;
    CHIMERA_SIPHASH_ROUND;
    v0 ^= b;

    v2 ^= 0xff;
    CHIMERA_SIPHASH_ROUND;
    CHIMERA_SIPHASH_ROUND;
    CHIMERA_SIPHASH_ROUND;
    CHIMERA_SIPHASH_ROUND;

    return v0 ^ v1 ^ v2 ^ v3;
} /* chimera_nfs_siphash */
