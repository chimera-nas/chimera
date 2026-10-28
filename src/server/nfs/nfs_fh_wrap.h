// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <string.h>

#include "vfs/sdk/vfs_attrs.h"
#include "nfs_common/nfs_fh_limits.h"
#include "nfs_siphash.h"

/*
 * NFS wire file-handle wrapping.
 *
 * The VFS layer mints opaque file handles that encode only the backing mount
 * (mount_id + fragment) and carry no NFS export identity.  To attribute every
 * request to its export (and thus its squash/access policy) the NFS server
 * wraps each VFS handle before putting it on the wire and unwraps it on the
 * way back in.  Two on-wire formats coexist:
 *
 *   V1 (legacy):  [ tag=0xC5 PLAIN  ] [ export_id : 2 ] [ vfs_fh : N ]
 *                 [ tag=0xC6 SIGNED ] [ export_id : 2 ] [ vfs_fh : N ] [ mac : 8 ]
 *
 *   V2 (fleet-wide signing):
 *                 [ tag=0xC8|(kid-1) SIGNED_V2 ] [ export_id : 2 ]
 *                 [ vfs_fh : N ] [ mac : 8 ]
 *
 * The tag byte identifies the format so toggling signing/rotation yields a
 * clean BADHANDLE for handles minted under a different config rather than a
 * silent misparse.  The MAC is SipHash-2-4 over the whole prefix (everything
 * left of the MAC, inclusive of the tag and so of the kid in V2) keyed by:
 *
 *   V1: a per-server 128-bit secret (`shared->fh_key`, persisted under
 *       state_dir or supplied via config).  Unless every node is configured
 *       with the same nfs_fh_key, another node cannot verify them.  Still
 *       accepted, so handles minted before the keyring existed keep working;
 *       new handles are minted V2 once the keyring is loaded.
 *
 *   V2: a keyring loaded from the KV store (KV type
 *       CHIMERA_KV_TYPE_NFS_FH_SIGNING, shared by every node), so a V2
 *       handle minted by node A is verifiable on any node B.  The kid, in the
 *       tag's low three bits, selects the entry in the ring, supporting key
 *       rotation: sign with the newest ACTIVE kid, verify with any ACTIVE or
 *       RETIRED kid.  See `struct nfs_fh_keyring` below.
 *
 * The NFSv4 pseudo-root handle is exempt and handled separately by its caller;
 * it never passes through here.
 */

#define CHIMERA_NFS_FH_TAG_PLAIN     0xC5u
#define CHIMERA_NFS_FH_TAG_SIGNED    0xC6u
/*
 * SIGNED_V2: fleet-wide, key-rotatable signing, indexing the keyring every
 * node loads from the KV store, so a V2 handle minted by node A verifies on
 * node B -- what lets a client keep its handles when it fails over to another
 * node.  The kid rides in the tag (0xC8..0xCF for kid 1..8) rather than in a
 * byte of its own: a V2 handle is then no longer than V1, which a proxying
 * chimera has no room to absorb (see the assertion below).
 */
#define CHIMERA_NFS_FH_TAG_SIGNED_V2 0xC8u
#define CHIMERA_NFS_FH_TAG_V2_MASK   0xF8u
#define CHIMERA_NFS_FH_V2_KID_MAX    8

#define CHIMERA_NFS_FH_HDR           3   /* V1: tag(1) + export_id(2)              */
#define CHIMERA_NFS_FH_HDR_V2        3   /* V2: tag|kid(1) + export_id(2)          */
#define CHIMERA_NFS_FH_MAC           8   /* SipHash-2-4 tag, 64-bit                */
/*
 * Sizing budget: header (3) + max VFS FH (64) + MAC (8) = 75, in both formats.
 * The XDR limit for NFSv4 file handles is 128 bytes.
 */
#define CHIMERA_NFS_FH_MAX           (CHIMERA_NFS_FH_HDR_V2 + CHIMERA_VFS_FH_SIZE + CHIMERA_NFS_FH_MAC)

/* Valid for kid 1..CHIMERA_NFS_FH_V2_KID_MAX only. */
static inline uint8_t
chimera_nfs_fh_v2_tag(uint8_t kid)
{
    return (uint8_t) (CHIMERA_NFS_FH_TAG_SIGNED_V2 | (kid - 1));
} /* chimera_nfs_fh_v2_tag */

/*
 * Proxying one chimera through another has to keep working: the downstream
 * chimera's NFS client re-embeds whatever handle this wrap puts on the wire,
 * and it has only CHIMERA_NFS_PROXY_REMOTE_FH_MAX bytes to do it in.  A share on
 * an inum-varint backend (memfs, diskfs, cairn) fits that ceiling -- exactly, in
 * the named-stream case, with no slack.  That is the shipped, CI-exercised
 * topology (kvm/kvm_pnfs_proxy_test_wrapper.sh mounts a memfs or diskfs share
 * through the nfs module), so it is the case pinned below: widen any term of the
 * sum and this fails to build rather than silently making chimera refuse to
 * proxy chimera.  See the margin table at CHIMERA_NFS_PROXY_REMOTE_FH_MAX.
 *
 * It is deliberately NOT an assertion about every backend, because two of them
 * do not fit and cannot be made to:
 *
 *   linux  a fragment of up to 32 wraps and signs to 59
 *   nfs    a fragment of up to 48 wraps and signs to 75
 *
 * Chaining a third chimera behind those is refused at the ceiling -- EOVERFLOW
 * per operation, EIO at mount -- by the re-encoder checks named in
 * nfs_common/nfs_fh_limits.h.  Refused, not corrupted: before those checks
 * existed this was the memory-corruption path.  Lifting the restriction needs a
 * handle indirection table, not a wider assert.
 *
 * smb sits between the two: 44 bytes, three to spare, unasserted.  Expressing it
 * here would mean either reaching into vfs/smb/smb_internal.h from the NFS
 * server -- wrong direction -- or restating sizeof(chimera_smb_client_file_id)
 * as a literal, which would stop tracking the struct the moment it changed.  A
 * silent 16 is worse than a documented gap, so it is documented.
 */
_Static_assert(CHIMERA_NFS_FH_HDR + CHIMERA_VFS_FH_INUM_EMITTED_MAX + CHIMERA_NFS_FH_MAC
               <= CHIMERA_NFS_PROXY_REMOTE_FH_MAX,
               "a signed chimera wire handle from an inum-varint backend no longer fits"
               " what the NFS proxy client can re-embed");
_Static_assert(CHIMERA_NFS_FH_HDR_V2 + CHIMERA_VFS_FH_INUM_EMITTED_MAX + CHIMERA_NFS_FH_MAC
               <= CHIMERA_NFS_PROXY_REMOTE_FH_MAX,
               "a V2 (keyring-signed) chimera wire handle from an inum-varint"
               " backend no longer fits what the NFS proxy client can re-embed");

enum chimera_nfs_fh_status {
    CHIMERA_NFS_FH_OK        = 0,
    CHIMERA_NFS_FH_BADHANDLE = -1,   /* malformed, wrong tag, or MAC mismatch */
    CHIMERA_NFS_FH_WRONGSEC  = -2,   /* handle ok, but RPC sec flavor not permitted
                                      * for the owning export (NFS4ERR_WRONGSEC) */
};

/* How a handle authenticated (reported by chimera_nfs_fh_unwrap_ring). */
enum chimera_nfs_fh_authn_kind {
    CHIMERA_NFS_FH_AUTHN_PLAIN     = 0, /* signing disabled, tag=0xC5           */
    CHIMERA_NFS_FH_AUTHN_LEGACY_V1 = 1, /* tag=0xC6, per-server-key MAC ok      */
    CHIMERA_NFS_FH_AUTHN_V2        = 2, /* tag=0xC8..0xCF, kid from keyring   */
};

/*
 * Wrap a VFS file handle into a wire file handle.
 *
 * out      : buffer of at least CHIMERA_NFS_FH_MAX bytes.
 * outlen   : set to the wrapped length.
 * export_id: export this handle belongs to (stamped into the wire handle).
 * vfs_fh   : the VFS handle bytes, vfs_len bytes.
 * key      : 16-byte signing secret (ignored when sign == 0).
 * sign     : non-zero to append the MAC.
 *
 * Returns CHIMERA_NFS_FH_OK, or CHIMERA_NFS_FH_BADHANDLE if vfs_len is out of
 * range (which would indicate an internal error, not a client one).
 */
static inline int
chimera_nfs_fh_wrap(
    uint8_t       *out,
    int           *outlen,
    uint16_t       export_id,
    const uint8_t *vfs_fh,
    int            vfs_len,
    const uint8_t *key,
    int            sign)
{
    int len;

    /* Always define the output length, including on the error path, so callers
    * (which treat wrapping as infallible) never see an uninitialized value. */
    *outlen = 0;

    if (vfs_len < 0 || vfs_len > CHIMERA_VFS_FH_SIZE) {
        return CHIMERA_NFS_FH_BADHANDLE;
    }

    out[0] = sign ? CHIMERA_NFS_FH_TAG_SIGNED : CHIMERA_NFS_FH_TAG_PLAIN;
    out[1] = (uint8_t) (export_id >> 8);
    out[2] = (uint8_t) (export_id & 0xff);
    memcpy(out + CHIMERA_NFS_FH_HDR, vfs_fh, vfs_len);
    len = CHIMERA_NFS_FH_HDR + vfs_len;

    if (sign) {
        uint64_t mac = chimera_nfs_siphash(key, out, len);
        memcpy(out + len, &mac, CHIMERA_NFS_FH_MAC);
        len += CHIMERA_NFS_FH_MAC;
    }

    *outlen = len;
    return CHIMERA_NFS_FH_OK;
} /* chimera_nfs_fh_wrap */

/*
 * Unwrap and authenticate a wire file handle.
 *
 * wire/wirelen : the bytes received from the client.
 * export_id    : set to the embedded export id.
 * vfs_fh_out   : buffer of at least CHIMERA_VFS_FH_SIZE bytes; receives the
 *                inner VFS handle.
 * vfs_len_out  : set to the inner handle length.
 * key          : 16-byte signing secret (checked when sign != 0).
 * sign         : non-zero if this server signs handles; the wire tag must match.
 *
 * Returns CHIMERA_NFS_FH_OK, or CHIMERA_NFS_FH_BADHANDLE on any malformation,
 * tag mismatch, or MAC verification failure.
 */
static inline int
chimera_nfs_fh_unwrap(
    const uint8_t *wire,
    int            wirelen,
    uint16_t      *export_id,
    uint8_t       *vfs_fh_out,
    int           *vfs_len_out,
    const uint8_t *key,
    int            sign)
{
    int     vfs_len;
    uint8_t want_tag = sign ? CHIMERA_NFS_FH_TAG_SIGNED : CHIMERA_NFS_FH_TAG_PLAIN;
    int     min_len  = CHIMERA_NFS_FH_HDR + (sign ? CHIMERA_NFS_FH_MAC : 0);

    if (wirelen < min_len || wirelen > CHIMERA_NFS_FH_MAX || wire[0] != want_tag) {
        return CHIMERA_NFS_FH_BADHANDLE;
    }

    vfs_len = wirelen - CHIMERA_NFS_FH_HDR - (sign ? CHIMERA_NFS_FH_MAC : 0);

    if (vfs_len < 0 || vfs_len > CHIMERA_VFS_FH_SIZE) {
        return CHIMERA_NFS_FH_BADHANDLE;
    }

    if (sign) {
        uint64_t mac;
        uint64_t expect = chimera_nfs_siphash(key, wire, CHIMERA_NFS_FH_HDR + vfs_len);

        memcpy(&mac, wire + CHIMERA_NFS_FH_HDR + vfs_len, CHIMERA_NFS_FH_MAC);
        if (mac != expect) {
            return CHIMERA_NFS_FH_BADHANDLE;
        }
    }

    *export_id = ((uint16_t) wire[1] << 8) | (uint16_t) wire[2];
    memcpy(vfs_fh_out, wire + CHIMERA_NFS_FH_HDR, vfs_len);
    *vfs_len_out = vfs_len;

    return CHIMERA_NFS_FH_OK;
} /* chimera_nfs_fh_unwrap */

/* ------------------------------------------------------------------------- *
*  V2 fleet-wide signing keyring                               *
* ------------------------------------------------------------------------- */

/*
 * Maximum keys the ring holds simultaneously.  In steady state the ring has
 * one ACTIVE key; during rotation we briefly have {ACTIVE_new, RETIRED_old},
 * and RETIRED entries phase out after a bounded window.  A ring size of 8
 * absorbs pathological rotation bursts, and matches the kids (1..8) a V2 tag
 * can carry.
 */
#define NFS_FH_KEYRING_MAX_SLOTS CHIMERA_NFS_FH_V2_KID_MAX

enum nfs_fh_key_state {
    NFS_FH_KEY_UNUSED  = 0,
    NFS_FH_KEY_ACTIVE  = 1,   /* newest ACTIVE is the signing key            */
    NFS_FH_KEY_RETIRED = 2,   /* verify only                                 */
};

struct nfs_fh_key_slot {
    uint8_t  kid;             /* 1..CHIMERA_NFS_FH_V2_KID_MAX; 0 = empty     */
    uint8_t  state;           /* enum nfs_fh_key_state                       */
    uint8_t  key[16];         /* SipHash-2-4 128-bit secret                  */
    uint64_t created_ns;      /* mint time; newest ACTIVE = signing key      */
};

/*
 * In-memory keyring, loaded from the KV backend before the server accepts
 * connections and immutable after that: the sign/verify hot path reads it
 * without synchronization.  A future reload or rotation must not write a ring
 * that is being read; it must use a lock, or build a new ring and publish it
 * through an atomic pointer.  The wrap routines below take a const pointer.
 *
 * signing_kid is the kid the sign path writes into the wire; it MUST resolve
 * to a slot in state ACTIVE.  A ring with no ACTIVE slot is invalid and MUST
 * cause the server to refuse to accept NFS connections (fail-fast at boot).
 */
struct nfs_fh_keyring {
    uint8_t                signing_kid;
    uint8_t                _pad[7];
    struct nfs_fh_key_slot slots[NFS_FH_KEYRING_MAX_SLOTS];
};

/*
 * Look up a key by kid.  Returns pointer to the 16-byte key on success (any
 * state that permits verification: ACTIVE or RETIRED), or NULL if the kid is
 * absent or its slot is UNUSED.  The signing path uses kr->signing_kid; the
 * verify path uses the kid embedded in the wire handle.
 */
static inline const uint8_t *
nfs_fh_keyring_find(
    const struct nfs_fh_keyring *kr,
    uint8_t                      kid)
{
    int i;

    if (kid == 0 || kid > CHIMERA_NFS_FH_V2_KID_MAX) {
        /* kid 0 is reserved as "unused slot" sentinel; a wire handle carrying
         * kid=0 is malformed. */
        return NULL;
    }
    for (i = 0; i < NFS_FH_KEYRING_MAX_SLOTS; i++) {
        const struct nfs_fh_key_slot *s = &kr->slots[i];
        if (s->kid == kid &&
            (s->state == NFS_FH_KEY_ACTIVE || s->state == NFS_FH_KEY_RETIRED)) {
            return s->key;
        }
    }
    return NULL;
} /* nfs_fh_keyring_find */

/*
 * Wrap a VFS file handle into a V2 wire file handle using the ring's current
 * signing key.  Layout: [tag=0xC8|(kid-1)][export_id_hi][export_id_lo][vfs_fh][mac8].
 *
 * out      : buffer of at least CHIMERA_NFS_FH_MAX bytes.
 * outlen   : set to the wrapped length.
 * kr       : keyring; kr->signing_kid MUST resolve to an ACTIVE slot.
 *
 * Returns CHIMERA_NFS_FH_OK, or CHIMERA_NFS_FH_BADHANDLE on internal errors
 * (bogus vfs_len, missing signing key -- the latter indicates a boot-time
 * misconfiguration that should have failed fast).
 */
static inline int
chimera_nfs_fh_wrap_v2(
    uint8_t                     *out,
    int                         *outlen,
    uint16_t                     export_id,
    const uint8_t               *vfs_fh,
    int                          vfs_len,
    const struct nfs_fh_keyring *kr)
{
    const uint8_t *key;
    uint64_t       mac;
    int            len;

    *outlen = 0;

    if (vfs_len < 0 || vfs_len > CHIMERA_VFS_FH_SIZE) {
        return CHIMERA_NFS_FH_BADHANDLE;
    }

    key = nfs_fh_keyring_find(kr, kr->signing_kid);
    if (!key) {
        /* Ring has no valid signing key -- caller (server startup) must have
         * failed fast; reaching here is an internal invariant break. */
        return CHIMERA_NFS_FH_BADHANDLE;
    }

    out[0] = chimera_nfs_fh_v2_tag(kr->signing_kid);
    out[1] = (uint8_t) (export_id >> 8);
    out[2] = (uint8_t) (export_id & 0xff);
    memcpy(out + CHIMERA_NFS_FH_HDR_V2, vfs_fh, vfs_len);
    len = CHIMERA_NFS_FH_HDR_V2 + vfs_len;

    mac = chimera_nfs_siphash(key, out, len);
    memcpy(out + len, &mac, CHIMERA_NFS_FH_MAC);
    len += CHIMERA_NFS_FH_MAC;

    *outlen = len;
    return CHIMERA_NFS_FH_OK;
} /* chimera_nfs_fh_wrap_v2 */

/*
 * Unwrap and authenticate a wire file handle, accepting both V1 and V2
 * formats.  On success sets *authn to the format that authenticated it.
 *
 * kr        : V2 keyring; may be NULL if only V1 / plain is expected.
 * legacy_key: V1 per-server 128-bit secret; may be NULL to disable V1 accept.
 * sign      : non-zero if the server is configured to sign at all.  When 0,
 *             only PLAIN handles are accepted (kr and legacy_key are ignored).
 *
 * Returns CHIMERA_NFS_FH_OK, or CHIMERA_NFS_FH_BADHANDLE on any malformation,
 * unknown tag, unknown kid, or MAC verification failure.
 */
static inline int
chimera_nfs_fh_unwrap_ring(
    const uint8_t                  *wire,
    int                             wirelen,
    uint16_t                       *export_id,
    uint8_t                        *vfs_fh_out,
    int                            *vfs_len_out,
    const struct nfs_fh_keyring    *kr,
    const uint8_t                  *legacy_key,
    int                             sign,
    enum chimera_nfs_fh_authn_kind *authn)
{
    uint8_t tag;

    if (wirelen < 1 || wirelen > CHIMERA_NFS_FH_MAX) {
        return CHIMERA_NFS_FH_BADHANDLE;
    }
    tag = wire[0];

    if (!sign) {
        /* Signing disabled: only PLAIN accepted.  Callers switching signing
         * on/off intentionally see a clean BADHANDLE for the wrong tag rather
         * than a silent misparse. */
        int rc = chimera_nfs_fh_unwrap(wire, wirelen, export_id,
                                       vfs_fh_out, vfs_len_out, NULL, 0);
        if (rc == CHIMERA_NFS_FH_OK) {
            *authn = CHIMERA_NFS_FH_AUTHN_PLAIN;
        }
        return rc;
    }

    if ((tag & CHIMERA_NFS_FH_TAG_V2_MASK) == CHIMERA_NFS_FH_TAG_SIGNED_V2 && kr) {
        int            vfs_len;
        uint8_t        kid;
        const uint8_t *key;
        uint64_t       mac, expect;

        if (wirelen < CHIMERA_NFS_FH_HDR_V2 + CHIMERA_NFS_FH_MAC) {
            return CHIMERA_NFS_FH_BADHANDLE;
        }
        vfs_len = wirelen - CHIMERA_NFS_FH_HDR_V2 - CHIMERA_NFS_FH_MAC;
        if (vfs_len < 0 || vfs_len > CHIMERA_VFS_FH_SIZE) {
            return CHIMERA_NFS_FH_BADHANDLE;
        }

        kid = (uint8_t) ((tag & ~CHIMERA_NFS_FH_TAG_V2_MASK) + 1);
        key = nfs_fh_keyring_find(kr, kid);
        if (!key) {
            /* Unknown kid: either a client trying to forge, or (more likely
             * during rotation) a stale RETIRED key that has aged out.  Client
             * treats BADHANDLE as ESTALE and re-resolves. */
            return CHIMERA_NFS_FH_BADHANDLE;
        }

        expect = chimera_nfs_siphash(key, wire, CHIMERA_NFS_FH_HDR_V2 + vfs_len);
        memcpy(&mac, wire + CHIMERA_NFS_FH_HDR_V2 + vfs_len, CHIMERA_NFS_FH_MAC);
        if (mac != expect) {
            return CHIMERA_NFS_FH_BADHANDLE;
        }

        *export_id = ((uint16_t) wire[1] << 8) | (uint16_t) wire[2];
        memcpy(vfs_fh_out, wire + CHIMERA_NFS_FH_HDR_V2, vfs_len);
        *vfs_len_out = vfs_len;
        *authn       = CHIMERA_NFS_FH_AUTHN_V2;
        return CHIMERA_NFS_FH_OK;
    }

    if (tag == CHIMERA_NFS_FH_TAG_SIGNED && legacy_key) {
        int rc = chimera_nfs_fh_unwrap(wire, wirelen, export_id,
                                       vfs_fh_out, vfs_len_out, legacy_key, 1);
        if (rc == CHIMERA_NFS_FH_OK) {
            *authn = CHIMERA_NFS_FH_AUTHN_LEGACY_V1;
        }
        return rc;
    }

    /* PLAIN under sign=1, unknown tag, or missing keyring/legacy-key for the
     * observed tag: no path can authenticate this handle. */
    return CHIMERA_NFS_FH_BADHANDLE;
} /* chimera_nfs_fh_unwrap_ring */
