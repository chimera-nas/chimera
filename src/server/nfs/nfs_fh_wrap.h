// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <string.h>

#include "vfs/vfs_attrs.h"
#include "nfs_common/nfs_fh_limits.h"
#include "nfs_siphash.h"

/*
 * NFS wire file-handle wrapping.
 *
 * The VFS layer mints opaque file handles that encode only the backing mount
 * (mount_id + fragment) and carry no NFS export identity.  To attribute every
 * request to its export (and thus its squash/access policy) the NFS server
 * wraps each VFS handle before putting it on the wire and unwraps it on the way
 * back in:
 *
 *   wire FH = [ tag : 1 ] [ export_id : 2, big-endian ] [ vfs_fh : N ]
 *             [ mac : 8 ]   (only when signing is enabled)
 *
 * The 1-byte tag identifies the format (and distinguishes signed from unsigned
 * handles, so toggling the signing config yields a clean BADHANDLE for handles
 * minted under the other setting rather than a silent misparse).  When signing
 * is on, the trailing MAC is SipHash-2-4 over the whole [tag||export_id||vfs_fh]
 * prefix keyed by the per-server secret, which makes handles unforgeable: a
 * client can neither fabricate a handle for an object it was never given nor
 * swap the export_id to escape squashing.
 *
 * The NFSv4 pseudo-root handle is exempt and handled separately by its caller;
 * it never passes through here.
 */

#define CHIMERA_NFS_FH_TAG_PLAIN  0xC5u
#define CHIMERA_NFS_FH_TAG_SIGNED 0xC6u

#define CHIMERA_NFS_FH_HDR        3   /* tag(1) + export_id(2) */
#define CHIMERA_NFS_FH_MAC        8   /* SipHash-2-4 tag, 64-bit */
#define CHIMERA_NFS_FH_MAX        (CHIMERA_NFS_FH_HDR + CHIMERA_VFS_FH_SIZE + CHIMERA_NFS_FH_MAC)

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

enum chimera_nfs_fh_status {
    CHIMERA_NFS_FH_OK        = 0,
    CHIMERA_NFS_FH_BADHANDLE = -1,   /* malformed, wrong tag, or MAC mismatch */
    CHIMERA_NFS_FH_WRONGSEC  = -2,   /* handle ok, but RPC sec flavor not permitted
                                      * for the owning export (NFS4ERR_WRONGSEC) */
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
