// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <string.h>

/*
 * RFC 8276 carries extended-attribute names in the user namespace with no
 * prefix on the wire: the NFS client strips "user." before sending and re-adds
 * it when listing.  Chimera's VFS contract, by contrast, is fully-qualified
 * Linux-style names ("user.foo") -- the passthrough backends (linux, io_uring)
 * hand the name straight to fgetxattr/fsetxattr/flistxattr, which require a
 * namespace prefix, and the opaque-store backends (memfs, cairn) round-trip
 * whatever they are given.  The NFS protocol layer therefore translates between
 * the two: prepend "user." on the way in, strip it (and drop other namespaces)
 * on the way out.
 *
 * These helpers are pure so they can be unit tested without a dbuf.  The prefix
 * constant lives here so other protocol servers (SMB/S3) can reuse it when they
 * grow xattr support.
 */

#define CHIMERA_NFS4_XATTR_USER_PREFIX     "user."
#define CHIMERA_NFS4_XATTR_USER_PREFIX_LEN 5

/* Longest fully-qualified xattr name the Linux backends accept (XATTR_NAME_MAX). */
#define CHIMERA_NFS4_XATTR_NAME_MAX        255

/*
 * True iff a fully-qualified name belongs to the user namespace and has a
 * non-empty key after the prefix.  A bare "user." (len == prefix len) is not a
 * valid user attribute and returns false.
 */
static inline int
chimera_nfs4_xattr_is_user(
    const char *name,
    uint32_t    len)
{
    return len > CHIMERA_NFS4_XATTR_USER_PREFIX_LEN &&
           memcmp(name, CHIMERA_NFS4_XATTR_USER_PREFIX,
                  CHIMERA_NFS4_XATTR_USER_PREFIX_LEN) == 0;
} /* chimera_nfs4_xattr_is_user */

/*
 * Build the fully-qualified "user."+key name into dst.  Returns the total
 * length written, or -1 if the key is empty, the result would exceed
 * XATTR_NAME_MAX, or dst is too small.  dst is NOT NUL-terminated (callers pass
 * an explicit length to the VFS); allocate at least the returned length.
 */
static inline int
chimera_nfs4_xattr_build_user(
    char       *dst,
    uint32_t    dstcap,
    const char *key,
    uint32_t    keylen)
{
    uint32_t total;

    /* Bound keylen before the addition so a hostile wire length cannot overflow
     * the uint32 and slip a huge memcpy past the size checks. */
    if (keylen == 0 ||
        keylen > CHIMERA_NFS4_XATTR_NAME_MAX -
        CHIMERA_NFS4_XATTR_USER_PREFIX_LEN) {
        return -1;
    }

    total = CHIMERA_NFS4_XATTR_USER_PREFIX_LEN + keylen;

    if (total > dstcap) {
        return -1;
    }

    memcpy(dst, CHIMERA_NFS4_XATTR_USER_PREFIX,
           CHIMERA_NFS4_XATTR_USER_PREFIX_LEN);
    memcpy(dst + CHIMERA_NFS4_XATTR_USER_PREFIX_LEN, key, keylen);

    return (int) total;
} /* chimera_nfs4_xattr_build_user */
