// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>

#include "nfs4_xdr.h"

/*
 * Cookie policy for the NFSv4 pseudo-fs root READDIR (nfs4_root_readdir).
 *
 * The pseudo-root has its own cookie space: an entry's cookie is its position
 * in the export snapshot biased by +3, staying clear of the reserved cookie
 * values 0-2 (RFC 7530 16.24.4).  Resuming with cookie C therefore restarts
 * the walk at position C - 2, just past the entry that carried C.
 *
 * Valid cookies are 0 (start of directory) and [3, num_exports + 2].  Anything
 * else is a client error: 1 and 2 are reserved and never emitted, and a cookie
 * above the range is either garbage or a stale cookie into a list that has
 * since shrunk.  Both get NFS4ERR_BAD_COOKIE, which makes a client restart the
 * directory read.  The alternative -- truncating a 64-bit cookie into the int
 * resume position -- turns a cookie like 0x80000002 into a negative first_pos,
 * which never trips the resume test and silently restarts the listing at 0,
 * feeding the client duplicate entries.
 *
 * Bounding the cookie by num_exports is also what keeps *first_pos within
 * [0, num_exports], so the resume position can stay a plain int.
 *
 * num_exports is the snapshot's entry count and must not be negative: the
 * range check widens it to uint64_t, so a negative count would wrap and accept
 * almost any cookie.
 *
 * *first_pos is written only on NFS4_OK.
 */

/* Bias applied to a snapshot position to produce its wire cookie.  Encode and
 * decode below are the only two places that know it. */
#define NFS4_ROOT_COOKIE_BIAS 3

/* The cookie carried by the entry at snapshot position pos. */
static inline uint64_t
nfs4_root_readdir_pos_cookie(int pos)
{
    return (uint64_t) pos + NFS4_ROOT_COOKIE_BIAS;
} /* nfs4_root_readdir_pos_cookie */

/* Inverse of nfs4_root_readdir_pos_cookie, shifted by one: resuming with a
 * cookie restarts *after* the entry that carried it. */
static inline nfsstat4
nfs4_root_readdir_cookie_first_pos(
    uint64_t cookie,
    int      num_exports,
    int     *first_pos)
{
    if (cookie == 0) {
        *first_pos = 0;
        return NFS4_OK;
    }

    /* Reserved (RFC 7530 16.24.4); the pseudo-root never emits them, so
     * receiving one back is a client error. */
    if (cookie < NFS4_ROOT_COOKIE_BIAS) {
        return NFS4ERR_BAD_COOKIE;
    }

    /* The largest cookie emitted is the last entry's, i.e. the cookie for
     * position num_exports - 1.  Written out rather than as
     * nfs4_root_readdir_pos_cookie(num_exports - 1) so an empty snapshot does
     * not depend on -1 wrapping.  Compared in uint64_t so neither side is
     * truncated. */
    if (cookie > (uint64_t) num_exports + NFS4_ROOT_COOKIE_BIAS - 1) {
        return NFS4ERR_BAD_COOKIE;
    }

    *first_pos = (int) (cookie - (NFS4_ROOT_COOKIE_BIAS - 1));

    return NFS4_OK;
} /* nfs4_root_readdir_cookie_first_pos */
