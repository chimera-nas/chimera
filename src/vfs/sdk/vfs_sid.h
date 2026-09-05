// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * Native Windows security identifier (SID) value type.
 *
 * A SID is carried in its MS-DTYP 2.4.2.2 binary form: revision (1 byte),
 * sub-authority count (1 byte), a 48-bit big-endian identifier authority
 * (6 bytes), then count little-endian 32-bit sub-authorities.  That is the
 * exact layout an SMB security descriptor puts on the wire, so an ACE SID can
 * be copied in and out without a string round trip, and it is what native
 * backends persist.  A zero length means "no native SID is known" -- the
 * numeric uid/gid on the principal (or attrs) then stands alone and the
 * marshaller falls back to the identity cache or the algorithmic SID.
 *
 * The cap of 15 sub-authorities is the MS-DTYP maximum (SID_MAX_SUB_AUTHORITIES).
 */

#include <stdint.h>
#include <stddef.h>

#define CHIMERA_SID_MAX_SUB_AUTHS 15
#define CHIMERA_SID_MIN_LEN       8  /* revision + count + authority */
#define CHIMERA_SID_MAX_LEN       (CHIMERA_SID_MIN_LEN + 4 * CHIMERA_SID_MAX_SUB_AUTHS)

/* "S-<rev>-<authority>" plus 15 x "-<u32>" plus NUL fits comfortably. */
#define CHIMERA_SID_STR_MAX       192

struct chimera_sid {
    uint8_t len;                       /* 0 = absent; else 8 + 4 * count */
    uint8_t data[CHIMERA_SID_MAX_LEN]; /* binary SID, valid for `len` bytes */
};

/*
 * Validate the binary SID at the start of `buf` (`avail` bytes) and return
 * its length (8 + 4 * sub_authority_count), or -1 if the buffer is too short
 * or the sub-authority count exceeds CHIMERA_SID_MAX_SUB_AUTHS.
 */
int chimera_sid_bin_len(
    const uint8_t *buf,
    uint32_t       avail);

/*
 * Format the binary SID at `buf` (`len` bytes available) as
 * "S-<rev>-<authority>-<sub>..." into `out`.  Returns the number of SID bytes
 * consumed from `buf`, or -1 on a malformed/truncated SID or a too-small
 * `out`.
 */
int chimera_sid_bin_to_str(
    const uint8_t *buf,
    uint32_t       len,
    char          *out,
    int            outlen);

/*
 * Parse "S-<rev>-<authority>-<sub>..." into the binary form at `out`
 * (capacity `outcap`).  Strict: digits only, no trailing text.  Returns the
 * binary length written, or -1 on malformed input / insufficient capacity.
 */
int chimera_sid_str_to_bin(
    const char *str,
    uint8_t    *out,
    int         outcap);

/*
 * Copy the binary SID at the start of `buf` into `sid`.  Returns the number
 * of bytes consumed, or -1 (and `sid` cleared) if it is malformed.
 */
int chimera_sid_from_bin(
    struct chimera_sid *sid,
    const uint8_t      *buf,
    uint32_t            avail);

/* Format `sid` as text.  Returns the string length, or -1 if absent/too small. */
int chimera_sid_to_str(
    const struct chimera_sid *sid,
    char                     *out,
    int                       outlen);

/* Parse text into `sid`.  Returns 0, or -1 (and `sid` cleared) on bad input. */
int chimera_sid_from_str(
    struct chimera_sid *sid,
    const char         *str);

static inline int
chimera_sid_present(const struct chimera_sid *sid)
{
    return sid != NULL && sid->len != 0;
} /* chimera_sid_present */

/* Two SIDs are equal only when both are present and byte-identical. */
static inline int
chimera_sid_equal(
    const struct chimera_sid *a,
    const struct chimera_sid *b)
{
    if (!chimera_sid_present(a) || !chimera_sid_present(b) || a->len != b->len) {
        return 0;
    }
    for (unsigned i = 0; i < a->len; i++) {
        if (a->data[i] != b->data[i]) {
            return 0;
        }
    }
    return 1;
} /* chimera_sid_equal */
