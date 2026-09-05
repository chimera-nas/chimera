// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <string.h>

#include "sdk/vfs_sid.h"
#include "common/macros.h"

static inline uint32_t
sid_get_u32(const uint8_t *p)
{
    return (uint32_t) p[0] | ((uint32_t) p[1] << 8) |
           ((uint32_t) p[2] << 16) | ((uint32_t) p[3] << 24);
} /* sid_get_u32 */

static inline void
sid_put_u32(
    uint8_t *p,
    uint32_t v)
{
    p[0] = (uint8_t) (v & 0xff);
    p[1] = (uint8_t) ((v >> 8) & 0xff);
    p[2] = (uint8_t) ((v >> 16) & 0xff);
    p[3] = (uint8_t) ((v >> 24) & 0xff);
} /* sid_put_u32 */

/*
 * Parse a run of decimal digits at *pp into *out (up to `max`), advancing
 * *pp past them.  Requires at least one digit and rejects overflow.  Returns
 * 0 on success, -1 otherwise.
 */
static int
sid_parse_dec(
    const char **pp,
    uint64_t     max,
    uint64_t    *out)
{
    const char *p = *pp;
    uint64_t    v = 0;

    if (*p < '0' || *p > '9') {
        return -1;
    }
    while (*p >= '0' && *p <= '9') {
        uint64_t d = (uint64_t) (*p - '0');

        if (v > (max - d) / 10) {
            return -1;
        }
        v = v * 10 + d;
        p++;
    }
    *out = v;
    *pp  = p;
    return 0;
} /* sid_parse_dec */

SYMBOL_EXPORT int
chimera_sid_bin_len(
    const uint8_t *buf,
    uint32_t       avail)
{
    uint32_t count;

    if (!buf || avail < CHIMERA_SID_MIN_LEN) {
        return -1;
    }
    count = buf[1];
    if (count > CHIMERA_SID_MAX_SUB_AUTHS) {
        return -1;
    }
    if (CHIMERA_SID_MIN_LEN + count * 4 > avail) {
        return -1;
    }
    return (int) (CHIMERA_SID_MIN_LEN + count * 4);
} /* chimera_sid_bin_len */

SYMBOL_EXPORT int
chimera_sid_bin_to_str(
    const uint8_t *buf,
    uint32_t       len,
    char          *out,
    int            outlen)
{
    uint64_t authority = 0;
    int      binlen, count, pos;

    binlen = chimera_sid_bin_len(buf, len);
    if (binlen < 0 || !out || outlen <= 0) {
        return -1;
    }
    count = buf[1];

    for (int i = 0; i < 6; i++) {
        authority = (authority << 8) | buf[2 + i];
    }

    pos = snprintf(out, outlen, "S-%u-%llu", buf[0], (unsigned long long) authority);
    if (pos < 0 || pos >= outlen) {
        return -1;
    }
    for (int i = 0; i < count; i++) {
        int n = snprintf(out + pos, outlen - pos, "-%u",
                         sid_get_u32(buf + CHIMERA_SID_MIN_LEN + i * 4));

        if (n < 0 || pos + n >= outlen) {
            return -1;
        }
        pos += n;
    }
    return binlen;
} /* chimera_sid_bin_to_str */

SYMBOL_EXPORT int
chimera_sid_str_to_bin(
    const char *str,
    uint8_t    *out,
    int         outcap)
{
    const char *p = str;
    uint64_t    v;
    int         count = 0;

    if (!str || !out || outcap < CHIMERA_SID_MIN_LEN) {
        return -1;
    }
    if (str[0] != 'S' && str[0] != 's') {
        return -1;
    }
    p++;
    if (*p != '-') {
        return -1;
    }
    p++;

    /* revision */
    if (sid_parse_dec(&p, 255, &v) < 0 || *p != '-') {
        return -1;
    }
    out[0] = (uint8_t) v;
    p++;

    /* 48-bit identifier authority, stored big-endian */
    if (sid_parse_dec(&p, 0xffffffffffffULL, &v) < 0) {
        return -1;
    }
    for (int i = 0; i < 6; i++) {
        out[2 + i] = (uint8_t) ((v >> (8 * (5 - i))) & 0xff);
    }

    while (*p == '-') {
        p++;
        if (count >= CHIMERA_SID_MAX_SUB_AUTHS) {
            return -1;
        }
        if (sid_parse_dec(&p, 0xffffffffULL, &v) < 0) {
            return -1;
        }
        if (CHIMERA_SID_MIN_LEN + (count + 1) * 4 > outcap) {
            return -1;
        }
        sid_put_u32(out + CHIMERA_SID_MIN_LEN + count * 4, (uint32_t) v);
        count++;
    }

    if (*p != '\0') {
        return -1; /* trailing text is not part of a SID */
    }

    out[1] = (uint8_t) count;
    return CHIMERA_SID_MIN_LEN + count * 4;
} /* chimera_sid_str_to_bin */

SYMBOL_EXPORT int
chimera_sid_from_bin(
    struct chimera_sid *sid,
    const uint8_t      *buf,
    uint32_t            avail)
{
    int len = chimera_sid_bin_len(buf, avail);

    if (len < 0) {
        memset(sid, 0, sizeof(*sid));
        return -1;
    }
    /* Fully define the struct: zero the tail past the SID so a chimera_sid
     * (and any ACE carrying one) is byte-deterministic and safe to memcmp. */
    memcpy(sid->data, buf, len);
    memset(sid->data + len, 0, CHIMERA_SID_MAX_LEN - len);
    sid->len = (uint8_t) len;
    return len;
} /* chimera_sid_from_bin */

SYMBOL_EXPORT int
chimera_sid_to_str(
    const struct chimera_sid *sid,
    char                     *out,
    int                       outlen)
{
    if (!chimera_sid_present(sid)) {
        return -1;
    }
    if (chimera_sid_bin_to_str(sid->data, sid->len, out, outlen) < 0) {
        return -1;
    }
    return (int) strlen(out);
} /* chimera_sid_to_str */

SYMBOL_EXPORT int
chimera_sid_from_str(
    struct chimera_sid *sid,
    const char         *str)
{
    int len = chimera_sid_str_to_bin(str, sid->data, CHIMERA_SID_MAX_LEN);

    if (len < 0) {
        memset(sid, 0, sizeof(*sid));
        return -1;
    }
    memset(sid->data + len, 0, CHIMERA_SID_MAX_LEN - len);
    sid->len = (uint8_t) len;
    return 0;
} /* chimera_sid_from_str */
