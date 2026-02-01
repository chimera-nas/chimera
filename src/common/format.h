// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

static inline int
format_hex(
    char       *out,
    int         maxoutlen,
    const void *data,
    int         len)
{
    static const char    hex[]  = "0123456789abcdef";
    int                  outlen = 0;
    const unsigned char *bytes  = data;

    /* Sanity: ensure we can always write at least a null terminator */
    if (maxoutlen < 1) {
        return -1;
    }

    /* Handle NULL data or non-positive length */
    if (!data || len <= 0) {
        out[0] = '\0';
        return 0;
    }

    /* Need 2 chars per byte plus null terminator */
    if (maxoutlen < (len * 2 + 1)) {
        out[0] = '\0';
        return -1;
    }

    for (int i = 0; i < len; i++) {
        out[outlen++] = hex[bytes[i] >> 4];
        out[outlen++] = hex[bytes[i] & 0xf];
    }

    out[outlen] = '\0';
    return outlen;
} /* format_hex */

/* Buffer size for format_safe_name output.
 * Worst case: every byte in a 256-char name is escaped as \xHH (4 chars each)
 * plus a null terminator. */
#define FORMAT_SAFE_NAME_MAX (256 * 4 + 1)

static inline int
format_safe_name(
    char       *out,
    int         maxoutlen,
    const char *data,
    int         len)
{
    int outlen = 0;

    if (maxoutlen < 1) {
        return -1;
    }

    if (!data || len <= 0) {
        out[0] = '\0';
        return 0;
    }

    for (int i = 0; i < len; i++) {
        unsigned char c = (unsigned char) data[i];

        if (c >= 0x20 && c < 0x7f) {
            /* Printable ASCII - copy as-is */
            if (outlen + 1 >= maxoutlen) {
                break;
            }
            out[outlen++] = (char) c;
        } else {
            /* Non-printable or non-ASCII byte - escape as \xHH */
            static const char hex[] = "0123456789abcdef";

            if (outlen + 4 >= maxoutlen) {
                break;
            }
            out[outlen++] = '\\';
            out[outlen++] = 'x';
            out[outlen++] = hex[c >> 4];
            out[outlen++] = hex[c & 0xf];
        }
    }

    out[outlen] = '\0';
    return outlen;
} /* format_safe_name */
