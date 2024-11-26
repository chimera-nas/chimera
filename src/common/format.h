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

    /* Need 2 chars per byte plus null terminator */
    if (maxoutlen < (len * 2 + 1)) {
        return -1;
    }

    for (int i = 0; i < len; i++) {
        out[outlen++] = hex[bytes[i] >> 4];
        out[outlen++] = hex[bytes[i] & 0xf];
    }

    out[outlen] = '\0';
    return outlen;
} /* format_hex */
