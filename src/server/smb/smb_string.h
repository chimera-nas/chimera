// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#ifdef _WIN32
#include <evpl/evpl_platform.h>
#include <limits.h>
#else // ifdef _WIN32
#include <iconv.h>
#endif // ifdef _WIN32
#include <stdint.h>

struct chimera_smb_iconv_ctx {
#ifdef _WIN32
    int     unused;
#else // ifdef _WIN32
    iconv_t utf16le_to_utf8;
    iconv_t utf8_to_utf16le;
#endif // ifdef _WIN32
};

static void
chimera_smb_iconv_init(struct chimera_smb_iconv_ctx *ctx)
{
#ifdef _WIN32
    ctx->unused = 0;
#else // ifdef _WIN32
    ctx->utf16le_to_utf8 = iconv_open("UTF-8", "UTF-16LE");
    ctx->utf8_to_utf16le = iconv_open("UTF-16LE", "UTF-8");
#endif // ifdef _WIN32
} /* chimera_smb_iconv_init */

static void
chimera_smb_iconv_destroy(struct chimera_smb_iconv_ctx *ctx)
{
#ifdef _WIN32
    (void) ctx;
#else // ifdef _WIN32
    iconv_close(ctx->utf16le_to_utf8);
    iconv_close(ctx->utf8_to_utf16le);
#endif // ifdef _WIN32
} /* chimera_smb_iconv_destroy */

static inline int
chimera_smb_slash_forward_to_back(
    char  *path,
    size_t len)
{
    char *p;

    for (p = path; p < path + len; p++) {
        if (*p == '/') {
            *p = '\\';
        }
    }
    return 0;
} /* chimera_smb_slash_forward_to_back */

static inline int
chimera_smb_slash_back_to_forward(
    char  *path,
    size_t len)
{
    char *p;

    for (p = path; p < path + len; p++) {
        if (*p == '\\') {
            *p = '/';
        }
    }
    return 0;
} /* chimera_smb_slash_back_to_forward */

static inline int
chimera_smb_utf16le_to_utf8(
    struct chimera_smb_iconv_ctx *ctx,
    const uint16_t               *src,
    size_t                        srclen,
    char                         *dst,
    size_t                        dstmaxlen)
{
#ifdef _WIN32
    int count;
    (void) ctx;
    if ((srclen & 1) || srclen / 2 > INT_MAX || !dstmaxlen || dstmaxlen > INT_MAX) {
        return -1;
    }
    if (!srclen) {
        dst[0] = 0;
        return 0;
    }
    if (dstmaxlen <= 1) {
        return -1;
    }
    count = WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, (const WCHAR *) src,
                                (int) (srclen / 2), dst, (int) dstmaxlen - 1, NULL, NULL);
    if (!count) {
        return -1;
    }
    dst[count] = 0;
    return count;
#else // ifdef _WIN32
    int    rc;
    size_t srcleft = srclen, dstleft;
    char  *dstleftp = dst;
    char  *srcleftp = (char *) src;

    if (!dstmaxlen) {
        return -1;
    }
    dstleft = dstmaxlen - 1;
    rc      = iconv(ctx->utf16le_to_utf8, &srcleftp, &srcleft, &dstleftp, &dstleft);

    if (rc != 0) {
        return -1;
    }

    *dstleftp = '\0';

    return dstleftp - dst;
#endif // ifdef _WIN32
} /* smb_utf16le_to_utf8 */

static inline int
chimera_smb_utf8_to_utf16le(
    struct chimera_smb_iconv_ctx *ctx,
    const char                   *src,
    size_t                        srclen,
    uint16_t                     *dst,
    size_t                        dstmaxlen)
{
#ifdef _WIN32
    int count;
    (void) ctx;
    if (srclen > INT_MAX || dstmaxlen > INT_MAX) {
        return -1;
    }
    if (!srclen) {
        return 0;
    }
    if (dstmaxlen < 2) {
        return -1;
    }
    count = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, src, (int) srclen,
                                (WCHAR *) dst, (int) (dstmaxlen / 2));
    return count ? count * 2 : -1;
#else // ifdef _WIN32
    int    rc;
    size_t dstlen, srcleft = srclen, dstleft = dstmaxlen;
    char  *dstleftp = (char *) dst;
    char  *srcleftp = (char *) src;

    rc = iconv(ctx->utf8_to_utf16le, &srcleftp, &srcleft, &dstleftp, &dstleft);

    if (rc != 0) {
        return -1;
    }

    dstlen = dstmaxlen - dstleft;

    return dstlen;
#endif // ifdef _WIN32
} /* chimera_smb_utf8_to_utf16le */