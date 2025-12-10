// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <iconv.h>
#include <stdint.h>

struct chimera_smb_iconv_ctx {
    iconv_t utf16le_to_utf8;
    iconv_t utf8_to_utf16le;
};

static void
chimera_smb_iconv_init(struct chimera_smb_iconv_ctx *ctx)
{
    ctx->utf16le_to_utf8 = iconv_open("UTF-8", "UTF-16LE");
    ctx->utf8_to_utf16le = iconv_open("UTF-16LE", "UTF-8");
} /* chimera_smb_iconv_init */

static void
chimera_smb_iconv_destroy(struct chimera_smb_iconv_ctx *ctx)
{
    iconv_close(ctx->utf16le_to_utf8);
    iconv_close(ctx->utf8_to_utf16le);
} /* chimera_smb_iconv_destroy */

static inline int
chimera_smb_slash_forward_to_back(
    char  *path,
    size_t len)
{
    char *p;

    for (p = path; *p; p++) {
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

    for (p = path; *p; p++) {
        if (*p == '\\') {
            *p = '/';
        }
    }
    return 0;
} /* chimera_smb_convert_slashes */

static inline int
chimera_smb_utf16le_to_utf8(
    struct chimera_smb_iconv_ctx *ctx,
    const uint16_t               *src,
    size_t                        srclen,
    char                         *dst,
    size_t                        dstmaxlen)
{
    int    rc;
    size_t srcleft = srclen, dstleft = dstmaxlen;
    char  *dstleftp = dst;
    char  *srcleftp = (char *) src;

    rc = iconv(ctx->utf16le_to_utf8, &srcleftp, &srcleft, &dstleftp, &dstleft);

    if (rc != 0) {
        return -1;
    }

    *dstleftp = '\0';

    return dstleftp - dst;
} /* smb_utf16le_to_utf8 */

static inline int
chimera_smb_utf8_to_utf16le(
    struct chimera_smb_iconv_ctx *ctx,
    const char                   *src,
    size_t                        srclen,
    uint16_t                     *dst,
    size_t                        dstmaxlen)
{
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
} /* chimera_smb_utf8_to_utf16le */