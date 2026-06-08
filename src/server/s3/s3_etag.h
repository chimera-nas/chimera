// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <time.h>
#include <string.h>

#include "vfs/vfs.h"

struct chimera_s3_etag_key {
    uint64_t        size;
    struct timespec mtime;
} __attribute__((packed));

struct chimera_s3_etag_ctx {
    struct chimera_s3_etag_key key;
    uint8_t                    fh[CHIMERA_VFS_FH_SIZE];
} __attribute__((packed));

static inline void
chimera_s3_compute_etag(
    uint64_t                       *result,
    const struct chimera_vfs_attrs *attr)
{
    struct chimera_s3_etag_ctx ctx;
    int                        len;


    chimera_s3_abort_if(!(attr->va_set_mask & CHIMERA_VFS_ATTR_FH) ||
                        !(attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) ||
                        !(attr->va_set_mask & CHIMERA_VFS_ATTR_MTIME),
                        "etag: missing required attributes");

    ctx.key.size  = attr->va_size;
    ctx.key.mtime = attr->va_mtime;
    memcpy(ctx.fh, attr->va_fh, attr->va_fh_len);
    len = sizeof(ctx.key) + attr->va_fh_len;

    *(XXH128_hash_t *) result = XXH3_128bits((const void *) &ctx, len);
} /* chimera_s3_compute_etag */

static inline void
chimera_s3_etag_hex(
    char                           *hex,
    int                             maxlen,
    const struct chimera_vfs_attrs *attr)
{
    uint64_t etag[2];
    char    *hexp = hex;

    chimera_s3_compute_etag(etag, attr);

    *hexp++ = '\"';

    hexp += format_hex(hexp, maxlen - (hexp - hex), etag, sizeof(etag));

    *hexp++ = '\"';
    *hexp   = '\0';
} /* chimera_s3_etag_hex */


static inline void
chimera_s3_attach_etag(
    struct evpl_http_request       *request,
    const struct chimera_vfs_attrs *attr)
{
    char hex[80];

    chimera_s3_etag_hex(hex, sizeof(hex), attr);

    evpl_http_request_add_header(request, "ETag", hex);
} /* chimera_s3_attach_etag */

/*
 * Parse an HTTP-date (RFC 7231 IMF-fixdate, e.g.
 * "Sat, 29 Oct 1994 19:43:31 GMT") into a time_t. Returns 0 on success and
 * stores the parsed UTC epoch seconds in *out, -1 if the value does not parse.
 * The result is treated as UTC via timegm() (HTTP-dates are always GMT).
 */
static inline int
chimera_s3_parse_http_date(
    const char *str,
    time_t     *out)
{
    struct tm tm;
    char     *ret;

    if (!str) {
        return -1;
    }

    memset(&tm, 0, sizeof(tm));

    /* IMF-fixdate is the form AWS/boto3 always sends. */
    ret = strptime(str, "%a, %d %b %Y %H:%M:%S GMT", &tm);

    if (!ret) {
        /* RFC 850 (obsolete) fallback: "Sunday, 06-Nov-94 08:49:37 GMT". */
        memset(&tm, 0, sizeof(tm));
        ret = strptime(str, "%A, %d-%b-%y %H:%M:%S GMT", &tm);
    }

    if (!ret) {
        return -1;
    }

    *out = timegm(&tm);

    return 0;
} /* chimera_s3_parse_http_date */

/*
 * Compare an If-Match / If-None-Match header value against an object's ETag.
 * The header may be:
 *   "*"               -> matches any existing object
 *   "etag"            -> a single (quoted or bare) entity tag
 *   "etag1", "etag2"  -> a comma-separated list (any element matches)
 * Returns 1 if any candidate matches the object's computed ETag, else 0.
 * Comparison is on the hex body only, so quoted ("xxx") and bare (xxx) forms
 * and weak validators (W/"xxx") all compare equal.
 */
static inline int
chimera_s3_etag_matches(
    const char                     *header,
    const struct chimera_vfs_attrs *attr)
{
    char        obj[80];
    const char *obj_hex;
    int         obj_len;
    const char *p = header;

    if (!header) {
        return 0;
    }

    chimera_s3_etag_hex(obj, sizeof(obj), attr);

    /* Strip surrounding quotes from the object ETag for body comparison. */
    obj_hex = obj;
    if (*obj_hex == '"') {
        obj_hex++;
    }
    obj_len = strlen(obj_hex);
    if (obj_len > 0 && obj_hex[obj_len - 1] == '"') {
        obj_len--;
    }

    while (*p) {
        const char *start;
        int         len;

        while (*p == ' ' || *p == ',') {
            p++;
        }

        if (*p == '\0') {
            break;
        }

        if (*p == '*') {
            return 1;
        }

        /* Skip a weak-validator prefix. */
        if (p[0] == 'W' && p[1] == '/') {
            p += 2;
        }

        if (*p == '"') {
            p++;
        }

        start = p;

        while (*p && *p != ',' && *p != '"') {
            p++;
        }

        len = p - start;

        if (*p == '"') {
            p++;
        }

        if (len == obj_len && memcmp(start, obj_hex, len) == 0) {
            return 1;
        }
    }

    return 0;
} /* chimera_s3_etag_matches */
