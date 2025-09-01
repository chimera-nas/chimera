// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

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
