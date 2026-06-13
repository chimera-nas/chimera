// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * S3 object metadata <-> VFS extended attribute bridge.
 *
 * S3 object metadata (the Content-Type / Content-Encoding / ... system headers
 * plus arbitrary x-amz-meta-* user headers) is persisted on the backing file
 * as VFS extended attributes under a single "user.s3." namespace:
 *
 *   Content-Type        -> user.s3.content-type
 *   Content-Encoding     -> user.s3.content-encoding
 *   Content-Disposition  -> user.s3.content-disposition
 *   Cache-Control        -> user.s3.cache-control
 *   Expires              -> user.s3.expires
 *   x-amz-meta-<key>     -> user.s3.meta.<key>
 *
 * Because these are real extended attributes, metadata set through S3 is
 * visible to NFS/SMB clients (and vice-versa).
 */

struct evpl;
struct chimera_s3_request;
struct chimera_vfs_open_handle;

/* Common prefix of every S3 metadata xattr. */
#define CHIMERA_S3_XATTR_PREFIX     "user.s3."
#define CHIMERA_S3_XATTR_PREFIX_LEN (sizeof(CHIMERA_S3_XATTR_PREFIX) - 1)

/* Sub-namespace used for x-amz-meta-* user metadata. */
#define CHIMERA_S3_XATTR_META       "user.s3.meta."
#define CHIMERA_S3_XATTR_META_LEN   (sizeof(CHIMERA_S3_XATTR_META) - 1)

typedef void (*chimera_s3_metadata_done_t)(
    struct chimera_s3_request *request,
    int                        error,
    void                      *private_data);

/*
 * Capture the metadata headers from request->http_request and persist them as
 * extended attributes on `handle`, then invoke `done`. Suitable for PutObject
 * and CopyObject (x-amz-metadata-directive: REPLACE).
 */
void
chimera_s3_metadata_store_from_headers(
    struct chimera_s3_request      *request,
    struct chimera_vfs_open_handle *handle,
    chimera_s3_metadata_done_t      done,
    void                           *private_data);

/*
 * Copy every "user.s3.*" extended attribute from `src_handle` to `dst_handle`,
 * then invoke `done`. Suitable for CopyObject (x-amz-metadata-directive: COPY).
 */
void
chimera_s3_metadata_copy(
    struct chimera_s3_request      *request,
    struct chimera_vfs_open_handle *src_handle,
    struct chimera_vfs_open_handle *dst_handle,
    chimera_s3_metadata_done_t      done,
    void                           *private_data);

/*
 * Read the metadata extended attributes from `handle` and attach the matching
 * HTTP response headers (Content-Type, ..., x-amz-meta-*) to the request, then
 * invoke `done`. Used by GetObject / HeadObject. When no content-type xattr is
 * present the caller's default (application/octet-stream) is left in place.
 */
void
chimera_s3_metadata_attach_headers(
    struct chimera_s3_request      *request,
    struct chimera_vfs_open_handle *handle,
    chimera_s3_metadata_done_t      done,
    void                           *private_data);
