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
struct chimera_vfs_compound;
struct chimera_s3_metadata;

/* Compound builders. Capture request headers before submitting; during an
 * attempt only compound-private metadata is changed. Emit response headers
 * after the compound's aggregate status has been accepted. */
typedef int (*chimera_s3_metadata_continue_t)(
    struct chimera_vfs_compound *compound,
    void *private_data);

struct chimera_s3_metadata *chimera_s3_metadata_capture(
    struct chimera_s3_request *request);
struct chimera_s3_metadata *chimera_s3_metadata_read_alloc(void);
int chimera_s3_metadata_append_store(
    struct chimera_vfs_compound *compound,
    struct chimera_s3_metadata *metadata);
int chimera_s3_metadata_append_read(
    struct chimera_vfs_compound *compound,
    struct chimera_s3_metadata *metadata,
    chimera_s3_metadata_continue_t after,
    void *private_data);
int chimera_s3_metadata_append_copy(
    struct chimera_vfs_compound *compound,
    struct chimera_s3_metadata *metadata,
    struct chimera_vfs_open_handle *source,
    struct chimera_vfs_open_handle *destination,
    chimera_s3_metadata_continue_t after,
    void *private_data);
void chimera_s3_metadata_emit(
    struct chimera_vfs_compound *compound,
    struct chimera_s3_metadata *metadata,
    struct chimera_s3_request *request,
    int include_tag_count);
void chimera_s3_metadata_free(struct chimera_s3_metadata *metadata);

/* Common prefix of every S3 metadata xattr. */
#define CHIMERA_S3_XATTR_PREFIX     "user.s3."
#define CHIMERA_S3_XATTR_PREFIX_LEN (sizeof(CHIMERA_S3_XATTR_PREFIX) - 1)

/* Sub-namespace used for x-amz-meta-* user metadata. */
#define CHIMERA_S3_XATTR_META       "user.s3.meta."
#define CHIMERA_S3_XATTR_META_LEN   (sizeof(CHIMERA_S3_XATTR_META) - 1)
