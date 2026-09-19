// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>

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
 *
 * Every xattr operation here is issued as a VFS sequence.  A store is a run
 * of SETXATTR ops; a read is a LISTXATTRS whose names then fan out into
 * GETXATTR ops -- consecutive sequences, because the fan-out depends on the
 * list's answer.  Both are chunked at CHIMERA_VFS_COMPOUND_MAX_OPS.
 */

struct evpl;
struct chimera_s3_request;
struct chimera_vfs_open_handle;
struct chimera_vfs_compound;
struct chimera_s3_meta_store;

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
 * Capture the metadata headers from request->http_request into a store the
 * caller drives.  NULL when the request carries no metadata at all.  The
 * store owns the captured names and values, which is what lets a caller
 * append SETXATTR ops borrowing them to a sequence of its own.
 */
struct chimera_s3_meta_store *
chimera_s3_metadata_capture(
    struct chimera_s3_request *request);

/*
 * Append up to `budget` SETXATTR ops -- one per captured header not yet
 * appended -- to `compound`.  With `handle` non-NULL each op addresses that
 * handle; otherwise, with `handle_from` >= 0, the handle the op at that index
 * produced (chimera_vfs_compound_op_use_handle); otherwise the sequence's
 * current object.  Returns the number appended.  The values are BORROWED
 * from the store, which must outlive the submission.
 */
int
chimera_s3_metadata_add_ops(
    struct chimera_s3_meta_store   *store,
    struct chimera_vfs_compound    *compound,
    int                             handle_from,
    struct chimera_vfs_open_handle *handle,
    int                             budget);

/*
 * Persist every captured header not yet appended, as consecutive sequences of
 * SETXATTR ops on `handle`; then free the store and invoke `done` with error
 * set if any sequence failed.  Takes a reference on the request for the
 * duration.
 */
void
chimera_s3_metadata_store_drive(
    struct chimera_s3_meta_store   *store,
    struct chimera_s3_request      *request,
    struct chimera_vfs_open_handle *handle,
    chimera_s3_metadata_done_t      done,
    void                           *private_data);

/* Discard a store without persisting what it holds. */
void
chimera_s3_metadata_store_free(
    struct chimera_s3_meta_store *store);

/*
 * Capture the metadata headers from request->http_request and persist them as
 * extended attributes on `handle`, then invoke `done`.  capture + drive in
 * one call, for CopyObject (x-amz-metadata-directive: REPLACE).
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
 * Read the values of the "user.s3.*" names in `names` -- a LISTXATTRS result:
 * back-to-back NUL-terminated names, `names_len` bytes -- through `handle`,
 * and attach the matching HTTP response headers (Content-Type, ...,
 * x-amz-meta-*) to the request, then invoke `done`.  Used by GetObject /
 * HeadObject, whose head sequence lists the names itself so that the list
 * rides in the same sequence as the open.  When no content-type xattr is
 * present the caller's default (application/octet-stream) is left in place.
 * `names` is copied and need not outlive the call.
 */
void
chimera_s3_metadata_attach_from_list(
    struct chimera_s3_request      *request,
    struct chimera_vfs_open_handle *handle,
    const char                     *names,
    uint32_t                        names_len,
    chimera_s3_metadata_done_t      done,
    void                           *private_data);
