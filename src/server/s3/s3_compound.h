// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "s3_internal.h"
#include "vfs/vfs_compound.h"
#include "common/compound_retry.h"

/* The transfer limit is also the retry unit. Objects up to this size fit in
 * their initial compound; larger objects retain an open handle and submit
 * subsequent chunks individually. Never retain an unbounded HTTP body for a
 * retry. The configured I/O size defaults to 128 KiB. */
static inline uint32_t
chimera_s3_compound_chunk_size(struct chimera_s3_request *request)
{
    uint64_t size = request->thread->shared->config->io_size;

    if (size == 0) {
        size = 128 * 1024;
    }
    if (size > 1024 * 1024) {
        size = 1024 * 1024;
    }
    return (uint32_t) size;
} // chimera_s3_compound_chunk_size

/* Bucket resolution is part of the request's filesystem sequence, not an
 * earlier completed VFS request. The dispatcher owns a copy of bucket_path so
 * a concurrent bucket-map update cannot change the input during a retry. */
static inline struct chimera_vfs_compound *
chimera_s3_compound_alloc(struct chimera_s3_request *request)
{
    struct chimera_vfs_compound     *compound;
    struct chimera_server_s3_shared *shared = request->thread->shared;

    compound = chimera_vfs_compound_alloc(request->thread->vfs, &request->cred);
    if (request->bucket_path) {
        chimera_vfs_compound_add_putfh(compound, shared->root_fh, shared->root_fh_len);
        chimera_vfs_compound_add_lookup_path(compound, request->bucket_path,
                                             strlen(request->bucket_path),
                                             CHIMERA_VFS_ATTR_FH,
                                             CHIMERA_VFS_LOOKUP_FOLLOW);
    } else {
        chimera_vfs_compound_add_putfh(compound, request->bucket_fh, request->bucket_fhlen);
    }
    return compound;
} // chimera_s3_compound_alloc

/* Preserve the distinction between a missing configured bucket backing path
 * and a missing object after moving bucket lookup into each request compound.
 * Match the initial lookup input so this also works for later chunk compounds
 * whose first operations are unrelated to bucket resolution. */
static inline enum chimera_s3_status
chimera_s3_compound_error(
    struct chimera_vfs_compound *compound,
    struct chimera_s3_request   *request,
    enum chimera_s3_status       fallback)
{
    enum chimera_vfs_error status = chimera_vfs_compound_status(compound);

    if (status == CHIMERA_VFS_OK) {
        return CHIMERA_S3_STATUS_OK;
    }
    /* A rejected transaction did not establish any of its tentative lookup
     * results. In particular exhausted EAGAIN must not become NoSuchKey just
     * because that is this handler's ordinary lookup-error fallback. */
    if (chimera_vfs_compound_finish_status(compound) != CHIMERA_VFS_OK) {
        return chimera_s3_status_from_vfs(status, CHIMERA_S3_STATUS_INTERNAL_ERROR);
    }
    if (status == CHIMERA_VFS_ENOENT && request->bucket_path &&
        chimera_vfs_compound_num_ops(compound) > 1) {
        const struct chimera_vfs_compound_op *bucket = chimera_vfs_compound_op(compound, 1);
        size_t length = strlen(request->bucket_path);

        if (bucket->type == CHIMERA_VFS_COMPOUND_OP_LOOKUP_PATH &&
            bucket->status == CHIMERA_VFS_ENOENT && bucket->path_len == length &&
            !memcmp(bucket->path, request->bucket_path, length)) {
            return CHIMERA_S3_STATUS_NO_SUCH_BUCKET;
        }
    }
    return chimera_s3_status_from_vfs(status, fallback);
} // chimera_s3_compound_error
