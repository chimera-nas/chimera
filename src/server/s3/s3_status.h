// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "vfs/sdk/vfs_error.h"

struct chimera_s3_request;

enum chimera_s3_status {
    CHIMERA_S3_STATUS_OK,
    CHIMERA_S3_STATUS_NOT_FOUND,
    CHIMERA_S3_STATUS_NOT_IMPLEMENTED,
    CHIMERA_S3_STATUS_BAD_REQUEST,
    CHIMERA_S3_STATUS_INTERNAL_ERROR,
    CHIMERA_S3_STATUS_ACCESS_DENIED,
    CHIMERA_S3_STATUS_PRECONDITION_FAILED,
    CHIMERA_S3_STATUS_REQUEST_TIMEOUT,
    CHIMERA_S3_STATUS_NO_SUCH_BUCKET,
    CHIMERA_S3_STATUS_NO_SUCH_KEY,
    CHIMERA_S3_STATUS_INVALID_ACCESS_KEY_ID,
    CHIMERA_S3_STATUS_SIGNATURE_MISMATCH,
    CHIMERA_S3_STATUS_MISSING_AUTH_HEADER,
    CHIMERA_S3_STATUS_NO_SUCH_UPLOAD,
    CHIMERA_S3_STATUS_INVALID_PART,
    CHIMERA_S3_STATUS_INVALID_PART_ORDER,
    CHIMERA_S3_STATUS_INVALID_PART_NUMBER,
    CHIMERA_S3_STATUS_ENTITY_TOO_SMALL,
    CHIMERA_S3_STATUS_MALFORMED_XML,
    CHIMERA_S3_STATUS_NO_CONTENT,
    CHIMERA_S3_STATUS_BUCKET_NOT_EMPTY,
    CHIMERA_S3_STATUS_METHOD_NOT_ALLOWED,
    CHIMERA_S3_STATUS_INVALID_RANGE,
    CHIMERA_S3_STATUS_INVALID_ARGUMENT,
    CHIMERA_S3_STATUS_INVALID_TAG,
    CHIMERA_S3_STATUS_NO_SUCH_TAG_SET,
};

/*
 * Map a VFS error to the S3 status a handler should report for it, falling back
 * to whatever that handler would otherwise have used.
 *
 * Access failures need this: now that each request runs under the identity of
 * the access key that authenticated it, the store answers EACCES/EPERM for work
 * the caller may not do, and reporting that as InternalError would tell a client
 * the server is broken when it is in fact enforcing permissions.
 */
static inline enum chimera_s3_status
chimera_s3_status_from_vfs(
    enum chimera_vfs_error error_code,
    enum chimera_s3_status fallback)
{
    switch (error_code) {
        case CHIMERA_VFS_EACCES:
        case CHIMERA_VFS_EPERM:
            return CHIMERA_S3_STATUS_ACCESS_DENIED;
        default:
            return fallback;
    } /* switch */
} /* chimera_s3_status_from_vfs */

const char *
chimera_s3_status_to_string(
    enum chimera_s3_status status);

int
chimera_s3_prepare_error_response(
    struct chimera_s3_request *request,
    char                      *buffer,
    int                       *length);