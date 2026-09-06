// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "vfs/sdk/vfs_cred.h"

struct evpl_http_request;
struct chimera_s3_cred_cache;

/*
 * AWS Signature V4 authentication result codes
 */
enum chimera_s3_auth_result {
    CHIMERA_S3_AUTH_OK = 0,
    CHIMERA_S3_AUTH_NO_AUTH_HEADER,
    CHIMERA_S3_AUTH_INVALID_AUTH_HEADER,
    CHIMERA_S3_AUTH_UNKNOWN_ACCESS_KEY,
    CHIMERA_S3_AUTH_SIGNATURE_MISMATCH,
    CHIMERA_S3_AUTH_DATE_MISSING,
    CHIMERA_S3_AUTH_DATE_EXPIRED,
};

/*
 * Verify AWS Signature V4 authentication on an incoming request.
 *
 * On success, out_cred is filled with the POSIX identity the matched access
 * key acts as; the caller runs every VFS operation for the request under it.
 * out_canon_id and out_display, when non-NULL, receive the key's S3 canonical
 * id and display name (buffers of CHIMERA_S3_CANON_ID_MAX / _DISPLAY_MAX).
 * The identity is copied out here because the cached credential is only valid
 * inside this call's RCU read section.  out_cred is untouched on failure.
 *
 * Parameters:
 *   cred_cache: The credential cache to look up access keys
 *   request: The HTTP request to verify
 *   out_cred: Filled with the authenticated identity on CHIMERA_S3_AUTH_OK
 *
 * Returns:
 *   CHIMERA_S3_AUTH_OK on success, or an error code
 */
enum chimera_s3_auth_result
chimera_s3_auth_verify(
    struct chimera_s3_cred_cache *cred_cache,
    struct evpl_http_request     *request,
    struct chimera_vfs_cred      *out_cred,
    char                         *out_canon_id,
    char                         *out_display);

/*
 * Get a human-readable error message for an auth result
 */
const char *
chimera_s3_auth_error_message(
    enum chimera_s3_auth_result result);
