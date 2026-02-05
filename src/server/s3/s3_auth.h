// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

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
 * Parameters:
 *   cred_cache: The credential cache to look up access keys
 *   request: The HTTP request to verify
 *
 * Returns:
 *   CHIMERA_S3_AUTH_OK on success, or an error code
 */
enum chimera_s3_auth_result
chimera_s3_auth_verify(
    struct chimera_s3_cred_cache *cred_cache,
    struct evpl_http_request     *request);

/*
 * Get a human-readable error message for an auth result
 */
const char *
chimera_s3_auth_error_message(
    enum chimera_s3_auth_result result);
