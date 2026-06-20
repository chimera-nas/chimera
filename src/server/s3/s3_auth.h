// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>

#include "s3_cred_cache.h"

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
 * Resolved identity of an authenticated S3 request. Filled by
 * chimera_s3_auth_verify on CHIMERA_S3_AUTH_OK. The string fields are copied
 * out of the (RCU-protected) credential cache entry so the caller need not
 * hold a read lock.
 */
struct chimera_s3_auth_identity {
    uint32_t uid;
    uint32_t gid;
    char     canon_id[CHIMERA_S3_CANON_ID_MAX];
    char     display_name[CHIMERA_S3_DISPLAY_MAX];
};

/*
 * Verify AWS Signature V4 authentication on an incoming request.
 *
 * Parameters:
 *   cred_cache: The credential cache to look up access keys
 *   request: The HTTP request to verify
 *   identity: On CHIMERA_S3_AUTH_OK, filled with the matched principal's
 *             filesystem identity and canonical id/display name (may be NULL).
 *
 * Returns:
 *   CHIMERA_S3_AUTH_OK on success, or an error code
 */
enum chimera_s3_auth_result
chimera_s3_auth_verify(
    struct chimera_s3_cred_cache    *cred_cache,
    struct evpl_http_request        *request,
    struct chimera_s3_auth_identity *identity);

/*
 * Get a human-readable error message for an auth result
 */
const char *
chimera_s3_auth_error_message(
    enum chimera_s3_auth_result result);
