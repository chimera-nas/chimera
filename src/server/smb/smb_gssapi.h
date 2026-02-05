// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stddef.h>
#include <gssapi/gssapi.h>
#include <gssapi/gssapi_krb5.h>

#define SMB_GSSAPI_SESSION_KEY_SIZE 16

struct chimera_smb_auth_config;

// GSSAPI/Kerberos authentication context (per-connection)
struct smb_gssapi_ctx {
    gss_ctx_id_t  gss_ctx;
    gss_cred_id_t server_cred;
    char          principal_name[256];
    uint8_t       session_key[SMB_GSSAPI_SESSION_KEY_SIZE];
    int           authenticated;
    int           initialized;
};

// Initialize GSSAPI context
// keytab can be NULL to use default keytab
int
smb_gssapi_init(
    struct smb_gssapi_ctx *ctx,
    const char            *keytab);

// Cleanup GSSAPI context
void
smb_gssapi_cleanup(
    struct smb_gssapi_ctx *ctx);

// Process incoming GSSAPI/Kerberos token
// Returns: 0 = success (authentication complete)
//          1 = continue needed
//         -1 = error
int
smb_gssapi_process(
    struct smb_gssapi_ctx *ctx,
    const uint8_t         *input,
    size_t                 input_len,
    uint8_t              **output,
    size_t                *output_len);

// Get the session key after successful authentication
int
smb_gssapi_get_session_key(
    struct smb_gssapi_ctx *ctx,
    uint8_t               *key,
    size_t                 key_len);

// Get the authenticated principal name
const char *
smb_gssapi_get_principal(
    struct smb_gssapi_ctx *ctx);

// Check if authentication completed successfully
int
smb_gssapi_is_authenticated(
    struct smb_gssapi_ctx *ctx);
