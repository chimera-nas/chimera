// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <gssapi/gssapi.h>
#include <gssapi/gssapi_krb5.h>

#include "smb_gssapi.h"
#include "smb_internal.h"

// OID for session key inquiry
// GSS_C_INQ_SSPI_SESSION_KEY = 1.2.840.113554.1.2.2.5.5
static gss_OID_desc session_key_oid = {
    11,
    (void *) "\x2a\x86\x48\x86\xf7\x12\x01\x02\x02\x05\x05"
};

int
smb_gssapi_init(
    struct smb_gssapi_ctx *ctx,
    const char            *keytab)
{
    OM_uint32 major, minor;

    memset(ctx, 0, sizeof(*ctx));
    ctx->gss_ctx     = GSS_C_NO_CONTEXT;
    ctx->server_cred = GSS_C_NO_CREDENTIAL;

    // Set keytab for server credentials
    // gsskrb5_register_acceptor_identity() tells GSSAPI which keytab to use
    // for accepting security contexts (server-side authentication)
    if (keytab && keytab[0]) {
        major = gsskrb5_register_acceptor_identity(keytab);
        if (GSS_ERROR(major)) {
            chimera_smb_debug("smb_gssapi: Failed to set keytab to %s, "
                              "falling back to KRB5_KTNAME", keytab);
            // Continue - KRB5_KTNAME environment variable may work
        } else {
            chimera_smb_debug("smb_gssapi: Using keytab %s", keytab);
        }
    }

    // Verify keytab is usable by acquiring credentials, but don't store
    // them. We pass GSS_C_NO_CREDENTIAL to gss_accept_sec_context()
    // instead, which dynamically looks up the matching principal from
    // the keytab. This avoids binding to a single service principal
    // when the keytab contains multiple (e.g. cifs/host1 and cifs/host2).
    {
        gss_cred_id_t cred  = GSS_C_NO_CREDENTIAL;
        gss_OID_set   mechs = GSS_C_NO_OID_SET;

        major = gss_create_empty_oid_set(&minor, &mechs);
        if (!GSS_ERROR(major)) {
            gss_add_oid_set_member(&minor, (gss_OID) gss_mech_krb5, &mechs);
            major = gss_acquire_cred(&minor,
                                     GSS_C_NO_NAME,
                                     GSS_C_INDEFINITE,
                                     mechs,
                                     GSS_C_ACCEPT,
                                     &cred,
                                     NULL,
                                     NULL);
            gss_release_oid_set(&minor, &mechs);

            if (GSS_ERROR(major)) {
                chimera_smb_debug("smb_gssapi: gss_acquire_cred failed: "
                                  "%u.%u (keytab may be invalid)", major, minor);
            } else {
                chimera_smb_debug("smb_gssapi: Keytab credentials verified");
                gss_release_cred(&minor, &cred);
            }
        }
    }

    ctx->initialized = 1;
    return 0;
} // smb_gssapi_init

void
smb_gssapi_cleanup(struct smb_gssapi_ctx *ctx)
{
    OM_uint32 minor;

    if (!ctx->initialized) {
        return;
    }

    if (ctx->gss_ctx != GSS_C_NO_CONTEXT) {
        gss_delete_sec_context(&minor, &ctx->gss_ctx, NULL);
        ctx->gss_ctx = GSS_C_NO_CONTEXT;
    }

    if (ctx->server_cred != GSS_C_NO_CREDENTIAL) {
        gss_release_cred(&minor, &ctx->server_cred);
        ctx->server_cred = GSS_C_NO_CREDENTIAL;
    }

    ctx->initialized   = 0;
    ctx->authenticated = 0;
} // smb_gssapi_cleanup

int
smb_gssapi_process(
    struct smb_gssapi_ctx *ctx,
    const uint8_t         *input,
    size_t                 input_len,
    uint8_t              **output,
    size_t                *output_len)
{
    OM_uint32        major, minor;
    gss_buffer_desc  input_token;
    gss_buffer_desc  output_token = GSS_C_EMPTY_BUFFER;
    gss_name_t       src_name     = GSS_C_NO_NAME;
    gss_buffer_desc  name_buf;
    gss_OID          mech_type;
    OM_uint32        ret_flags;
    gss_buffer_set_t data_set = GSS_C_NO_BUFFER_SET;

    *output     = NULL;
    *output_len = 0;

    if (!ctx->initialized) {
        chimera_smb_error("smb_gssapi: Context not initialized");
        return -1;
    }

    input_token.value  = (void *) input;
    input_token.length = input_len;

    major = gss_accept_sec_context(&minor,
                                   &ctx->gss_ctx,
                                   ctx->server_cred,
                                   &input_token,
                                   GSS_C_NO_CHANNEL_BINDINGS,
                                   &src_name,
                                   &mech_type,
                                   &output_token,
                                   &ret_flags,
                                   NULL,
                                   NULL);

    if (GSS_ERROR(major)) {
        chimera_smb_error("smb_gssapi: gss_accept_sec_context failed: %u.%u", major, minor);
        if (output_token.length > 0) {
            gss_release_buffer(&minor, &output_token);
        }
        return -1;
    }

    // Copy output token if any
    if (output_token.length > 0) {
        *output = malloc(output_token.length);
        if (*output) {
            memcpy(*output, output_token.value, output_token.length);
            *output_len = output_token.length;
        }
        gss_release_buffer(&minor, &output_token);
    }

    // Check if complete
    if (major == GSS_S_COMPLETE) {
        // Authentication complete - extract principal name
        if (src_name != GSS_C_NO_NAME) {
            major = gss_display_name(&minor, src_name, &name_buf, NULL);
            if (!GSS_ERROR(major) && name_buf.length > 0) {
                size_t copy_len = name_buf.length;
                if (copy_len >= sizeof(ctx->principal_name)) {
                    copy_len = sizeof(ctx->principal_name) - 1;
                }
                memcpy(ctx->principal_name, name_buf.value, copy_len);
                ctx->principal_name[copy_len] = '\0';
                gss_release_buffer(&minor, &name_buf);
            }
            gss_release_name(&minor, &src_name);
        }

        // Try to extract session key
        major = gss_inquire_sec_context_by_oid(&minor,
                                               ctx->gss_ctx,
                                               &session_key_oid,
                                               &data_set);

        if (!GSS_ERROR(major) && data_set && data_set->count > 0) {
            size_t key_len = data_set->elements[0].length;
            if (key_len > SMB_GSSAPI_SESSION_KEY_SIZE) {
                key_len = SMB_GSSAPI_SESSION_KEY_SIZE;
            }
            memcpy(ctx->session_key, data_set->elements[0].value, key_len);
            gss_release_buffer_set(&minor, &data_set);
        } else {
            // Session key extraction failed - use zeros
            chimera_smb_debug("smb_gssapi: Failed to extract session key");
            memset(ctx->session_key, 0, SMB_GSSAPI_SESSION_KEY_SIZE);
        }

        ctx->authenticated = 1;
        chimera_smb_info("smb_gssapi: Kerberos auth complete: principal=%s",
                         ctx->principal_name);
        return 0;
    }

    // Continue needed
    if (src_name != GSS_C_NO_NAME) {
        gss_release_name(&minor, &src_name);
    }
    return 1;
} // smb_gssapi_process

int
smb_gssapi_get_session_key(
    struct smb_gssapi_ctx *ctx,
    uint8_t               *key,
    size_t                 key_len)
{
    if (!ctx->authenticated) {
        return -1;
    }

    size_t copy_len = key_len < SMB_GSSAPI_SESSION_KEY_SIZE ?
        key_len : SMB_GSSAPI_SESSION_KEY_SIZE;

    memcpy(key, ctx->session_key, copy_len);
    return 0;
} // smb_gssapi_get_session_key

const char *
smb_gssapi_get_principal(struct smb_gssapi_ctx *ctx)
{
    return ctx->principal_name;
} // smb_gssapi_get_principal

int
smb_gssapi_is_authenticated(struct smb_gssapi_ctx *ctx)
{
    return ctx->authenticated;
} // smb_gssapi_is_authenticated
