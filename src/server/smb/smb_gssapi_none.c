// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#include "smb_gssapi.h"
#include <string.h>
#include <errno.h>
int
smb_gssapi_init(
    struct smb_gssapi_ctx *ctx,
    const char            *keytab)
{
    (void) keytab;
    memset(ctx, 0, sizeof(*ctx));
    errno = ENOTSUP;
    return -1;
} /* smb_gssapi_init */
void
smb_gssapi_cleanup(struct smb_gssapi_ctx *ctx)
{
    memset(ctx, 0, sizeof(*ctx));
} /* smb_gssapi_cleanup */
int
smb_gssapi_process(
    struct smb_gssapi_ctx *ctx,
    const uint8_t         *input,
    size_t                 input_len,
    uint8_t              **output,
    size_t                *output_len)
{
    (void) ctx; (void) input; (void) input_len;
    *output     = NULL;
    *output_len = 0;
    return -1;
} /* smb_gssapi_process */
int
smb_gssapi_get_session_key(
    struct smb_gssapi_ctx *ctx,
    uint8_t               *key,
    size_t                 len)
{
    (void) ctx; (void) key; (void) len;
    return -1;
} /* smb_gssapi_get_session_key */
const char *
smb_gssapi_get_principal(struct smb_gssapi_ctx *ctx)
{
    (void) ctx;
    return NULL;
} /* smb_gssapi_get_principal */
int
smb_gssapi_is_authenticated(struct smb_gssapi_ctx *ctx)
{
    (void) ctx;
    return 0;
} /* smb_gssapi_is_authenticated */
