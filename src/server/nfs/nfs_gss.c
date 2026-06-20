// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>
#include <pwd.h>
#include <gssapi/gssapi.h>
#include <gssapi/gssapi_krb5.h>

#include "nfs_gss.h"
#include "nfs_internal.h"
#include "vfs/vfs_cred.h"

/*
 * Feed one leg of the context-establishment token exchange into
 * gss_accept_sec_context().  The opaque *gss_ctx cookie is the gss_ctx_id_t
 * itself (a pointer), carried across continuation legs by libevpl.
 */
static int
chimera_nfs_gss_accept(
    void       *arg,
    void      **gss_ctx,
    const void *in_token,
    size_t      in_len,
    void      **out_token,
    size_t     *out_len,
    int        *complete,
    char       *principal,
    size_t      principal_sz)
{
    OM_uint32       major, minor;
    gss_ctx_id_t    gctx = *gss_ctx ? (gss_ctx_id_t) *gss_ctx : GSS_C_NO_CONTEXT;
    gss_buffer_desc itok = { in_len, (void *) in_token };
    gss_buffer_desc otok = GSS_C_EMPTY_BUFFER;
    gss_name_t      src  = GSS_C_NO_NAME;
    OM_uint32       flags;

    (void) arg;

    *out_token = NULL;
    *out_len   = 0;
    *complete  = 0;

    major = gss_accept_sec_context(&minor, &gctx, GSS_C_NO_CREDENTIAL, &itok,
                                   GSS_C_NO_CHANNEL_BINDINGS, &src, NULL,
                                   &otok, &flags, NULL, NULL);

    /* gctx may be allocated even on a continue or a soft failure. */
    *gss_ctx = gctx;

    if (GSS_ERROR(major)) {
        chimera_nfs_error("rpcsec_gss: accept_sec_context failed: %u.%u",
                          major, minor);
        if (otok.length) {
            gss_release_buffer(&minor, &otok);
        }
        if (src != GSS_C_NO_NAME) {
            gss_release_name(&minor, &src);
        }
        return -1;
    }

    if (otok.length) {
        *out_token = malloc(otok.length);
        if (*out_token) {
            memcpy(*out_token, otok.value, otok.length);
            *out_len = otok.length;
        }
        gss_release_buffer(&minor, &otok);
    }

    if (major == GSS_S_COMPLETE) {
        *complete = 1;

        if (src != GSS_C_NO_NAME) {
            gss_buffer_desc nb = GSS_C_EMPTY_BUFFER;

            if (!GSS_ERROR(gss_display_name(&minor, src, &nb, NULL)) &&
                nb.length) {
                size_t n = nb.length < principal_sz - 1 ?
                    nb.length : principal_sz - 1;
                memcpy(principal, nb.value, n);
                principal[n] = '\0';
                gss_release_buffer(&minor, &nb);
            }
        }
    }

    if (src != GSS_C_NO_NAME) {
        gss_release_name(&minor, &src);
    }

    return 0;
} /* chimera_nfs_gss_accept */

static int
chimera_nfs_gss_get_mic(
    void       *arg,
    void       *gss_ctx,
    const void *msg,
    size_t      msg_len,
    void      **mic,
    size_t     *mic_len)
{
    OM_uint32       major, minor;
    gss_buffer_desc m   = { msg_len, (void *) msg };
    gss_buffer_desc out = GSS_C_EMPTY_BUFFER;

    (void) arg;
    *mic     = NULL;
    *mic_len = 0;

    major = gss_get_mic(&minor, (gss_ctx_id_t) gss_ctx, GSS_C_QOP_DEFAULT,
                        &m, &out);
    if (GSS_ERROR(major)) {
        return -1;
    }

    *mic = malloc(out.length);
    if (!*mic) {
        gss_release_buffer(&minor, &out);
        return -1;
    }
    memcpy(*mic, out.value, out.length);
    *mic_len = out.length;
    gss_release_buffer(&minor, &out);
    return 0;
} /* chimera_nfs_gss_get_mic */

static int
chimera_nfs_gss_verify_mic(
    void       *arg,
    void       *gss_ctx,
    const void *msg,
    size_t      msg_len,
    const void *mic,
    size_t      mic_len)
{
    OM_uint32       major, minor;
    gss_qop_t       qop;
    gss_buffer_desc m = { msg_len, (void *) msg };
    gss_buffer_desc t = { mic_len, (void *) mic };

    (void) arg;

    major = gss_verify_mic(&minor, (gss_ctx_id_t) gss_ctx, &m, &t, &qop);
    return GSS_ERROR(major) ? -1 : 0;
} /* chimera_nfs_gss_verify_mic */

static int
chimera_nfs_gss_wrap(
    void       *arg,
    void       *gss_ctx,
    const void *in,
    size_t      in_len,
    void      **out,
    size_t     *out_len)
{
    OM_uint32       major, minor;
    int             conf_state;
    gss_buffer_desc i = { in_len, (void *) in };
    gss_buffer_desc o = GSS_C_EMPTY_BUFFER;

    (void) arg;
    *out     = NULL;
    *out_len = 0;

    major = gss_wrap(&minor, (gss_ctx_id_t) gss_ctx, 1 /* conf_req */,
                     GSS_C_QOP_DEFAULT, &i, &conf_state, &o);
    if (GSS_ERROR(major)) {
        return -1;
    }

    *out = malloc(o.length);
    if (!*out) {
        gss_release_buffer(&minor, &o);
        return -1;
    }
    memcpy(*out, o.value, o.length);
    *out_len = o.length;
    gss_release_buffer(&minor, &o);
    return 0;
} /* chimera_nfs_gss_wrap */

static int
chimera_nfs_gss_unwrap(
    void       *arg,
    void       *gss_ctx,
    const void *in,
    size_t      in_len,
    void      **out,
    size_t     *out_len)
{
    OM_uint32       major, minor;
    int             conf_state;
    gss_qop_t       qop;
    gss_buffer_desc i = { in_len, (void *) in };
    gss_buffer_desc o = GSS_C_EMPTY_BUFFER;

    (void) arg;
    *out     = NULL;
    *out_len = 0;

    major = gss_unwrap(&minor, (gss_ctx_id_t) gss_ctx, &i, &o, &conf_state,
                       &qop);
    if (GSS_ERROR(major)) {
        return -1;
    }

    *out = malloc(o.length);
    if (!*out) {
        gss_release_buffer(&minor, &o);
        return -1;
    }
    memcpy(*out, o.value, o.length);
    *out_len = o.length;
    gss_release_buffer(&minor, &o);
    return 0;
} /* chimera_nfs_gss_unwrap */

static void
chimera_nfs_gss_destroy(
    void *arg,
    void *gss_ctx)
{
    OM_uint32    minor;
    gss_ctx_id_t g = (gss_ctx_id_t) gss_ctx;

    (void) arg;

    if (g != GSS_C_NO_CONTEXT) {
        gss_delete_sec_context(&minor, &g, GSS_C_NO_BUFFER);
    }
} /* chimera_nfs_gss_destroy */

const struct evpl_rpc2_gss_provider chimera_nfs_gss_provider = {
    .accept     = chimera_nfs_gss_accept,
    .get_mic    = chimera_nfs_gss_get_mic,
    .verify_mic = chimera_nfs_gss_verify_mic,
    .wrap       = chimera_nfs_gss_wrap,
    .unwrap     = chimera_nfs_gss_unwrap,
    .destroy    = chimera_nfs_gss_destroy,
};

int
chimera_nfs_gss_init(const char *keytab)
{
    OM_uint32 major;

    if (keytab && keytab[0]) {
        major = gsskrb5_register_acceptor_identity(keytab);
        if (GSS_ERROR(major)) {
            chimera_nfs_error("rpcsec_gss: failed to register keytab '%s'",
                              keytab);
            return -1;
        }
        chimera_nfs_info("rpcsec_gss: using keytab '%s'", keytab);
    } else {
        chimera_nfs_info("rpcsec_gss: using default keytab (KRB5_KTNAME)");
    }

    return 0;
} /* chimera_nfs_gss_init */

void
chimera_nfs_gss_map_principal(
    const char              *principal,
    struct chimera_vfs_cred *cred)
{
    char           user[256];
    const char    *at;
    size_t         ulen;
    struct passwd  pw;
    struct passwd *result = NULL;
    char           buf[4096];

    if (!principal || !principal[0]) {
        chimera_vfs_cred_init_anonymous(cred, CHIMERA_VFS_ANON_UID,
                                        CHIMERA_VFS_ANON_GID);
        return;
    }

    /* Take the primary component up to '@REALM'. */
    at   = strchr(principal, '@');
    ulen = at ? (size_t) (at - principal) : strlen(principal);
    if (ulen >= sizeof(user)) {
        ulen = sizeof(user) - 1;
    }
    memcpy(user, principal, ulen);
    user[ulen] = '\0';

    /*
     * A machine/service principal ("host/h", "nfs/h", "root/h") is the
     * credential rpc.gssd presents for the mount itself and for root's I/O on
     * the client.  Map it to root: it is the authenticated machine identity,
     * which is exactly the "no root squash" trust chimera already extends to
     * AUTH_SYS by default.  (A later per-export sec/squash policy can refine
     * this; see the export-options work.)
     */
    if (strchr(user, '/')) {
        chimera_vfs_cred_init_unix(cred, 0, 0, 0, NULL);
        return;
    }

    if (getpwnam_r(user, &pw, buf, sizeof(buf), &result) == 0 && result) {
        chimera_vfs_cred_init_unix(cred, pw.pw_uid, pw.pw_gid, 0, NULL);
        return;
    }

    /* A user principal with no local account squashes to anonymous. */
    chimera_vfs_cred_init_anonymous(cred, CHIMERA_VFS_ANON_UID,
                                    CHIMERA_VFS_ANON_GID);
} /* chimera_nfs_gss_map_principal */
