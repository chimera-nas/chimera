// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>
#include <pwd.h>
#include <gssapi/gssapi.h>
#include <gssapi/gssapi_krb5.h>
/* The IOV extension carries gss_verify_mic_iov, which lets krb5i check the
 * integrity databody where it already lies in the receive buffers instead of
 * gathering the whole call -- WRITE payload included -- into one block.
 * Absent it, the provider leaves verify_mic_iov NULL and rpc2 gathers. */
#if defined(__has_include)
#if __has_include(<gssapi/gssapi_ext.h>)
#include <gssapi/gssapi_ext.h>
#endif /* __has_include(<gssapi/gssapi_ext.h>) */
#endif /* defined(__has_include) */

#include "evpl/evpl.h"          /* struct evpl_iovec, for verify_mic_iov */
#include "nfs_gss.h"
#include "server/server.h"
#include "nfs_internal.h"
#include "vfs/sdk/vfs_cred.h"

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

#ifdef GSS_IOV_BUFFER_TYPE_MIC_TOKEN
/* Number of iovecs verified without touching the heap.  A received RPC is a
 * handful of segments; the malloc path is there for correctness, not speed. */
#define CHIMERA_GSS_IOV_STACK 16

static int
chimera_nfs_gss_verify_mic_iov(
    void                    *arg,
    void                    *gss_ctx,
    const struct evpl_iovec *iov,
    int                      niov,
    const void              *mic,
    size_t                   mic_len)
{
    OM_uint32            major, minor;
    gss_qop_t            qop;
    gss_iov_buffer_desc  stack_biov[CHIMERA_GSS_IOV_STACK];
    gss_iov_buffer_desc *biov = stack_biov;
    int                  i, n  = niov + 1;
    int                  rc;

    (void) arg;

    if (n > CHIMERA_GSS_IOV_STACK) {
        biov = calloc(n, sizeof(*biov));
        if (!biov) {
            return -1;
        }
    }

    for (i = 0; i < niov; i++) {
        biov[i].type          = GSS_IOV_BUFFER_TYPE_DATA;
        biov[i].buffer.length = iov[i].length;
        biov[i].buffer.value  = iov[i].data;
    }

    biov[niov].type          = GSS_IOV_BUFFER_TYPE_MIC_TOKEN;
    biov[niov].buffer.length = mic_len;
    biov[niov].buffer.value  = (void *) mic;

    major = gss_verify_mic_iov(&minor, (gss_ctx_id_t) gss_ctx, &qop, biov, n);
    rc    = GSS_ERROR(major) ? -1 : 0;

    if (biov != stack_biov) {
        free(biov);
    }

    return rc;
} /* chimera_nfs_gss_verify_mic_iov */
#endif /* GSS_IOV_BUFFER_TYPE_MIC_TOKEN */

static int
chimera_nfs_gss_wrap(
    void       *arg,
    void       *gss_ctx,
    const void *in,
    size_t      in_len,
    void       *out,
    size_t      out_cap,
    size_t     *r_out_len)
{
    OM_uint32       major, minor;
    int             conf_state;
    gss_buffer_desc i = { in_len, (void *) in };
    gss_buffer_desc o = GSS_C_EMPTY_BUFFER;

    (void) arg;
    *r_out_len = 0;

    major = gss_wrap(&minor, (gss_ctx_id_t) gss_ctx, 1 /* conf_req */,
                     GSS_C_QOP_DEFAULT, &i, &conf_state, &o);
    if (GSS_ERROR(major)) {
        return -1;
    }

    /* The mechanism owns o and we own out, so this copy is the one the GSS
     * interface makes unavoidable.  It used to be two: the result was copied
     * into a malloc'd buffer here purely so rpc2 could free() it, and rpc2
     * then copied that into the iovec it actually wanted.  rpc2 now hands
     * down the destination, so the middle buffer is gone. */
    if (o.length > out_cap) {
        gss_release_buffer(&minor, &o);
        return -1;
    }

    memcpy(out, o.value, o.length);
    *r_out_len = o.length;
    gss_release_buffer(&minor, &o);
    return 0;
} /* chimera_nfs_gss_wrap */

static int
chimera_nfs_gss_unwrap(
    void       *arg,
    void       *gss_ctx,
    const void *in,
    size_t      in_len,
    void       *out,
    size_t      out_cap,
    size_t     *r_out_len)
{
    OM_uint32       major, minor;
    int             conf_state;
    gss_qop_t       qop;
    gss_buffer_desc i = { in_len, (void *) in };
    gss_buffer_desc o = GSS_C_EMPTY_BUFFER;

    (void) arg;
    *r_out_len = 0;

    major = gss_unwrap(&minor, (gss_ctx_id_t) gss_ctx, &i, &o, &conf_state,
                       &qop);
    if (GSS_ERROR(major)) {
        return -1;
    }

    /* The mechanism owns o and we own out, so this copy is the one the GSS
     * interface makes unavoidable.  It used to be two: the result was copied
     * into a malloc'd buffer here purely so rpc2 could free() it, and rpc2
     * then copied that into the iovec it actually wanted.  rpc2 now hands
     * down the destination, so the middle buffer is gone. */
    if (o.length > out_cap) {
        gss_release_buffer(&minor, &o);
        return -1;
    }

    memcpy(out, o.value, o.length);
    *r_out_len = o.length;
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
#ifdef GSS_IOV_BUFFER_TYPE_MIC_TOKEN
    .verify_mic_iov = chimera_nfs_gss_verify_mic_iov,
#endif /* GSS_IOV_BUFFER_TYPE_MIC_TOKEN */
    .wrap    = chimera_nfs_gss_wrap,
    .unwrap  = chimera_nfs_gss_unwrap,
    .destroy = chimera_nfs_gss_destroy,
};

/*
 * The static principal map (see chimera_server_config_add_nfs_principal_map).
 *
 * Process-global for the same reason the acceptor identity above is: a
 * principal means one local identity to this process, and the acceptor keytab
 * is already registered process-wide by gsskrb5_register_acceptor_identity.
 */
struct chimera_nfs_gss_map_entry {
    char     principal[256];
    uint32_t uid;
    uint32_t gid;
    uint32_t num_gids;
    uint32_t gids[CHIMERA_VFS_CRED_MAX_GIDS];
};

static struct chimera_nfs_gss_map_entry chimera_nfs_gss_map[64];
static int                              chimera_nfs_gss_map_count;

void
chimera_nfs_gss_set_principal_map(const struct chimera_server_config *config)
{
    int         i, n;
    const char *principal;

    chimera_nfs_gss_map_count = 0;

    if (!config) {
        return;
    }

    n = chimera_server_config_get_nfs_principal_map_count(config);

    for (i = 0; i < n && i < (int) (sizeof(chimera_nfs_gss_map) /
                                    sizeof(chimera_nfs_gss_map[0])); i++) {
        struct chimera_nfs_gss_map_entry *e = &chimera_nfs_gss_map[i];
        const uint32_t                   *gids;
        uint32_t                          j;

        principal = chimera_server_config_get_nfs_principal_map_entry(
            config, i, &e->uid, &e->gid, &e->num_gids, &gids);

        if (!principal) {
            break;
        }

        snprintf(e->principal, sizeof(e->principal), "%s", principal);

        for (j = 0; j < e->num_gids; j++) {
            e->gids[j] = gids[j];
        }

        chimera_nfs_gss_map_count++;
    }

    if (chimera_nfs_gss_map_count) {
        chimera_nfs_info("rpcsec_gss: %d principal mapping(s) configured",
                         chimera_nfs_gss_map_count);
    }
} /* chimera_nfs_gss_set_principal_map */

/* The mapping for `name` (realm already stripped), or NULL. */
static const struct chimera_nfs_gss_map_entry *
chimera_nfs_gss_map_lookup(const char *name)
{
    int i;

    for (i = 0; i < chimera_nfs_gss_map_count; i++) {
        if (!strcmp(chimera_nfs_gss_map[i].principal, name)) {
            return &chimera_nfs_gss_map[i];
        }
    }

    return NULL;
} /* chimera_nfs_gss_map_lookup */

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
     * An explicit mapping wins over everything below it: it is the operator
     * saying what this principal means, which is not a question nsswitch can
     * answer for a Kerberos principal with no local account.  Matched on the
     * full name for a service principal and on the primary component for a
     * user one, which is why it is consulted before the '/' split below.
     */
    {
        const struct chimera_nfs_gss_map_entry *e;

        e = chimera_nfs_gss_map_lookup(user);

        if (!e && at) {
            /* Also allow an entry written with its realm. */
            e = chimera_nfs_gss_map_lookup(principal);
        }

        if (e) {
            chimera_vfs_cred_init_unix(cred, e->uid, e->gid, e->num_gids,
                                       e->num_gids ? e->gids : NULL);
            return;
        }
    }

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
