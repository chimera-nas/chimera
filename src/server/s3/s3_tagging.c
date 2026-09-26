// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * S3 object (and bucket) tagging, stored as filesystem extended attributes.
 *
 * Tags are stored as xattrs under the "user.s3.tag.<key>" namespace so they
 * are visible through NFS/SMB as well, on theme for chimera's mixed-mode
 * filesystem-backed object store.
 *
 *   PutObject  + x-amz-tagging:  store the (URL-encoded k=v&...) header tags
 *   PUT  object?tagging:         replace the full tag set from a <Tagging> body
 *   GET  object?tagging:         read the tag xattrs and emit <Tagging> XML
 *   DELETE object?tagging:       remove the tag xattrs (204)
 *
 * S3 tagging limits are enforced: <= 10 tags, key <= 128, value <= 256 chars;
 * a violation returns 400 InvalidTag.
 */

#define _GNU_SOURCE /* memmem */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "evpl/evpl.h"
#include "evpl/evpl_http.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "s3_internal.h"
#include "s3_compound.h"
#include "s3_procs.h"
#include "s3_tagging.h"

#define CHIMERA_S3_TAG_BODY_HARD_CAP (256 * 1024)
#define CHIMERA_S3_TAG_XATTR_BUFSZ   8192
#define CHIMERA_S3_TAG_VAL_BUFSZ     (CHIMERA_S3_TAG_MAX_VAL_LEN + 1)

/* ----- ctx lifecycle ----- */

static struct chimera_s3_tagging_ctx *
chimera_s3_tagging_ctx_alloc(struct chimera_s3_request *request)
{
    if (!request->tagging) {
        request->tagging = calloc(1, sizeof(*request->tagging));
    }
    return request->tagging;
} /* chimera_s3_tagging_ctx_alloc */

static void
chimera_s3_tagging_ctx_free(struct chimera_s3_request *request)
{
    struct chimera_s3_tagging_ctx *ctx = request->tagging;

    if (!ctx) {
        return;
    }
    free(ctx->body_buf);
    free(ctx->names);
    free(ctx->resp_buf);
    free(ctx);
    request->tagging = NULL;
} /* chimera_s3_tagging_ctx_free */

void
chimera_s3_tagging_request_cleanup(struct chimera_s3_request *request)
{
    struct chimera_s3_tagging_ctx *ctx = request->tagging;

    if (!ctx) {
        return;
    }
    chimera_s3_tagging_ctx_free(request);
} /* chimera_s3_tagging_request_cleanup */

/* ----- percent + xml decoding ----- */

static int
chimera_s3_tag_hexval(int c)
{
    if (c >= '0' && c <= '9') {
        return c - '0';
    } else if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    } else if (c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
    }
    return -1;
} /* chimera_s3_tag_hexval */

/* Percent-decode src[0..srclen) into dst (NUL-terminated). '+' becomes space
 * (x-amz-tagging is form-urlencoded). Returns decoded length, or -1 if the
 * result would exceed dstcap-1. */
static int
chimera_s3_tag_pct_decode(
    char       *dst,
    int         dstcap,
    const char *src,
    int         srclen)
{
    int o = 0, i = 0;

    while (i < srclen) {
        int c = (unsigned char) src[i];

        if (c == '%' && i + 2 < srclen) {
            int hi = chimera_s3_tag_hexval((unsigned char) src[i + 1]);
            int lo = chimera_s3_tag_hexval((unsigned char) src[i + 2]);
            if (hi >= 0 && lo >= 0) {
                c  = (hi << 4) | lo;
                i += 3;
                goto emit;
            }
        }
        if (c == '+') {
            c = ' ';
        }
        i++;
 emit:
        if (o >= dstcap - 1) {
            return -1;
        }
        dst[o++] = (char) c;
    }
    dst[o] = '\0';
    return o;
} /* chimera_s3_tag_pct_decode */

static int
chimera_s3_tag_xml_unescape(
    char *s,
    int   len)
{
    int r = 0, w = 0;

    while (r < len) {
        if (s[r] == '&') {
            if (r + 5 <= len && memcmp(s + r, "&amp;", 5) == 0) {
                s[w++] = '&'; r += 5; continue;
            }
            if (r + 4 <= len && memcmp(s + r, "&lt;", 4) == 0) {
                s[w++] = '<'; r += 4; continue;
            }
            if (r + 4 <= len && memcmp(s + r, "&gt;", 4) == 0) {
                s[w++] = '>'; r += 4; continue;
            }
            if (r + 6 <= len && memcmp(s + r, "&quot;", 6) == 0) {
                s[w++] = '"'; r += 6; continue;
            }
            if (r + 6 <= len && memcmp(s + r, "&apos;", 6) == 0) {
                s[w++] = '\''; r += 6; continue;
            }
        }
        s[w++] = s[r++];
    }
    return w;
} /* chimera_s3_tag_xml_unescape */

/* ----- tag-set validation / insertion ----- */

/* Append one (key,value) to the ctx tag set, validating the S3 limits.
 * Returns 0 on success, -1 on a limit violation. */
static int
chimera_s3_tag_add(
    struct chimera_s3_tagging_ctx *ctx,
    const char                    *key,
    int                            key_len,
    const char                    *val,
    int                            val_len)
{
    struct chimera_s3_tag *t;

    if (key_len <= 0 || key_len > CHIMERA_S3_TAG_MAX_KEY_LEN) {
        return -1;
    }
    if (val_len < 0 || val_len > CHIMERA_S3_TAG_MAX_VAL_LEN) {
        return -1;
    }
    if (ctx->n_tags >= CHIMERA_S3_TAG_MAX_TAGS) {
        return -1;
    }

    /* Duplicate keys are not permitted. */
    for (int i = 0; i < ctx->n_tags; i++) {
        if ((int) strlen(ctx->tags[i].key) == key_len &&
            memcmp(ctx->tags[i].key, key, key_len) == 0) {
            return -1;
        }
    }

    t = &ctx->tags[ctx->n_tags++];
    memcpy(t->key, key, key_len);
    t->key[key_len] = '\0';
    memcpy(t->val, val, val_len);
    t->val[val_len] = '\0';
    return 0;
} /* chimera_s3_tag_add */

int
chimera_s3_tagging_parse_header(
    struct chimera_s3_tagging_ctx *ctx,
    const char                    *value)
{
    const char *p = value;
    char        key[CHIMERA_S3_TAG_MAX_KEY_LEN * 3 + 1];
    char        val[CHIMERA_S3_TAG_MAX_VAL_LEN * 3 + 1];

    ctx->n_tags = 0;

    while (*p) {
        const char *kstart = p, *kend, *vstart = "", *vend;
        int         klen, vlen;

        while (*p && *p != '=' && *p != '&') {
            p++;
        }
        kend = p;

        if (*p == '=') {
            vstart = ++p;
            while (*p && *p != '&') {
                p++;
            }
            vend = p;
        } else {
            vend = vstart;
        }

        klen = chimera_s3_tag_pct_decode(key, sizeof(key), kstart, kend - kstart);
        vlen = chimera_s3_tag_pct_decode(val, sizeof(val), vstart, vend - vstart);

        if (klen < 0 || vlen < 0) {
            return -1;
        }

        if (klen > 0 && chimera_s3_tag_add(ctx, key, klen, val, vlen) != 0) {
            return -1;
        }

        if (*p == '&') {
            p++;
        }
    }

    return 0;
} /* chimera_s3_tagging_parse_header */

/* Parse a <Tagging><TagSet><Tag><Key>..</Key><Value>..</Value></Tag>...
 * document into the ctx tag set. Returns 0 on success, -1 on malformed XML,
 * -2 on a tag-limit violation. The buffer is modified in place (unescape). */
static int
chimera_s3_tagging_parse_xml(
    struct chimera_s3_tagging_ctx *ctx,
    char                          *body,
    int                            body_len)
{
    char *p   = body;
    char *end = body + body_len;

    ctx->n_tags = 0;

    while (p < end) {
        char *kopen, *kclose, *vopen, *vclose, *tagclose;
        int   klen, vlen;

        kopen = memmem(p, end - p, "<Key>", 5);
        if (!kopen) {
            break;
        }
        kopen += 5;
        kclose = memmem(kopen, end - kopen, "</Key>", 6);
        if (!kclose) {
            return -1;
        }

        vopen = memmem(kclose, end - kclose, "<Value>", 7);
        if (!vopen) {
            return -1;
        }
        vopen += 7;
        vclose = memmem(vopen, end - vopen, "</Value>", 8);
        if (!vclose) {
            return -1;
        }

        klen = chimera_s3_tag_xml_unescape(kopen, kclose - kopen);
        vlen = chimera_s3_tag_xml_unescape(vopen, vclose - vopen);

        if (chimera_s3_tag_add(ctx, kopen, klen, vopen, vlen) != 0) {
            return -2;
        }

        tagclose = memmem(vclose, end - vclose, "</Tag>", 6);
        p        = tagclose ? tagclose + 6 : (char *) end;
    }

    return 0;
} /* chimera_s3_tagging_parse_xml */

/* ----- response (<Tagging>) builder ----- */

static void
chimera_s3_tag_resp_append(
    struct chimera_s3_tagging_ctx *ctx,
    const char                    *s,
    int                            len)
{
    if (ctx->resp_len + len > ctx->resp_cap) {
        int new_cap = ctx->resp_cap ? ctx->resp_cap * 2 : 4096;
        while (new_cap < ctx->resp_len + len) {
            new_cap *= 2;
        }
        ctx->resp_buf = realloc(ctx->resp_buf, new_cap);
        ctx->resp_cap = new_cap;
    }
    memcpy(ctx->resp_buf + ctx->resp_len, s, len);
    ctx->resp_len += len;
} /* chimera_s3_tag_resp_append */

static void
chimera_s3_tag_resp_escaped(
    struct chimera_s3_tagging_ctx *ctx,
    const char                    *s)
{
    for (; *s; s++) {
        switch (*s) {
            case '&':
                chimera_s3_tag_resp_append(ctx, "&amp;", 5); break;
            case '<':
                chimera_s3_tag_resp_append(ctx, "&lt;", 4); break;
            case '>':
                chimera_s3_tag_resp_append(ctx, "&gt;", 4); break;
            case '"':
                chimera_s3_tag_resp_append(ctx, "&quot;", 6); break;
            case '\'':
                chimera_s3_tag_resp_append(ctx, "&apos;", 6); break;
            default:
                chimera_s3_tag_resp_append(ctx, s, 1); break;
        } /* switch */
    }
} /* chimera_s3_tag_resp_escaped */

static int
chimera_s3_tag_cmp(
    const void *a,
    const void *b)
{
    const struct chimera_s3_tag *ta = a;
    const struct chimera_s3_tag *tb = b;

    return strcmp(ta->key, tb->key);
} /* chimera_s3_tag_cmp */

/* Render the ctx tag set (sorted by key, for stable test output) into resp_buf
 * and dispatch a 200 application/xml response. */
static void
chimera_s3_tagging_send_xml(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    struct chimera_s3_tagging_ctx *ctx = request->tagging;
    struct evpl_iovec              iov;
    char                          *dst;
    int                            i;

    qsort(ctx->tags, ctx->n_tags, sizeof(ctx->tags[0]), chimera_s3_tag_cmp);

    ctx->resp_len = 0;
    chimera_s3_tag_resp_append(ctx,
                               "<?xml version=\"1.0\" encoding=\"UTF-8\"?>", 38);
    chimera_s3_tag_resp_append(ctx, "<Tagging><TagSet>", 17);
    for (i = 0; i < ctx->n_tags; i++) {
        chimera_s3_tag_resp_append(ctx, "<Tag><Key>", 10);
        chimera_s3_tag_resp_escaped(ctx, ctx->tags[i].key);
        chimera_s3_tag_resp_append(ctx, "</Key><Value>", 13);
        chimera_s3_tag_resp_escaped(ctx, ctx->tags[i].val);
        chimera_s3_tag_resp_append(ctx, "</Value></Tag>", 14);
    }
    chimera_s3_tag_resp_append(ctx, "</TagSet></Tagging>", 19);

    evpl_iovec_alloc(evpl, ctx->resp_len, 0, 1, 0, &iov);
    dst = evpl_iovec_data(&iov);
    memcpy(dst, ctx->resp_buf, ctx->resp_len);
    evpl_iovec_set_length(&iov, ctx->resp_len);
    chimera_s3_response_add_datav(evpl, request, &iov, 1);

    request->file_length      = ctx->resp_len;
    request->file_real_length = ctx->resp_len;
    request->file_offset      = 0;
    request->is_list          = 1; /* application/xml Content-Type */
    request->status           = CHIMERA_S3_STATUS_OK;
    request->vfs_state        = CHIMERA_S3_VFS_STATE_COMPLETE;

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }
} /* chimera_s3_tagging_send_xml */

/* Terminal response helper; compound teardown has already released handles. */
static void
chimera_s3_tagging_finish(
    struct evpl               *evpl,
    struct chimera_s3_request *request,
    enum chimera_s3_status     status)
{
    /* No body: clear any stale Range-derived lengths so s3_server_respond
     * emits a clean 200/204 rather than a spurious 206. */
    request->file_offset      = 0;
    request->file_length      = 0;
    request->file_real_length = 0;

    request->status    = status;
    request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;

    chimera_s3_tagging_ctx_free(request);

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }
} /* chimera_s3_tagging_finish */

/* All intermediate callbacks below stage request-private data or describe the
 * next operations. Only the compound completion emits a response. On replay,
 * the initial LISTXATTRS resets staged names and GET results before rebuilding
 * the dynamic suffix. Parsed PUT tags remain immutable. */
enum chimera_s3_tagging_op {
    CHIMERA_S3_TAGGING_GET,
    CHIMERA_S3_TAGGING_PUT,
    CHIMERA_S3_TAGGING_DELETE,
};

int
chimera_s3_tagging_compound_store(
    struct chimera_vfs_compound *compound,
    struct chimera_s3_request   *request)
{
    struct chimera_s3_tagging_ctx *ctx = request->tagging;
    char                           name[CHIMERA_S3_TAG_PREFIX_LEN + CHIMERA_S3_TAG_MAX_KEY_LEN + 1];

    if (!ctx) {
        return 0;
    }
    for (int i = 0; i < ctx->n_tags; i++) {
        int len = snprintf(name, sizeof(name), CHIMERA_S3_TAG_PREFIX "%s", ctx->tags[i].key);
        if (chimera_vfs_compound_add_setxattr(compound, 0, name, len,
                                              ctx->tags[i].val, strlen(ctx->tags[i].val)) < 0) {
            return -1;
        }
    }
    return 0;
} /* chimera_s3_tagging_compound_store */

static void
chimera_s3_tagging_ignore_missing(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    /* Preserve the old best-effort per-name removal policy. This is an
     * operation result, never a backend compound-finish error. */
    *status = CHIMERA_VFS_OK;
} /* chimera_s3_tagging_ignore_missing */

static void
chimera_s3_tagging_value(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct chimera_s3_request            *request = private_data;
    struct chimera_s3_tagging_ctx        *ctx     = request->tagging;
    const struct chimera_vfs_compound_op *op      = chimera_vfs_compound_op(compound, index);
    int                                   keylen  = op->name_len - CHIMERA_S3_TAG_PREFIX_LEN;

    if (*status == CHIMERA_VFS_OK && ctx->n_tags < CHIMERA_S3_TAG_MAX_TAGS &&
        keylen > 0 && keylen <= CHIMERA_S3_TAG_MAX_KEY_LEN) {
        struct chimera_s3_tag *tag = &ctx->tags[ctx->n_tags++];
        unsigned int           len = op->buffer_len;

        memcpy(tag->key, op->name + CHIMERA_S3_TAG_PREFIX_LEN, keylen);
        tag->key[keylen] = '\0';
        if (len > CHIMERA_S3_TAG_MAX_VAL_LEN) {
            len = CHIMERA_S3_TAG_MAX_VAL_LEN;
        }
        memcpy(tag->val, op->buffer, len);
        tag->val[len] = '\0';
    }
    *status = CHIMERA_VFS_OK;
} /* chimera_s3_tagging_value */

static void
chimera_s3_tagging_list(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct chimera_s3_request            *request = private_data;
    struct chimera_s3_tagging_ctx        *ctx     = request->tagging;
    const struct chimera_vfs_compound_op *op      = chimera_vfs_compound_op(compound, index);
    uint32_t                              off     = 0;
    int                                   next;

    if (op->cookie == 0) {
        ctx->names_len = 0;
        ctx->total     = 0;
        if (ctx->op == CHIMERA_S3_TAGGING_GET) {
            ctx->n_tags = 0;
        }
    }
    if (*status == CHIMERA_VFS_OK) {
        while (off < op->buffer_len) {
            const char *name = (const char *) op->buffer + off;
            size_t      len  = strnlen(name, op->buffer_len - off);
            if (len == op->buffer_len - off) {
                *status = CHIMERA_VFS_EIO;
                return;
            }
            off += len + 1;
            if (len <= CHIMERA_S3_TAG_PREFIX_LEN ||
                memcmp(name, CHIMERA_S3_TAG_PREFIX, CHIMERA_S3_TAG_PREFIX_LEN)) {
                continue;
            }
            ctx->total++;
            /* Enumerate completely before scheduling removals: mutating the
             * xattr list while walking backend cookies can skip names. */
            {
                if (ctx->names_len + len + 1 > CHIMERA_S3_TAG_BODY_HARD_CAP) {
                    *status = CHIMERA_VFS_EOVERFLOW;
                    return;
                }
                char *names = realloc(ctx->names, ctx->names_len + len + 1);
                if (!names) {
                    *status = CHIMERA_VFS_ENOSPC;
                    return;
                }
                ctx->names = names;
                memcpy(names + ctx->names_len, name, len + 1);
                ctx->names_len += len + 1;
            }
        }
        if (!op->eof) {
            if (op->r_cookie == op->cookie) {
                *status = CHIMERA_VFS_EIO;
                return;
            }
            next = chimera_vfs_compound_add_listxattrs(compound, op->r_cookie,
                                                       CHIMERA_S3_TAG_XATTR_BUFSZ);
            if (next < 0) {
                *status = CHIMERA_VFS_EOVERFLOW;
                return;
            }
            chimera_vfs_compound_set_op_callbacks(compound, next, NULL,
                                                  chimera_s3_tagging_list, request);
            return;
        }
    }
    /* Backends with no xattr support historically expose an empty tag set. */
    *status = CHIMERA_VFS_OK;
    /* Reserve the entire mutation suffix before any mutation is executed. */
    if ((unsigned int) ctx->total + chimera_vfs_compound_num_ops(compound) +
        ((ctx->op == CHIMERA_S3_TAGGING_PUT) ? ctx->n_tags : 0)
        > CHIMERA_VFS_COMPOUND_MAX_OPS) {
        *status = CHIMERA_VFS_EOVERFLOW;
        return;
    }
    for (int pos = 0; pos < ctx->names_len;) {
        const char *name = ctx->names + pos;
        int         len  = strlen(name);
        pos += len + 1;
        if (ctx->op == CHIMERA_S3_TAGGING_GET) {
            next = chimera_vfs_compound_add_getxattr(compound, name, len,
                                                     CHIMERA_S3_TAG_MAX_VAL_LEN);
        } else {
            next = chimera_vfs_compound_add_removexattr(compound, name, len);
        }
        if (next < 0) {
            *status = CHIMERA_VFS_EOVERFLOW;
            return;
        }
        chimera_vfs_compound_set_op_callbacks(compound, next, NULL,
                                              ctx->op == CHIMERA_S3_TAGGING_GET ? chimera_s3_tagging_value :
                                              chimera_s3_tagging_ignore_missing, request);
    }
    if ((ctx->op == CHIMERA_S3_TAGGING_PUT) &&
        chimera_s3_tagging_compound_store(compound, request) < 0) {
        *status = CHIMERA_VFS_EOVERFLOW;
    }
} /* chimera_s3_tagging_list */

static void
chimera_s3_tagging_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    CHIMERA_S3_HOLD_REQUEST(private_data);
    struct chimera_s3_request     *request = private_data;
    struct chimera_s3_tagging_ctx *ctx     = request->tagging;
    struct evpl                   *evpl    = request->thread->evpl;
    enum chimera_vfs_error         status  = chimera_vfs_compound_status(compound);
    enum chimera_s3_status         error   = chimera_s3_compound_error(compound, request,
                                                                       status == CHIMERA_VFS_ENOENT ?
                                                                       CHIMERA_S3_STATUS_NO_SUCH_KEY :
                                                                       CHIMERA_S3_STATUS_INTERNAL_ERROR);
    int                            op = ctx->op;

    chimera_vfs_compound_free(compound);
    if (status != CHIMERA_VFS_OK) {
        chimera_s3_tagging_finish(evpl, request, error);
    } else if (op == CHIMERA_S3_TAGGING_GET) {
        if (request->path_len == 0 && ctx->n_tags == 0) {
            chimera_s3_tagging_finish(evpl, request, CHIMERA_S3_STATUS_NO_SUCH_TAG_SET);
        } else {
            chimera_s3_tagging_send_xml(evpl, request);
            chimera_s3_tagging_ctx_free(request);
        }
    } else {
        chimera_s3_tagging_finish(evpl, request,
                                  op == CHIMERA_S3_TAGGING_DELETE ? CHIMERA_S3_STATUS_NO_CONTENT : CHIMERA_S3_STATUS_OK)
        ;
    }
} /* chimera_s3_tagging_complete */

static void
chimera_s3_tagging_submit(
    struct chimera_vfs_compound *compound,
    struct chimera_s3_request   *request)
{
    int index = chimera_vfs_compound_add_listxattrs(compound, 0,
                                                    CHIMERA_S3_TAG_XATTR_BUFSZ);

    chimera_vfs_compound_set_op_callbacks(compound, index, NULL,
                                          chimera_s3_tagging_list, request);
    chimera_s3_request_get(request);
    chimera_frontend_compound_submit(compound, chimera_s3_tagging_complete, request);
} /* chimera_s3_tagging_submit */

static void
chimera_s3_tagging_dispatch(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request,
    int                              op)
{
    struct chimera_s3_tagging_ctx *ctx      = chimera_s3_tagging_ctx_alloc(request);
    struct chimera_vfs_compound   *compound = chimera_s3_compound_alloc(request);

    ctx->op = op;
    if (request->path_len) {
        chimera_vfs_compound_add_lookup_path(compound, request->path,
                                             request->path_len, CHIMERA_VFS_ATTR_FH, CHIMERA_VFS_LOOKUP_FOLLOW);
    }
    chimera_vfs_compound_add_open_current(compound, CHIMERA_VFS_OPEN_INFERRED |
                                          (request->path_len ? 0 : CHIMERA_VFS_OPEN_DIRECTORY), 0);
    chimera_s3_tagging_submit(compound, request);
} /* chimera_s3_tagging_dispatch */

/* ----- public entry points ----- */

void
chimera_s3_get_tagging(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    chimera_s3_tagging_dispatch(evpl, thread, request, CHIMERA_S3_TAGGING_GET);
} /* chimera_s3_get_tagging */

void
chimera_s3_delete_tagging(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    chimera_s3_tagging_dispatch(evpl, thread, request, CHIMERA_S3_TAGGING_DELETE);
} /* chimera_s3_delete_tagging */

void
chimera_s3_put_tagging(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    /* The <Tagging> body has been accumulated and parsed by
     * chimera_s3_put_tagging_body_done; the parsed/validated tag set lives in
     * request->tagging. */
    chimera_s3_tagging_dispatch(evpl, thread, request, CHIMERA_S3_TAGGING_PUT);
} /* chimera_s3_put_tagging */

/* ----- PutObjectTagging request-body accumulation ----- */

void
chimera_s3_put_tagging_recv(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    struct chimera_s3_tagging_ctx *ctx = chimera_s3_tagging_ctx_alloc(request);
    struct evpl_iovec              iov[CHIMERA_S3_IOV_MAX];
    uint64_t                       avail, total;
    int                            niov, i;

    while ((avail = evpl_http_request_get_data_avail(request->http_request)) > 0) {
        niov  = evpl_http_request_get_datav(evpl, request->http_request, iov, avail);
        total = 0;
        for (i = 0; i < niov; i++) {
            total += iov[i].length;
        }

        if (total > 0 &&
            ctx->body_len + (int) total <= CHIMERA_S3_TAG_BODY_HARD_CAP) {
            if (ctx->body_len + (int) total > ctx->body_cap) {
                int new_cap = ctx->body_cap ? ctx->body_cap * 2 : 4096;
                while (new_cap < ctx->body_len + (int) total) {
                    new_cap *= 2;
                }
                ctx->body_buf = realloc(ctx->body_buf, new_cap);
                ctx->body_cap = new_cap;
            }
            for (i = 0; i < niov; i++) {
                if (iov[i].length) {
                    memcpy(ctx->body_buf + ctx->body_len, iov[i].data, iov[i].length);
                    ctx->body_len += iov[i].length;
                }
            }
        }

        evpl_iovecs_release(evpl, iov, niov);
    }
} /* chimera_s3_put_tagging_recv */

void
chimera_s3_put_tagging_body_done(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread *thread = request->thread;
    struct chimera_s3_tagging_ctx   *ctx    = chimera_s3_tagging_ctx_alloc(request);
    int                              rc;

    rc = chimera_s3_tagging_parse_xml(ctx, ctx->body_buf, ctx->body_len);

    if (rc == -1) {
        chimera_s3_tagging_finish(evpl, request, CHIMERA_S3_STATUS_MALFORMED_XML);
        return;
    }
    if (rc == -2) {
        chimera_s3_tagging_finish(evpl, request, CHIMERA_S3_STATUS_INVALID_TAG);
        return;
    }

    chimera_s3_put_tagging(evpl, thread, request);
} /* chimera_s3_put_tagging_body_done */
