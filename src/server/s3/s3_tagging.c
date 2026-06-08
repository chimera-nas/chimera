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
    free(ctx->valbuf);
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
    if (ctx->handle) {
        chimera_vfs_release(request->thread->vfs, ctx->handle);
        ctx->handle = NULL;
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
    evpl_http_request_add_datav(request->http_request, &iov, 1);

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

/* Terminal helpers: release the open handle, free the ctx, finish the request. */
static void
chimera_s3_tagging_finish(
    struct evpl               *evpl,
    struct chimera_s3_request *request,
    enum chimera_s3_status     status)
{
    struct chimera_server_s3_thread *thread = request->thread;
    struct chimera_s3_tagging_ctx   *ctx    = request->tagging;

    if (ctx && ctx->handle) {
        chimera_vfs_release(thread->vfs, ctx->handle);
        ctx->handle = NULL;
    }

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

/* ----- removal of the existing tag xattrs (shared set/delete prelude) -----
 * Walk the names returned by list_xattrs and remove every "user.s3.tag.*".
 * On completion calls ctx->after (set new tags, or finish). */

static void chimera_s3_tagging_remove_next(
    struct evpl               *evpl,
    struct chimera_s3_request *request);

static void
chimera_s3_tagging_remove_cb(
    enum chimera_vfs_error          error_code,
    const struct chimera_vfs_attrs *pre_attr,
    const struct chimera_vfs_attrs *post_attr,
    void                           *private_data)
{
    struct chimera_s3_request *request = private_data;

    /* Ignore per-name errors (ENOENT from a racing remove); keep going. */
    chimera_s3_tagging_remove_next(request->thread->evpl, request);
} /* chimera_s3_tagging_remove_cb */

static void
chimera_s3_tagging_remove_next(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread *thread = request->thread;
    struct chimera_s3_tagging_ctx   *ctx    = request->tagging;
    const char                      *name;
    int                              namelen;

    while (ctx->cur < ctx->names_len) {
        name      = ctx->names + ctx->cur;
        namelen   = strlen(name);
        ctx->cur += namelen + 1;

        if (namelen > CHIMERA_S3_TAG_PREFIX_LEN &&
            memcmp(name, CHIMERA_S3_TAG_PREFIX, CHIMERA_S3_TAG_PREFIX_LEN) == 0) {
            chimera_vfs_remove_xattr(thread->vfs, &thread->shared->cred,
                                     ctx->handle, name, namelen,
                                     chimera_s3_tagging_remove_cb, request);
            return;
        }
    }

    /* All existing tag xattrs removed; proceed to the next phase. */
    ctx->after(evpl, request);
} /* chimera_s3_tagging_remove_next */

/* ----- list existing tag xattrs (entry to the remove phase) ----- */

static void
chimera_s3_tagging_list_for_remove_cb(
    enum chimera_vfs_error error_code,
    const char            *names,
    uint32_t               names_len,
    uint32_t               count,
    uint32_t               eof,
    uint64_t               cookie,
    void                  *private_data)
{
    struct chimera_s3_request     *request = private_data;
    struct chimera_s3_tagging_ctx *ctx     = request->tagging;

    if (error_code != CHIMERA_VFS_OK) {
        /* No xattrs (or backend has none): nothing to remove. */
        ctx->names_len = 0;
        ctx->cur       = 0;
        ctx->after(request->thread->evpl, request);
        return;
    }

    /* `names` points into the buffer we supplied (ctx->names); don't free it.
     * Normalize the data to the front of the buffer and record its length. */
    if (names != ctx->names && names_len) {
        memmove(ctx->names, names, names_len);
    }
    ctx->names_len = names_len;
    ctx->cur       = 0;

    chimera_s3_tagging_remove_next(request->thread->evpl, request);
} /* chimera_s3_tagging_list_for_remove_cb */

/* Clear all existing tag xattrs on ctx->handle, then invoke `after`. */
static void
chimera_s3_tagging_clear_existing(
    struct evpl *evpl,
    struct chimera_s3_request *request,
    void ( *after )(struct evpl *evpl, struct chimera_s3_request *request))
{
    struct chimera_server_s3_thread *thread = request->thread;
    struct chimera_s3_tagging_ctx   *ctx    = request->tagging;

    ctx->after = after;

    if (!ctx->names) {
        ctx->names = malloc(CHIMERA_S3_TAG_XATTR_BUFSZ);
    }

    chimera_vfs_list_xattrs(thread->vfs, &thread->shared->cred,
                            ctx->handle, 0,
                            ctx->names, CHIMERA_S3_TAG_XATTR_BUFSZ,
                            chimera_s3_tagging_list_for_remove_cb, request);
} /* chimera_s3_tagging_clear_existing */

/* ----- set the new tag set (write each tag as a user.s3.tag.<key> xattr) ----- */

static void chimera_s3_tagging_set_next(
    struct evpl               *evpl,
    struct chimera_s3_request *request);

static void
chimera_s3_tagging_set_cb(
    enum chimera_vfs_error          error_code,
    const struct chimera_vfs_attrs *pre_attr,
    const struct chimera_vfs_attrs *post_attr,
    void                           *private_data)
{
    struct chimera_s3_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_s3_tagging_finish(request->thread->evpl, request,
                                  CHIMERA_S3_STATUS_INTERNAL_ERROR);
        return;
    }

    request->tagging->cur++;
    chimera_s3_tagging_set_next(request->thread->evpl, request);
} /* chimera_s3_tagging_set_cb */

static void
chimera_s3_tagging_set_next(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread *thread = request->thread;
    struct chimera_s3_tagging_ctx   *ctx    = request->tagging;
    struct chimera_s3_tag           *t;
    char                             name[CHIMERA_S3_TAG_PREFIX_LEN + CHIMERA_S3_TAG_MAX_KEY_LEN + 1];
    int                              namelen;

    if (ctx->cur >= ctx->n_tags) {
        /* All tags written. PUT tagging returns 200 with no body. */
        chimera_s3_tagging_finish(evpl, request, CHIMERA_S3_STATUS_OK);
        return;
    }

    t       = &ctx->tags[ctx->cur];
    namelen = snprintf(name, sizeof(name), CHIMERA_S3_TAG_PREFIX "%s", t->key);

    chimera_vfs_set_xattr(thread->vfs, &thread->shared->cred,
                          ctx->handle, 0 /* create-or-replace */,
                          name, namelen,
                          t->val, strlen(t->val),
                          chimera_s3_tagging_set_cb, request);
} /* chimera_s3_tagging_set_next */

/* ----- GET object?tagging: read each tag xattr's value, emit <Tagging> ----- */

static void chimera_s3_tagging_get_next(
    struct evpl               *evpl,
    struct chimera_s3_request *request);

static void
chimera_s3_tagging_get_value_cb(
    enum chimera_vfs_error error_code,
    uint32_t               value_len,
    void                  *private_data)
{
    struct chimera_s3_request     *request = private_data;
    struct chimera_s3_tagging_ctx *ctx     = request->tagging;
    const char                    *name;
    int                            namelen, keylen, vlen;

    /* The name we just read sits at the prior cursor; recover it. */
    name    = ctx->names + ctx->prev_cur;
    namelen = strlen(name);
    keylen  = namelen - CHIMERA_S3_TAG_PREFIX_LEN;

    if (error_code == CHIMERA_VFS_OK && ctx->n_tags < CHIMERA_S3_TAG_MAX_TAGS &&
        keylen > 0 && keylen <= CHIMERA_S3_TAG_MAX_KEY_LEN) {
        struct chimera_s3_tag *t = &ctx->tags[ctx->n_tags++];

        memcpy(t->key, name + CHIMERA_S3_TAG_PREFIX_LEN, keylen);
        t->key[keylen] = '\0';

        vlen = value_len;
        if (vlen > CHIMERA_S3_TAG_MAX_VAL_LEN) {
            vlen = CHIMERA_S3_TAG_MAX_VAL_LEN;
        }
        memcpy(t->val, ctx->valbuf, vlen);
        t->val[vlen] = '\0';
    }

    chimera_s3_tagging_get_next(request->thread->evpl, request);
} /* chimera_s3_tagging_get_value_cb */

static void
chimera_s3_tagging_get_next(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread *thread = request->thread;
    struct chimera_s3_tagging_ctx   *ctx    = request->tagging;
    const char                      *name;
    int                              namelen;

    while (ctx->cur < ctx->names_len) {
        name    = ctx->names + ctx->cur;
        namelen = strlen(name);

        if (namelen > CHIMERA_S3_TAG_PREFIX_LEN &&
            memcmp(name, CHIMERA_S3_TAG_PREFIX, CHIMERA_S3_TAG_PREFIX_LEN) == 0) {
            ctx->prev_cur = ctx->cur;
            ctx->cur     += namelen + 1;
            chimera_vfs_get_xattr(thread->vfs, &thread->shared->cred,
                                  ctx->handle, name, namelen,
                                  ctx->valbuf, CHIMERA_S3_TAG_VAL_BUFSZ - 1,
                                  chimera_s3_tagging_get_value_cb, request);
            return;
        }
        ctx->cur += namelen + 1;
    }

    /* Bucket-level GetBucketTagging with no tags returns 404 NoSuchTagSet
     * (unlike GetObjectTagging, which returns an empty 200 tag set). */
    if (request->path_len == 0 && ctx->n_tags == 0) {
        chimera_s3_tagging_finish(evpl, request,
                                  CHIMERA_S3_STATUS_NO_SUCH_TAG_SET);
        return;
    }

    /* All tag values gathered: emit the response and finish. */
    chimera_s3_tagging_send_xml(evpl, request);

    if (ctx->handle) {
        chimera_vfs_release(thread->vfs, ctx->handle);
        ctx->handle = NULL;
    }
    chimera_s3_tagging_ctx_free(request);
} /* chimera_s3_tagging_get_next */

static void
chimera_s3_tagging_get_list_cb(
    enum chimera_vfs_error error_code,
    const char            *names,
    uint32_t               names_len,
    uint32_t               count,
    uint32_t               eof,
    uint64_t               cookie,
    void                  *private_data)
{
    struct chimera_s3_request     *request = private_data;
    struct chimera_s3_tagging_ctx *ctx     = request->tagging;

    if (error_code != CHIMERA_VFS_OK) {
        names_len = 0;
    }

    /* `names` points into the buffer we supplied (ctx->names); don't free it.
     * Normalize the data to the front of the buffer and record its length. */
    if (names != ctx->names && names_len) {
        memmove(ctx->names, names, names_len);
    }
    ctx->names_len = names_len;
    ctx->cur       = 0;
    ctx->n_tags    = 0;

    if (!ctx->valbuf) {
        ctx->valbuf = malloc(CHIMERA_S3_TAG_VAL_BUFSZ);
    }

    chimera_s3_tagging_get_next(request->thread->evpl, request);
} /* chimera_s3_tagging_get_list_cb */

/* ----- per-op continuations after the existing tag xattrs are cleared ----- */

enum chimera_s3_tagging_op {
    CHIMERA_S3_TAGGING_GET,
    CHIMERA_S3_TAGGING_PUT,
    CHIMERA_S3_TAGGING_DELETE,
};

/* PUT: existing tags cleared, now write the parsed tag set. */
static void
chimera_s3_tagging_put_after_clear(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    request->tagging->cur = 0;
    chimera_s3_tagging_set_next(evpl, request);
} /* chimera_s3_tagging_put_after_clear */

/* DELETE: existing tags cleared, respond 204 No Content. */
static void
chimera_s3_tagging_delete_after_clear(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    chimera_s3_tagging_finish(evpl, request, CHIMERA_S3_STATUS_NO_CONTENT);
} /* chimera_s3_tagging_delete_after_clear */

/* The handle is open: drive the requested operation. */
static void
chimera_s3_tagging_begin_op(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread *thread = request->thread;
    struct chimera_s3_tagging_ctx   *ctx    = request->tagging;

    switch ((enum chimera_s3_tagging_op) ctx->op) {
        case CHIMERA_S3_TAGGING_GET:
            if (!ctx->names) {
                ctx->names = malloc(CHIMERA_S3_TAG_XATTR_BUFSZ);
            }
            chimera_vfs_list_xattrs(thread->vfs, &thread->shared->cred,
                                    ctx->handle, 0,
                                    ctx->names, CHIMERA_S3_TAG_XATTR_BUFSZ,
                                    chimera_s3_tagging_get_list_cb, request);
            break;
        case CHIMERA_S3_TAGGING_PUT:
            chimera_s3_tagging_clear_existing(evpl, request,
                                              chimera_s3_tagging_put_after_clear);
            break;
        case CHIMERA_S3_TAGGING_DELETE:
            chimera_s3_tagging_clear_existing(evpl, request,
                                              chimera_s3_tagging_delete_after_clear);
            break;
    } /* switch */
} /* chimera_s3_tagging_begin_op */

/* ----- shared lookup + open prelude ----- */

static void
chimera_s3_tagging_open_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_s3_tagging_finish(thread->evpl, request,
                                  CHIMERA_S3_STATUS_NO_SUCH_KEY);
        return;
    }

    request->tagging->handle = oh;
    chimera_s3_tagging_begin_op(thread->evpl, request);
} /* chimera_s3_tagging_open_cb */

static void
chimera_s3_tagging_lookup_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;

    if (error_code != CHIMERA_VFS_OK ||
        !(attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        chimera_s3_tagging_finish(thread->evpl, request,
                                  CHIMERA_S3_STATUS_NO_SUCH_KEY);
        return;
    }

    chimera_vfs_open_fh(thread->vfs, &thread->shared->cred,
                        attr->va_fh, attr->va_fh_len,
                        CHIMERA_VFS_OPEN_INFERRED,
                        chimera_s3_tagging_open_cb, request);
} /* chimera_s3_tagging_lookup_cb */

/* Resolve request->path under request->bucket_fh, open it, run the op. For an
 * empty path (bucket tagging) the bucket directory handle is opened directly.
 * The tag-set (for PUT) must already be parsed/validated into request->tagging. */
static void
chimera_s3_tagging_dispatch(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request,
    int                              op)
{
    struct chimera_s3_tagging_ctx *ctx = chimera_s3_tagging_ctx_alloc(request);

    ctx->op = op;

    if (request->path_len == 0) {
        /* Bucket-level tagging: the bucket directory FH is already in hand. */
        chimera_vfs_open_fh(thread->vfs, &thread->shared->cred,
                            request->bucket_fh, request->bucket_fhlen,
                            CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
                            chimera_s3_tagging_open_cb, request);
        return;
    }

    chimera_vfs_lookup(thread->vfs, &thread->shared->cred,
                       request->bucket_fh, request->bucket_fhlen,
                       request->path, request->path_len,
                       CHIMERA_VFS_ATTR_FH,
                       CHIMERA_VFS_LOOKUP_FOLLOW,
                       chimera_s3_tagging_lookup_cb, request);
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

/* ----- store-by-path: PutObject x-amz-tagging / CompleteMultipartUpload ----- */

static void
chimera_s3_tagging_store_set_done(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread *thread = request->thread;
    struct chimera_s3_tagging_ctx   *ctx    = request->tagging;

    void                             (*done)(
        struct evpl *,
        struct chimera_s3_request *) = ctx->store_done;

    if (ctx->handle) {
        chimera_vfs_release(thread->vfs, ctx->handle);
        ctx->handle = NULL;
    }
    chimera_s3_tagging_ctx_free(request);

    done(evpl, request);
} /* chimera_s3_tagging_store_set_done */

/* Override of set_next's terminal step for the store-by-path flow: instead of
 * emitting an HTTP response, hand control back to the caller's done_cb. */
static void chimera_s3_tagging_store_set_next(
    struct evpl               *evpl,
    struct chimera_s3_request *request);

static void
chimera_s3_tagging_store_set_cb(
    enum chimera_vfs_error          error_code,
    const struct chimera_vfs_attrs *pre_attr,
    const struct chimera_vfs_attrs *post_attr,
    void                           *private_data)
{
    struct chimera_s3_request *request = private_data;

    request->tagging->cur++;
    chimera_s3_tagging_store_set_next(request->thread->evpl, request);
} /* chimera_s3_tagging_store_set_cb */

static void
chimera_s3_tagging_store_set_next(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread *thread = request->thread;
    struct chimera_s3_tagging_ctx   *ctx    = request->tagging;
    struct chimera_s3_tag           *t;
    char                             name[CHIMERA_S3_TAG_PREFIX_LEN + CHIMERA_S3_TAG_MAX_KEY_LEN + 1];
    int                              namelen;

    if (ctx->cur >= ctx->n_tags) {
        chimera_s3_tagging_store_set_done(evpl, request);
        return;
    }

    t       = &ctx->tags[ctx->cur];
    namelen = snprintf(name, sizeof(name), CHIMERA_S3_TAG_PREFIX "%s", t->key);

    chimera_vfs_set_xattr(thread->vfs, &thread->shared->cred,
                          ctx->handle, 0,
                          name, namelen,
                          t->val, strlen(t->val),
                          chimera_s3_tagging_store_set_cb, request);
} /* chimera_s3_tagging_store_set_next */

static void
chimera_s3_tagging_store_after_clear(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    request->tagging->cur = 0;
    chimera_s3_tagging_store_set_next(evpl, request);
} /* chimera_s3_tagging_store_after_clear */

static void
chimera_s3_tagging_store_open_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct chimera_s3_tagging_ctx   *ctx     = request->tagging;

    void                             (*done)(
        struct evpl *,
        struct chimera_s3_request *) = ctx->store_done;

    if (error_code != CHIMERA_VFS_OK) {
        /* Best-effort: object tags couldn't be stored; proceed anyway. */
        chimera_s3_tagging_ctx_free(request);
        done(thread->evpl, request);
        return;
    }

    ctx->handle = oh;
    chimera_s3_tagging_clear_existing(thread->evpl, request,
                                      chimera_s3_tagging_store_after_clear);
} /* chimera_s3_tagging_store_open_cb */

static void
chimera_s3_tagging_store_lookup_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct chimera_s3_tagging_ctx   *ctx     = request->tagging;

    void                             (*done)(
        struct evpl *,
        struct chimera_s3_request *) = ctx->store_done;

    if (error_code != CHIMERA_VFS_OK ||
        !(attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        chimera_s3_tagging_ctx_free(request);
        done(thread->evpl, request);
        return;
    }

    chimera_vfs_open_fh(thread->vfs, &thread->shared->cred,
                        attr->va_fh, attr->va_fh_len,
                        CHIMERA_VFS_OPEN_INFERRED,
                        chimera_s3_tagging_store_open_cb, request);
} /* chimera_s3_tagging_store_lookup_cb */

void
chimera_s3_tagging_store_by_path(
    struct evpl *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request *request,
    void ( *done_cb )(struct evpl *evpl, struct chimera_s3_request *request))
{
    struct chimera_s3_tagging_ctx *ctx = request->tagging;

    if (!ctx || ctx->n_tags == 0) {
        if (ctx) {
            chimera_s3_tagging_ctx_free(request);
        }
        done_cb(evpl, request);
        return;
    }

    ctx->store_done = done_cb;

    chimera_vfs_lookup(thread->vfs, &thread->shared->cred,
                       request->bucket_fh, request->bucket_fhlen,
                       request->path, request->path_len,
                       CHIMERA_VFS_ATTR_FH,
                       CHIMERA_VFS_LOOKUP_FOLLOW,
                       chimera_s3_tagging_store_lookup_cb, request);
} /* chimera_s3_tagging_store_by_path */

/* ----- HEAD object: x-amz-tagging-count ----- */

static void
chimera_s3_tagging_count_list_cb(
    enum chimera_vfs_error error_code,
    const char            *names,
    uint32_t               names_len,
    uint32_t               count,
    uint32_t               eof,
    uint64_t               cookie,
    void                  *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct chimera_s3_tagging_ctx   *ctx     = request->tagging;

    void                             (*done)(
        struct evpl *,
        struct chimera_s3_request *) = ctx->store_done;
    uint32_t                         off = 0;
    int                              n   = 0;
    char                             hdr[16];

    if (error_code == CHIMERA_VFS_OK) {
        while (off < names_len) {
            const char *name    = names + off;
            int         namelen = strlen(name);

            if (namelen > CHIMERA_S3_TAG_PREFIX_LEN &&
                memcmp(name, CHIMERA_S3_TAG_PREFIX, CHIMERA_S3_TAG_PREFIX_LEN) == 0) {
                n++;
            }
            off += namelen + 1;
        }
    }

    snprintf(hdr, sizeof(hdr), "%d", n);
    evpl_http_request_add_header(request->http_request, "x-amz-tagging-count", hdr);

    if (ctx->handle) {
        chimera_vfs_release(thread->vfs, ctx->handle);
        ctx->handle = NULL;
    }
    chimera_s3_tagging_ctx_free(request);

    done(thread->evpl, request);
} /* chimera_s3_tagging_count_list_cb */

static void
chimera_s3_tagging_count_open_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct chimera_s3_tagging_ctx   *ctx     = request->tagging;

    void                             (*done)(
        struct evpl *,
        struct chimera_s3_request *) = ctx->store_done;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_s3_tagging_ctx_free(request);
        done(thread->evpl, request);
        return;
    }

    ctx->handle = oh;

    if (!ctx->names) {
        ctx->names = malloc(CHIMERA_S3_TAG_XATTR_BUFSZ);
    }

    chimera_vfs_list_xattrs(thread->vfs, &thread->shared->cred,
                            ctx->handle, 0,
                            ctx->names, CHIMERA_S3_TAG_XATTR_BUFSZ,
                            chimera_s3_tagging_count_list_cb, request);
} /* chimera_s3_tagging_count_open_cb */

void
chimera_s3_tagging_count_for_head(
    struct evpl *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request *request,
    const void *fh,
    int fh_len,
    void ( *done_cb )(struct evpl *evpl, struct chimera_s3_request *request))
{
    struct chimera_s3_tagging_ctx *ctx = chimera_s3_tagging_ctx_alloc(request);

    ctx->store_done = done_cb;

    chimera_vfs_open_fh(thread->vfs, &thread->shared->cred,
                        fh, fh_len,
                        CHIMERA_VFS_OPEN_INFERRED,
                        chimera_s3_tagging_count_open_cb, request);
} /* chimera_s3_tagging_count_for_head */
