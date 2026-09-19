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
#ifdef _WIN32
#include "common/platform.h"
#else  /* ifdef _WIN32 */
#include <strings.h>
#endif /* ifdef _WIN32 */

#include "evpl/evpl.h"
#include "evpl/evpl_http.h"
#include "vfs/vfs.h"
#include "vfs/vfs_compound.h"
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

/* ----- the fan-outs over the listed names -----
 *
 * Every tagging operation starts from one LISTXATTRS page, taken in the same
 * sequence as the open (see chimera_s3_tagging_dispatch), and then walks the
 * "user.s3.tag.*" names in it: REMOVEXATTR for a set or delete, GETXATTR for
 * a get.  The walk is a following sequence, chunked at the sequence limit,
 * because how many names there are is the list's answer.  A per-name error
 * is ignored and the walk resumes after it, as the one-op-at-a-time walk
 * ignored it. */

/*
 * Append one op per tag name from ctx->cur on, up to the sequence limit, each
 * addressing ctx->handle.  `add` builds the op for a name; op_next[] records
 * where each name ends so the cursor can be moved past whatever ran.  Returns
 * the number of ops appended (0: no tag names remain).
 */
static int
chimera_s3_tagging_add_name_ops(
    struct chimera_s3_request   *request,
    struct chimera_vfs_compound *compound,
    int (                       *add )(
        struct chimera_vfs_compound *compound,
        const char                  *name,
        int                          namelen))
{
    struct chimera_s3_tagging_ctx *ctx = request->tagging;
    const char                    *name;
    int                            namelen, n = 0, index;

    while (ctx->cur < ctx->names_len && n < CHIMERA_VFS_COMPOUND_MAX_OPS) {
        name    = ctx->names + ctx->cur;
        namelen = strnlen(name, ctx->names_len - ctx->cur);

        if (namelen > CHIMERA_S3_TAG_PREFIX_LEN &&
            memcmp(name, CHIMERA_S3_TAG_PREFIX, CHIMERA_S3_TAG_PREFIX_LEN) == 0) {
            index = add(compound, name, namelen);
            if (index < 0) {
                break;
            }
            chimera_vfs_compound_op_set_handle(compound, index, ctx->handle);
            ctx->op_next[n++] = ctx->cur + namelen + 1;
        }

        ctx->cur += namelen + 1;
    }

    return n;
} /* chimera_s3_tagging_add_name_ops */

/* Move the cursor to just past the last name the sequence ran over: the
 * names behind a failure did not run and are walked again by the next
 * chunk. */
static void
chimera_s3_tagging_resume_after(
    struct chimera_s3_request   *request,
    struct chimera_vfs_compound *compound)
{
    struct chimera_s3_tagging_ctx *ctx = request->tagging;
    uint32_t                       ran = chimera_vfs_compound_num_completed(compound);

    if (ran > 0) {
        ctx->cur = ctx->op_next[ran - 1];
    }
} /* chimera_s3_tagging_resume_after */

static int
chimera_s3_tagging_add_removexattr(
    struct chimera_vfs_compound *compound,
    const char                  *name,
    int                          namelen)
{
    return chimera_vfs_compound_add_removexattr(compound, name, namelen);
} /* chimera_s3_tagging_add_removexattr */

static int
chimera_s3_tagging_add_getxattr(
    struct chimera_vfs_compound *compound,
    const char                  *name,
    int                          namelen)
{
    return chimera_vfs_compound_add_getxattr(compound, name, namelen,
                                             CHIMERA_S3_TAG_VAL_BUFSZ - 1);
} /* chimera_s3_tagging_add_getxattr */

/* ----- removal of the existing tag xattrs (shared set/delete prelude) -----
 * Walk the listed names and remove every "user.s3.tag.*".  On completion
 * calls ctx->after (set new tags, or finish). */

static void chimera_s3_tagging_remove_next(
    struct evpl               *evpl,
    struct chimera_s3_request *request);

static void
chimera_s3_tagging_remove_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_s3_request *request = private_data;

    /* Ignore per-name errors (ENOENT from a racing remove); keep going. */
    chimera_s3_tagging_resume_after(request, compound);
    chimera_vfs_compound_free(compound);

    chimera_s3_tagging_remove_next(request->thread->evpl, request);
} /* chimera_s3_tagging_remove_complete */

static void
chimera_s3_tagging_remove_next(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread *thread = request->thread;
    struct chimera_s3_tagging_ctx   *ctx    = request->tagging;
    struct chimera_vfs_compound     *compound;

    compound = chimera_vfs_compound_alloc(thread->vfs, &request->cred);

    if (chimera_s3_tagging_add_name_ops(request, compound,
                                        chimera_s3_tagging_add_removexattr) == 0) {
        /* All existing tag xattrs removed; proceed to the next phase. */
        chimera_vfs_compound_free(compound);
        ctx->after(evpl, request);
        return;
    }

    chimera_s3_request_get(request);

    chimera_vfs_compound_submit(compound, chimera_s3_tagging_remove_complete,
                                request);
} /* chimera_s3_tagging_remove_next */

/* Clear all existing tag xattrs on ctx->handle, then invoke `after`. */
static void
chimera_s3_tagging_clear_existing(
    struct evpl               *evpl,
    struct chimera_s3_request *request,
    void (                    *after )(
        struct evpl               *evpl,
        struct chimera_s3_request *request))
{
    struct chimera_s3_tagging_ctx *ctx = request->tagging;

    ctx->after = after;
    ctx->cur   = 0;

    chimera_s3_tagging_remove_next(evpl, request);
} /* chimera_s3_tagging_clear_existing */

/* ----- set the new tag set (write each tag as a user.s3.tag.<key> xattr) -----
 * ctx->cur is the index of the next tag to write.  `tolerant` says whether a
 * failed set is skipped (the best-effort store-by-path) or fatal (PUT). */

/* Append up to a sequence's worth of SETXATTRs for tags[cur..], advancing
 * cur.  Returns how many were appended. */
static int
chimera_s3_tagging_add_set_ops(
    struct chimera_s3_request   *request,
    struct chimera_vfs_compound *compound)
{
    struct chimera_s3_tagging_ctx *ctx = request->tagging;
    struct chimera_s3_tag         *t;
    char                           name[CHIMERA_S3_TAG_PREFIX_LEN +
                                        CHIMERA_S3_TAG_MAX_KEY_LEN + 1];
    int                            namelen, n = 0, index;

    while (ctx->cur < ctx->n_tags && n < CHIMERA_VFS_COMPOUND_MAX_OPS) {
        t       = &ctx->tags[ctx->cur];
        namelen = snprintf(name, sizeof(name), CHIMERA_S3_TAG_PREFIX "%s",
                           t->key);

        /* The adder copies the name; the value is borrowed from the tag
         * set, which lives on the ctx until the request is answered. */
        index = chimera_vfs_compound_add_setxattr(compound,
                                                  0 /* create-or-replace */,
                                                  name, namelen,
                                                  t->val, strlen(t->val));
        if (index < 0) {
            break;
        }
        chimera_vfs_compound_op_set_handle(compound, index, ctx->handle);

        ctx->cur++;
        n++;
    }

    return n;
} /* chimera_s3_tagging_add_set_ops */

static void chimera_s3_tagging_set_next(
    struct evpl               *evpl,
    struct chimera_s3_request *request);

static void
chimera_s3_tagging_set_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_s3_request *request = private_data;
    enum chimera_vfs_error     status  = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);

    if (status != CHIMERA_VFS_OK) {
        chimera_s3_tagging_finish(request->thread->evpl, request,
                                  CHIMERA_S3_STATUS_INTERNAL_ERROR);
        return;
    }

    chimera_s3_tagging_set_next(request->thread->evpl, request);
} /* chimera_s3_tagging_set_complete */

static void
chimera_s3_tagging_set_next(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread *thread = request->thread;
    struct chimera_vfs_compound     *compound;

    compound = chimera_vfs_compound_alloc(thread->vfs, &request->cred);

    if (chimera_s3_tagging_add_set_ops(request, compound) == 0) {
        /* All tags written. PUT tagging returns 200 with no body. */
        chimera_vfs_compound_free(compound);
        chimera_s3_tagging_finish(evpl, request, CHIMERA_S3_STATUS_OK);
        return;
    }

    chimera_s3_request_get(request);

    chimera_vfs_compound_submit(compound, chimera_s3_tagging_set_complete,
                                request);
} /* chimera_s3_tagging_set_next */

/* ----- GET object?tagging: read each tag xattr's value, emit <Tagging> ----- */

static void chimera_s3_tagging_get_next(
    struct evpl               *evpl,
    struct chimera_s3_request *request);

static void
chimera_s3_tagging_get_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    CHIMERA_S3_HOLD_REQUEST(private_data);
    struct chimera_s3_request            *request = private_data;
    struct chimera_s3_tagging_ctx        *ctx     = request->tagging;
    const struct chimera_vfs_compound_op *op;
    uint32_t                              ran, i;
    const char                           *name;
    int                                   namelen, keylen, vlen;

    ran = chimera_vfs_compound_num_completed(compound);

    for (i = 0; i < ran; i++) {
        op = chimera_vfs_compound_op(compound, i);

        if (op->status != CHIMERA_VFS_OK) {
            continue;
        }

        name    = op->name;
        namelen = op->name_len;
        keylen  = namelen - CHIMERA_S3_TAG_PREFIX_LEN;

        if (ctx->n_tags < CHIMERA_S3_TAG_MAX_TAGS &&
            keylen > 0 && keylen <= CHIMERA_S3_TAG_MAX_KEY_LEN) {
            struct chimera_s3_tag *t = &ctx->tags[ctx->n_tags++];

            memcpy(t->key, name + CHIMERA_S3_TAG_PREFIX_LEN, keylen);
            t->key[keylen] = '\0';

            vlen = op->buffer_len;
            if (vlen > CHIMERA_S3_TAG_MAX_VAL_LEN) {
                vlen = CHIMERA_S3_TAG_MAX_VAL_LEN;
            }
            memcpy(t->val, op->buffer, vlen);
            t->val[vlen] = '\0';
        }
    }

    chimera_s3_tagging_resume_after(request, compound);
    chimera_vfs_compound_free(compound);

    chimera_s3_tagging_get_next(request->thread->evpl, request);
} /* chimera_s3_tagging_get_complete */

static void
chimera_s3_tagging_get_next(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread *thread = request->thread;
    struct chimera_s3_tagging_ctx   *ctx    = request->tagging;
    struct chimera_vfs_compound     *compound;

    compound = chimera_vfs_compound_alloc(thread->vfs, &request->cred);

    if (chimera_s3_tagging_add_name_ops(request, compound,
                                        chimera_s3_tagging_add_getxattr) > 0) {
        chimera_s3_request_get(request);
        chimera_vfs_compound_submit(compound, chimera_s3_tagging_get_complete,
                                    request);
        return;
    }

    chimera_vfs_compound_free(compound);

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

/* The handle is open and the names are listed: drive the requested op. */
static void
chimera_s3_tagging_begin_op(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    struct chimera_s3_tagging_ctx *ctx = request->tagging;

    switch ((enum chimera_s3_tagging_op) ctx->op) {
        case CHIMERA_S3_TAGGING_GET:
            ctx->cur    = 0;
            ctx->n_tags = 0;
            chimera_s3_tagging_get_next(evpl, request);
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

/* ----- shared open + list prelude -----
 *
 * PUTFH(bucket) -> [LOOKUP_PATH(key)] -> OPEN_CURRENT -> GETHANDLE ->
 * LISTXATTRS.  The handle is taken for the fan-outs that follow; the names
 * are copied out before the sequence is freed.  A list that fails leaves an
 * empty name set, as it did: the object has no xattrs to speak of.
 *
 * Returns 0 with the handle and names staged, or -1 when the object could
 * not be resolved or opened (the failure came before the GETHANDLE ran). */
static int
chimera_s3_tagging_prelude_collect(
    struct chimera_s3_request   *request,
    struct chimera_vfs_compound *compound)
{
    struct chimera_s3_tagging_ctx        *ctx = request->tagging;
    const struct chimera_vfs_compound_op *op;
    uint32_t                              num_ops, completed, list_index;

    num_ops    = chimera_vfs_compound_num_ops(compound);
    completed  = chimera_vfs_compound_num_completed(compound);
    list_index = num_ops - 1;

    /* Everything before the LISTXATTRS has to have run OK for the handle to
     * exist; the GETHANDLE sits immediately before the list. */
    if (completed < list_index) {
        return -1;
    }

    ctx->handle = chimera_vfs_compound_take_handle(compound, list_index - 1);

    if (!ctx->handle) {
        return -1;
    }

    if (!ctx->names) {
        ctx->names = malloc(CHIMERA_S3_TAG_XATTR_BUFSZ);
    }

    op = chimera_vfs_compound_op(compound, list_index);

    if (op->status == CHIMERA_VFS_OK && op->buffer_len) {
        memcpy(ctx->names, op->buffer, op->buffer_len);
        ctx->names_len = op->buffer_len;
    } else {
        ctx->names_len = 0;
    }

    ctx->cur = 0;

    return 0;
} /* chimera_s3_tagging_prelude_collect */

static void
chimera_s3_tagging_prelude_build(
    struct chimera_s3_request   *request,
    struct chimera_vfs_compound *compound)
{
    chimera_vfs_compound_add_putfh(compound, request->bucket_fh,
                                   request->bucket_fhlen);

    if (request->path_len == 0) {
        /* Bucket-level tagging: the bucket directory FH is already in hand. */
        chimera_vfs_compound_add_open_current(compound,
                                              CHIMERA_VFS_OPEN_INFERRED |
                                              CHIMERA_VFS_OPEN_DIRECTORY, 0);
    } else {
        chimera_vfs_compound_add_lookup_path(compound,
                                             request->path, request->path_len,
                                             CHIMERA_VFS_ATTR_FH,
                                             CHIMERA_VFS_LOOKUP_FOLLOW);
        chimera_vfs_compound_add_open_current(compound,
                                              CHIMERA_VFS_OPEN_INFERRED, 0);
    }

    chimera_vfs_compound_add_gethandle(compound);
    chimera_vfs_compound_add_listxattrs(compound, 0,
                                        CHIMERA_S3_TAG_XATTR_BUFSZ);
} /* chimera_s3_tagging_prelude_build */

static void
chimera_s3_tagging_dispatch_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;
    int                              rc;

    rc = chimera_s3_tagging_prelude_collect(request, compound);

    chimera_vfs_compound_free(compound);

    if (rc < 0) {
        chimera_s3_tagging_finish(thread->evpl, request,
                                  CHIMERA_S3_STATUS_NO_SUCH_KEY);
        return;
    }

    chimera_s3_tagging_begin_op(thread->evpl, request);
} /* chimera_s3_tagging_dispatch_complete */

/* Resolve request->path under request->bucket_fh, open it, list its xattrs
 * and run the op. For an empty path (bucket tagging) the bucket directory
 * handle is opened directly.  The tag-set (for PUT) must already be
 * parsed/validated into request->tagging. */
static void
chimera_s3_tagging_dispatch(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request,
    int                              op)
{
    struct chimera_s3_tagging_ctx *ctx = chimera_s3_tagging_ctx_alloc(request);
    struct chimera_vfs_compound   *compound;

    ctx->op = op;

    compound = chimera_vfs_compound_alloc(thread->vfs, &request->cred);

    chimera_s3_tagging_prelude_build(request, compound);

    chimera_s3_request_get(request);

    chimera_vfs_compound_submit(compound, chimera_s3_tagging_dispatch_complete,
                                request);
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

/* ----- store-by-path: PutObject x-amz-tagging / CompleteMultipartUpload -----
 *
 * The same prelude as the subresource ops, then clear the existing tag
 * xattrs and write the parsed set -- best-effort throughout: the object is
 * already in place, and a tag that could not be stored does not fail the
 * PUT that stored the object. */

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

static void chimera_s3_tagging_store_set_next(
    struct evpl               *evpl,
    struct chimera_s3_request *request);

/* A failed set is skipped rather than fatal here; the tags behind it were
 * appended to the same sequence and did not run, so the cursor goes back to
 * the first of them. */
static void
chimera_s3_tagging_store_set_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    CHIMERA_S3_HOLD_REQUEST(private_data);
    struct chimera_s3_request     *request = private_data;
    struct chimera_s3_tagging_ctx *ctx     = request->tagging;
    uint32_t                       num_ops, ran;

    num_ops = chimera_vfs_compound_num_ops(compound);
    ran     = chimera_vfs_compound_num_completed(compound);

    ctx->cur -= (int) (num_ops - ran);

    chimera_vfs_compound_free(compound);

    chimera_s3_tagging_store_set_next(request->thread->evpl, request);
} /* chimera_s3_tagging_store_set_complete */

static void
chimera_s3_tagging_store_set_next(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread *thread = request->thread;
    struct chimera_vfs_compound     *compound;

    compound = chimera_vfs_compound_alloc(thread->vfs, &request->cred);

    if (chimera_s3_tagging_add_set_ops(request, compound) == 0) {
        chimera_vfs_compound_free(compound);
        chimera_s3_tagging_store_set_done(evpl, request);
        return;
    }

    chimera_s3_request_get(request);

    chimera_vfs_compound_submit(compound, chimera_s3_tagging_store_set_complete,
                                request);
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
chimera_s3_tagging_store_prelude_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct chimera_s3_tagging_ctx   *ctx     = request->tagging;
    int                              rc;

    void                             (*done)(
        struct evpl *,
        struct chimera_s3_request *) = ctx->store_done;

    rc = chimera_s3_tagging_prelude_collect(request, compound);

    chimera_vfs_compound_free(compound);

    if (rc < 0) {
        /* Best-effort: object tags couldn't be stored; proceed anyway. */
        chimera_s3_tagging_ctx_free(request);
        done(thread->evpl, request);
        return;
    }

    chimera_s3_tagging_clear_existing(thread->evpl, request,
                                      chimera_s3_tagging_store_after_clear);
} /* chimera_s3_tagging_store_prelude_complete */

void
chimera_s3_tagging_store_by_path(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request,
    void (                          *done_cb )(
        struct evpl               *evpl,
        struct chimera_s3_request *request))
{
    struct chimera_s3_tagging_ctx *ctx = request->tagging;
    struct chimera_vfs_compound   *compound;

    if (!ctx || ctx->n_tags == 0) {
        if (ctx) {
            chimera_s3_tagging_ctx_free(request);
        }
        done_cb(evpl, request);
        return;
    }

    ctx->store_done = done_cb;

    compound = chimera_vfs_compound_alloc(thread->vfs, &request->cred);

    chimera_s3_tagging_prelude_build(request, compound);

    chimera_s3_request_get(request);

    chimera_vfs_compound_submit(compound,
                                chimera_s3_tagging_store_prelude_complete,
                                request);
} /* chimera_s3_tagging_store_by_path */

/* ----- HEAD object: x-amz-tagging-count ----- */

int
chimera_s3_tagging_count_names(
    const char *names,
    uint32_t    names_len)
{
    uint32_t off = 0;
    int      n   = 0;

    while (off < names_len) {
        const char *name    = names + off;
        int         namelen = strnlen(name, names_len - off);

        if (namelen > CHIMERA_S3_TAG_PREFIX_LEN &&
            memcmp(name, CHIMERA_S3_TAG_PREFIX, CHIMERA_S3_TAG_PREFIX_LEN) == 0) {
            n++;
        }
        off += namelen + 1;
    }

    return n;
} /* chimera_s3_tagging_count_names */
