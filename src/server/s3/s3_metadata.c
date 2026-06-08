// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * S3 object metadata <-> VFS extended attribute bridge.
 *
 * See s3_metadata.h for the namespace mapping. The store and attach helpers
 * here drive a sequential async state machine (one xattr op outstanding at a
 * time) over a heap-allocated context so that an arbitrary number of headers
 * can be persisted/read without blocking the event loop.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "s3_internal.h"
#include "s3_metadata.h"

/*
 * The fixed system headers we round-trip, paired with the suffix used under
 * the "user.s3." namespace. The xattr name is CHIMERA_S3_XATTR_PREFIX + suffix
 * and the response header re-emitted on GET/HEAD is the original header name.
 */
struct chimera_s3_meta_sys {
    const char *header;     /* HTTP header name                 */
    const char *suffix;     /* xattr name after CHIMERA_S3_XATTR_PREFIX */
};

static const struct chimera_s3_meta_sys chimera_s3_meta_sys_headers[] = {
    { "Content-Type",        "content-type"                    },
    { "Content-Encoding",    "content-encoding"                },
    { "Content-Disposition", "content-disposition"             },
    { "Cache-Control",       "cache-control"                   },
    { "Expires",             "expires"                         },
};

#define CHIMERA_S3_META_SYS_COUNT \
        (int) (sizeof(chimera_s3_meta_sys_headers) / \
               sizeof(chimera_s3_meta_sys_headers[0]))

/* One name/value pair to persist. */
struct chimera_s3_meta_kv {
    char *name;     /* full xattr name, e.g. "user.s3.content-type" */
    char *value;
    int   name_len;
    int   value_len;
};

#define CHIMERA_S3_META_MAX 256

/* Context for the store (header -> xattr) state machine. */
struct chimera_s3_meta_store_ctx {
    struct chimera_s3_request      *request;
    struct chimera_vfs_open_handle *handle;
    chimera_s3_metadata_done_t      done;
    void                           *private_data;
    int                             count;
    int                             cur;
    int                             error;
    struct chimera_s3_meta_kv       kv[CHIMERA_S3_META_MAX];
};

/* Context for the attach (xattr -> response header) state machine. */
struct chimera_s3_meta_attach_ctx {
    struct chimera_s3_request      *request;
    struct chimera_vfs_open_handle *handle;
    chimera_s3_metadata_done_t      done;
    void                           *private_data;
    int                             count;
    int                             cur;
    char                           *names[CHIMERA_S3_META_MAX];
    int                             name_lens[CHIMERA_S3_META_MAX];
    char                            list_buf[16384];
    char                            value[16384];
};

/* ---------- header capture ---------- */

static char *
chimera_s3_meta_dup(
    const char *s,
    int         len)
{
    char *p = malloc(len + 1);

    memcpy(p, s, len);
    p[len] = '\0';
    return p;
} /* chimera_s3_meta_dup */

static void
chimera_s3_meta_add_kv(
    struct chimera_s3_meta_store_ctx *ctx,
    const char                       *suffix,
    int                               suffix_len,
    const char                       *value)
{
    struct chimera_s3_meta_kv *kv;
    char                       name[512];
    int                        name_len;

    if (ctx->count >= CHIMERA_S3_META_MAX) {
        return;
    }

    name_len = snprintf(name, sizeof(name), "%s%.*s",
                        CHIMERA_S3_XATTR_PREFIX, suffix_len, suffix);

    if (name_len <= 0 || name_len >= (int) sizeof(name)) {
        return;
    }

    kv            = &ctx->kv[ctx->count++];
    kv->name      = chimera_s3_meta_dup(name, name_len);
    kv->name_len  = name_len;
    kv->value     = chimera_s3_meta_dup(value, strlen(value));
    kv->value_len = strlen(value);
} /* chimera_s3_meta_add_kv */

/*
 * Iterate-callback that picks up x-amz-meta-* headers. The fixed system headers
 * are captured separately (header_iterate hands us every header, but explicit
 * lookups are clearer for the small fixed set).
 */
static void
chimera_s3_meta_capture_user_cb(
    const char *name,
    const char *value,
    void       *private_data)
{
    struct chimera_s3_meta_store_ctx *ctx = private_data;
    char                              suffix[512];
    const char                       *key;
    int                               key_len, suffix_len, i;

    if (strncasecmp(name, "x-amz-meta-", 11) != 0) {
        return;
    }

    key     = name + 11;
    key_len = strlen(key);

    if (key_len == 0) {
        return;
    }

    /* Lower-case the user key so the xattr name is canonical (S3 metadata keys
     * are case-insensitive and AWS returns them lower-cased). */
    suffix_len = snprintf(suffix, sizeof(suffix), "%s",
                          CHIMERA_S3_XATTR_META + CHIMERA_S3_XATTR_PREFIX_LEN);

    for (i = 0; i < key_len && suffix_len < (int) sizeof(suffix) - 1; i++) {
        suffix[suffix_len++] = (char) tolower((unsigned char) key[i]);
    }
    suffix[suffix_len] = '\0';

    chimera_s3_meta_add_kv(ctx, suffix, suffix_len, value);
} /* chimera_s3_meta_capture_user_cb */

static void
chimera_s3_meta_capture(struct chimera_s3_meta_store_ctx *ctx)
{
    const char *value;
    int         i;

    for (i = 0; i < CHIMERA_S3_META_SYS_COUNT; i++) {
        value = evpl_http_request_header(ctx->request->http_request,
                                         chimera_s3_meta_sys_headers[i].header);
        if (value) {
            chimera_s3_meta_add_kv(ctx,
                                   chimera_s3_meta_sys_headers[i].suffix,
                                   strlen(chimera_s3_meta_sys_headers[i].suffix),
                                   value);
        }
    }

    evpl_http_request_header_iterate(ctx->request->http_request,
                                     chimera_s3_meta_capture_user_cb, ctx);
} /* chimera_s3_meta_capture */

/* ---------- store state machine ---------- */

static void chimera_s3_meta_store_next(
    struct chimera_s3_meta_store_ctx *ctx);

static void
chimera_s3_meta_store_finish(struct chimera_s3_meta_store_ctx *ctx)
{
    struct chimera_s3_request *request = ctx->request;
    chimera_s3_metadata_done_t done    = ctx->done;
    void                      *pd      = ctx->private_data;
    int                        error   = ctx->error;
    int                        i;

    for (i = 0; i < ctx->count; i++) {
        free(ctx->kv[i].name);
        free(ctx->kv[i].value);
    }
    free(ctx);

    done(request, error, pd);
} /* chimera_s3_meta_store_finish */

static void
chimera_s3_meta_store_set_callback(
    enum chimera_vfs_error          error_code,
    const struct chimera_vfs_attrs *pre_attr,
    const struct chimera_vfs_attrs *post_attr,
    void                           *private_data)
{
    struct chimera_s3_meta_store_ctx *ctx = private_data;

    if (error_code) {
        ctx->error = 1;
        chimera_s3_meta_store_finish(ctx);
        return;
    }

    ctx->cur++;
    chimera_s3_meta_store_next(ctx);
} /* chimera_s3_meta_store_set_callback */

static void
chimera_s3_meta_store_next(struct chimera_s3_meta_store_ctx *ctx)
{
    struct chimera_server_s3_thread *thread = ctx->request->thread;
    struct chimera_s3_meta_kv       *kv;

    if (ctx->cur >= ctx->count) {
        chimera_s3_meta_store_finish(ctx);
        return;
    }

    kv = &ctx->kv[ctx->cur];

    chimera_vfs_set_xattr(thread->vfs, &thread->shared->cred,
                          ctx->handle,
                          CHIMERA_VFS_XATTR_EITHER,
                          kv->name,
                          kv->name_len,
                          kv->value,
                          kv->value_len,
                          chimera_s3_meta_store_set_callback,
                          ctx);
} /* chimera_s3_meta_store_next */

void
chimera_s3_metadata_store_from_headers(
    struct chimera_s3_request      *request,
    struct chimera_vfs_open_handle *handle,
    chimera_s3_metadata_done_t      done,
    void                           *private_data)
{
    struct chimera_s3_meta_store_ctx *ctx;

    ctx               = calloc(1, sizeof(*ctx));
    ctx->request      = request;
    ctx->handle       = handle;
    ctx->done         = done;
    ctx->private_data = private_data;

    chimera_s3_meta_capture(ctx);

    chimera_s3_meta_store_next(ctx);
} /* chimera_s3_metadata_store_from_headers */

/* ---------- attach (read xattrs -> response headers) ---------- */

static void chimera_s3_meta_attach_next(
    struct chimera_s3_meta_attach_ctx *ctx);

static void
chimera_s3_meta_attach_finish(struct chimera_s3_meta_attach_ctx *ctx)
{
    struct chimera_s3_request *request = ctx->request;
    chimera_s3_metadata_done_t done    = ctx->done;
    void                      *pd      = ctx->private_data;
    int                        i;

    for (i = 0; i < ctx->count; i++) {
        free(ctx->names[i]);
    }
    free(ctx);

    done(request, 0, pd);
} /* chimera_s3_meta_attach_finish */

/*
 * Turn a stored xattr name back into the HTTP response header it should be
 * emitted as, and add it. `name` is the full "user.s3...." xattr name; `value`
 * is NUL-terminated (xattr values are stored verbatim, but S3 metadata values
 * are text so this is safe).
 */
static void
chimera_s3_meta_emit_header(
    struct chimera_s3_request *request,
    const char                *name,
    const char                *value)
{
    const char *suffix;
    int         i;

    if (strncmp(name, CHIMERA_S3_XATTR_PREFIX, CHIMERA_S3_XATTR_PREFIX_LEN) !=
        0) {
        return;
    }

    /* user-meta: user.s3.meta.<key> -> x-amz-meta-<key> */
    if (strncmp(name, CHIMERA_S3_XATTR_META, CHIMERA_S3_XATTR_META_LEN) == 0) {
        char hdr[512];

        suffix = name + CHIMERA_S3_XATTR_META_LEN;
        snprintf(hdr, sizeof(hdr), "x-amz-meta-%s", suffix);
        evpl_http_request_add_header(request->http_request, hdr, value);
        return;
    }

    /* fixed system header */
    suffix = name + CHIMERA_S3_XATTR_PREFIX_LEN;

    for (i = 0; i < CHIMERA_S3_META_SYS_COUNT; i++) {
        if (strcmp(suffix, chimera_s3_meta_sys_headers[i].suffix) == 0) {
            evpl_http_request_add_header(request->http_request,
                                         chimera_s3_meta_sys_headers[i].header,
                                         value);
            if (strcmp(chimera_s3_meta_sys_headers[i].suffix,
                       "content-type") == 0) {
                request->have_content_type = 1;
            }
            return;
        }
    }
} /* chimera_s3_meta_emit_header */

static void
chimera_s3_meta_attach_get_callback(
    enum chimera_vfs_error error_code,
    uint32_t               value_len,
    void                  *private_data)
{
    struct chimera_s3_meta_attach_ctx *ctx = private_data;

    if (error_code == CHIMERA_VFS_OK) {
        if (value_len >= sizeof(ctx->value)) {
            value_len = sizeof(ctx->value) - 1;
        }
        ctx->value[value_len] = '\0';
        chimera_s3_meta_emit_header(ctx->request,
                                    ctx->names[ctx->cur],
                                    ctx->value);
    }

    ctx->cur++;
    chimera_s3_meta_attach_next(ctx);
} /* chimera_s3_meta_attach_get_callback */

static void
chimera_s3_meta_attach_next(struct chimera_s3_meta_attach_ctx *ctx)
{
    struct chimera_server_s3_thread *thread = ctx->request->thread;

    if (ctx->cur >= ctx->count) {
        chimera_s3_meta_attach_finish(ctx);
        return;
    }

    chimera_vfs_get_xattr(thread->vfs, &thread->shared->cred,
                          ctx->handle,
                          ctx->names[ctx->cur],
                          ctx->name_lens[ctx->cur],
                          ctx->value,
                          sizeof(ctx->value) - 1,
                          chimera_s3_meta_attach_get_callback,
                          ctx);
} /* chimera_s3_meta_attach_next */

static void
chimera_s3_meta_attach_list_callback(
    enum chimera_vfs_error error_code,
    const char            *names,
    uint32_t               names_len,
    uint32_t               count,
    uint32_t               eof,
    uint64_t               cookie,
    void                  *private_data)
{
    struct chimera_s3_meta_attach_ctx *ctx = private_data;
    uint32_t                           off = 0;

    if (error_code != CHIMERA_VFS_OK) {
        /* No xattrs (ENODATA/ENOTSUP) or read error: leave defaults. */
        chimera_s3_meta_attach_finish(ctx);
        return;
    }

    /* names is a back-to-back NUL-terminated list of full xattr names. Keep the
     * ones in our namespace; the per-name value fetches happen next. */
    while (off < names_len && ctx->count < CHIMERA_S3_META_MAX) {
        const char *name = names + off;
        int         len  = strlen(name);

        if (len > 0 &&
            strncmp(name, CHIMERA_S3_XATTR_PREFIX,
                    CHIMERA_S3_XATTR_PREFIX_LEN) == 0) {
            ctx->names[ctx->count]     = chimera_s3_meta_dup(name, len);
            ctx->name_lens[ctx->count] = len;
            ctx->count++;
        }

        off += len + 1;
    }

    chimera_s3_meta_attach_next(ctx);
} /* chimera_s3_meta_attach_list_callback */

void
chimera_s3_metadata_attach_headers(
    struct chimera_s3_request      *request,
    struct chimera_vfs_open_handle *handle,
    chimera_s3_metadata_done_t      done,
    void                           *private_data)
{
    struct chimera_s3_meta_attach_ctx *ctx;
    struct chimera_server_s3_thread   *thread = request->thread;

    ctx               = calloc(1, sizeof(*ctx));
    ctx->request      = request;
    ctx->handle       = handle;
    ctx->done         = done;
    ctx->private_data = private_data;

    chimera_vfs_list_xattrs(thread->vfs, &thread->shared->cred,
                            handle,
                            0,
                            ctx->list_buf,
                            sizeof(ctx->list_buf),
                            chimera_s3_meta_attach_list_callback,
                            ctx);
} /* chimera_s3_metadata_attach_headers */

/* ---------- copy (src xattrs -> dst xattrs) ---------- */

struct chimera_s3_meta_copy_ctx {
    struct chimera_s3_request      *request;
    struct chimera_vfs_open_handle *src_handle;
    struct chimera_vfs_open_handle *dst_handle;
    chimera_s3_metadata_done_t      done;
    void                           *private_data;
    int                             count;
    int                             cur;
    int                             error;
    char                           *names[CHIMERA_S3_META_MAX];
    int                             name_lens[CHIMERA_S3_META_MAX];
    char                            list_buf[16384];
    char                            value[16384];
    int                             value_len;
};

static void chimera_s3_meta_copy_next(
    struct chimera_s3_meta_copy_ctx *ctx);

static void
chimera_s3_meta_copy_finish(struct chimera_s3_meta_copy_ctx *ctx)
{
    struct chimera_s3_request *request = ctx->request;
    chimera_s3_metadata_done_t done    = ctx->done;
    void                      *pd      = ctx->private_data;
    int                        error   = ctx->error;
    int                        i;

    for (i = 0; i < ctx->count; i++) {
        free(ctx->names[i]);
    }
    free(ctx);

    done(request, error, pd);
} /* chimera_s3_meta_copy_finish */

static void
chimera_s3_meta_copy_set_callback(
    enum chimera_vfs_error          error_code,
    const struct chimera_vfs_attrs *pre_attr,
    const struct chimera_vfs_attrs *post_attr,
    void                           *private_data)
{
    struct chimera_s3_meta_copy_ctx *ctx = private_data;

    if (error_code) {
        ctx->error = 1;
        chimera_s3_meta_copy_finish(ctx);
        return;
    }

    ctx->cur++;
    chimera_s3_meta_copy_next(ctx);
} /* chimera_s3_meta_copy_set_callback */

static void
chimera_s3_meta_copy_get_callback(
    enum chimera_vfs_error error_code,
    uint32_t               value_len,
    void                  *private_data)
{
    struct chimera_s3_meta_copy_ctx *ctx    = private_data;
    struct chimera_server_s3_thread *thread = ctx->request->thread;

    if (error_code != CHIMERA_VFS_OK) {
        /* Source xattr vanished between list and get; skip it. */
        ctx->cur++;
        chimera_s3_meta_copy_next(ctx);
        return;
    }

    chimera_vfs_set_xattr(thread->vfs, &thread->shared->cred,
                          ctx->dst_handle,
                          CHIMERA_VFS_XATTR_EITHER,
                          ctx->names[ctx->cur],
                          ctx->name_lens[ctx->cur],
                          ctx->value,
                          value_len,
                          chimera_s3_meta_copy_set_callback,
                          ctx);
} /* chimera_s3_meta_copy_get_callback */

static void
chimera_s3_meta_copy_next(struct chimera_s3_meta_copy_ctx *ctx)
{
    struct chimera_server_s3_thread *thread = ctx->request->thread;

    if (ctx->cur >= ctx->count) {
        chimera_s3_meta_copy_finish(ctx);
        return;
    }

    chimera_vfs_get_xattr(thread->vfs, &thread->shared->cred,
                          ctx->src_handle,
                          ctx->names[ctx->cur],
                          ctx->name_lens[ctx->cur],
                          ctx->value,
                          sizeof(ctx->value),
                          chimera_s3_meta_copy_get_callback,
                          ctx);
} /* chimera_s3_meta_copy_next */

static void
chimera_s3_meta_copy_list_callback(
    enum chimera_vfs_error error_code,
    const char            *names,
    uint32_t               names_len,
    uint32_t               count,
    uint32_t               eof,
    uint64_t               cookie,
    void                  *private_data)
{
    struct chimera_s3_meta_copy_ctx *ctx = private_data;
    uint32_t                         off = 0;

    if (error_code != CHIMERA_VFS_OK) {
        /* Nothing to copy. */
        chimera_s3_meta_copy_finish(ctx);
        return;
    }

    while (off < names_len && ctx->count < CHIMERA_S3_META_MAX) {
        const char *name = names + off;
        int         len  = strlen(name);

        if (len > 0 &&
            strncmp(name, CHIMERA_S3_XATTR_PREFIX,
                    CHIMERA_S3_XATTR_PREFIX_LEN) == 0) {
            ctx->names[ctx->count]     = chimera_s3_meta_dup(name, len);
            ctx->name_lens[ctx->count] = len;
            ctx->count++;
        }

        off += len + 1;
    }

    chimera_s3_meta_copy_next(ctx);
} /* chimera_s3_meta_copy_list_callback */

void
chimera_s3_metadata_copy(
    struct chimera_s3_request      *request,
    struct chimera_vfs_open_handle *src_handle,
    struct chimera_vfs_open_handle *dst_handle,
    chimera_s3_metadata_done_t      done,
    void                           *private_data)
{
    struct chimera_s3_meta_copy_ctx *ctx;
    struct chimera_server_s3_thread *thread = request->thread;

    ctx               = calloc(1, sizeof(*ctx));
    ctx->request      = request;
    ctx->src_handle   = src_handle;
    ctx->dst_handle   = dst_handle;
    ctx->done         = done;
    ctx->private_data = private_data;

    chimera_vfs_list_xattrs(thread->vfs, &thread->shared->cred,
                            src_handle,
                            0,
                            ctx->list_buf,
                            sizeof(ctx->list_buf),
                            chimera_s3_meta_copy_list_callback,
                            ctx);
} /* chimera_s3_metadata_copy */
