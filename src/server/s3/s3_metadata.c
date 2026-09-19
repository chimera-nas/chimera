// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * S3 object metadata <-> VFS extended attribute bridge.
 *
 * See s3_metadata.h for the namespace mapping.  Every xattr operation here
 * runs as a VFS sequence.  A store is a run of SETXATTR ops, chunked at the
 * sequence limit; a read is a LISTXATTRS (issued by the caller, in the same
 * sequence as its open) whose names fan out into GETXATTR ops as a following
 * sequence, because how many there are is the list's answer.  A copy is the
 * three in a row -- list the source, read a chunk of values, write them to
 * the destination -- since a SETXATTR's value has to be in hand when the op
 * is built and a GETXATTR's only arrives when its sequence is over.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "vfs/vfs.h"
#include "vfs/vfs_compound.h"
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

/* *INDENT-OFF* */ /* uncrustify oscillates on the aligned struct-init table */
static const struct chimera_s3_meta_sys chimera_s3_meta_sys_headers[] = {
    { "Content-Type",        "content-type"        },
    { "Content-Encoding",    "content-encoding"    },
    { "Content-Disposition", "content-disposition" },
    { "Cache-Control",       "cache-control"        },
    { "Expires",             "expires"             },
};
/* *INDENT-ON* */

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

#define CHIMERA_S3_META_MAX       256
#define CHIMERA_S3_META_VALUE_MAX 16384

/* The captured headers, and the cursor over which of them have been appended
 * to a sequence.  The request/handle/done triple is set when the store is
 * driven, which not every store is: a PUT folds what fits into its own setup
 * sequence and drives only the remainder. */
struct chimera_s3_meta_store {
    struct chimera_s3_request      *request;
    struct chimera_vfs_open_handle *handle;
    chimera_s3_metadata_done_t      done;
    void                           *private_data;
    int                             count;
    int                             cur;
    int                             error;
    struct chimera_s3_meta_kv       kv[CHIMERA_S3_META_MAX];
};

/* Context for the attach (xattr -> response header) fan-out. */
struct chimera_s3_meta_attach_ctx {
    struct chimera_s3_request      *request;
    struct chimera_vfs_open_handle *handle;
    chimera_s3_metadata_done_t      done;
    void                           *private_data;
    int                             count;
    int                             cur;
    int                             chunk_n;
    char                           *names[CHIMERA_S3_META_MAX];
    int                             name_lens[CHIMERA_S3_META_MAX];
    char                            value[CHIMERA_S3_META_VALUE_MAX];
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
    struct chimera_s3_meta_store *store,
    const char                   *suffix,
    int                           suffix_len,
    const char                   *value)
{
    struct chimera_s3_meta_kv *kv;
    char                       name[512];
    int                        name_len;

    if (store->count >= CHIMERA_S3_META_MAX) {
        return;
    }

    name_len = snprintf(name, sizeof(name), "%s%.*s",
                        CHIMERA_S3_XATTR_PREFIX, suffix_len, suffix);

    if (name_len <= 0 || name_len >= (int) sizeof(name)) {
        return;
    }

    kv            = &store->kv[store->count++];
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
    struct chimera_s3_meta_store *store = private_data;
    char                          suffix[512];
    const char                   *key;
    int                           key_len, suffix_len, i;

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
                          &CHIMERA_S3_XATTR_META[CHIMERA_S3_XATTR_PREFIX_LEN]);

    for (i = 0; i < key_len && suffix_len < (int) sizeof(suffix) - 1; i++) {
        suffix[suffix_len++] = (char) tolower((unsigned char) key[i]);
    }
    suffix[suffix_len] = '\0';

    chimera_s3_meta_add_kv(store, suffix, suffix_len, value);
} /* chimera_s3_meta_capture_user_cb */

struct chimera_s3_meta_store *
chimera_s3_metadata_capture(struct chimera_s3_request *request)
{
    struct chimera_s3_meta_store *store;
    const char                   *value;
    int                           i;

    store = calloc(1, sizeof(*store));

    for (i = 0; i < CHIMERA_S3_META_SYS_COUNT; i++) {
        value = evpl_http_request_header(request->http_request,
                                         chimera_s3_meta_sys_headers[i].header);
        if (value) {
            chimera_s3_meta_add_kv(store,
                                   chimera_s3_meta_sys_headers[i].suffix,
                                   strlen(chimera_s3_meta_sys_headers[i].suffix),
                                   value);
        }
    }

    evpl_http_request_header_iterate(request->http_request,
                                     chimera_s3_meta_capture_user_cb, store);

    if (store->count == 0) {
        free(store);
        return NULL;
    }

    return store;
} /* chimera_s3_metadata_capture */

void
chimera_s3_metadata_store_free(struct chimera_s3_meta_store *store)
{
    int i;

    if (!store) {
        return;
    }

    for (i = 0; i < store->count; i++) {
        free(store->kv[i].name);
        free(store->kv[i].value);
    }
    free(store);
} /* chimera_s3_metadata_store_free */

int
chimera_s3_metadata_add_ops(
    struct chimera_s3_meta_store   *store,
    struct chimera_vfs_compound    *compound,
    int                             handle_from,
    struct chimera_vfs_open_handle *handle,
    int                             budget)
{
    struct chimera_s3_meta_kv *kv;
    int                        n = 0;
    int                        index;

    while (store->cur < store->count && n < budget) {
        kv = &store->kv[store->cur];

        index = chimera_vfs_compound_add_setxattr(compound,
                                                  CHIMERA_VFS_XATTR_EITHER,
                                                  kv->name, kv->name_len,
                                                  kv->value, kv->value_len);
        if (index < 0) {
            break;
        }

        if (handle) {
            chimera_vfs_compound_op_set_handle(compound, index, handle);
        } else if (handle_from >= 0) {
            chimera_vfs_compound_op_use_handle(compound, index, handle_from);
        }

        store->cur++;
        n++;
    }

    return n;
} /* chimera_s3_metadata_add_ops */

/* ---------- store driver: the remaining headers, a sequence at a time ---------- */

static void chimera_s3_meta_store_next(
    struct chimera_s3_meta_store *store);

static void
chimera_s3_meta_store_finish(struct chimera_s3_meta_store *store)
{
    struct chimera_s3_request *request = store->request;
    chimera_s3_metadata_done_t done    = store->done;
    void                      *pd      = store->private_data;
    int                        error   = store->error;

    chimera_s3_metadata_store_free(store);
    chimera_s3_request_drop(request);

    done(request, error, pd);
} /* chimera_s3_meta_store_finish */

static void
chimera_s3_meta_store_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_s3_meta_store *store = private_data;

    if (chimera_vfs_compound_status(compound) != CHIMERA_VFS_OK) {
        store->error = 1;
    }

    chimera_vfs_compound_free(compound);

    if (store->error) {
        chimera_s3_meta_store_finish(store);
        return;
    }

    chimera_s3_meta_store_next(store);
} /* chimera_s3_meta_store_sequence_complete */

static void
chimera_s3_meta_store_next(struct chimera_s3_meta_store *store)
{
    struct chimera_server_s3_thread *thread = store->request->thread;
    struct chimera_vfs_compound     *compound;

    if (store->cur >= store->count) {
        chimera_s3_meta_store_finish(store);
        return;
    }

    compound = chimera_vfs_compound_alloc(thread->vfs, &store->request->cred);

    chimera_s3_metadata_add_ops(store, compound, -1, store->handle,
                                CHIMERA_VFS_COMPOUND_MAX_OPS);

    chimera_vfs_compound_submit(compound,
                                chimera_s3_meta_store_sequence_complete, store);
} /* chimera_s3_meta_store_next */

void
chimera_s3_metadata_store_drive(
    struct chimera_s3_meta_store   *store,
    struct chimera_s3_request      *request,
    struct chimera_vfs_open_handle *handle,
    chimera_s3_metadata_done_t      done,
    void                           *private_data)
{
    store->request      = request;
    store->handle       = handle;
    store->done         = done;
    store->private_data = private_data;

    /* The store outlives the call that drives it: its completion callbacks
     * dereference the request, so it holds a reference of its own. */
    chimera_s3_request_get(request);

    chimera_s3_meta_store_next(store);
} /* chimera_s3_metadata_store_drive */

void
chimera_s3_metadata_store_from_headers(
    struct chimera_s3_request      *request,
    struct chimera_vfs_open_handle *handle,
    chimera_s3_metadata_done_t      done,
    void                           *private_data)
{
    struct chimera_s3_meta_store *store;

    store = chimera_s3_metadata_capture(request);

    if (!store) {
        done(request, 0, private_data);
        return;
    }

    chimera_s3_metadata_store_drive(store, request, handle, done, private_data);
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
    chimera_s3_request_drop(ctx->request);
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
        chimera_s3_response_add_header(request, hdr, value);
        return;
    }

    /* fixed system header */
    suffix = name + CHIMERA_S3_XATTR_PREFIX_LEN;

    for (i = 0; i < CHIMERA_S3_META_SYS_COUNT; i++) {
        if (strcmp(suffix, chimera_s3_meta_sys_headers[i].suffix) == 0) {
            chimera_s3_response_add_header(request,
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

/*
 * One chunk of GETXATTRs is over.  Every op that ran OK is emitted; an op
 * that failed is skipped -- a name that vanished between the list and the
 * read is not an error worth a header -- and the fan-out resumes from the
 * op after it, since the sequence stopped there.
 */
static void
chimera_s3_meta_attach_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_s3_meta_attach_ctx    *ctx = private_data;
    const struct chimera_vfs_compound_op *op;
    uint32_t                              ran, i, value_len;

    ran = chimera_vfs_compound_num_completed(compound);

    for (i = 0; i < ran; i++) {
        op = chimera_vfs_compound_op(compound, i);

        if (op->status != CHIMERA_VFS_OK) {
            continue;
        }

        value_len = op->buffer_len;
        if (value_len >= sizeof(ctx->value)) {
            value_len = sizeof(ctx->value) - 1;
        }
        memcpy(ctx->value, op->buffer, value_len);
        ctx->value[value_len] = '\0';

        chimera_s3_meta_emit_header(ctx->request, ctx->names[ctx->cur + i],
                                    ctx->value);
    }

    chimera_vfs_compound_free(compound);

    ctx->cur += ran;

    chimera_s3_meta_attach_next(ctx);
} /* chimera_s3_meta_attach_sequence_complete */

static void
chimera_s3_meta_attach_next(struct chimera_s3_meta_attach_ctx *ctx)
{
    struct chimera_server_s3_thread *thread = ctx->request->thread;
    struct chimera_vfs_compound     *compound;
    int                              i, index;

    if (ctx->cur >= ctx->count) {
        chimera_s3_meta_attach_finish(ctx);
        return;
    }

    compound = chimera_vfs_compound_alloc(thread->vfs, &ctx->request->cred);

    ctx->chunk_n = 0;

    for (i = ctx->cur; i < ctx->count &&
         ctx->chunk_n < CHIMERA_VFS_COMPOUND_MAX_OPS; i++) {
        index = chimera_vfs_compound_add_getxattr(compound,
                                                  ctx->names[i],
                                                  ctx->name_lens[i],
                                                  sizeof(ctx->value) - 1);
        if (index < 0) {
            break;
        }
        chimera_vfs_compound_op_set_handle(compound, index, ctx->handle);
        ctx->chunk_n++;
    }

    chimera_vfs_compound_submit(compound,
                                chimera_s3_meta_attach_sequence_complete, ctx);
} /* chimera_s3_meta_attach_next */

/* Keep the names in our namespace out of a LISTXATTRS page (back-to-back
 * NUL-terminated); the per-name value fetches follow. */
static int
chimera_s3_meta_collect_names(
    const char *names,
    uint32_t    names_len,
    char      **out_names,
    int        *out_lens)
{
    uint32_t off   = 0;
    int      count = 0;

    while (off < names_len && count < CHIMERA_S3_META_MAX) {
        const char *name = names + off;
        int         len  = strnlen(name, names_len - off);

        if (len > 0 &&
            strncmp(name, CHIMERA_S3_XATTR_PREFIX,
                    CHIMERA_S3_XATTR_PREFIX_LEN) == 0) {
            out_names[count] = chimera_s3_meta_dup(name, len);
            out_lens[count]  = len;
            count++;
        }

        off += len + 1;
    }

    return count;
} /* chimera_s3_meta_collect_names */

void
chimera_s3_metadata_attach_from_list(
    struct chimera_s3_request      *request,
    struct chimera_vfs_open_handle *handle,
    const char                     *names,
    uint32_t                        names_len,
    chimera_s3_metadata_done_t      done,
    void                           *private_data)
{
    struct chimera_s3_meta_attach_ctx *ctx;

    ctx          = calloc(1, sizeof(*ctx));
    ctx->request = request;
    chimera_s3_request_get(request);
    ctx->handle       = handle;
    ctx->done         = done;
    ctx->private_data = private_data;

    ctx->count = chimera_s3_meta_collect_names(names, names_len,
                                               ctx->names, ctx->name_lens);

    chimera_s3_meta_attach_next(ctx);
} /* chimera_s3_metadata_attach_from_list */

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
    /* The chunk in flight: how many names its GETXATTR sequence ran over,
     * and the values it brought back (NULL for a name whose read failed,
     * which is skipped as the per-op copy skipped it). */
    int                             chunk_ran;
    char                           *values[CHIMERA_VFS_COMPOUND_MAX_OPS];
    uint32_t                        value_lens[CHIMERA_VFS_COMPOUND_MAX_OPS];
};

static void chimera_s3_meta_copy_next(
    struct chimera_s3_meta_copy_ctx *ctx);

static void
chimera_s3_meta_copy_release_values(struct chimera_s3_meta_copy_ctx *ctx)
{
    int i;

    for (i = 0; i < ctx->chunk_ran; i++) {
        free(ctx->values[i]);
        ctx->values[i] = NULL;
    }
    ctx->chunk_ran = 0;
} /* chimera_s3_meta_copy_release_values */

static void
chimera_s3_meta_copy_finish(struct chimera_s3_meta_copy_ctx *ctx)
{
    struct chimera_s3_request *request = ctx->request;
    chimera_s3_metadata_done_t done    = ctx->done;
    void                      *pd      = ctx->private_data;
    int                        error   = ctx->error;
    int                        i;

    chimera_s3_meta_copy_release_values(ctx);

    for (i = 0; i < ctx->count; i++) {
        free(ctx->names[i]);
    }
    chimera_s3_request_drop(ctx->request);
    free(ctx);

    done(request, error, pd);
} /* chimera_s3_meta_copy_finish */

/* The SETXATTR chunk landed on the destination.  A failure stops the copy,
 * as it did one xattr at a time. */
static void
chimera_s3_meta_copy_set_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_s3_meta_copy_ctx *ctx = private_data;

    if (chimera_vfs_compound_status(compound) != CHIMERA_VFS_OK) {
        ctx->error = 1;
    }

    chimera_vfs_compound_free(compound);

    ctx->cur += ctx->chunk_ran;
    chimera_s3_meta_copy_release_values(ctx);

    if (ctx->error) {
        chimera_s3_meta_copy_finish(ctx);
        return;
    }

    chimera_s3_meta_copy_next(ctx);
} /* chimera_s3_meta_copy_set_complete */

/* The GETXATTR chunk answered; write what it brought back to the destination
 * as the following sequence.  A name whose read failed is skipped, and the
 * names behind a failure (which did not run) are picked up by the next
 * chunk. */
static void
chimera_s3_meta_copy_get_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_s3_meta_copy_ctx      *ctx    = private_data;
    struct chimera_server_s3_thread      *thread = ctx->request->thread;
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_compound          *set;
    uint32_t                              ran, i;
    int                                   index, n = 0;

    ran = chimera_vfs_compound_num_completed(compound);

    for (i = 0; i < ran; i++) {
        op = chimera_vfs_compound_op(compound, i);

        if (op->status != CHIMERA_VFS_OK) {
            ctx->values[i] = NULL;
            continue;
        }

        ctx->values[i] = malloc(op->buffer_len ? op->buffer_len : 1);
        memcpy(ctx->values[i], op->buffer, op->buffer_len);
        ctx->value_lens[i] = op->buffer_len;
    }

    ctx->chunk_ran = ran;

    chimera_vfs_compound_free(compound);

    set = chimera_vfs_compound_alloc(thread->vfs, &ctx->request->cred);

    for (i = 0; i < ran; i++) {
        if (!ctx->values[i]) {
            continue;
        }
        index = chimera_vfs_compound_add_setxattr(set,
                                                  CHIMERA_VFS_XATTR_EITHER,
                                                  ctx->names[ctx->cur + i],
                                                  ctx->name_lens[ctx->cur + i],
                                                  ctx->values[i],
                                                  ctx->value_lens[i]);
        chimera_vfs_compound_op_set_handle(set, index, ctx->dst_handle);
        n++;
    }

    if (n == 0) {
        /* Nothing readable in this chunk: advance past it. */
        chimera_vfs_compound_free(set);
        ctx->cur += ran;
        chimera_s3_meta_copy_release_values(ctx);
        chimera_s3_meta_copy_next(ctx);
        return;
    }

    chimera_vfs_compound_submit(set, chimera_s3_meta_copy_set_complete, ctx);
} /* chimera_s3_meta_copy_get_complete */

static void
chimera_s3_meta_copy_next(struct chimera_s3_meta_copy_ctx *ctx)
{
    struct chimera_server_s3_thread *thread = ctx->request->thread;
    struct chimera_vfs_compound     *compound;
    int                              i, n = 0, index;

    if (ctx->cur >= ctx->count) {
        chimera_s3_meta_copy_finish(ctx);
        return;
    }

    compound = chimera_vfs_compound_alloc(thread->vfs, &ctx->request->cred);

    for (i = ctx->cur; i < ctx->count && n < CHIMERA_VFS_COMPOUND_MAX_OPS; i++) {
        index = chimera_vfs_compound_add_getxattr(compound,
                                                  ctx->names[i],
                                                  ctx->name_lens[i],
                                                  CHIMERA_S3_META_VALUE_MAX);
        if (index < 0) {
            break;
        }
        chimera_vfs_compound_op_set_handle(compound, index, ctx->src_handle);
        n++;
    }

    chimera_vfs_compound_submit(compound, chimera_s3_meta_copy_get_complete,
                                ctx);
} /* chimera_s3_meta_copy_next */

static void
chimera_s3_meta_copy_list_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_s3_meta_copy_ctx      *ctx = private_data;
    const struct chimera_vfs_compound_op *op;

    if (chimera_vfs_compound_status(compound) != CHIMERA_VFS_OK) {
        /* Nothing to copy. */
        chimera_vfs_compound_free(compound);
        chimera_s3_meta_copy_finish(ctx);
        return;
    }

    op = chimera_vfs_compound_op(compound, 0);

    ctx->count = chimera_s3_meta_collect_names(op->buffer, op->buffer_len,
                                               ctx->names, ctx->name_lens);

    chimera_vfs_compound_free(compound);

    chimera_s3_meta_copy_next(ctx);
} /* chimera_s3_meta_copy_list_complete */

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
    struct chimera_vfs_compound     *compound;
    int                              index;

    ctx          = calloc(1, sizeof(*ctx));
    ctx->request = request;
    chimera_s3_request_get(request);
    ctx->src_handle   = src_handle;
    ctx->dst_handle   = dst_handle;
    ctx->done         = done;
    ctx->private_data = private_data;

    compound = chimera_vfs_compound_alloc(thread->vfs, &request->cred);

    index = chimera_vfs_compound_add_listxattrs(compound, 0,
                                                CHIMERA_S3_META_VALUE_MAX);
    chimera_vfs_compound_op_set_handle(compound, index, src_handle);

    chimera_vfs_compound_submit(compound, chimera_s3_meta_copy_list_complete,
                                ctx);
} /* chimera_s3_metadata_copy */
