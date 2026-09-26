// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * S3 object metadata <-> VFS extended attribute bridge.
 *
 * See s3_metadata.h for the namespace mapping. Builders append xattr operations
 * to their caller's compound. Read results remain private until acceptance;
 * capturing request headers happens once, before any replayable execution.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "vfs/vfs.h"
#include "vfs/vfs_internal_procs.h"
#include "s3_internal.h"
#include "s3_metadata.h"
#include "s3_tagging.h"
#include "s3_compound.h"

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
#define CHIMERA_S3_META_LIST_SIZE 16384
#define CHIMERA_S3_META_LIST_MAX  (1024 * 1024)

/* Immutable request-header capture, held until compound acceptance. */
struct chimera_s3_meta_store_ctx {
    struct chimera_s3_request *request;
    int                        count;
    struct chimera_s3_meta_kv  kv[CHIMERA_S3_META_MAX];
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
                          &CHIMERA_S3_XATTR_META[CHIMERA_S3_XATTR_PREFIX_LEN]);

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

/* Compound metadata belongs to one attempt. HTTP headers are emitted only by
 * metadata_emit after acceptance; list/get callouts never touch the request. */
struct chimera_s3_metadata_copy_slot {
    struct chimera_s3_metadata *metadata;
    int                         slot;
};

struct chimera_s3_metadata {
    struct chimera_s3_meta_store_ctx     captured;
    chimera_s3_metadata_continue_t       after;
    void                                *private_data;
    int                                  count;
    int                                  tag_count;
    int                                  handle_result;
    int                                  indices[CHIMERA_S3_META_MAX];
    uint8_t                              valid[CHIMERA_S3_META_MAX];
    struct chimera_vfs_open_handle      *source;
    struct chimera_vfs_open_handle      *destination;
    struct chimera_s3_metadata_copy_slot slots[CHIMERA_S3_META_MAX];
};

struct chimera_s3_metadata *
chimera_s3_metadata_capture(struct chimera_s3_request *request)
{
    struct chimera_s3_metadata *metadata = calloc(1, sizeof(*metadata));

    metadata->captured.request = request;
    chimera_s3_meta_capture(&metadata->captured);
    return metadata;
} /* chimera_s3_metadata_capture */

struct chimera_s3_metadata *
chimera_s3_metadata_read_alloc(void)
{
    return calloc(1, sizeof(struct chimera_s3_metadata));
} /* chimera_s3_metadata_read_alloc */

void
chimera_s3_metadata_free(struct chimera_s3_metadata *metadata)
{
    if (!metadata) {
        return;
    }
    for (int i = 0; i < metadata->captured.count; i++) {
        free(metadata->captured.kv[i].name);
        free(metadata->captured.kv[i].value);
    }
    free(metadata);
} /* chimera_s3_metadata_free */

int
chimera_s3_metadata_append_store(
    struct chimera_vfs_compound *compound,
    struct chimera_s3_metadata  *metadata)
{
    int handle = chimera_vfs_compound_add_gethandle(compound);

    if (handle < 0) {
        return -1;
    }
    for (int i = 0; i < metadata->captured.count; i++) {
        struct chimera_s3_meta_kv *kv  = &metadata->captured.kv[i];
        int                        set = chimera_vfs_compound_add_setxattr(compound, CHIMERA_VFS_XATTR_EITHER,
                                                                           kv->name, kv->name_len,
                                                                           kv->value, kv->value_len);
        if (set < 0) {
            return -1;
        }
        chimera_vfs_compound_op_use_handle(compound, set, handle);
    }
    return 0;
} /* chimera_s3_metadata_append_store */

static void
chimera_s3_metadata_get_done(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    uint8_t *valid = private_data;

    *valid = (*status == CHIMERA_VFS_OK);
    /* Preserve the existing GET metadata policy: inaccessible or unsupported
     * xattrs leave the response defaults in place. */
    *status = CHIMERA_VFS_OK;
} /* chimera_s3_metadata_get_done */

static void
chimera_s3_metadata_copy_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct chimera_s3_metadata_copy_slot *slot     = private_data;
    struct chimera_s3_metadata           *metadata = slot->metadata;
    struct chimera_vfs_compound_op       *set      = chimera_vfs_compound_op_args(compound, index);
    const struct chimera_vfs_compound_op *get      = chimera_vfs_compound_op(compound,
                                                                             metadata->indices[slot->slot]);

    if (!metadata->valid[slot->slot]) {
        chimera_vfs_compound_op_skip(compound, index);
        return;
    }
    set->xattr_value     = get->buffer;
    set->xattr_value_len = get->buffer_len;
} /* chimera_s3_metadata_copy_prepare */

static int chimera_s3_metadata_append_list(
    struct chimera_vfs_compound *compound,
    struct chimera_s3_metadata  *metadata,
    uint64_t                     cookie,
    uint32_t                     max_bytes);

static void
chimera_s3_metadata_list_done(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct chimera_s3_metadata           *metadata = private_data;
    const struct chimera_vfs_compound_op *op       = chimera_vfs_compound_op(compound, index);
    uint32_t                              off      = 0;

    /* Cookie zero starts a new attempt. Subsequent pages extend the same
     * private result set; retry rebuilds their dynamic suffix from this page. */
    if (!op->cookie) {
        metadata->count     = 0;
        metadata->tag_count = 0;
        memset(metadata->valid, 0, sizeof(metadata->valid));
    }
    if (*status == CHIMERA_VFS_ERANGE) {
        /* Some backends return one complete list instead of cookies. Grow
        * that request without silently treating a large list as empty. */
        if (op->buffer_max >= CHIMERA_S3_META_LIST_MAX) {
            return;
        }
        if (chimera_s3_metadata_append_list(compound, metadata, op->cookie,
                                            op->buffer_max * 2) < 0) {
            *status = CHIMERA_VFS_ENOSPC;
        } else {
            *status = CHIMERA_VFS_OK;
        }
        return;
    }
    if (*status == CHIMERA_VFS_OK) {
        while (off < op->buffer_len) {
            const char *name = (const char *) op->buffer + off;
            size_t      len  = strnlen(name, op->buffer_len - off);
            if (len == op->buffer_len - off) {
                *status = CHIMERA_VFS_EIO;
                return;
            }
            if (len >= CHIMERA_S3_TAG_PREFIX_LEN &&
                !strncmp(name, CHIMERA_S3_TAG_PREFIX, CHIMERA_S3_TAG_PREFIX_LEN)) {
                metadata->tag_count++;
            }
            if (len >= CHIMERA_S3_XATTR_PREFIX_LEN &&
                !strncmp(name, CHIMERA_S3_XATTR_PREFIX, CHIMERA_S3_XATTR_PREFIX_LEN) &&
                metadata->count < CHIMERA_S3_META_MAX) {
                int slot = metadata->count++;
                int get  = chimera_vfs_compound_add_getxattr(compound, name, len,
                                                             metadata->source ? 16384 : 16383);
                if (get < 0) {
                    *status = CHIMERA_VFS_ENOSPC;
                    return;
                }
                metadata->indices[slot] = get;
                chimera_vfs_compound_set_op_callbacks(compound, get, NULL,
                                                      chimera_s3_metadata_get_done,
                                                      &metadata->valid[slot]);
                if (!metadata->source) {
                    chimera_vfs_compound_op_use_handle(compound, get, metadata->handle_result);
                }
                if (metadata->source) {
                    int set;
                    chimera_vfs_compound_op_set_handle(compound, get, metadata->source);
                    set = chimera_vfs_compound_add_setxattr(compound, CHIMERA_VFS_XATTR_EITHER,
                                                            name, len, NULL, 0);
                    if (set < 0) {
                        *status = CHIMERA_VFS_ENOSPC;
                        return;
                    }
                    metadata->slots[slot].metadata = metadata;
                    metadata->slots[slot].slot     = slot;
                    chimera_vfs_compound_op_set_handle(compound, set, metadata->destination);
                    chimera_vfs_compound_set_op_callbacks(compound, set,
                                                          chimera_s3_metadata_copy_prepare,
                                                          NULL, &metadata->slots[slot]);
                }
            }
            off += len + 1;
        }
        if (!op->eof) {
            if (op->r_cookie == op->cookie || !op->r_cookie) {
                *status = CHIMERA_VFS_EIO;
                return;
            }
            if (chimera_s3_metadata_append_list(compound, metadata, op->r_cookie,
                                                CHIMERA_S3_META_LIST_SIZE) < 0) {
                *status = CHIMERA_VFS_ENOSPC;
            }
            return;
        }
    }
    *status = CHIMERA_VFS_OK;
    if (metadata->after && metadata->after(compound, metadata->private_data)) {
        *status = CHIMERA_VFS_ENOSPC;
    }
} /* chimera_s3_metadata_list_done */

static int
chimera_s3_metadata_append_list(
    struct chimera_vfs_compound *compound,
    struct chimera_s3_metadata  *metadata,
    uint64_t                     cookie,
    uint32_t                     max_bytes)
{
    int index = chimera_vfs_compound_add_listxattrs(compound, cookie, max_bytes);

    if (index >= 0) {
        if (metadata->source) {
            chimera_vfs_compound_op_set_handle(compound, index, metadata->source);
        } else {
            chimera_vfs_compound_op_use_handle(compound, index, metadata->handle_result);
        }
        chimera_vfs_compound_set_op_callbacks(compound, index, NULL,
                                              chimera_s3_metadata_list_done, metadata);
    }
    return index;
} /* chimera_s3_metadata_append_list */

int
chimera_s3_metadata_append_copy(
    struct chimera_vfs_compound    *compound,
    struct chimera_s3_metadata     *metadata,
    struct chimera_vfs_open_handle *source,
    struct chimera_vfs_open_handle *destination,
    chimera_s3_metadata_continue_t  after,
    void                           *private_data)
{
    int index;

    metadata->source      = source;
    metadata->destination = destination;
    index                 = chimera_s3_metadata_append_read(compound, metadata, after, private_data);
    return index;
} /* chimera_s3_metadata_append_copy */

int
chimera_s3_metadata_append_read(
    struct chimera_vfs_compound   *compound,
    struct chimera_s3_metadata    *metadata,
    chimera_s3_metadata_continue_t after,
    void                          *private_data)
{
    int index;

    metadata->handle_result = metadata->source ? -1 : chimera_vfs_compound_add_gethandle(compound);
    metadata->after         = after;
    metadata->private_data  = private_data;
    if (!metadata->source && metadata->handle_result < 0) {
        return -1;
    }
    index = chimera_s3_metadata_append_list(compound, metadata, 0,
                                            CHIMERA_S3_META_LIST_SIZE);
    return index;
} /* chimera_s3_metadata_append_read */

void
chimera_s3_metadata_emit(
    struct chimera_vfs_compound *compound,
    struct chimera_s3_metadata  *metadata,
    struct chimera_s3_request   *request,
    int                          include_tag_count)
{
    if (request->abandoned) {
        return;
    }
    for (int i = 0; i < metadata->count; i++) {
        const struct chimera_vfs_compound_op *op;
        char                                  value[16384];
        if (!metadata->valid[i]) {
            continue;
        }
        op = chimera_vfs_compound_op(compound, metadata->indices[i]);
        memcpy(value, op->buffer, op->buffer_len);
        value[op->buffer_len] = '\0';
        chimera_s3_meta_emit_header(request, op->name, value);
    }
    if (include_tag_count) {
        char count[32];
        snprintf(count, sizeof(count), "%d", metadata->tag_count);
        chimera_s3_response_add_header(request, "x-amz-tagging-count", count);
    }
} /* chimera_s3_metadata_emit */
