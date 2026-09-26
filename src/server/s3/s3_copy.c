// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * S3 CopyObject (server-side copy).
 *
 * Handles PUT requests carrying an x-amz-copy-source header. The source
 * object is located and opened, a fresh destination file is created, the
 * bytes are transferred server-side, and the destination is linked/renamed
 * into place. The reply is a CopyObjectResult document carrying the ETag and
 * LastModified of the new object.
 *
 * Byte transfer prefers the cheapest primitive the destination module can
 * offer:
 *   - clone_range  (reflink / copy-on-write, zero data movement) as a
 *     whole-file fast path. clone has block-alignment constraints (memfs
 *     rejects an unaligned length; FICLONERANGE only tolerates the unaligned
 *     remainder at source EOF), so on any failure we transparently fall back.
 *   - copy_range   (server-side byte copy, arbitrary length) — same primitive
 *     the multipart-completion assembler uses.
 *   - read + write (buffered) — final fallback, and the only option when the
 *     source and destination live on different VFS modules (range ops are
 *     intra-module).
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <time.h>

#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "s3_internal.h"
#include "s3_procs.h"
#include "s3_etag.h"
#include "s3_metadata.h"
#include "s3.h"
#include "s3_compound.h"
#include "s3_temp.h"

enum chimera_s3_copy_mode {
    CHIMERA_S3_COPY_CLONE,
    CHIMERA_S3_COPY_COPY,
    CHIMERA_S3_COPY_RW,
};

/* x-amz-metadata-directive */
enum chimera_s3_copy_meta_directive {
    CHIMERA_S3_COPY_META_COPY,      /* inherit the source object's metadata */
    CHIMERA_S3_COPY_META_REPLACE,   /* take metadata from this request's headers */
};

struct chimera_s3_copy_ctx {
    int                                 submitting, again, terminal, terminal_have_attr;
    enum chimera_vfs_error              terminal_error;
    enum chimera_s3_status              terminal_status;
    struct chimera_vfs_attrs            terminal_attr;
    struct chimera_s3_request          *request;
    struct chimera_vfs_open_handle     *src_handle;
    enum chimera_s3_copy_mode           mode;
    enum chimera_s3_copy_meta_directive meta_directive;
    uint64_t                            src_size;
    uint64_t                            offset;
    int                                 tmp_name_len;
    struct chimera_vfs_open_handle     *attempt_src, *attempt_dst, *attempt_dir;
    struct chimera_s3_metadata         *metadata;
    char                               *src_path;
    const char                         *dirpath;
    int                                 dirpath_len;
    int                                 destination_bucket, source_bucket, source_attr;
    int                                 source_open, directory_open, destination_open;
    int                                 final_attr;
    uint64_t                            attempt_offset;
    enum chimera_s3_copy_mode           attempt_mode;
    enum chimera_s3_status              failure;
    struct timespec                     src_mtime;
    int                                 src_bucket_namelen;
    int                                 src_key_len;
    char                                src_bucket_name[256];
    char                                src_key[1024];
    char                                tmp_name[64];
    struct evpl_iovec                   rw_iov[CHIMERA_S3_IOV_MAX];
};

/*
 * URL-decode percent escapes (%XX) from src into dst in place-safe fashion.
 * '+' is left untouched: S3 copy-source values percent-encode spaces as %20
 * and do not use form-encoding. Returns the decoded length.
 */
static int
chimera_s3_url_decode(
    char       *dst,
    const char *src,
    int         src_len)
{
    int i = 0, o = 0;

    while (i < src_len) {
        if (src[i] == '%' && i + 2 < src_len &&
            isxdigit((unsigned char) src[i + 1]) &&
            isxdigit((unsigned char) src[i + 2])) {
            char hex[3] = { src[i + 1], src[i + 2], '\0' };
            dst[o++] = (char) strtol(hex, NULL, 16);
            i       += 3;
        } else {
            dst[o++] = src[i++];
        }
    }
    dst[o] = '\0';
    return o;
} /* chimera_s3_url_decode */

/*
 * Parse an x-amz-copy-source value of the form "[/]bucket/key[?versionId=..]"
 * into the ctx's source bucket name and key. Returns 0 on success, -1 if the
 * value is malformed (no key component).
 */
static int
chimera_s3_parse_copy_source(
    struct chimera_s3_copy_ctx *ctx,
    const char                 *copy_source)
{
    const char *p = copy_source;
    const char *slash, *qmark;
    int         bucket_len, key_len;

    while (*p == '/') {
        p++;
    }

    slash = strchr(p, '/');

    if (!slash || slash[1] == '\0') {
        return -1;
    }

    bucket_len = slash - p;

    if (bucket_len <= 0 || bucket_len >= (int) sizeof(ctx->src_bucket_name)) {
        return -1;
    }

    /* Versioning is not supported; drop any ?versionId= suffix. */
    key_len = strlen(slash + 1);
    qmark   = memchr(slash + 1, '?', key_len);
    if (qmark) {
        key_len = qmark - (slash + 1);
    }

    if (key_len <= 0 || key_len >= (int) sizeof(ctx->src_key)) {
        return -1;
    }

    ctx->src_bucket_namelen = chimera_s3_url_decode(ctx->src_bucket_name,
                                                    p, bucket_len);
    ctx->src_key_len = chimera_s3_url_decode(ctx->src_key,
                                             slash + 1, key_len);

    return 0;
} /* chimera_s3_parse_copy_source */

/*
 * Terminal path: release any open handles, free the ctx, and either build the
 * CopyObjectResult reply (success) or set the error status. The HTTP response
 * is dispatched here if the request body has already been drained, otherwise
 * the notify path will dispatch it once it has.
 */
static void
chimera_s3_copy_finish(
    struct chimera_s3_copy_ctx *ctx,
    enum chimera_vfs_error      error_code,
    enum chimera_s3_status      status,
    struct chimera_vfs_attrs   *dst_attr)
{
    struct chimera_s3_request       *request = ctx->request;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct evpl                     *evpl    = thread->evpl;

    if (ctx->submitting) {
        ctx->terminal           = 1;
        ctx->terminal_error     = error_code;
        ctx->terminal_status    = status;
        ctx->terminal_have_attr = dst_attr != NULL;
        if (dst_attr) {
            ctx->terminal_attr = *dst_attr;
        }
        return;
    }
    CHIMERA_S3_HOLD_REQUEST(request);
    if ((error_code || status != CHIMERA_S3_STATUS_OK) && ctx->tmp_name_len) {
        chimera_s3_remove_temp(request, ctx->dirpath, ctx->dirpath_len,
                               ctx->tmp_name, ctx->tmp_name_len);
    }

    if (ctx->src_handle) {
        chimera_vfs_release(thread->vfs, ctx->src_handle);
    }
    if (request->file_handle) {
        chimera_vfs_release(thread->vfs, request->file_handle);
        request->file_handle = NULL;
    }
    if (request->dir_handle) {
        chimera_vfs_release(thread->vfs, request->dir_handle);
        request->dir_handle = NULL;
    }

    if (request->abandoned) {
        error_code = CHIMERA_VFS_EIO;
    }

    if (error_code || status != CHIMERA_S3_STATUS_OK) {
        request->status = status != CHIMERA_S3_STATUS_OK ?
            status : CHIMERA_S3_STATUS_INTERNAL_ERROR;
    } else {
        char *bp, *body_start;
        char  etag[80], date[64];

        chimera_s3_attach_etag(request->http_request, dst_attr);
        chimera_s3_etag_hex(etag, sizeof(etag), dst_attr);
        chimera_s3_format_date(date, sizeof(date), &dst_attr->va_mtime);

        evpl_iovec_alloc(evpl, 4096, 0, 1, 0, &request->multipart.response);

        bp = body_start = evpl_iovec_data(&request->multipart.response);

        bp += sprintf(bp, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        bp += sprintf(bp, "<CopyObjectResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");
        bp += sprintf(bp, "  <LastModified>%s</LastModified>\n", date);
        bp += sprintf(bp, "  <ETag>%s</ETag>\n", etag);
        bp += sprintf(bp, "</CopyObjectResult>\n");

        evpl_iovec_set_length(&request->multipart.response, bp - body_start);
        chimera_s3_response_add_datav(evpl, request,
                                      &request->multipart.response, 1);

        request->status           = CHIMERA_S3_STATUS_OK;
        request->file_length      = bp - body_start;
        request->file_real_length = request->file_length;
        request->file_offset      = 0;
        request->is_list          = 1; /* triggers application/xml Content-Type */
    }

    request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;

    chimera_s3_metadata_free(ctx->metadata);
    free(ctx->src_path);
    free(ctx);

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }
} /* chimera_s3_copy_finish */

/* A transfer is one compound while its data fits the bounded retry unit.
 * Larger buffered copies retain handles only after each accepted chunk. */
static int chimera_s3_copy_append_transfer(
    struct chimera_vfs_compound *compound,
    struct chimera_s3_copy_ctx  *ctx);
static void chimera_s3_copy_submit(
    struct chimera_s3_copy_ctx *ctx);

static void
chimera_s3_copy_attempt_reset(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_s3_copy_ctx *ctx = private_data;

    ctx->attempt_src    = ctx->src_handle;
    ctx->attempt_dst    = ctx->request->file_handle;
    ctx->attempt_dir    = ctx->request->dir_handle;
    ctx->attempt_offset = ctx->offset;
    ctx->attempt_mode   = ctx->mode;
    ctx->final_attr     = -1;
    ctx->failure        = CHIMERA_S3_STATUS_INTERNAL_ERROR;
} /* chimera_s3_copy_attempt_reset */

static int
chimera_s3_copy_append_publish(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_s3_copy_ctx *ctx     = private_data;
    struct chimera_s3_request  *request = ctx->request;

    if (ctx->tmp_name_len) {
        chimera_vfs_compound_add_putfh(compound, ctx->attempt_dir->fh,
                                       ctx->attempt_dir->fh_len);
        chimera_vfs_compound_add_savefh(compound);
        chimera_vfs_compound_add_rename(compound, ctx->tmp_name,
                                        ctx->tmp_name_len, request->name,
                                        request->name_len, 0);
    } else {
        chimera_vfs_compound_add_putfh(compound, ctx->attempt_dst->fh,
                                       ctx->attempt_dst->fh_len);
        chimera_vfs_compound_add_savefh(compound);
        chimera_vfs_compound_add_putfh(compound, ctx->attempt_dir->fh,
                                       ctx->attempt_dir->fh_len);
        chimera_vfs_compound_add_link_replace(compound, request->name,
                                              request->name_len, 0);
    }
    chimera_vfs_compound_add_puthandle(compound, ctx->attempt_dst,
                                       CHIMERA_VFS_OPEN_INFERRED);
    ctx->final_attr = chimera_vfs_compound_add_getattr(
        compound, CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT);
    return ctx->final_attr < 0 ? -1 : 0;
} /* chimera_s3_copy_append_publish */

static int
chimera_s3_copy_append_metadata(
    struct chimera_vfs_compound *compound,
    struct chimera_s3_copy_ctx  *ctx)
{
    if (ctx->meta_directive == CHIMERA_S3_COPY_META_COPY) {
        return chimera_s3_metadata_append_copy(compound, ctx->metadata,
                                               ctx->attempt_src,
                                               ctx->attempt_dst,
                                               chimera_s3_copy_append_publish,
                                               ctx);
    }
    chimera_vfs_compound_add_puthandle(compound, ctx->attempt_dst,
                                       CHIMERA_VFS_OPEN_INFERRED);
    if (chimera_s3_metadata_append_store(compound, ctx->metadata) < 0) {
        return -1;
    }
    return chimera_s3_copy_append_publish(compound, ctx);
} /* chimera_s3_copy_append_metadata */

static void
chimera_s3_copy_transfer_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct chimera_s3_copy_ctx           *ctx = private_data;
    const struct chimera_vfs_compound_op *op  = chimera_vfs_compound_op(compound, index);
    uint64_t                              length;

    if (ctx->attempt_mode == CHIMERA_S3_COPY_CLONE && *status != CHIMERA_VFS_OK) {
        struct chimera_vfs_module *module = chimera_vfs_get_module(
            ctx->request->thread->vfs, ctx->attempt_dst->fh,
            ctx->attempt_dst->fh_len);

        /* Reflink is optional; preserve the ordinary byte-copy fallback. */
        ctx->attempt_mode = module && (module->capabilities & CHIMERA_VFS_CAP_COPY_RANGE) ?
            CHIMERA_S3_COPY_COPY : CHIMERA_S3_COPY_RW;
        *status = CHIMERA_VFS_OK;
        if (chimera_s3_copy_append_transfer(compound, ctx) < 0) {
            *status = CHIMERA_VFS_ENOSPC;
        }
        return;
    }
    if (*status != CHIMERA_VFS_OK) {
        return;
    }
    length = ctx->attempt_mode == CHIMERA_S3_COPY_CLONE ?
        ctx->src_size - ctx->attempt_offset : op->written;
    if (length == 0 || length > ctx->src_size - ctx->attempt_offset) {
        *status = CHIMERA_VFS_EIO;
        return;
    }
    ctx->attempt_offset += length;
    if (ctx->attempt_offset == ctx->src_size &&
        chimera_s3_copy_append_metadata(compound, ctx) < 0) {
        *status = CHIMERA_VFS_ENOSPC;
    }
} /* chimera_s3_copy_transfer_complete */

static void
chimera_s3_copy_read_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct chimera_s3_copy_ctx           *ctx = private_data;
    const struct chimera_vfs_compound_op *op  = chimera_vfs_compound_op(compound, index);
    int                                   write_index;

    if (*status != CHIMERA_VFS_OK) {
        return;
    }
    if (op->read_len == 0) {
        *status = CHIMERA_VFS_EIO;
        return;
    }
    /* The compound retains the READ references until its WRITE and final
     * completion are done. Nothing is transferred to the frontend here. */
    write_index = chimera_vfs_compound_add_write(
        compound, ctx->attempt_dst, ctx->attempt_offset, op->read_len, 1,
        op->iov, op->niov, NULL);
    chimera_vfs_compound_set_op_callbacks(compound, write_index, NULL,
                                          chimera_s3_copy_transfer_complete, ctx);
} /* chimera_s3_copy_read_complete */

static int
chimera_s3_copy_append_transfer(
    struct chimera_vfs_compound *compound,
    struct chimera_s3_copy_ctx  *ctx)
{
    uint64_t remaining = ctx->src_size - ctx->attempt_offset;
    uint32_t chunk     = chimera_s3_compound_chunk_size(ctx->request);
    int      index;

    if (!remaining) {
        return chimera_s3_copy_append_metadata(compound, ctx);
    }
    if (remaining < chunk) {
        chunk = remaining;
    }
    if (ctx->attempt_mode == CHIMERA_S3_COPY_CLONE) {
        index = chimera_vfs_compound_add_clone_range(
            compound, ctx->attempt_src, ctx->attempt_offset,
            ctx->attempt_dst, ctx->attempt_offset, remaining, 0, 0);
    } else if (ctx->attempt_mode == CHIMERA_S3_COPY_COPY) {
        index = chimera_vfs_compound_add_copy_range(
            compound, ctx->attempt_src, ctx->attempt_offset,
            ctx->attempt_dst, ctx->attempt_offset, chunk, 0, 0, 0);
    } else {
        index = chimera_vfs_compound_add_read(
            compound, ctx->attempt_src, ctx->attempt_offset, chunk,
            ctx->rw_iov, CHIMERA_S3_IOV_MAX, NULL);
    }
    chimera_vfs_compound_set_op_callbacks(
        compound, index, NULL,
        ctx->attempt_mode == CHIMERA_S3_COPY_RW ?
        chimera_s3_copy_read_complete : chimera_s3_copy_transfer_complete, ctx);
    return index < 0 ? -1 : 0;
} /* chimera_s3_copy_append_transfer */

static void
chimera_s3_copy_destination_ready(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct chimera_s3_copy_ctx *ctx = private_data;
    struct chimera_vfs_module  *source_module, *destination_module;

    if (*status != CHIMERA_VFS_OK) {
        return;
    }
    ctx->attempt_dst = chimera_vfs_compound_op(compound, index)->out_handle;
    source_module    = chimera_vfs_get_module(ctx->request->thread->vfs,
                                              ctx->attempt_src->fh,
                                              ctx->attempt_src->fh_len);
    destination_module = chimera_vfs_get_module(ctx->request->thread->vfs,
                                                ctx->attempt_dst->fh,
                                                ctx->attempt_dst->fh_len);
    if (!destination_module || source_module != destination_module) {
        ctx->attempt_mode = CHIMERA_S3_COPY_RW;
    } else if (destination_module->capabilities & CHIMERA_VFS_CAP_CLONE_RANGE) {
        ctx->attempt_mode = CHIMERA_S3_COPY_CLONE;
    } else if (destination_module->capabilities & CHIMERA_VFS_CAP_COPY_RANGE) {
        ctx->attempt_mode = CHIMERA_S3_COPY_COPY;
    } else {
        ctx->attempt_mode = CHIMERA_S3_COPY_RW;
    }
    if (chimera_s3_copy_append_transfer(compound, ctx) < 0) {
        *status = CHIMERA_VFS_ENOSPC;
    }
} /* chimera_s3_copy_destination_ready */

static void
chimera_s3_copy_directory_ready(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct chimera_s3_copy_ctx *ctx = private_data;
    struct chimera_vfs_module  *module;
    struct chimera_vfs_attrs    set_attr = { 0 };

    if (*status != CHIMERA_VFS_OK) {
        ctx->failure = CHIMERA_S3_STATUS_NO_SUCH_KEY;
        return;
    }
    ctx->attempt_dir = chimera_vfs_compound_op(compound, index)->out_handle;
    module           = chimera_vfs_get_module(ctx->request->thread->vfs,
                                              ctx->attempt_dir->fh,
                                              ctx->attempt_dir->fh_len);
    if (module && (module->capabilities & CHIMERA_VFS_CAP_CREATE_UNLINKED)) {
        ctx->tmp_name_len     = 0;
        ctx->destination_open = chimera_vfs_compound_add_create_unlinked(
            compound, &set_attr, CHIMERA_VFS_ATTR_FH);
    } else {
        ctx->tmp_name_len     = strlen(ctx->tmp_name);
        ctx->destination_open = chimera_vfs_compound_add_open(
            compound, ctx->tmp_name, ctx->tmp_name_len,
            CHIMERA_VFS_OPEN_CREATE, 0, &set_attr, CHIMERA_VFS_ATTR_FH);
    }
    chimera_vfs_compound_set_op_callbacks(
        compound, ctx->destination_open, NULL,
        chimera_s3_copy_destination_ready, ctx);
} /* chimera_s3_copy_directory_ready */

static void
chimera_s3_copy_source_ready(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct chimera_s3_copy_ctx           *ctx = private_data;
    const struct chimera_vfs_compound_op *source, *bucket;
    struct chimera_vfs_attrs              set_attr = { 0 };

    if (*status != CHIMERA_VFS_OK) {
        ctx->failure = CHIMERA_S3_STATUS_NO_SUCH_KEY;
        return;
    }
    ctx->attempt_src = chimera_vfs_compound_op(compound, index)->out_handle;
    source           = chimera_vfs_compound_op(compound, ctx->source_attr);
    if (!(source->attr.va_set_mask & CHIMERA_VFS_ATTR_SIZE)) {
        *status = CHIMERA_VFS_EIO;
        return;
    }
    ctx->src_size = source->attr.va_size;
    bucket        = chimera_vfs_compound_op(compound, ctx->destination_bucket);
    chimera_vfs_compound_add_putfh(compound, bucket->fh, bucket->fh_len);
    chimera_vfs_compound_add_create_tree(compound, ctx->dirpath,
                                         ctx->dirpath_len, &set_attr,
                                         CHIMERA_VFS_ATTR_FH);
    chimera_vfs_compound_add_open_current(
        compound, CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED |
        CHIMERA_VFS_OPEN_DIRECTORY, 0);
    ctx->directory_open = chimera_vfs_compound_add_gethandle(compound);
    chimera_vfs_compound_set_op_callbacks(
        compound, ctx->directory_open, NULL, chimera_s3_copy_directory_ready, ctx);
} /* chimera_s3_copy_source_ready */

static void
chimera_s3_copy_compound_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_s3_copy_ctx *ctx    = private_data;
    enum chimera_vfs_error      status = chimera_vfs_compound_status(compound);
    struct chimera_vfs_attrs    attr;

    if (status != CHIMERA_VFS_OK) {
        enum chimera_s3_status failure = chimera_s3_compound_error(compound, ctx->request, ctx->failure);
        if (!ctx->src_handle && chimera_vfs_compound_finish_status(compound) == CHIMERA_VFS_OK) {
            const struct chimera_vfs_compound_op *bucket =
                chimera_vfs_compound_op(compound, ctx->source_bucket);
            if (bucket->status == CHIMERA_VFS_ENOENT) {
                failure = CHIMERA_S3_STATUS_NO_SUCH_BUCKET;
            } else if (chimera_vfs_compound_op(compound, ctx->source_attr)->status ==
                       CHIMERA_VFS_ENOENT) {
                failure = CHIMERA_S3_STATUS_NO_SUCH_KEY;
            }
        }
        chimera_vfs_compound_free(compound);
        chimera_s3_copy_finish(ctx, status, failure, NULL);
        return;
    }
    if (ctx->final_attr >= 0) {
        attr = chimera_vfs_compound_op(compound, ctx->final_attr)->attr;
        chimera_vfs_compound_free(compound);
        chimera_s3_copy_finish(ctx, CHIMERA_VFS_OK, CHIMERA_S3_STATUS_OK, &attr);
        return;
    }
    /* Only accepted chunks advance the externally retained transfer state. */
    if (!ctx->src_handle) {
        ctx->src_handle           = chimera_vfs_compound_take_handle(compound, ctx->source_open);
        ctx->request->dir_handle  = chimera_vfs_compound_take_handle(compound, ctx->directory_open);
        ctx->request->file_handle = chimera_vfs_compound_take_handle(compound, ctx->destination_open);
    }
    ctx->offset = ctx->attempt_offset;
    ctx->mode   = ctx->attempt_mode;
    chimera_vfs_compound_free(compound);
    chimera_s3_copy_submit(ctx);
} /* chimera_s3_copy_compound_complete */

/* Build resumed work during execution, after the attempt reset. In particular,
 * a zero-byte tail may append metadata/publication immediately; constructing it
 * before submit would reset its result index and lose that completion. */
static void
chimera_s3_copy_resume(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    if (*status == CHIMERA_VFS_OK &&
        chimera_s3_copy_append_transfer(compound, private_data) < 0) {
        *status = CHIMERA_VFS_ENOSPC;
    }
} /* chimera_s3_copy_resume */

static void
chimera_s3_copy_submit(struct chimera_s3_copy_ctx *ctx)
{
    struct chimera_s3_request   *request = ctx->request;
    struct chimera_vfs_compound *compound;

    if (ctx->submitting) {
        ctx->again = 1;
        return;
    }
    ctx->submitting = 1;
    do {
        ctx->again = 0;
        if (ctx->src_handle) {
            compound = chimera_vfs_compound_alloc(request->thread->vfs, &request->cred);
            chimera_vfs_compound_add_puthandle(compound, request->file_handle,
                                               CHIMERA_VFS_OPEN_INFERRED);
            int resume = chimera_vfs_compound_add_gethandle(compound);
            chimera_vfs_compound_set_op_callbacks(compound, resume, NULL,
                                                  chimera_s3_copy_resume, ctx);
        } else {
            compound                = chimera_s3_compound_alloc(request);
            ctx->destination_bucket = chimera_vfs_compound_add_getfh(compound);
            chimera_vfs_compound_add_putfh(compound, request->thread->shared->root_fh,
                                           request->thread->shared->root_fh_len);
            ctx->source_bucket = chimera_vfs_compound_add_lookup_path(
                compound, ctx->src_path, strlen(ctx->src_path),
                CHIMERA_VFS_ATTR_FH, CHIMERA_VFS_LOOKUP_FOLLOW);
            ctx->source_attr = chimera_vfs_compound_add_lookup_path(
                compound, ctx->src_key, ctx->src_key_len,
                CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
                CHIMERA_VFS_LOOKUP_FOLLOW);
            chimera_vfs_compound_add_open_current(compound, 0, 0);
            ctx->source_open = chimera_vfs_compound_add_gethandle(compound);
            chimera_vfs_compound_set_op_callbacks(compound, ctx->source_open, NULL,
                                                  chimera_s3_copy_source_ready, ctx);
        }
        chimera_vfs_compound_set_attempt_reset(compound, chimera_s3_copy_attempt_reset, ctx);
        chimera_frontend_compound_submit(compound, chimera_s3_copy_compound_complete, ctx);
    } while (ctx->again && !ctx->terminal);
    ctx->submitting = 0;
    if (ctx->terminal) {
        chimera_s3_copy_finish(ctx, ctx->terminal_error, ctx->terminal_status,
                               ctx->terminal_have_attr ? &ctx->terminal_attr : NULL);
    }
} /* chimera_s3_copy_submit */

void
chimera_s3_copy(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    struct chimera_server_s3_shared *shared = thread->shared;
    struct chimera_s3_copy_ctx      *ctx;
    const struct s3_bucket          *src_bucket;
    const char                      *directive, *copy_source, *slash;

    ctx          = calloc(1, sizeof(*ctx));
    ctx->request = request;
    chimera_s3_request_get(request);
    request->dir_handle  = NULL;
    request->file_handle = NULL;
    copy_source          = evpl_http_request_header(request->http_request, "x-amz-copy-source");
    if (!copy_source || chimera_s3_parse_copy_source(ctx, copy_source) != 0) {
        chimera_s3_copy_finish(ctx, 0, CHIMERA_S3_STATUS_BAD_REQUEST, NULL);
        return;
    }
    directive           = evpl_http_request_header(request->http_request, "x-amz-metadata-directive");
    ctx->meta_directive = directive && !strcasecmp(directive, "REPLACE") ?
        CHIMERA_S3_COPY_META_REPLACE : CHIMERA_S3_COPY_META_COPY;
    ctx->metadata = ctx->meta_directive == CHIMERA_S3_COPY_META_REPLACE ?
        chimera_s3_metadata_capture(request) : chimera_s3_metadata_read_alloc();
    src_bucket = chimera_s3_get_bucket(shared, ctx->src_bucket_name);
    if (src_bucket) {
        ctx->src_path = strdup(chimera_s3_bucket_get_path(src_bucket));
    }
    chimera_s3_release_bucket(shared);
    if (!src_bucket || !ctx->src_path || !ctx->metadata) {
        chimera_s3_copy_finish(ctx, 0, src_bucket ? CHIMERA_S3_STATUS_INTERNAL_ERROR :
                               CHIMERA_S3_STATUS_NO_SUCH_BUCKET, NULL);
        return;
    }
    slash            = strrchr(request->path, '/');
    ctx->dirpath     = slash ? request->path : "/";
    ctx->dirpath_len = slash ? slash - request->path : 1;
    request->name    = slash ? slash + 1 : request->path;
    while (*request->name == '/') {
        request->name++;
    }
    request->name_len = strlen(request->name);
    if (!request->name_len) {
        chimera_s3_copy_finish(ctx, 0, CHIMERA_S3_STATUS_BAD_REQUEST, NULL);
        return;
    }
    snprintf(ctx->tmp_name, sizeof(ctx->tmp_name), "._chimera_cp_%" PRIx64 "%" PRIx64,
             (uint64_t) request, (uint64_t) request->start_time.tv_nsec);
    chimera_s3_copy_submit(ctx);
} /* chimera_s3_copy */
