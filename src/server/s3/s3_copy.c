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
 *
 * Every step is a VFS sequence:
 *   source       PUTROOT -> LOOKUP_PATH(source bucket) -> LOOKUP_PATH(key)
 *                -> OPEN_CURRENT -> GETHANDLE
 *   destination  PutObject's setup: PUTFH(bucket) -> [CREATE_PATH(dir)]
 *                -> OPEN_CURRENT(dir) -> GETHANDLE -> CREATE_UNLINKED or
 *                OPEN(tmpname, CREATE)
 *   transfer     a CLONE_RANGE, one COPY_RANGE per chunk, or a READ then a
 *                WRITE per chunk, both handles supplied; the fallbacks are
 *                the following sequences
 *   metadata     the store's SETXATTR runs, or the list/get/set copy
 *   publish      PutObject's publish, with the CopyObjectResult GETATTR
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <time.h>

#include "vfs/vfs.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_release.h"
#include "s3_internal.h"
#include "s3_procs.h"
#include "s3_etag.h"
#include "s3_metadata.h"
#include "s3.h"

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

/* Op indexes in the source sequence. */
#define CHIMERA_S3_COPY_SRC_OP_BUCKET    1
#define CHIMERA_S3_COPY_SRC_OP_KEY       2
#define CHIMERA_S3_COPY_SRC_OP_OPEN      3
#define CHIMERA_S3_COPY_SRC_OP_GETHANDLE 4

struct chimera_s3_copy_ctx {
    struct chimera_s3_request          *request;
    struct chimera_vfs_open_handle     *src_handle;
    enum chimera_s3_copy_mode           mode;
    enum chimera_s3_copy_meta_directive meta_directive;
    uint64_t                            src_size;
    uint64_t                            offset;
    int                                 tmp_name_len;
    int                                 rw_niov;
    uint32_t                            rw_count;
    /* Where the destination sequence put the directory's GETHANDLE and the
     * object's create, so their handles can be taken from it. */
    int                                 dir_index;
    int                                 create_index;
    struct timespec                     src_mtime;
    int                                 src_bucket_namelen;
    int                                 src_key_len;
    char                                src_bucket_name[256];
    char                                src_key[1024];
    char                                tmp_name[64];
    struct evpl_iovec                   rw_iov[CHIMERA_S3_IOV_MAX];
};

static void chimera_s3_copy_step(
    struct chimera_s3_copy_ctx *ctx);

static void chimera_s3_copy_finalize(
    struct chimera_s3_copy_ctx *ctx);

static void chimera_s3_copy_apply_metadata(
    struct chimera_s3_copy_ctx *ctx);

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

    chimera_s3_request_drop(ctx->request);
    free(ctx);

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }
} /* chimera_s3_copy_finish */

/* ----- publish: rename/link into place, then the reply's attributes ----- */

/*
 * The publish sequence is over.  The RENAME / LINK is the second-to-last op
 * and the GETATTR the last; the reply needs the attributes, so here (unlike
 * PutObject's ETag) a failed getattr fails the copy, as it did.
 */
static void
chimera_s3_copy_publish_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_s3_copy_ctx *ctx = private_data;
    enum chimera_vfs_error      error_code;
    struct chimera_vfs_attrs    attr;
    uint32_t                    num_ops;

    error_code = chimera_vfs_compound_status(compound);
    num_ops    = chimera_vfs_compound_num_ops(compound);

    if (error_code == CHIMERA_VFS_OK) {
        attr = chimera_vfs_compound_op(compound, num_ops - 1)->attr;
    }

    chimera_vfs_compound_free(compound);

    chimera_s3_copy_finish(ctx, error_code, CHIMERA_S3_STATUS_OK,
                           error_code == CHIMERA_VFS_OK ? &attr : NULL);
} /* chimera_s3_copy_publish_complete */

/*
 * Bytes are in the (still hidden) destination file; publish it under the
 * destination key. Backends that created the file unlinked link it into the
 * directory (replacing any existing object); backends that used a temp name
 * rename over the key.
 */
static void
chimera_s3_copy_finalize(struct chimera_s3_copy_ctx *ctx)
{
    struct chimera_s3_request       *request = ctx->request;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct chimera_vfs_compound     *compound;
    int                              index;

    compound = chimera_vfs_compound_alloc(thread->vfs, &request->cred);

    if (ctx->tmp_name_len) {
        chimera_vfs_compound_add_putfh(compound,
                                       request->dir_handle->fh,
                                       request->dir_handle->fh_len);
        chimera_vfs_compound_add_savefh(compound);
        chimera_vfs_compound_add_rename(compound,
                                        ctx->tmp_name,
                                        ctx->tmp_name_len,
                                        request->name,
                                        request->name_len,
                                        0, 0, 0);
    } else {
        chimera_vfs_compound_add_putfh(compound,
                                       request->file_handle->fh,
                                       request->file_handle->fh_len);
        chimera_vfs_compound_add_savefh(compound);
        chimera_vfs_compound_add_putfh(compound,
                                       request->dir_handle->fh,
                                       request->dir_handle->fh_len);
        index = chimera_vfs_compound_add_link(compound,
                                              request->name,
                                              request->name_len,
                                              CHIMERA_VFS_ATTR_FH |
                                              CHIMERA_VFS_ATTR_MASK_STAT,
                                              0, 0);
        chimera_vfs_compound_op_set_link_opts(compound, index, 1, NULL, NULL);
    }

    index = chimera_vfs_compound_add_getattr(compound,
                                             CHIMERA_VFS_ATTR_FH |
                                             CHIMERA_VFS_ATTR_MASK_STAT);
    chimera_vfs_compound_op_set_handle(compound, index, request->file_handle);

    chimera_vfs_compound_submit(compound, chimera_s3_copy_publish_complete, ctx);
} /* chimera_s3_copy_finalize */

/*
 * Metadata has been applied to the destination (or there was none); publish the
 * object. A metadata error is non-fatal — the bytes copied fine — so we proceed
 * to finalize regardless and let the (unlikely) failure surface only if it
 * recurs on rename/link.
 */
static void
chimera_s3_copy_metadata_done(
    struct chimera_s3_request *request,
    int                        error,
    void                      *private_data)
{
    struct chimera_s3_copy_ctx *ctx = private_data;

    chimera_s3_copy_finalize(ctx);
} /* chimera_s3_copy_metadata_done */

/*
 * The destination bytes are in place. Apply object metadata per the
 * x-amz-metadata-directive header: COPY inherits the source object's stored
 * metadata xattrs; REPLACE takes the metadata from this copy request's headers.
 */
static void
chimera_s3_copy_apply_metadata(struct chimera_s3_copy_ctx *ctx)
{
    if (ctx->meta_directive == CHIMERA_S3_COPY_META_REPLACE) {
        chimera_s3_metadata_store_from_headers(ctx->request,
                                               ctx->request->file_handle,
                                               chimera_s3_copy_metadata_done,
                                               ctx);
    } else {
        chimera_s3_metadata_copy(ctx->request,
                                 ctx->src_handle,
                                 ctx->request->file_handle,
                                 chimera_s3_copy_metadata_done,
                                 ctx);
    }
} /* chimera_s3_copy_apply_metadata */

/* ----- byte transfer ----- */

static void
chimera_s3_copy_clone_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_s3_copy_ctx      *ctx    = private_data;
    struct chimera_server_s3_thread *thread = ctx->request->thread;
    struct chimera_vfs_module       *module;
    enum chimera_vfs_error           error_code;

    error_code = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);

    if (error_code) {
        /* clone is best-effort: alignment limits (EINVAL), cross-module
         * handles (ENOTSUP), or a backend that advertised the capability but
         * cannot satisfy this particular range. A failed clone makes no
         * changes to the destination, so restart the whole transfer with a
         * byte-copy primitive. */
        module = chimera_vfs_get_module(thread->vfs,
                                        ctx->request->file_handle->fh,
                                        ctx->request->file_handle->fh_len);

        ctx->mode = (module &&
                     (module->capabilities & CHIMERA_VFS_CAP_COPY_RANGE)) ?
            CHIMERA_S3_COPY_COPY : CHIMERA_S3_COPY_RW;
        ctx->offset = 0;
        chimera_s3_copy_step(ctx);
        return;
    }

    /* clone transfers the whole remaining range in one shot. */
    ctx->offset = ctx->src_size;
    chimera_s3_copy_step(ctx);
} /* chimera_s3_copy_clone_complete */

static void
chimera_s3_copy_copy_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_s3_copy_ctx *ctx = private_data;
    enum chimera_vfs_error      error_code;
    uint64_t                    length = 0;

    error_code = chimera_vfs_compound_status(compound);

    if (error_code == CHIMERA_VFS_OK) {
        length = chimera_vfs_compound_op(compound, 0)->written;
    }

    chimera_vfs_compound_free(compound);

    if (error_code) {
        chimera_s3_copy_finish(ctx, error_code, CHIMERA_S3_STATUS_OK, NULL);
        return;
    }

    ctx->offset += length;
    chimera_s3_copy_step(ctx);
} /* chimera_s3_copy_copy_complete */

static void
chimera_s3_copy_write_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_s3_copy_ctx      *ctx    = private_data;
    struct chimera_server_s3_thread *thread = ctx->request->thread;
    struct evpl                     *evpl   = thread->evpl;
    enum chimera_vfs_error           error_code;
    uint32_t                         length = 0;

    error_code = chimera_vfs_compound_status(compound);

    if (error_code == CHIMERA_VFS_OK) {
        length = chimera_vfs_compound_op(compound, 0)->written;
    }

    chimera_vfs_compound_free(compound);

    /* The buffers the READ took are ours; the WRITE only borrowed them. */
    evpl_iovecs_release(evpl, ctx->rw_iov, ctx->rw_niov);
    ctx->rw_niov = 0;

    if (error_code) {
        chimera_s3_copy_finish(ctx, error_code, CHIMERA_S3_STATUS_OK, NULL);
        return;
    }

    ctx->offset += length;
    chimera_s3_copy_step(ctx);
} /* chimera_s3_copy_write_complete */

/* The READ half of a buffered chunk answered; the WRITE of what it brought
 * back is the following sequence, since its data is that READ's result. */
static void
chimera_s3_copy_read_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_s3_copy_ctx      *ctx    = private_data;
    struct chimera_server_s3_thread *thread = ctx->request->thread;
    struct chimera_vfs_compound     *write;
    enum chimera_vfs_error           error_code;
    struct evpl_iovec               *iov  = NULL;
    int                              niov = 0;

    error_code = chimera_vfs_compound_status(compound);

    if (error_code == CHIMERA_VFS_OK) {
        ctx->rw_count = chimera_vfs_compound_op(compound, 0)->read_len;
        /* The backend filled ctx->rw_iov (the array handed to the READ);
         * take the references and pass them straight through to the WRITE,
         * releasing them when that has run. */
        chimera_vfs_compound_take_iov(compound, 0, &iov, &niov);
    }

    chimera_vfs_compound_free(compound);

    if (error_code) {
        chimera_s3_copy_finish(ctx, error_code, CHIMERA_S3_STATUS_OK, NULL);
        return;
    }

    ctx->rw_niov = niov;

    write = chimera_vfs_compound_alloc(thread->vfs, &ctx->request->cred);

    chimera_vfs_compound_add_write(write,
                                   ctx->request->file_handle,
                                   ctx->offset,
                                   ctx->rw_count,
                                   1,
                                   ctx->rw_iov,
                                   ctx->rw_niov,
                                   0, 0,
                                   NULL);

    chimera_vfs_compound_submit(write, chimera_s3_copy_write_complete, ctx);
} /* chimera_s3_copy_read_complete */

static void
chimera_s3_copy_step(struct chimera_s3_copy_ctx *ctx)
{
    struct chimera_s3_request       *request = ctx->request;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct chimera_vfs_compound     *compound;
    uint64_t                         remaining, chunk;

    remaining = ctx->src_size - ctx->offset;

    if (remaining == 0) {
        chimera_s3_copy_apply_metadata(ctx);
        return;
    }

    compound = chimera_vfs_compound_alloc(thread->vfs, &request->cred);

    switch (ctx->mode) {
        case CHIMERA_S3_COPY_CLONE:
            /* Whole remaining range in a single reflink. */
            chimera_vfs_compound_add_clone_range(compound,
                                                 ctx->src_handle,
                                                 ctx->offset,
                                                 request->file_handle,
                                                 ctx->offset,
                                                 remaining,
                                                 0, 0);
            chimera_vfs_compound_submit(compound, chimera_s3_copy_clone_complete,
                                        ctx);
            break;
        case CHIMERA_S3_COPY_COPY:
            chunk = thread->shared->config->io_size;
            if (chunk > remaining) {
                chunk = remaining;
            }
            chimera_vfs_compound_add_copy_range(compound,
                                                ctx->src_handle,
                                                ctx->offset,
                                                request->file_handle,
                                                ctx->offset,
                                                chunk,
                                                0,
                                                0, 0);
            chimera_vfs_compound_submit(compound, chimera_s3_copy_copy_complete,
                                        ctx);
            break;
        case CHIMERA_S3_COPY_RW:
            chunk = thread->shared->config->io_size;
            if (chunk > remaining) {
                chunk = remaining;
            }
            ctx->rw_niov = 0;
            chimera_vfs_compound_add_read(compound,
                                          ctx->src_handle,
                                          ctx->offset,
                                          chunk,
                                          ctx->rw_iov,
                                          CHIMERA_S3_IOV_MAX,
                                          0,
                                          NULL,
                                          NULL, 0);
            chimera_vfs_compound_submit(compound, chimera_s3_copy_read_complete,
                                        ctx);
            break;
    } /* switch */
} /* chimera_s3_copy_step */

/*
 * Source and destination are both open; choose the transfer primitive.
 * Range ops are intra-module, so a cross-module copy must use read/write.
 */
static void
chimera_s3_copy_start_transfer(struct chimera_s3_copy_ctx *ctx)
{
    struct chimera_s3_request       *request = ctx->request;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct chimera_vfs_module       *src_module, *dst_module;

    if (ctx->src_size == 0) {
        /* Nothing to transfer; still apply metadata, then publish. */
        ctx->offset = 0;
        chimera_s3_copy_apply_metadata(ctx);
        return;
    }

    src_module = chimera_vfs_get_module(thread->vfs,
                                        ctx->src_handle->fh,
                                        ctx->src_handle->fh_len);
    dst_module = chimera_vfs_get_module(thread->vfs,
                                        request->file_handle->fh,
                                        request->file_handle->fh_len);

    if (!dst_module || src_module != dst_module) {
        /* Range ops require both handles on the same (resolvable) module;
         * otherwise the buffered read+write path is the only option. */
        ctx->mode = CHIMERA_S3_COPY_RW;
    } else if (dst_module->capabilities & CHIMERA_VFS_CAP_CLONE_RANGE) {
        ctx->mode = CHIMERA_S3_COPY_CLONE;
    } else if (dst_module->capabilities & CHIMERA_VFS_CAP_COPY_RANGE) {
        ctx->mode = CHIMERA_S3_COPY_COPY;
    } else {
        ctx->mode = CHIMERA_S3_COPY_RW;
    }

    ctx->offset = 0;
    chimera_s3_copy_step(ctx);
} /* chimera_s3_copy_start_transfer */

/* ----- destination creation (mirrors the PutObject setup sequence) ----- */

static void
chimera_s3_copy_dest_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_s3_copy_ctx *ctx     = private_data;
    struct chimera_s3_request  *request = ctx->request;
    enum chimera_vfs_error      error_code;
    uint32_t                    completed;

    error_code = chimera_vfs_compound_status(compound);
    completed  = chimera_vfs_compound_num_completed(compound);

    if (error_code) {
        /* The sequence releases whatever it opened.  Failing to reach the
         * directory is a missing key; failing to create in it is the error
         * it is. */
        chimera_vfs_compound_free(compound);
        chimera_s3_copy_finish(ctx, error_code,
                               (int) completed - 1 <= ctx->dir_index ?
                               CHIMERA_S3_STATUS_NO_SUCH_KEY :
                               CHIMERA_S3_STATUS_OK,
                               NULL);
        return;
    }

    request->dir_handle  = chimera_vfs_compound_take_handle(compound, ctx->dir_index);
    request->file_handle = chimera_vfs_compound_take_handle(compound, ctx->create_index);

    chimera_vfs_compound_free(compound);

    chimera_s3_copy_start_transfer(ctx);
} /* chimera_s3_copy_dest_complete */

static void
chimera_s3_copy_create_dest(struct chimera_s3_copy_ctx *ctx)
{
    struct chimera_s3_request       *request = ctx->request;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct chimera_vfs_compound     *compound;
    struct chimera_vfs_module       *module;
    const char                      *slash;
    const char                      *dirpath;
    int                              dirpathlen;

    /* Resolve the destination directory and key from the request path,
     * exactly as PutObject does, then create the destination there. */
    slash = strrchr(request->path, '/');

    if (slash) {
        dirpath       = request->path;
        dirpathlen    = slash - request->path;
        request->name = slash + 1;
        while (*request->name == '/') {
            request->name++;
        }
    } else {
        dirpath       = "/";
        dirpathlen    = 1;
        request->name = request->path;
    }

    request->name_len = strlen(request->name);

    if (request->name_len == 0) {
        chimera_s3_copy_finish(ctx, 0, CHIMERA_S3_STATUS_BAD_REQUEST, NULL);
        return;
    }

    request->set_attr.va_req_mask = 0;
    request->set_attr.va_set_mask = 0;

    compound = chimera_vfs_compound_alloc(thread->vfs, &request->cred);

    chimera_vfs_compound_add_putfh(compound, request->bucket_fh,
                                   request->bucket_fhlen);

    if (slash) {
        chimera_vfs_compound_add_create_path(compound,
                                             CHIMERA_VFS_COMPOUND_CREATE_DIR,
                                             dirpath, dirpathlen,
                                             NULL, 0,
                                             &request->set_attr,
                                             CHIMERA_VFS_ATTR_FH,
                                             1);
    }

    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_PATH |
                                          CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_DIRECTORY,
                                          0);

    ctx->dir_index = chimera_vfs_compound_add_gethandle(compound);

    module = chimera_vfs_get_module(thread->vfs, request->bucket_fh,
                                    request->bucket_fhlen);

    if (module->capabilities & CHIMERA_VFS_CAP_CREATE_UNLINKED) {
        ctx->tmp_name_len = 0;
        ctx->create_index =
            chimera_vfs_compound_add_create_unlinked(compound,
                                                     CHIMERA_VFS_OPEN_WRITE_ONLY,
                                                     &request->set_attr,
                                                     CHIMERA_VFS_ATTR_FH);
    } else {
        ctx->tmp_name_len = snprintf(ctx->tmp_name, sizeof(ctx->tmp_name),
                                     "._chimera_cp_%" PRIx64 "%" PRIx64,
                                     (uint64_t) request,
                                     (uint64_t) request->start_time.tv_nsec);
        ctx->create_index =
            chimera_vfs_compound_add_open(compound,
                                          ctx->tmp_name,
                                          ctx->tmp_name_len,
                                          CHIMERA_VFS_OPEN_CREATE,
                                          0,
                                          &request->set_attr,
                                          CHIMERA_VFS_ATTR_FH,
                                          0, 0);
    }

    chimera_vfs_compound_submit(compound, chimera_s3_copy_dest_complete, ctx);
} /* chimera_s3_copy_create_dest */

/* ----- source resolution and open ----- */

static void
chimera_s3_copy_source_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_s3_copy_ctx           *ctx = private_data;
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                error_code;
    uint32_t                              completed;

    error_code = chimera_vfs_compound_status(compound);
    completed  = chimera_vfs_compound_num_completed(compound);

    if (error_code) {
        chimera_vfs_compound_free(compound);
        chimera_s3_copy_finish(ctx, error_code,
                               (int) completed - 1 == CHIMERA_S3_COPY_SRC_OP_BUCKET ?
                               CHIMERA_S3_STATUS_NO_SUCH_BUCKET :
                               CHIMERA_S3_STATUS_NO_SUCH_KEY,
                               NULL);
        return;
    }

    op = chimera_vfs_compound_op(compound, CHIMERA_S3_COPY_SRC_OP_KEY);

    chimera_s3_abort_if(
        (op->attr.va_set_mask & (CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_SIZE |
                                 CHIMERA_VFS_ATTR_MTIME)) !=
        (CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_SIZE | CHIMERA_VFS_ATTR_MTIME),
        "copy source lookup: missing attributes");

    ctx->src_size  = op->attr.va_size;
    ctx->src_mtime = op->attr.va_mtime;

    ctx->src_handle = chimera_vfs_compound_take_handle(compound,
                                                       CHIMERA_S3_COPY_SRC_OP_GETHANDLE);

    chimera_vfs_compound_free(compound);

    chimera_s3_copy_create_dest(ctx);
} /* chimera_s3_copy_source_complete */

void
chimera_s3_copy(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    struct chimera_server_s3_shared *shared = thread->shared;
    struct chimera_s3_copy_ctx      *ctx;
    struct chimera_vfs_compound     *compound;
    const char                      *copy_source;
    const char                      *directive;
    const struct s3_bucket          *src_bucket;
    char                            *src_path;

    copy_source = evpl_http_request_header(request->http_request,
                                           "x-amz-copy-source");

    ctx          = calloc(1, sizeof(*ctx));
    ctx->request = request;
    chimera_s3_request_get(request);

    /* x-amz-metadata-directive defaults to COPY (inherit source metadata). */
    directive = evpl_http_request_header(request->http_request,
                                         "x-amz-metadata-directive");
    if (directive && strcasecmp(directive, "REPLACE") == 0) {
        ctx->meta_directive = CHIMERA_S3_COPY_META_REPLACE;
    } else {
        ctx->meta_directive = CHIMERA_S3_COPY_META_COPY;
    }

    request->dir_handle  = NULL;
    request->file_handle = NULL;

    if (!copy_source || chimera_s3_parse_copy_source(ctx, copy_source) != 0) {
        chimera_s3_copy_finish(ctx, 0, CHIMERA_S3_STATUS_BAD_REQUEST, NULL);
        return;
    }

    /* Copying an object onto ITSELF with nothing to change is an
     * InvalidRequest (S3 API Reference, CopyObject): the copy would be a
     * no-op, and AWS refuses it rather than silently rewriting the object.
     * The metadata directive is the only axis chimera implements, so REPLACE
     * is what makes a self-copy legal here -- storage class, website redirect
     * and encryption are the other AWS exemptions and none of them exists
     * yet.  Both names are compared as slices: bucket_name points into the
     * URL and is not NUL-terminated. */
    if (ctx->meta_directive == CHIMERA_S3_COPY_META_COPY &&
        ctx->src_bucket_namelen == request->bucket_namelen &&
        memcmp(ctx->src_bucket_name, request->bucket_name,
               request->bucket_namelen) == 0 &&
        ctx->src_key_len == (int) request->path_len &&
        memcmp(ctx->src_key, request->path, request->path_len) == 0) {
        chimera_s3_copy_finish(ctx, 0, CHIMERA_S3_STATUS_INVALID_REQUEST, NULL);
        return;
    }

    /* Snapshot the source path while the bucket-map lock protects it. VFS
    * callbacks may run inline, so do not hold the map across dispatch. */
    src_bucket = chimera_s3_get_bucket(shared, ctx->src_bucket_name);

    if (src_bucket == NULL) {
        chimera_s3_release_bucket(shared);
        chimera_s3_copy_finish(ctx, 0, CHIMERA_S3_STATUS_NO_SUCH_BUCKET, NULL);
        return;
    }

    src_path = strdup(chimera_s3_bucket_get_path(src_bucket));
    chimera_s3_release_bucket(shared);
    if (!src_path) {
        chimera_s3_copy_finish(ctx, 0, CHIMERA_S3_STATUS_INTERNAL_ERROR, NULL);
        return;
    }

    compound = chimera_vfs_compound_alloc(thread->vfs, &request->cred);

    chimera_vfs_compound_add_putroot(compound);
    chimera_vfs_compound_add_lookup_path(compound, src_path, strlen(src_path),
                                         CHIMERA_VFS_ATTR_FH,
                                         CHIMERA_VFS_LOOKUP_FOLLOW);
    chimera_vfs_compound_add_lookup_path(compound, ctx->src_key, ctx->src_key_len,
                                         CHIMERA_VFS_ATTR_FH |
                                         CHIMERA_VFS_ATTR_MASK_STAT,
                                         CHIMERA_VFS_LOOKUP_FOLLOW);
    chimera_vfs_compound_add_open_current(compound, 0, 0);
    chimera_vfs_compound_add_gethandle(compound);

    free(src_path);

    chimera_vfs_compound_submit(compound, chimera_s3_copy_source_complete, ctx);
} /* chimera_s3_copy */
