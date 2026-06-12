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
#include "s3.h"

enum chimera_s3_copy_mode {
    CHIMERA_S3_COPY_CLONE,
    CHIMERA_S3_COPY_COPY,
    CHIMERA_S3_COPY_RW,
};

struct chimera_s3_copy_ctx {
    struct chimera_s3_request      *request;
    struct chimera_vfs_open_handle *src_handle;
    enum chimera_s3_copy_mode       mode;
    uint64_t                        src_size;
    uint64_t                        offset;
    int                             tmp_name_len;
    int                             rw_niov;
    struct timespec                 src_mtime;
    int                             src_bucket_namelen;
    int                             src_key_len;
    char                            src_bucket_name[256];
    char                            src_key[1024];
    char                            tmp_name[64];
    struct evpl_iovec               rw_iov[CHIMERA_S3_IOV_MAX];
};

static void chimera_s3_copy_step(
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
        evpl_http_request_add_datav(request->http_request,
                                    &request->multipart.response, 1);

        request->status           = CHIMERA_S3_STATUS_OK;
        request->file_length      = bp - body_start;
        request->file_real_length = request->file_length;
        request->file_offset      = 0;
        request->is_list          = 1; /* triggers application/xml Content-Type */
    }

    request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;

    free(ctx);

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }
} /* chimera_s3_copy_finish */

/* ----- destination attributes (for the reply ETag/LastModified) ----- */

static void
chimera_s3_copy_getattr_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_s3_copy_ctx *ctx = private_data;

    chimera_s3_copy_finish(ctx, error_code, CHIMERA_S3_STATUS_OK, attr);
} /* chimera_s3_copy_getattr_callback */

static void
chimera_s3_copy_link_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *r_attr,
    struct chimera_vfs_attrs *r_dir_pre_attr,
    struct chimera_vfs_attrs *r_dir_post_attr,
    void                     *private_data)
{
    struct chimera_s3_copy_ctx      *ctx    = private_data;
    struct chimera_server_s3_thread *thread = ctx->request->thread;

    if (error_code) {
        chimera_s3_copy_finish(ctx, error_code, CHIMERA_S3_STATUS_OK, NULL);
        return;
    }

    chimera_vfs_getattr(thread->vfs, &thread->shared->cred, NULL,
                        ctx->request->file_handle,
                        CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
                        chimera_s3_copy_getattr_callback,
                        ctx);
} /* chimera_s3_copy_link_callback */

static void
chimera_s3_copy_rename_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *fromdir_pre_attr,
    struct chimera_vfs_attrs *fromdir_post_attr,
    struct chimera_vfs_attrs *todir_pre_attr,
    struct chimera_vfs_attrs *todir_post_attr,
    void                     *private_data)
{
    struct chimera_s3_copy_ctx      *ctx    = private_data;
    struct chimera_server_s3_thread *thread = ctx->request->thread;

    if (error_code) {
        chimera_s3_copy_finish(ctx, error_code, CHIMERA_S3_STATUS_OK, NULL);
        return;
    }

    chimera_vfs_getattr(thread->vfs, &thread->shared->cred, NULL,
                        ctx->request->file_handle,
                        CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
                        chimera_s3_copy_getattr_callback,
                        ctx);
} /* chimera_s3_copy_rename_callback */

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

    if (ctx->tmp_name_len) {
        chimera_vfs_rename_at(
            thread->vfs,
            &thread->shared->cred, NULL,
            request->dir_handle->fh,
            request->dir_handle->fh_len,
            ctx->tmp_name,
            ctx->tmp_name_len,
            request->dir_handle->fh,
            request->dir_handle->fh_len,
            request->name,
            request->name_len,
            NULL,
            0,
            0,
            0,
            chimera_s3_copy_rename_callback,
            ctx);
    } else {
        chimera_vfs_link_at(
            thread->vfs,
            &thread->shared->cred, NULL,
            request->file_handle->fh,
            request->file_handle->fh_len,
            request->dir_handle->fh,
            request->dir_handle->fh_len,
            request->name,
            request->name_len,
            1,
            CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
            0,
            0,
            chimera_s3_copy_link_callback,
            ctx);
    }
} /* chimera_s3_copy_finalize */

/* ----- byte transfer ----- */

static void
chimera_s3_copy_clone_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_s3_copy_ctx      *ctx    = private_data;
    struct chimera_server_s3_thread *thread = ctx->request->thread;
    struct chimera_vfs_module       *module;

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
} /* chimera_s3_copy_clone_callback */

static void
chimera_s3_copy_copy_callback(
    enum chimera_vfs_error    error_code,
    uint64_t                  length,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_s3_copy_ctx *ctx = private_data;

    if (error_code) {
        chimera_s3_copy_finish(ctx, error_code, CHIMERA_S3_STATUS_OK, NULL);
        return;
    }

    ctx->offset += length;
    chimera_s3_copy_step(ctx);
} /* chimera_s3_copy_copy_callback */

static void
chimera_s3_copy_write_callback(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_s3_copy_ctx      *ctx    = private_data;
    struct chimera_server_s3_thread *thread = ctx->request->thread;
    struct evpl                     *evpl   = thread->evpl;

    evpl_iovecs_release(evpl, ctx->rw_iov, ctx->rw_niov);
    ctx->rw_niov = 0;

    if (error_code) {
        chimera_s3_copy_finish(ctx, error_code, CHIMERA_S3_STATUS_OK, NULL);
        return;
    }

    ctx->offset += length;
    chimera_s3_copy_step(ctx);
} /* chimera_s3_copy_write_callback */

static void
chimera_s3_copy_read_callback(
    enum chimera_vfs_error    error_code,
    uint32_t                  count,
    uint32_t                  eof,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_s3_copy_ctx      *ctx    = private_data;
    struct chimera_server_s3_thread *thread = ctx->request->thread;

    if (error_code) {
        chimera_s3_copy_finish(ctx, error_code, CHIMERA_S3_STATUS_OK, NULL);
        return;
    }

    /* The backend filled ctx->rw_iov (the array handed to read); pass it
     * straight through to write, which will release the descriptors. */
    ctx->rw_niov = niov;

    chimera_vfs_write(
        thread->vfs, &thread->shared->cred, NULL,
        ctx->request->file_handle,
        ctx->offset,
        count,
        1,
        0,
        0,
        ctx->rw_iov,
        ctx->rw_niov,
        chimera_s3_copy_write_callback,
        ctx);
} /* chimera_s3_copy_read_callback */

static void
chimera_s3_copy_step(struct chimera_s3_copy_ctx *ctx)
{
    struct chimera_s3_request       *request = ctx->request;
    struct chimera_server_s3_thread *thread  = request->thread;
    uint64_t                         remaining, chunk;

    remaining = ctx->src_size - ctx->offset;

    if (remaining == 0) {
        chimera_s3_copy_finalize(ctx);
        return;
    }

    switch (ctx->mode) {
        case CHIMERA_S3_COPY_CLONE:
            /* Whole remaining range in a single reflink. */
            chimera_vfs_clone_range(
                thread->vfs, &thread->shared->cred,
                ctx->src_handle,
                ctx->offset,
                request->file_handle,
                ctx->offset,
                remaining,
                0, 0,
                chimera_s3_copy_clone_callback,
                ctx);
            break;
        case CHIMERA_S3_COPY_COPY:
            chunk = thread->shared->config->io_size;
            if (chunk > remaining) {
                chunk = remaining;
            }
            chimera_vfs_copy_range(
                thread->vfs, &thread->shared->cred,
                ctx->src_handle,
                ctx->offset,
                request->file_handle,
                ctx->offset,
                chunk,
                0, 0,
                chimera_s3_copy_copy_callback,
                ctx);
            break;
        case CHIMERA_S3_COPY_RW:
            chunk = thread->shared->config->io_size;
            if (chunk > remaining) {
                chunk = remaining;
            }
            ctx->rw_niov = CHIMERA_S3_IOV_MAX;
            chimera_vfs_read(
                thread->vfs, &thread->shared->cred, NULL,
                ctx->src_handle,
                ctx->offset,
                chunk,
                ctx->rw_iov,
                ctx->rw_niov,
                0,
                chimera_s3_copy_read_callback,
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
        /* Nothing to transfer; just publish the empty object. */
        ctx->offset = 0;
        chimera_s3_copy_finalize(ctx);
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

/* ----- destination creation (mirrors the PutObject create path) ----- */

static void
chimera_s3_copy_create_unlinked_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data)
{
    struct chimera_s3_copy_ctx *ctx = private_data;

    if (error_code) {
        chimera_s3_copy_finish(ctx, error_code, CHIMERA_S3_STATUS_OK, NULL);
        return;
    }

    ctx->request->file_handle = oh;
    chimera_s3_copy_start_transfer(ctx);
} /* chimera_s3_copy_create_unlinked_callback */

static void
chimera_s3_copy_create_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data)
{
    struct chimera_s3_copy_ctx *ctx = private_data;

    if (error_code) {
        chimera_s3_copy_finish(ctx, error_code, CHIMERA_S3_STATUS_OK, NULL);
        return;
    }

    ctx->request->file_handle = oh;
    chimera_s3_copy_start_transfer(ctx);
} /* chimera_s3_copy_create_callback */

static void
chimera_s3_copy_open_dir_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_s3_copy_ctx      *ctx     = private_data;
    struct chimera_s3_request       *request = ctx->request;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct chimera_vfs_module       *module;

    if (error_code) {
        chimera_s3_copy_finish(ctx, error_code, CHIMERA_S3_STATUS_NO_SUCH_KEY,
                               NULL);
        return;
    }

    request->dir_handle = oh;

    request->set_attr.va_req_mask = 0;
    request->set_attr.va_set_mask = 0;

    module = chimera_vfs_get_module(thread->vfs, oh->fh, oh->fh_len);

    if (module->capabilities & CHIMERA_VFS_CAP_CREATE_UNLINKED) {
        ctx->tmp_name_len = 0;
        chimera_vfs_create_unlinked(
            thread->vfs, &thread->shared->cred, NULL,
            oh->fh,
            oh->fh_len,
            &request->set_attr,
            CHIMERA_VFS_ATTR_FH,
            chimera_s3_copy_create_unlinked_callback,
            ctx);
    } else {
        ctx->tmp_name_len = snprintf(ctx->tmp_name, sizeof(ctx->tmp_name),
                                     "._chimera_cp_%lx%lx", (uint64_t) request,
                                     (uint64_t) request->start_time.tv_nsec);
        chimera_vfs_open_at(
            thread->vfs, &thread->shared->cred, NULL,
            oh,
            ctx->tmp_name,
            ctx->tmp_name_len,
            CHIMERA_VFS_OPEN_CREATE,
            &request->set_attr,
            CHIMERA_VFS_ATTR_FH,
            0,
            0,
            chimera_s3_copy_create_callback,
            ctx);
    }
} /* chimera_s3_copy_open_dir_callback */

static void
chimera_s3_copy_create_dir_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_s3_copy_ctx      *ctx    = private_data;
    struct chimera_server_s3_thread *thread = ctx->request->thread;

    if (error_code) {
        chimera_s3_copy_finish(ctx, error_code, CHIMERA_S3_STATUS_NO_SUCH_KEY,
                               NULL);
        return;
    }

    chimera_vfs_open_fh(thread->vfs, &thread->shared->cred, NULL,
                        attr->va_fh,
                        attr->va_fh_len,
                        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED |
                        CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_s3_copy_open_dir_callback,
                        ctx);
} /* chimera_s3_copy_create_dir_callback */

/* ----- source open ----- */

static void
chimera_s3_copy_open_src_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_s3_copy_ctx      *ctx     = private_data;
    struct chimera_s3_request       *request = ctx->request;
    struct chimera_server_s3_thread *thread  = request->thread;
    const char                      *slash;
    const char                      *dirpath;
    int                              dirpathlen;

    if (error_code) {
        chimera_s3_copy_finish(ctx, error_code, CHIMERA_S3_STATUS_NO_SUCH_KEY,
                               NULL);
        return;
    }

    ctx->src_handle = oh;

    /* Resolve the destination directory and key from the request path,
     * exactly as PutObject does, then create the destination there. */
    slash = rindex(request->path, '/');

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

    chimera_vfs_create(thread->vfs, &thread->shared->cred, NULL,
                       request->bucket_fh,
                       request->bucket_fhlen,
                       dirpath,
                       dirpathlen,
                       &request->set_attr,
                       CHIMERA_VFS_ATTR_FH,
                       chimera_s3_copy_create_dir_callback,
                       ctx);
} /* chimera_s3_copy_open_src_callback */

static void
chimera_s3_copy_lookup_src_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_s3_copy_ctx      *ctx    = private_data;
    struct chimera_server_s3_thread *thread = ctx->request->thread;

    if (error_code) {
        chimera_s3_copy_finish(ctx, error_code, CHIMERA_S3_STATUS_NO_SUCH_KEY,
                               NULL);
        return;
    }

    chimera_s3_abort_if(
        (attr->va_set_mask & (CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_SIZE |
                              CHIMERA_VFS_ATTR_MTIME)) !=
        (CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_SIZE | CHIMERA_VFS_ATTR_MTIME),
        "copy source lookup: missing attributes");

    ctx->src_size  = attr->va_size;
    ctx->src_mtime = attr->va_mtime;

    chimera_vfs_open_fh(thread->vfs, &thread->shared->cred, NULL,
                        attr->va_fh,
                        attr->va_fh_len,
                        0,
                        chimera_s3_copy_open_src_callback,
                        ctx);
} /* chimera_s3_copy_lookup_src_callback */

static void
chimera_s3_copy_lookup_src_bucket_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_s3_copy_ctx      *ctx    = private_data;
    struct chimera_server_s3_thread *thread = ctx->request->thread;

    if (error_code) {
        chimera_s3_copy_finish(ctx, error_code,
                               CHIMERA_S3_STATUS_NO_SUCH_BUCKET, NULL);
        return;
    }

    chimera_vfs_lookup(thread->vfs, &thread->shared->cred, NULL,
                       attr->va_fh,
                       attr->va_fh_len,
                       ctx->src_key,
                       ctx->src_key_len,
                       CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
                       CHIMERA_VFS_LOOKUP_FOLLOW,
                       chimera_s3_copy_lookup_src_callback,
                       ctx);
} /* chimera_s3_copy_lookup_src_bucket_callback */

void
chimera_s3_copy(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    struct chimera_server_s3_shared *shared = thread->shared;
    struct chimera_s3_copy_ctx      *ctx;
    const char                      *copy_source;
    const struct s3_bucket          *src_bucket;
    const char                      *src_path;

    copy_source = evpl_http_request_header(request->http_request,
                                           "x-amz-copy-source");

    ctx          = calloc(1, sizeof(*ctx));
    ctx->request = request;

    request->dir_handle  = NULL;
    request->file_handle = NULL;

    if (!copy_source || chimera_s3_parse_copy_source(ctx, copy_source) != 0) {
        chimera_s3_copy_finish(ctx, 0, CHIMERA_S3_STATUS_BAD_REQUEST, NULL);
        return;
    }

    /* chimera_s3_get_bucket() acquires the bucket-map read lock and leaves it
     * held; chimera_s3_release_bucket() drops it. The path string stays valid
     * past the unlock for the duration of the request (buckets are not freed
     * underneath in-flight operations), matching the dispatch path. */
    src_bucket = chimera_s3_get_bucket(shared, ctx->src_bucket_name);

    if (src_bucket == NULL) {
        chimera_s3_release_bucket(shared);
        chimera_s3_copy_finish(ctx, 0, CHIMERA_S3_STATUS_NO_SUCH_BUCKET, NULL);
        return;
    }

    src_path = chimera_s3_bucket_get_path(src_bucket);

    chimera_vfs_lookup(thread->vfs,
                       &thread->shared->cred, NULL,
                       shared->root_fh,
                       shared->root_fh_len,
                       src_path,
                       strlen(src_path),
                       CHIMERA_VFS_ATTR_FH,
                       CHIMERA_VFS_LOOKUP_FOLLOW,
                       chimera_s3_copy_lookup_src_bucket_callback,
                       ctx);

    chimera_s3_release_bucket(shared);
} /* chimera_s3_copy */
