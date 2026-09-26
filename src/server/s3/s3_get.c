// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <inttypes.h>
#include <stdio.h>
#include <time.h>
#include <sys/stat.h>
#ifdef _WIN32
#include "common/platform.h"
#endif /* ifdef _WIN32 */
#include "common/format.h"
#include "vfs/vfs_release.h"
#include "s3_compound.h"
#include "s3_etag.h"
#include "s3_procs.h"
#include "s3_metadata.h"

struct s3_get_attempt {
    struct chimera_s3_request  *request;
    struct chimera_s3_metadata *metadata;
    struct chimera_s3_io       *io;
    int                         lookup;
    int                         read;
    int                         handle;
    int                         head;
    int64_t                     input_offset;
    int64_t                     input_length;
    int64_t                     offset;
    int64_t                     length;
    int64_t                     real_length;
    enum chimera_s3_status status;
};

static void
chimera_s3_get_finish(struct chimera_s3_request *request)
{
    if (request->file_handle) {
        chimera_vfs_release(request->thread->vfs, request->file_handle);
        request->file_handle = NULL;
    }
    request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
} /* chimera_s3_get_finish */

void
chimera_s3_get_cleanup(struct chimera_s3_request *request)
{
    if (request->get_active) {
        chimera_s3_get_finish(request);
        request->get_active = 0;
    }
} /* chimera_s3_get_cleanup */

static void
chimera_s3_get_attempt_reset(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct s3_get_attempt *ctx = private_data;

    ctx->status      = CHIMERA_S3_STATUS_OK;
    ctx->read        = ctx->handle = -1;
    ctx->real_length = ctx->length = ctx->offset = 0;
} /* chimera_s3_get_attempt_reset */

static void
chimera_s3_get_check_read(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, index);

    if (*status == CHIMERA_VFS_OK && op->count && !op->read_len) {
        *status = CHIMERA_VFS_EIO;
    }
} /* chimera_s3_get_check_read */

/* A retry resolves the original sentinels against the current attempt's size. */
static void
chimera_s3_get_check(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct s3_get_attempt          *ctx  = private_data;
    const struct chimera_vfs_attrs *attr = &chimera_vfs_compound_op(compound, index)->attr;
    const uint64_t                  need = CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_SIZE |
        CHIMERA_VFS_ATTR_MTIME;

    ctx->status = CHIMERA_S3_STATUS_OK;
    ctx->read   = -1;
    ctx->handle = -1;
    if (*status) {
        return;
    }
    if ((attr->va_set_mask & need) != need ||
        ((attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
         (attr->va_mode & S_IFMT) != S_IFREG)) {
        ctx->status = CHIMERA_S3_STATUS_NO_SUCH_KEY;
        *status     = CHIMERA_VFS_ENOENT;
        return;
    }
    ctx->real_length = attr->va_size;
    ctx->offset      = ctx->input_offset;
    ctx->length      = ctx->input_length;
    if ((ctx->offset < 0 || ctx->length != 0) &&
        (ctx->real_length == 0 || ctx->offset >= ctx->real_length)) {
        ctx->status = CHIMERA_S3_STATUS_INVALID_RANGE;
        *status     = CHIMERA_VFS_EINVAL;
        return;
    }
    if (ctx->offset < 0) {
        if (ctx->length > ctx->real_length) {
            ctx->length = ctx->real_length;
        }
        ctx->offset = ctx->real_length - ctx->length;
    } else if (ctx->length < 0) {
        ctx->length = ctx->real_length - ctx->offset;
    } else if (!ctx->length) {
        ctx->offset = 0;
        ctx->length = ctx->real_length;
    } else if (ctx->length > ctx->real_length - ctx->offset) {
        ctx->length = ctx->real_length - ctx->offset;
    }
} /* chimera_s3_get_check */

static int
chimera_s3_get_append_data(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct s3_get_attempt *ctx   = private_data;
    uint64_t               count = ctx->length;
    uint32_t               limit = chimera_s3_compound_chunk_size(ctx->request);

    if (ctx->head || !count) {
        return 0;
    }
    if (count > limit) {
        count = limit;
    }
    ctx->read = chimera_vfs_compound_add_read(compound, NULL, ctx->offset, count, ctx->io->iov, CHIMERA_S3_IOV_MAX, 0,
                                              NULL, NULL, 0);
    chimera_vfs_compound_set_op_callbacks(compound, ctx->read, NULL,
                                          chimera_s3_get_check_read, ctx);
    ctx->handle = chimera_vfs_compound_add_gethandle(compound);
    return ctx->read < 0 || ctx->handle < 0 ? -1 : 0;
} /* chimera_s3_get_append_data */

static void
chimera_s3_get_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct s3_get_attempt     *ctx     = private_data;
    struct chimera_s3_request *request = ctx->request;
    struct evpl               *evpl    = request->thread->evpl;
    enum chimera_vfs_error     status  = chimera_vfs_compound_status(compound);
    struct evpl_iovec         *iov;
    int                        niov;

    request->file_real_length = ctx->real_length;
    if (status || request->abandoned) {
        request->status = chimera_vfs_compound_finish_status(compound) == CHIMERA_VFS_OK &&
            ctx->status != CHIMERA_S3_STATUS_OK ? ctx->status :
            chimera_s3_compound_error(compound, request, CHIMERA_S3_STATUS_NO_SUCH_KEY);
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
    } else {
        const struct chimera_vfs_attrs *attr = &chimera_vfs_compound_op(compound, ctx->lookup)->attr;
        chimera_s3_attach_etag(request->http_request, attr);
        chimera_s3_attach_last_modified(request->http_request, attr);
        chimera_s3_metadata_emit(compound, ctx->metadata, request, ctx->head);
        request->file_offset     = ctx->offset;
        request->file_length     = ctx->length;
        request->file_cur_offset = ctx->offset;
        request->file_left       = ctx->head ? 0 : ctx->length;
        if (ctx->read >= 0) {
            const struct chimera_vfs_compound_op *read = chimera_vfs_compound_op(compound, ctx->read);
            request->file_handle      = chimera_vfs_compound_take_handle(compound, ctx->handle);
            request->file_cur_offset += read->read_len;
            request->file_left       -= read->read_len;
            chimera_vfs_compound_take_iov(compound, ctx->read, &iov, &niov);
            if (niov) {
                chimera_s3_response_add_datav(evpl, request, iov, niov);
            }
        }
        request->vfs_state = CHIMERA_S3_VFS_STATE_SEND;
        if (!request->file_left) {
            chimera_s3_get_finish(request);
        }
    }
    chimera_vfs_compound_free(compound);
    chimera_s3_metadata_free(ctx->metadata);
    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }
    chimera_s3_io_free(request->thread, ctx->io);
    free(ctx);
} /* chimera_s3_get_complete */

static void
chimera_s3_get_chunk_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_s3_io                 *io      = private_data;
    struct chimera_s3_request            *request = io->request;
    const struct chimera_vfs_compound_op *read    = chimera_vfs_compound_op(compound, 0);
    enum chimera_vfs_error                status  = chimera_vfs_compound_status(compound);
    struct evpl_iovec                    *iov;
    int                                   niov;

    request->io_pending = 0;
    if (status || request->abandoned || (!read->read_len && request->file_left)) {
        request->status = chimera_s3_status_from_vfs(status ? status : CHIMERA_VFS_EIO,
                                                     CHIMERA_S3_STATUS_INTERNAL_ERROR);
        chimera_s3_get_finish(request);
    } else {
        request->file_cur_offset += read->read_len;
        request->file_left       -= read->read_len;
        chimera_vfs_compound_take_iov(compound, 0, &iov, &niov);
        if (!request->file_left) {
            chimera_s3_get_finish(request);
        }
        if (niov) {
            chimera_s3_response_add_datav(request->thread->evpl, request, iov, niov);
        }
    }
    chimera_vfs_compound_free(compound);
    chimera_s3_io_free(request->thread, io);
} /* chimera_s3_get_chunk_complete */

void
chimera_s3_get_send(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    struct chimera_vfs_compound *compound;
    struct chimera_s3_io        *io;
    uint64_t                     count = request->file_left;
    uint32_t                     limit = chimera_s3_compound_chunk_size(request);

    if (request->io_pending || request->vfs_state != CHIMERA_S3_VFS_STATE_SEND) {
        return;
    }
    if (!count || request->abandoned) {
        chimera_s3_get_finish(request);
        return;
    }
    if (count > limit) {
        count = limit;
    }
    io                  = chimera_s3_io_alloc(request->thread, request);
    request->io_pending = 1;
    compound            = chimera_vfs_compound_alloc(request->thread->vfs, &request->cred);
    chimera_vfs_compound_add_read(compound, request->file_handle, request->file_cur_offset, count, io->iov,
                                  CHIMERA_S3_IOV_MAX, 0, NULL, NULL, 0);
    chimera_frontend_compound_submit(compound, chimera_s3_get_chunk_complete, io);
} /* chimera_s3_get_send */

void
chimera_s3_get(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    struct s3_get_attempt       *ctx      = calloc(1, sizeof(*ctx));
    struct chimera_vfs_compound *compound = chimera_s3_compound_alloc(request);

    request->get_active = 1;
    ctx->request        = request;
    ctx->metadata       = chimera_s3_metadata_read_alloc();
    ctx->io             = chimera_s3_io_alloc(thread, request);
    ctx->input_offset   = request->file_offset;
    ctx->input_length   = request->file_length;
    ctx->head           = evpl_http_request_type(request->http_request) == EVPL_HTTP_REQUEST_TYPE_HEAD;
    ctx->read           = ctx->handle = -1;
    ctx->lookup         = chimera_vfs_compound_add_lookup_path(compound, request->path, request->path_len,
                                                               CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
                                                               CHIMERA_VFS_LOOKUP_FOLLOW);
    chimera_vfs_compound_set_op_callbacks(compound, ctx->lookup, NULL, chimera_s3_get_check, ctx);
    chimera_vfs_compound_add_open_current(compound, 0, 0);
    chimera_s3_metadata_append_read(compound, ctx->metadata, chimera_s3_get_append_data, ctx);
    chimera_vfs_compound_set_attempt_reset(compound, chimera_s3_get_attempt_reset, ctx);
    chimera_frontend_compound_submit(compound, chimera_s3_get_complete, ctx);
} /* chimera_s3_get */

/*
 * GetObjectAttributes: GET /bucket/<key>?attributes with an
 * x-amz-object-attributes header. Returns a small XML document carrying the
 * attributes the filesystem can supply trivially (ETag, ObjectSize, and a
 * static StorageClass). Checksum and ObjectParts are not implemented and are
 * intentionally omitted; clients that request only those attributes still get
 * a well-formed 200 response.
 */
static void
chimera_s3_get_object_attributes_lookup_callback(
    enum chimera_s3_status    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{

    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct evpl                     *evpl    = thread->evpl;
    uint64_t                         etag[2];
    char                             etag_hex[80];
    char                            *bp, *body_start;

    if (error_code) {
        request->status    = error_code;
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
            s3_server_respond(evpl, request);
        }
        goto request_drop;
    }

    /* Mirror the regular-object guard in chimera_s3_get_lookup_callback: only
     * a regular file with the attributes the ETag is built from is an object. */
    {
        const uint64_t need = CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_SIZE |
            CHIMERA_VFS_ATTR_MTIME;
        int            is_dir = (attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
            (attr->va_mode & S_IFMT) == S_IFDIR;

        if (is_dir || (attr->va_set_mask & need) != need) {
            request->status    = CHIMERA_S3_STATUS_NO_SUCH_KEY;
            request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
            if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
                s3_server_respond(evpl, request);
            }
            goto request_drop;
        }
    }

    /* ETag without surrounding quotes (the GetObjectAttributes API returns the
     * raw value, unlike the HTTP ETag header). */
    chimera_s3_compute_etag(etag, attr);
    format_hex(etag_hex, sizeof(etag_hex), etag, sizeof(etag));

    chimera_s3_attach_last_modified(request->http_request, attr);

    evpl_iovec_alloc(evpl, 4096, 0, 1, 0, &request->multipart.response);

    bp = body_start = evpl_iovec_data(&request->multipart.response);

    bp += sprintf(bp, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    bp += sprintf(bp, "<GetObjectAttributesOutput xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");
    bp += sprintf(bp, "  <ETag>%s</ETag>\n", etag_hex);
    bp += sprintf(bp, "  <StorageClass>STANDARD</StorageClass>\n");
    bp += sprintf(bp, "  <ObjectSize>%" PRIu64 "</ObjectSize>\n", attr->va_size);
    bp += sprintf(bp, "</GetObjectAttributesOutput>\n");

    evpl_iovec_set_length(&request->multipart.response, bp - body_start);
    chimera_s3_response_add_datav(evpl, request,
                                  &request->multipart.response, 1);

    request->file_length      = bp - body_start;
    request->file_real_length = request->file_length;
    request->file_offset      = 0;
    request->is_list          = 1; /* triggers application/xml Content-Type */
    request->status           = CHIMERA_S3_STATUS_OK;
    request->vfs_state        = CHIMERA_S3_VFS_STATE_COMPLETE;

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }
 request_drop:
    chimera_s3_request_drop(private_data);
} /* chimera_s3_get_object_attributes_lookup_callback */

static void
chimera_s3_get_attributes_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound,
                                                                       chimera_vfs_compound_num_ops(compound) - 1);
    struct chimera_vfs_attrs              attr   = op->attr;
    enum chimera_s3_status                status = chimera_s3_compound_error(compound, private_data,
                                                                             CHIMERA_S3_STATUS_NO_SUCH_KEY);

    chimera_vfs_compound_free(compound);
    chimera_s3_get_object_attributes_lookup_callback(status, &attr, private_data);
} /* chimera_s3_get_attributes_complete */

void
chimera_s3_get_object_attributes(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    request->io_pending = 0;

    chimera_s3_request_get(request);

    struct chimera_vfs_compound *compound = chimera_s3_compound_alloc(request);
    chimera_vfs_compound_add_lookup_path(compound, request->path, request->path_len,
                                         CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
                                         CHIMERA_VFS_LOOKUP_FOLLOW);
    chimera_frontend_compound_submit(compound, chimera_s3_get_attributes_complete, request);
} /* chimera_s3_get_object_attributes */
