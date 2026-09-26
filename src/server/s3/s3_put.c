// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include "vfs/vfs_release.h"
#include "vfs/vfs_procs.h"
#include "s3_compound.h"
#include "s3_temp.h"
#include "s3_acl.h"
#include "s3_etag.h"
#include "s3_metadata.h"
#include "s3_tagging.h"
#include "s3_procs.h"

/* One logical upload, with bounded, immutable data for its current attempt.
 * The extra byte distinguishes an exact-limit object from a larger stream:
 * small objects can include publication in their first compound even when
 * the transport has not delivered RECEIVE_COMPLETE with the last data. */
struct s3_put_transfer {
    struct chimera_s3_request      *request;
    struct chimera_s3_metadata     *metadata;
    struct evpl_iovec               data;
    struct chimera_vfs_open_handle *directory;
    struct chimera_vfs_open_handle *file;
    struct chimera_vfs_open_handle *attempt_directory;
    struct chimera_vfs_open_handle *attempt_file;
    struct chimera_vfs_attrs        create_attr;
    char                           *directory_path;
    char                           *name;
    char                            temporary[96];
    uint64_t                        offset;
    uint32_t                        limit;
    uint32_t                        used;
    uint32_t                        count;
    int                             inflight;
    int                             pumping;
    int                             finished;
    int                             first;
    int                             final;
    int                             unnamed;
    int                             named_temporary;
    int                             published;
    int                             directory_result;
    int                             file_result;
    int                             attributes_result;
};

void
chimera_s3_put_cleanup(struct chimera_s3_request *request)
{
    struct s3_put_transfer *ctx = request->put_transfer;

    if (!ctx) {
        return;
    }
    if (ctx->file) {
        chimera_vfs_release(request->thread->vfs, ctx->file);
    }
    if (ctx->directory) {
        chimera_vfs_release(request->thread->vfs, ctx->directory);
    }
    if (ctx->named_temporary && !ctx->published) {
        chimera_s3_remove_temp(request, ctx->directory_path, strlen(ctx->directory_path),
                               ctx->temporary, strlen(ctx->temporary));
    }
    evpl_iovec_release(request->thread->evpl, &ctx->data);
    chimera_s3_metadata_free(ctx->metadata);
    free(ctx->directory_path);
    free(ctx->name);
    free(ctx);
    request->put_transfer = NULL;
} /* chimera_s3_put_cleanup */

static void
chimera_s3_put_fail(
    struct s3_put_transfer *ctx,
    enum chimera_s3_status  status)
{
    struct chimera_s3_request *request = ctx->request;

    ctx->finished      = 1;
    request->status    = status;
    request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(request->thread->evpl, request);
    }
} /* chimera_s3_put_fail */

static void
chimera_s3_put_attempt_reset(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct s3_put_transfer *ctx = private_data;

    ctx->attempt_directory = ctx->directory;
    ctx->attempt_file      = ctx->file;
    ctx->directory_result  = -1;
    ctx->file_result       = -1;
} /* chimera_s3_put_attempt_reset */

static void
chimera_s3_put_check_write(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, index);

    /* Publication must never succeed with a missing tail. A backend short
     * write is a failed upload; its temporary object has not been published. */
    if (*status == CHIMERA_VFS_OK && op->written != op->count) {
        *status = CHIMERA_VFS_EIO;
    }
} /* chimera_s3_put_check_write */

static int
chimera_s3_put_append_io(
    struct chimera_vfs_compound *compound,
    struct s3_put_transfer      *ctx)
{
    int index;

    if (ctx->count) {
        index = chimera_vfs_compound_add_write(compound, ctx->attempt_file,
                                               ctx->offset, ctx->count, 1,
                                               &ctx->data, 1, NULL);
        if (index < 0) {
            return -1;
        }
        chimera_vfs_compound_set_op_callbacks(compound, index, NULL,
                                              chimera_s3_put_check_write, ctx);
    }
    if (!ctx->final) {
        return 0;
    }

    /* All metadata and data precede publication. Keep the open file borrowed
     * for final GETATTR even after the current cursor returns to its parent. */
    if (ctx->unnamed) {
        chimera_vfs_compound_add_putfh(compound, ctx->attempt_file->fh,
                                       ctx->attempt_file->fh_len);
        chimera_vfs_compound_add_savefh(compound);
        chimera_vfs_compound_add_putfh(compound, ctx->attempt_directory->fh,
                                       ctx->attempt_directory->fh_len);
        index = chimera_vfs_compound_add_link_replace(compound, ctx->name, strlen(ctx->name), 0);
    } else {
        chimera_vfs_compound_add_putfh(compound, ctx->attempt_directory->fh,
                                       ctx->attempt_directory->fh_len);
        chimera_vfs_compound_add_savefh(compound);
        index = chimera_vfs_compound_add_rename(compound, ctx->temporary, strlen(ctx->temporary),
                                                ctx->name, strlen(ctx->name), 0);
    }
    if (index < 0) {
        return -1;
    }
    ctx->attributes_result = chimera_vfs_compound_add_getattr(compound,
                                                              CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_SIZE |
                                                              CHIMERA_VFS_ATTR_MTIME);
    if (ctx->attributes_result < 0) {
        return -1;
    }
    chimera_vfs_compound_op_set_handle(compound, ctx->attributes_result, ctx->attempt_file);
    return 0;
} /* chimera_s3_put_append_io */

static void
chimera_s3_put_file_ready(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct s3_put_transfer *ctx = private_data;

    if (*status) {
        return;
    }
    ctx->attempt_file = chimera_vfs_compound_op(compound, index)->out_handle;
    if (chimera_s3_metadata_append_store(compound, ctx->metadata) < 0 ||
        chimera_s3_tagging_compound_store(compound, ctx->request) < 0 ||
        chimera_s3_put_append_io(compound, ctx) < 0) {
        *status = CHIMERA_VFS_ENOSPC;
    }
} /* chimera_s3_put_file_ready */

static void
chimera_s3_put_directory_ready(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct s3_put_transfer    *ctx = private_data;
    struct chimera_vfs_module *module;
    int                        created;

    if (*status) {
        return;
    }
    ctx->directory_result  = index;
    ctx->attempt_directory = chimera_vfs_compound_op(compound, index)->out_handle;
    module                 = chimera_vfs_get_module(ctx->request->thread->vfs,
                                                    ctx->attempt_directory->fh, ctx->attempt_directory->fh_len);
    ctx->unnamed = !!(module->capabilities & CHIMERA_VFS_CAP_CREATE_UNLINKED);
    if (ctx->unnamed) {
        created = chimera_vfs_compound_add_create_unlinked(compound, &ctx->create_attr,
                                                           CHIMERA_VFS_ATTR_FH);
    } else {
        ctx->named_temporary = 1;
        created              = chimera_vfs_compound_add_open(compound, ctx->temporary, strlen(ctx->temporary),
                                                             CHIMERA_VFS_OPEN_CREATE, 0, &ctx->create_attr,
                                                             CHIMERA_VFS_ATTR_FH);
    }
    ctx->file_result = chimera_vfs_compound_add_gethandle(compound);
    if (created < 0 || ctx->file_result < 0) {
        *status = CHIMERA_VFS_ENOSPC;
        return;
    }
    chimera_vfs_compound_set_op_callbacks(compound, ctx->file_result, NULL,
                                          chimera_s3_put_file_ready, ctx);
} /* chimera_s3_put_directory_ready */

static void
chimera_s3_put_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct s3_put_transfer    *ctx     = private_data;
    struct chimera_s3_request *request = ctx->request;

    CHIMERA_S3_HOLD_REQUEST(request);
    enum chimera_vfs_error     status = chimera_vfs_compound_status(compound);

    ctx->inflight = 0;
    if (status || request->abandoned) {
        enum chimera_s3_status error = chimera_s3_compound_error(compound, request,
                                                                 CHIMERA_S3_STATUS_INTERNAL_ERROR);
        for (uint32_t i = 0; status && i < chimera_vfs_compound_num_ops(compound); i++) {
            const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, i);
            if (op->status && op->status != CHIMERA_VFS_UNSET) {
                chimera_s3_debug("PUT compound failed at operation %u type %u: %u",
                                 i, op->type, op->status);
                break;
            }
        }
        chimera_vfs_compound_free(compound);
        chimera_s3_put_fail(ctx, error);
        return;
    }
    if (ctx->final) {
        const struct chimera_vfs_attrs *attr =
            &chimera_vfs_compound_op(compound, ctx->attributes_result)->attr;
        const uint64_t                  need = CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_SIZE | CHIMERA_VFS_ATTR_MTIME;
        if ((attr->va_set_mask & need) == need) {
            chimera_s3_attach_etag(request->http_request, attr);
        }
        ctx->finished      = 1;
        ctx->published     = 1;
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
    } else if (ctx->first) {
        ctx->directory = chimera_vfs_compound_take_handle(compound, ctx->directory_result);
        ctx->file      = chimera_vfs_compound_take_handle(compound, ctx->file_result);
    }
    chimera_vfs_compound_free(compound);
    ctx->first   = 0;
    ctx->offset += ctx->count;
    ctx->used   -= ctx->count;
    if (ctx->used) {
        memmove(evpl_iovec_data(&ctx->data),
                (char *) evpl_iovec_data(&ctx->data) + ctx->count, ctx->used);
    }
    if (ctx->finished) {
        if (ctx->file) {
            chimera_vfs_release(request->thread->vfs, ctx->file);
            ctx->file = NULL;
        }
        if (ctx->directory) {
            chimera_vfs_release(request->thread->vfs, ctx->directory);
            ctx->directory = NULL;
        }
        if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
            s3_server_respond(request->thread->evpl, request);
        }
    } else if (!ctx->pumping) {
        chimera_s3_put_recv(request->thread->evpl, request);
    }
} /* chimera_s3_put_complete */

static void
chimera_s3_put_submit(struct s3_put_transfer *ctx)
{
    struct chimera_s3_request   *request = ctx->request;
    struct chimera_vfs_compound *compound;
    struct chimera_vfs_attrs     directory_attr = { 0 };

    ctx->count = ctx->used > ctx->limit ? ctx->limit : ctx->used;
    ctx->final = request->http_state == CHIMERA_S3_HTTP_STATE_RECVED &&
        evpl_http_request_get_data_avail(request->http_request) == 0 && ctx->used <= ctx->limit;
    evpl_iovec_set_length(&ctx->data, ctx->count);
    if (ctx->first) {
        compound = chimera_s3_compound_alloc(request);
        chimera_vfs_compound_add_create_tree(compound, ctx->directory_path, strlen(ctx->directory_path),
                                             &directory_attr, CHIMERA_VFS_ATTR_FH);
        chimera_vfs_compound_add_open_current(compound,
                                              CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED |
                                              CHIMERA_VFS_OPEN_DIRECTORY, 0);
        ctx->directory_result = chimera_vfs_compound_add_gethandle(compound);
        chimera_vfs_compound_set_op_callbacks(compound, ctx->directory_result, NULL,
                                              chimera_s3_put_directory_ready, ctx);
    } else {
        compound               = chimera_vfs_compound_alloc(request->thread->vfs, &request->cred);
        ctx->attempt_file      = ctx->file;
        ctx->attempt_directory = ctx->directory;
        chimera_s3_put_append_io(compound, ctx);
    }
    /* The static directory GETHANDLE index is invariant for this submission;
     * the reset callback resets only dynamic/private references below it. */
    chimera_vfs_compound_set_attempt_reset(compound, chimera_s3_put_attempt_reset, ctx);
    ctx->inflight = 1;
    chimera_s3_request_get(request);
    chimera_frontend_compound_submit(compound, chimera_s3_put_complete, ctx);
} /* chimera_s3_put_submit */

void
chimera_s3_put_recv(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    struct s3_put_transfer *ctx = request->put_transfer;
    struct evpl_iovec       incoming[CHIMERA_S3_IOV_MAX];
    uint64_t                avail;
    int                     niov;

    if (!ctx || ctx->inflight || ctx->pumping || ctx->finished || request->abandoned) {
        return;
    }
    chimera_s3_request_get(request);
    CHIMERA_S3_HOLD_REQUEST(request);
    ctx->pumping = 1;
    while (!ctx->inflight && !ctx->finished) {
        avail = evpl_http_request_get_data_avail(request->http_request);
        if (avail > ctx->limit + 1 - ctx->used) {
            avail = ctx->limit + 1 - ctx->used;
        }
        if (avail) {
            niov = evpl_http_request_get_datav(evpl, request->http_request, incoming, avail);
            for (int i = 0; i < niov; i++) {
                if (request->chunked) {
                    int used = ctx->used;
                    s3_chunk_decode(&request->chunk, evpl_iovec_data(&incoming[i]),
                                    evpl_iovec_length(&incoming[i]), evpl_iovec_data(&ctx->data), &used);
                    ctx->used = used;
                } else {
                    memcpy((char *) evpl_iovec_data(&ctx->data) + ctx->used,
                           evpl_iovec_data(&incoming[i]), evpl_iovec_length(&incoming[i]));
                    ctx->used += evpl_iovec_length(&incoming[i]);
                }
            }
            evpl_iovecs_release(evpl, incoming, niov);
            if (request->chunked && request->chunk.error) {
                chimera_s3_put_fail(ctx, CHIMERA_S3_STATUS_BAD_REQUEST);
                break;
            }
        }
        if (ctx->used > ctx->limit ||
            (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED &&
             evpl_http_request_get_data_avail(request->http_request) == 0)) {
            if (request->chunked && request->http_state == CHIMERA_S3_HTTP_STATE_RECVED &&
                evpl_http_request_get_data_avail(request->http_request) == 0 && !request->chunk.done) {
                chimera_s3_put_fail(ctx, CHIMERA_S3_STATUS_BAD_REQUEST);
                break;
            }
            chimera_s3_put_submit(ctx);
        } else if (!avail) {
            break;
        }
    }
    ctx->pumping = 0;
} /* chimera_s3_put_recv */

void
chimera_s3_put(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    const char             *tagging = evpl_http_request_header(request->http_request, "x-amz-tagging");
    const char             *slash   = NULL;
    struct s3_put_transfer *ctx;

    for (int i = 0; i < request->path_len; i++) {
        if (request->path[i] == '/') {
            slash = request->path + i;
        }
    }
    if (tagging && *tagging) {
        request->tagging = calloc(1, sizeof(*request->tagging));
        if (chimera_s3_tagging_parse_header(request->tagging, tagging)) {
            request->status    = CHIMERA_S3_STATUS_INVALID_TAG;
            request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
            if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
                s3_server_respond(evpl, request);
            }
            return;
        }
    }
    ctx                   = calloc(1, sizeof(*ctx));
    request->put_transfer = ctx;
    ctx->request          = request;
    ctx->metadata         = chimera_s3_metadata_capture(request);
    ctx->limit            = chimera_s3_compound_chunk_size(request);
    ctx->first            = 1;
    ctx->directory_path   = slash ? strndup(request->path, slash - request->path) : strdup("/");
    ctx->name             = slash ? strndup(slash + 1, request->path_len - (slash + 1 - request->path)) :
        strndup(request->path, request->path_len);
    snprintf(ctx->temporary, sizeof(ctx->temporary), "._chimera_%" PRIx64 "%" PRIx64 "%" PRIx64,
             (uint64_t) request, (uint64_t) request->start_time.tv_sec,
             (uint64_t) request->start_time.tv_nsec);
    ctx->create_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    ctx->create_attr.va_mode     = chimera_s3_canned_acl_to_mode(
        request->canned_acl == CHIMERA_S3_CANNED_NONE ? CHIMERA_S3_CANNED_PRIVATE : request->canned_acl, 0);
    evpl_iovec_alloc(evpl, ctx->limit + 1, 0, 1, 0, &ctx->data);
    request->vfs_state = CHIMERA_S3_VFS_STATE_RECV;
    chimera_s3_put_recv(evpl, request);
} /* chimera_s3_put */
