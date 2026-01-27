// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <time.h>
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "s3_internal.h"
#include "s3_etag.h"

static void
chimera_s3_get_finish(struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread *thread = request->thread;

    chimera_vfs_release(thread->vfs, request->file_handle);

    request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;

} /* chimera_s3_put_rename_callback */

static void
chimera_s3_get_send_callback(
    enum chimera_vfs_error    error_code,
    uint32_t                  count,
    uint32_t                  eof,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_s3_io            *io      = private_data;
    struct chimera_s3_request       *request = io->request;
    struct chimera_server_s3_thread *thread  = request->thread;

    if (error_code) {
        request->status    = CHIMERA_S3_STATUS_INTERNAL_ERROR;
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        return;
    }

    if (niov) {
        evpl_http_request_add_datav(request->http_request, iov, niov);
    }

    chimera_s3_io_free(thread, io);

    request->io_pending--;

    if (request->io_pending == 0 &&
        request->vfs_state == CHIMERA_S3_VFS_STATE_SENT) {
        chimera_s3_get_finish(request);
    }
} /* chimera_s3_put_recv_callback */

void
chimera_s3_get_send(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread *thread = request->thread;
    struct chimera_server_s3_shared *shared = thread->shared;
    struct chimera_s3_config        *config = shared->config;
    struct chimera_s3_io            *io;
    uint64_t                         left;

 again:

    left = request->file_left;

    if (left == 0) {
        request->vfs_state = CHIMERA_S3_VFS_STATE_SENT;

        if (request->io_pending == 0) {
            chimera_s3_get_finish(request);
        }
        return;
    }

    if (left > config->io_size) {
        left = config->io_size;
    }

    io = chimera_s3_io_alloc(thread, request);

    io->niov = CHIMERA_S3_IOV_MAX;

    request->io_pending++;

    chimera_vfs_read(request->thread->vfs,
                     &request->thread->shared->cred,
                     request->file_handle,
                     request->file_cur_offset,
                     left,
                     io->iov,
                     io->niov,
                     0,
                     chimera_s3_get_send_callback,
                     io);


    request->file_cur_offset += left;
    request->file_left       -= left;

    goto again;

} /* chimera_s3_get_send */

static void
chimera_s3_get_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct evpl                     *evpl    = thread->evpl;

    if (error_code) {
        request->status    = CHIMERA_S3_STATUS_NO_SUCH_KEY;
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        chimera_vfs_release(thread->vfs, request->dir_handle);
        return;
    }

    request->file_handle = oh;

    request->vfs_state = CHIMERA_S3_VFS_STATE_SEND;

    if (request->http_state == CHIMERA_S3_HTTP_STATE_SEND) {
        chimera_s3_get_send(evpl, request);
    }

} /* chimera_s3_put_create_callback */

static void
chimera_s3_get_lookup_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct evpl                     *evpl    = thread->evpl;

    if (error_code) {
        request->status    = CHIMERA_S3_STATUS_NO_SUCH_KEY;
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        return;
    }

    chimera_s3_attach_etag(request->http_request, attr);

    request->file_real_length = attr->va_size;

    if (request->file_length == 0) {
        request->file_length = request->file_real_length;
        request->file_left   = request->file_length;
    }

    chimera_s3_abort_if(!(attr->va_set_mask & CHIMERA_VFS_ATTR_FH), "put lookup callback: no fh");

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }

    if (evpl_http_request_type(request->http_request) == EVPL_HTTP_REQUEST_TYPE_HEAD) {
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
    } else {
        chimera_vfs_open(thread->vfs, NULL,
                         attr->va_fh,
                         attr->va_fh_len,
                         0,
                         chimera_s3_get_open_callback,
                         request);
    }
}  /* chimera_s3_get_lookup_callback */

void
chimera_s3_get(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    request->io_pending = 0;

    chimera_vfs_lookup_path(thread->vfs, NULL,
                            request->bucket_fh,
                            request->bucket_fhlen,
                            request->path,
                            request->path_len,
                            CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
                            CHIMERA_VFS_LOOKUP_FOLLOW,
                            chimera_s3_get_lookup_callback,
                            request);
} /* chimera_s3_get */