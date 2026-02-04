// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <time.h>
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "s3_internal.h"
#include "s3_etag.h"

static inline void
chimera_s3_put_finish_common(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct evpl                     *evpl    = thread->evpl;

    chimera_vfs_release(thread->vfs, request->dir_handle);
    chimera_vfs_release(thread->vfs, request->file_handle);

    if (error_code) {
        request->status = CHIMERA_S3_STATUS_INTERNAL_ERROR;
    }

    request->vfs_state = CHIMERA_S3_VFS_STATE_SEND;

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }
} /* chimera_s3_put_finish_common */

static void
chimera_s3_put_rename_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *fromdir_pre_attr,
    struct chimera_vfs_attrs *fromdir_post_attr,
    struct chimera_vfs_attrs *todir_pre_attr,
    struct chimera_vfs_attrs *todir_post_attr,
    void                     *private_data)
{
    chimera_s3_put_finish_common(error_code, private_data);
} /* chimera_s3_put_rename_callback */

static void
chimera_s3_put_link_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *r_attr,
    struct chimera_vfs_attrs *r_dir_pre_attr,
    struct chimera_vfs_attrs *r_dir_post_attr,
    void                     *private_data)
{
    chimera_s3_put_finish_common(error_code, private_data);
} /* chimera_s3_put_link_callback */

static inline void
chimera_s3_put_rename(struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread *thread = request->thread;

    if (request->put.tmp_name_len) {
        chimera_vfs_rename(
            thread->vfs,
            &thread->shared->cred,
            request->dir_handle->fh,
            request->dir_handle->fh_len,
            request->put.tmp_name,
            request->put.tmp_name_len,
            request->dir_handle->fh,
            request->dir_handle->fh_len,
            request->name,
            request->name_len,
            NULL,
            0,
            0,
            0,
            chimera_s3_put_rename_callback,
            request);
    } else {
        chimera_vfs_link(
            thread->vfs,
            &thread->shared->cred,
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
            chimera_s3_put_link_callback,
            request);
    }
} /* chimera_s3_put_finish */


static void
chimera_s3_put_recv_callback(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_s3_io            *io      = private_data;
    struct chimera_s3_request       *request = io->request;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct evpl                     *evpl    = thread->evpl;

    evpl_iovecs_release(evpl, io->iov, io->niov);
    chimera_s3_io_free(thread, io);

    request->io_pending--;

    if (error_code) {
        request->status    = CHIMERA_S3_STATUS_INTERNAL_ERROR;
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        return;
    }

    if (request->io_pending == 0 &&
        request->vfs_state == CHIMERA_S3_VFS_STATE_RECVED) {
        chimera_s3_put_rename(request);
    }
} /* chimera_s3_put_recv_callback */

void
chimera_s3_put_recv(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread *thread = request->thread;
    struct chimera_server_s3_shared *shared = thread->shared;
    struct chimera_s3_config        *config = shared->config;
    struct chimera_s3_io            *io;
    uint64_t                         avail;
    int                              final;

    final = (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED);

 again:

    avail = evpl_http_request_get_data_avail(request->http_request);

    if (avail < config->io_size && !final) {
        return;
    }

    if (avail > config->io_size) {
        avail = config->io_size;
    }

    if (avail == 0 && final) {
        request->vfs_state = CHIMERA_S3_VFS_STATE_RECVED;

        if (avail == 0 && request->io_pending == 0) {
            chimera_s3_put_rename(request);
        }
        return;
    }

    io = chimera_s3_io_alloc(thread, request);

    io->niov = evpl_http_request_get_datav(evpl, request->http_request, io->iov, avail);

    request->io_pending++;

    chimera_vfs_write(request->thread->vfs, NULL,
                      request->file_handle,
                      request->file_cur_offset,
                      avail,
                      1,
                      0,
                      0,
                      io->iov,
                      io->niov,
                      chimera_s3_put_recv_callback,
                      io);

    request->file_cur_offset += avail;

    goto again;

} /* chimera_s3_put_recv */


static void
chimera_s3_put_create_unlinked_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
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
    request->vfs_state   = CHIMERA_S3_VFS_STATE_RECV;

    chimera_s3_attach_etag(request->http_request, attr);

    chimera_s3_put_recv(evpl, request);

} /* chimera_s3_put_create_callback */

static void
chimera_s3_put_create_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
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
    request->vfs_state   = CHIMERA_S3_VFS_STATE_RECV;

    chimera_s3_attach_etag(request->http_request, attr);

    chimera_s3_put_recv(evpl, request);

} /* chimera_s3_put_create_callback */

static void
chimera_s3_put_open_dir_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct chimera_vfs_module       *module;

    if (error_code) {
        request->status    = CHIMERA_S3_STATUS_NO_SUCH_KEY;
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        return;
    }

    request->dir_handle = oh;

    request->set_attr.va_req_mask = 0;
    request->set_attr.va_set_mask = 0;

    module = chimera_vfs_get_module(thread->vfs, oh->fh, oh->fh_len);

    if (module->capabilities & CHIMERA_VFS_CAP_CREATE_UNLINKED) {

        request->put.tmp_name_len = 0;

        chimera_vfs_create_unlinked(thread->vfs, NULL,
                                    oh->fh,
                                    oh->fh_len,
                                    &request->set_attr,
                                    CHIMERA_VFS_ATTR_FH,
                                    chimera_s3_put_create_unlinked_callback,
                                    request);
    } else {
        request->put.tmp_name_len = snprintf(request->put.tmp_name, sizeof(request->put.tmp_name),
                                             "._chimera_%lx%lx%lx", (uint64_t) request,
                                             request->start_time.tv_sec, request->start_time.tv_nsec);
        chimera_vfs_open_at(thread->vfs, NULL,
                            oh,
                            request->put.tmp_name,
                            request->put.tmp_name_len,
                            CHIMERA_VFS_OPEN_CREATE,
                            &request->set_attr,
                            CHIMERA_VFS_ATTR_FH,
                            0,
                            0,
                            chimera_s3_put_create_callback,
                            request);
    }

} /* chimera_s3_put_open_dir_callback */

static void
chimera_s3_put_lookup_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;

    if (error_code) {
        request->status    = CHIMERA_S3_STATUS_NO_SUCH_KEY;
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        return;
    }

    chimera_s3_abort_if(!(attr->va_set_mask & CHIMERA_VFS_ATTR_FH), "put lookup callback: no fh");

    chimera_vfs_open(thread->vfs, NULL,
                     attr->va_fh,
                     attr->va_fh_len,
                     CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
                     chimera_s3_put_open_dir_callback,
                     request);
}  /* chimera_s3_put_lookup_callback */

void
chimera_s3_put(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    const char *slash;
    const char *dirpath = request->path;
    int         dirpathlen;

    slash = rindex(request->path, '/');

    if (slash) {

        dirpathlen = slash - request->path;

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

    request->set_attr.va_req_mask = 0;
    request->set_attr.va_set_mask = 0;

    request->io_pending = 0;

    chimera_vfs_create_path(thread->vfs, NULL,
                            request->bucket_fh,
                            request->bucket_fhlen,
                            dirpath,
                            dirpathlen,
                            &request->set_attr,
                            CHIMERA_VFS_ATTR_FH,
                            chimera_s3_put_lookup_callback,
                            request);
} /* chimera_s3_put */