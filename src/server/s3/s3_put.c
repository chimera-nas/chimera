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
chimera_s3_put_respond(
    struct chimera_s3_request *request,
    enum chimera_vfs_error     error_code)
{
    struct chimera_server_s3_thread *thread = request->thread;
    struct evpl                     *evpl   = thread->evpl;

    chimera_vfs_release(thread->vfs, request->dir_handle);
    chimera_vfs_release(thread->vfs, request->file_handle);

    if (error_code) {
        request->status = CHIMERA_S3_STATUS_INTERNAL_ERROR;
    }

    request->vfs_state = CHIMERA_S3_VFS_STATE_SEND;

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }
} /* chimera_s3_put_respond */

/* Final attributes of the published object: compute and attach the response
 * ETag from them. The ETag is a hash of size + mtime + fh, so it is only
 * stable once the body is written and the file is linked/renamed into place;
 * attaching it any earlier returns the ETag of the empty placeholder and
 * breaks the client's later conditional GET (If-Match) against the object. */
static void
chimera_s3_put_getattr_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_s3_request *request = private_data;

    if (!error_code &&
        (attr->va_set_mask & (CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_SIZE |
                              CHIMERA_VFS_ATTR_MTIME)) ==
        (CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_SIZE | CHIMERA_VFS_ATTR_MTIME)) {
        chimera_s3_attach_etag(request->http_request, attr);
    }

    chimera_s3_put_respond(request, error_code);
} /* chimera_s3_put_getattr_callback */

static inline void
chimera_s3_put_finish_common(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;

    if (error_code) {
        chimera_s3_put_respond(request, error_code);
        return;
    }

    chimera_vfs_getattr(thread->vfs, &thread->shared->cred,
                        request->file_handle,
                        CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
                        chimera_s3_put_getattr_callback,
                        request);
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
        chimera_vfs_rename_at(
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
        chimera_vfs_link_at(
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

    if (request->chunked) {
        /* De-chunk the aws-chunked framing into a fresh buffer. Decoding only
         * strips framing, so the decoded length never exceeds the raw input
         * and a single output iovec of `avail` bytes always suffices. The raw
         * input is pulled into a separate scratch array and released once
         * copied; the decoded output is allocated directly into io->iov[0] so
         * libevpl's iovec ownership tracking stays intact. */
        struct evpl_iovec in_iov[CHIMERA_S3_IOV_MAX];
        int               in_niov;
        int               out_len = 0;
        int               i;

        in_niov = evpl_http_request_get_datav(evpl, request->http_request,
                                              in_iov, avail);

        evpl_iovec_alloc(evpl, avail, 0, 1, 0, &io->iov[0]);

        for (i = 0; i < in_niov; i++) {
            s3_chunk_decode(&request->chunk,
                            evpl_iovec_data(&in_iov[i]),
                            evpl_iovec_length(&in_iov[i]),
                            evpl_iovec_data(&io->iov[0]),
                            &out_len);
        }

        evpl_iovecs_release(evpl, in_iov, in_niov);

        if (request->chunk.error) {
            evpl_iovec_release(evpl, &io->iov[0]);
            chimera_s3_io_free(thread, io);
            request->status    = CHIMERA_S3_STATUS_BAD_REQUEST;
            request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
            return;
        }

        if (out_len == 0) {
            /* This read carried only framing (e.g. a chunk header or the
             * trailer); nothing to write yet. */
            evpl_iovec_release(evpl, &io->iov[0]);
            chimera_s3_io_free(thread, io);
            goto again;
        }

        evpl_iovec_set_length(&io->iov[0], out_len);
        io->niov = 1;
        avail    = out_len;
    } else {
        io->niov = evpl_http_request_get_datav(evpl, request->http_request, io->iov, avail);
    }

    request->io_pending++;

    chimera_vfs_write(request->thread->vfs, &thread->shared->cred,
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

    /* The ETag is derived from the object's size + mtime + fh, none of which
     * are final until the body has been written. It is computed and attached
     * from the post-write attributes in chimera_s3_put_finish_common, not from
     * this just-created empty file. */

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

    /* ETag attached post-write; see chimera_s3_put_create_unlinked_callback. */

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

        chimera_vfs_create_unlinked(thread->vfs, &thread->shared->cred,
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
        chimera_vfs_open_at(thread->vfs, &thread->shared->cred,
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

    chimera_vfs_open_fh(thread->vfs, &thread->shared->cred,
                        attr->va_fh,
                        attr->va_fh_len,
                        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_s3_put_open_dir_callback,
                        request);
}  /* chimera_s3_put_lookup_callback */

/* Begin the actual object write: resolve/create the parent directory, then
 * create the (temp) destination file. Reached either directly (no conditional
 * headers) or after the destination-precondition lookup has passed. */
static void
chimera_s3_put_begin(struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread *thread = request->thread;
    const char                      *slash;
    const char                      *dirpath = request->path;
    int                              dirpathlen;

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

    chimera_vfs_create(thread->vfs, &thread->shared->cred,
                       request->bucket_fh,
                       request->bucket_fhlen,
                       dirpath,
                       dirpathlen,
                       &request->set_attr,
                       CHIMERA_VFS_ATTR_FH,
                       chimera_s3_put_lookup_callback,
                       request);
} /* chimera_s3_put_begin */

/* Conditional-PUT precondition lookup of the destination key.
 *
 * The header semantics (RFC 7232 / S3 PutObject) evaluated against the object
 * currently stored under the destination key:
 *   If-None-Match: "*"      -> fail (412) if the object already exists
 *   If-None-Match: <etag>   -> fail (412) if the existing ETag matches
 *   If-Match:      "*"      -> fail (404) if no object exists
 *   If-Match:      <etag>   -> fail (412) if the existing ETag does not match
 * A missing object surfaces here as an error_code; whether that is a failure
 * depends on which header was supplied.
 */
static void
chimera_s3_put_precond_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct evpl                     *evpl    = thread->evpl;
    const char                      *if_match;
    const char                      *if_none;
    enum chimera_s3_status           fail = CHIMERA_S3_STATUS_OK;

    if_match = evpl_http_request_header(request->http_request, "If-Match");
    if_none  = evpl_http_request_header(request->http_request, "If-None-Match");

    if (error_code ||
        (attr->va_set_mask & (CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_SIZE |
                              CHIMERA_VFS_ATTR_MTIME)) !=
        (CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_SIZE | CHIMERA_VFS_ATTR_MTIME)) {
        /* Destination key does not exist (or is not a readable object). */
        if (if_match) {
            /* If-Match against a missing object: NoSuchKey (per S3). */
            fail = CHIMERA_S3_STATUS_NO_SUCH_KEY;
        }
        /* If-None-Match (incl. "*") against a missing object: precondition
         * holds, proceed with the create. */
    } else {
        /* Destination key exists; compare ETags. */
        if (if_match && !chimera_s3_etag_matches(if_match, attr)) {
            fail = CHIMERA_S3_STATUS_PRECONDITION_FAILED;
        }
        if (if_none && chimera_s3_etag_matches(if_none, attr)) {
            fail = CHIMERA_S3_STATUS_PRECONDITION_FAILED;
        }
    }

    if (fail != CHIMERA_S3_STATUS_OK) {
        request->status    = fail;
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        /* The request body must still be drained so the connection stays in
         * sync; s3_server_notify handles that and dispatches the response once
         * the body is received. */
        if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
            s3_server_respond(evpl, request);
        }
        return;
    }

    chimera_s3_put_begin(request);
} /* chimera_s3_put_precond_callback */

void
chimera_s3_put(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    const char *if_match;
    const char *if_none;

    if_match = evpl_http_request_header(request->http_request, "If-Match");
    if_none  = evpl_http_request_header(request->http_request, "If-None-Match");

    if (if_match || if_none) {
        /* Evaluate the precondition against the object currently at the key
         * before creating anything. */
        request->io_pending = 0;
        chimera_vfs_lookup(thread->vfs, &thread->shared->cred,
                           request->bucket_fh,
                           request->bucket_fhlen,
                           request->path,
                           request->path_len,
                           CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
                           CHIMERA_VFS_LOOKUP_FOLLOW,
                           chimera_s3_put_precond_callback,
                           request);
        return;
    }

    chimera_s3_put_begin(request);
} /* chimera_s3_put */