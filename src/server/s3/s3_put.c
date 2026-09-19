// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * S3 PutObject, as three kinds of VFS sequence.
 *
 *   setup    PUTFH(bucket) -> [CREATE_PATH(DIR, dirpath, mkdir -p)]
 *            -> OPEN_CURRENT(dir) -> GETHANDLE -> CREATE_UNLINKED
 *            (or OPEN(tmpname, CREATE) on a backend without the capability)
 *            -> SETXATTR x metadata headers, as many as fit.  Any that do
 *            not fit are driven as following sequences by the metadata
 *            store once the object exists.
 *   write    one WRITE per io_size chunk of the body, each its own sequence
 *            on the taken object handle, left in flight concurrently as the
 *            per-op writes were.
 *   publish  PUTFH(dir) -> SAVEFH -> RENAME(tmp -> key), or PUTFH(object)
 *            -> SAVEFH -> PUTFH(dir) -> LINK(key, replace) for the unlinked
 *            object; then GETATTR(FH|SIZE|MTIME) through the object handle
 *            for the ETag, in the same sequence.
 */

#include <stdio.h>
#include <time.h>
#include "vfs/vfs.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_release.h"
#include "s3_internal.h"
#include "s3_acl.h"
#include "s3_etag.h"
#include "s3_metadata.h"
#include "s3_tagging.h"

/* Terminal: emit the PutObject response (called directly, or after the
 * optional x-amz-tagging xattrs have been stored). */
static void
chimera_s3_put_respond(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    request->vfs_state = CHIMERA_S3_VFS_STATE_SEND;

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }
} /* chimera_s3_put_respond */

/*
 * The publish sequence is over.  `error_code` is the publish's own status;
 * `attr` is the object's final attributes when the trailing GETATTR ran OK,
 * and NULL when it did not -- a failed getattr costs the response its ETag,
 * not the PUT its object, exactly as before.
 */
static void
chimera_s3_put_finish_common(
    enum chimera_vfs_error          error_code,
    const struct chimera_vfs_attrs *attr,
    struct chimera_s3_request      *request)
{
    struct chimera_server_s3_thread *thread = request->thread;
    struct evpl                     *evpl   = thread->evpl;
    const uint64_t                   need   = CHIMERA_VFS_ATTR_FH |
        CHIMERA_VFS_ATTR_SIZE | CHIMERA_VFS_ATTR_MTIME;

    if (request->dir_handle) {
        chimera_vfs_release(thread->vfs, request->dir_handle);
        request->dir_handle = NULL;
    }

    if (error_code) {
        request->status = chimera_s3_status_from_vfs(error_code, CHIMERA_S3_STATUS_INTERNAL_ERROR);
        if (request->file_handle) {
            chimera_vfs_release(thread->vfs, request->file_handle);
            request->file_handle = NULL;
        }
        request->vfs_state = CHIMERA_S3_VFS_STATE_SEND;
        if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
            s3_server_respond(evpl, request);
        }
        return;
    }

    /* Attach the object ETag computed from the *final* attributes (post-write
     * size + mtime + fh) so the value matches what a subsequent HEAD/GET will
     * return. Computing it earlier (e.g. at create time, when the object is
     * still empty) yields a different hash and desyncs client ETag checks. */
    if (attr && (attr->va_set_mask & need) == need) {
        chimera_s3_attach_etag(request->http_request, attr);
    }

    if (request->file_handle) {
        chimera_vfs_release(thread->vfs, request->file_handle);
        request->file_handle = NULL;
    }

    /* If the PutObject carried an x-amz-tagging header, store the parsed tags
     * as xattrs on the freshly-written object before responding. */
    if (request->status == CHIMERA_S3_STATUS_OK && request->tagging &&
        request->tagging->n_tags > 0) {
        chimera_s3_tagging_store_by_path(evpl, thread, request,
                                         chimera_s3_put_respond);
        return;
    }

    chimera_s3_put_respond(evpl, request);
} /* chimera_s3_put_finish_common */

static void
chimera_s3_put_publish_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    CHIMERA_S3_HOLD_REQUEST(private_data);
    struct chimera_s3_request            *request = private_data;
    const struct chimera_vfs_compound_op *publish, *getattr;
    struct chimera_vfs_attrs              attr;
    enum chimera_vfs_error                error_code = CHIMERA_VFS_OK;
    int                                   have_attr  = 0;
    uint32_t                              num_ops;

    num_ops = chimera_vfs_compound_num_ops(compound);

    /* The RENAME / LINK is the second-to-last op, the GETATTR the last. */
    publish = chimera_vfs_compound_op(compound, num_ops - 2);
    getattr = chimera_vfs_compound_op(compound, num_ops - 1);

    if (publish->status != CHIMERA_VFS_OK) {
        error_code = chimera_vfs_compound_status(compound);
    } else if (getattr->status == CHIMERA_VFS_OK) {
        attr      = getattr->attr;
        have_attr = 1;
    }

    chimera_vfs_compound_free(compound);

    chimera_s3_put_finish_common(error_code, have_attr ? &attr : NULL, request);
} /* chimera_s3_put_publish_complete */

static inline void
chimera_s3_put_rename(struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread *thread = request->thread;
    struct chimera_vfs_compound     *compound;
    int                              index;

    /* Both the last write completion and the metadata-done resume can reach
     * here believing the request is finished (see the field comments in
     * s3_internal.h); publish only once, and never before the metadata
     * xattrs are in place -- metadata_done re-drives this via put_recv. */
    if (request->put.meta_pending || request->put.published) {
        return;
    }
    request->put.published = 1;

    compound = chimera_vfs_compound_alloc(thread->vfs, &request->cred);

    if (request->put.tmp_name_len) {
        chimera_vfs_compound_add_putfh(compound,
                                       request->dir_handle->fh,
                                       request->dir_handle->fh_len);
        chimera_vfs_compound_add_savefh(compound);
        chimera_vfs_compound_add_rename(compound,
                                        request->put.tmp_name,
                                        request->put.tmp_name_len,
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
        /* An S3 PUT replaces whatever object held the key. */
        chimera_vfs_compound_op_set_link_opts(compound, index, 1, NULL, NULL);
    }

    /* The ETag comes from the object as published, read through the handle
     * the body was written with. */
    index = chimera_vfs_compound_add_getattr(compound,
                                             CHIMERA_VFS_ATTR_FH |
                                             CHIMERA_VFS_ATTR_SIZE |
                                             CHIMERA_VFS_ATTR_MTIME);
    chimera_vfs_compound_op_set_handle(compound, index, request->file_handle);

    chimera_s3_request_get(request);

    chimera_vfs_compound_submit(compound, chimera_s3_put_publish_complete,
                                request);
} /* chimera_s3_put_rename */

static void
chimera_s3_put_write_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_s3_io            *io      = private_data;
    struct chimera_s3_request       *request = io->request;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct evpl                     *evpl    = thread->evpl;
    enum chimera_vfs_error           error_code;

    error_code = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);

    evpl_iovecs_release(evpl, io->iov, io->niov);
    chimera_s3_io_free(thread, io);

    request->io_pending--;

    if (error_code) {
        request->status    = chimera_s3_status_from_vfs(error_code, CHIMERA_S3_STATUS_INTERNAL_ERROR);
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        return;
    }

    if (request->io_pending == 0 &&
        request->vfs_state == CHIMERA_S3_VFS_STATE_RECVED) {
        chimera_s3_put_rename(request);
    }
} /* chimera_s3_put_write_complete */

void
chimera_s3_put_recv(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread *thread = request->thread;
    struct chimera_server_s3_shared *shared = thread->shared;
    struct chimera_s3_config        *config = shared->config;
    struct chimera_s3_io            *io;
    struct chimera_vfs_compound     *compound;
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

    /* One WRITE per chunk, addressing the object handle the setup sequence
     * handed over; the iovecs are borrowed by the sequence and released in
     * its completion, on this thread. */
    compound = chimera_vfs_compound_alloc(thread->vfs, &request->cred);

    chimera_vfs_compound_add_write(compound,
                                   request->file_handle,
                                   request->file_cur_offset,
                                   avail,
                                   1,
                                   io->iov,
                                   io->niov,
                                   0, 0,
                                   NULL);

    chimera_vfs_compound_submit(compound, chimera_s3_put_write_complete, io);

    request->file_cur_offset += avail;

    goto again;

} /* chimera_s3_put_recv */


/*
 * The destination file exists and its metadata xattrs (Content-Type and any
 * x-amz-meta-*) have been written. Begin draining the request body into it.
 */
static void
chimera_s3_put_metadata_done(
    struct chimera_s3_request *request,
    int                        error,
    void                      *private_data)
{
    struct chimera_server_s3_thread *thread = request->thread;
    struct evpl                     *evpl   = thread->evpl;

    request->put.meta = NULL;

    if (error) {
        request->status    = CHIMERA_S3_STATUS_INTERNAL_ERROR;
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        chimera_vfs_release(thread->vfs, request->file_handle);
        request->file_handle = NULL;
        chimera_vfs_release(thread->vfs, request->dir_handle);
        request->dir_handle = NULL;
        if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
            s3_server_respond(evpl, request);
        }
        return;
    }

    request->put.meta_pending = 0;

    chimera_s3_put_recv(evpl, request);
} /* chimera_s3_put_metadata_done */

/*
 * The setup sequence is over.  On success the directory and object handles
 * are taken and the body drain begins -- concurrently with whatever metadata
 * headers did not fit in the setup sequence, which the store drives now.  On
 * failure nothing is taken: the sequence releases what it opened.
 */
static void
chimera_s3_put_setup_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;
    enum chimera_vfs_error           error_code;
    uint32_t                         completed;

    error_code = chimera_vfs_compound_status(compound);
    completed  = chimera_vfs_compound_num_completed(compound);

    if (error_code) {
        /* A metadata xattr that failed after the object was created is the
         * INTERNAL_ERROR it was; anything earlier -- resolving or opening
         * the directory, creating the object -- is a missing key. */
        if ((int) completed - 1 > request->put.create_index) {
            request->status = CHIMERA_S3_STATUS_INTERNAL_ERROR;
        } else {
            request->status = chimera_s3_status_from_vfs(error_code, CHIMERA_S3_STATUS_NO_SUCH_KEY);
        }
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;

        chimera_vfs_compound_free(compound);

        chimera_s3_metadata_store_free(request->put.meta);
        request->put.meta = NULL;

        if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
            s3_server_respond(thread->evpl, request);
        }
        return;
    }

    request->dir_handle  = chimera_vfs_compound_take_handle(compound, request->put.dir_index);
    request->file_handle = chimera_vfs_compound_take_handle(compound, request->put.create_index);

    chimera_vfs_compound_free(compound);

    request->put.meta_pending = 1;
    request->vfs_state        = CHIMERA_S3_VFS_STATE_RECV;

    /* The response ETag is attached after the body is written, from the
     * object's final attributes (see chimera_s3_put_publish_complete). */

    if (request->put.meta) {
        chimera_s3_metadata_store_drive(request->put.meta, request,
                                        request->file_handle,
                                        chimera_s3_put_metadata_done, NULL);
        return;
    }

    chimera_s3_put_metadata_done(request, 0, NULL);
} /* chimera_s3_put_setup_complete */

void
chimera_s3_put(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    struct chimera_vfs_compound *compound;
    struct chimera_vfs_module   *module;
    struct chimera_vfs_attrs     dir_attr;
    const char                  *slash;
    const char                  *dirpath = request->path;
    int                          dirpathlen;
    const char                  *tagging_hdr;
    int                          budget;

    request->put.meta_pending = 0;
    request->put.published    = 0;
    request->put.meta         = NULL;

    /* x-amz-tagging: parse + validate the tag set now so a violation is
     * reported as 400 InvalidTag before any object bytes are written. The tags
     * are applied as xattrs once the object is in place (put_finish_common). */
    tagging_hdr = evpl_http_request_header(request->http_request, "x-amz-tagging");

    if (tagging_hdr && tagging_hdr[0]) {
        struct chimera_s3_tagging_ctx *ctx = calloc(1, sizeof(*ctx));

        request->tagging = ctx;

        if (chimera_s3_tagging_parse_header(ctx, tagging_hdr) != 0) {
            free(ctx);
            request->tagging   = NULL;
            request->status    = CHIMERA_S3_STATUS_INVALID_TAG;
            request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
            if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
                s3_server_respond(evpl, request);
            }
            return;
        }
    }

    slash = strrchr(request->path, '/');

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

    request->io_pending = 0;

    /* Project the canned ACL onto the new object's mode.  An S3 object is
     * private unless its creator says otherwise, so an absent x-amz-acl means
     * "private" (0600) rather than whatever the backend would default to --
     * inheriting a world-readable default would publish every object. */
    request->set_attr.va_req_mask = 0;
    request->set_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    request->set_attr.va_mode     =
        chimera_s3_canned_acl_to_mode(
            request->canned_acl == CHIMERA_S3_CANNED_NONE ?
            CHIMERA_S3_CANNED_PRIVATE : request->canned_acl, 0);

    /* The interior directories take the backend's defaults, as they did. */
    dir_attr.va_req_mask = 0;
    dir_attr.va_set_mask = 0;

    compound = chimera_vfs_compound_alloc(thread->vfs, &request->cred);

    chimera_vfs_compound_add_putfh(compound, request->bucket_fh,
                                   request->bucket_fhlen);

    if (slash) {
        /* mkdir -p the key's parent chain; the final directory becomes
         * current.  A key with no '/' lives in the bucket itself. */
        chimera_vfs_compound_add_create_path(compound,
                                             CHIMERA_VFS_COMPOUND_CREATE_DIR,
                                             dirpath, dirpathlen,
                                             NULL, 0,
                                             &dir_attr,
                                             CHIMERA_VFS_ATTR_FH,
                                             1);
    }

    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_PATH |
                                          CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_DIRECTORY,
                                          0);

    request->put.dir_index = chimera_vfs_compound_add_gethandle(compound);

    /* The object's directory is inside the bucket, so the bucket's module is
     * the directory's: whether the object can be created unlinked is decided
     * here rather than after the directory is open. */
    module = chimera_vfs_get_module(thread->vfs, request->bucket_fh,
                                    request->bucket_fhlen);

    if (module->capabilities & CHIMERA_VFS_CAP_CREATE_UNLINKED) {

        request->put.tmp_name_len = 0;

        request->put.create_index =
            chimera_vfs_compound_add_create_unlinked(compound,
                                                     CHIMERA_VFS_OPEN_WRITE_ONLY,
                                                     &request->set_attr,
                                                     CHIMERA_VFS_ATTR_FH);
    } else {
        request->put.tmp_name_len = snprintf(request->put.tmp_name, sizeof(request->put.tmp_name),
                                             "._chimera_%" PRIx64 "%" PRIx64 "%" PRIx64,
                                             (uint64_t) request,
                                             (uint64_t) request->start_time.tv_sec,
                                             (uint64_t) request->start_time.tv_nsec);

        request->put.create_index =
            chimera_vfs_compound_add_open(compound,
                                          request->put.tmp_name,
                                          request->put.tmp_name_len,
                                          CHIMERA_VFS_OPEN_CREATE,
                                          0,
                                          &request->set_attr,
                                          CHIMERA_VFS_ATTR_FH,
                                          0, 0);
    }

    /* The metadata headers, on the handle the create produced.  What fits
     * in this sequence goes here; the rest is driven once it has run. */
    request->put.meta = chimera_s3_metadata_capture(request);

    if (request->put.meta) {
        budget = CHIMERA_VFS_COMPOUND_MAX_OPS -
            (int) chimera_vfs_compound_num_ops(compound);

        chimera_s3_metadata_add_ops(request->put.meta, compound,
                                    request->put.create_index, NULL, budget);
    }

    chimera_s3_request_get(request);

    chimera_vfs_compound_submit(compound, chimera_s3_put_setup_complete,
                                request);
} /* chimera_s3_put */
