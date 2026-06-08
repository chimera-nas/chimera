// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <time.h>
#include <sys/stat.h>
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
                     &request->thread->shared->cred, NULL,
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
        if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
            s3_server_respond(evpl, request);
        }
        return;
    }

    /* Only regular files are S3 objects. A key can resolve to a directory
     * (chimera stores hierarchical keys as a real directory tree) or to an
     * entry whose lookup did not return the size/mtime/fh the object ETag is
     * built from. Either way it is not a readable object: report NoSuchKey
     * instead of asserting in chimera_s3_compute_etag. */
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
            return;
        }
    }

    chimera_s3_attach_etag(request->http_request, attr);
    chimera_s3_attach_last_modified(request->http_request, attr);

    request->file_real_length = attr->va_size;

    /* Resolve the requested byte range now that the object size is known. The
     * range was parsed before the size was available, so open-ended and suffix
     * forms still carry sentinels:
     *   no range          -> file_offset 0, file_length 0
     *   bytes=N-M (closed) -> file_offset N, file_length M-N+1
     *   bytes=N-  (open)   -> file_offset N, file_length -1
     *   bytes=-N  (suffix) -> file_offset -1, file_length N
     * Leaving a negative length (or offset) in place makes file_left wrap to a
     * huge unsigned value and the read loop in chimera_s3_get_send spins
     * forever. */
    if (request->file_offset < 0) {
        /* suffix: last file_length bytes */
        int64_t n = request->file_length;
        if (n > request->file_real_length) {
            n = request->file_real_length;
        }
        request->file_offset = request->file_real_length - n;
        request->file_length = n;
    } else if (request->file_length < 0) {
        /* open-ended: from file_offset to EOF */
        request->file_length = request->file_real_length - request->file_offset;
    } else if (request->file_length == 0) {
        /* whole object */
        request->file_offset = 0;
        request->file_length = request->file_real_length;
    } else {
        /* closed range: clamp to EOF */
        if (request->file_offset > request->file_real_length) {
            request->file_offset = request->file_real_length;
        }
        if (request->file_offset + request->file_length > request->file_real_length) {
            request->file_length = request->file_real_length - request->file_offset;
        }
    }

    if (request->file_length < 0) {
        request->file_length = 0;
    }

    request->file_left       = request->file_length;
    request->file_cur_offset = request->file_offset;

    chimera_s3_abort_if(!(attr->va_set_mask & CHIMERA_VFS_ATTR_FH), "put lookup callback: no fh");

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }

    if (evpl_http_request_type(request->http_request) == EVPL_HTTP_REQUEST_TYPE_HEAD) {
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
    } else {
        chimera_vfs_open_fh(thread->vfs, &thread->shared->cred, NULL,
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

    chimera_vfs_lookup(thread->vfs, &thread->shared->cred, NULL,
                       request->bucket_fh,
                       request->bucket_fhlen,
                       request->path,
                       request->path_len,
                       CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
                       CHIMERA_VFS_LOOKUP_FOLLOW,
                       chimera_s3_get_lookup_callback,
                       request);
} /* chimera_s3_get */