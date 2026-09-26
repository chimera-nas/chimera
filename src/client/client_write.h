// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <string.h>

#include "client_internal.h"

/* Completion callback for chimera_writev (iovec variant) */
static void
chimera_writev_complete(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_client_request *request       = private_data;
    struct chimera_client_thread  *client_thread = request->thread;
    struct evpl                   *evpl          = client_thread->vfs_thread->evpl;
    chimera_write_callback_t       callback      = request->writev.callback;
    void                          *callback_arg  = request->writev.private_data;

    if (request->writev.niov > 0 && request->writev.iov[0].data != NULL) {
        evpl_iovecs_release(evpl, request->writev.iov, request->writev.niov);
    }

    chimera_client_request_free(client_thread, request);

    callback(client_thread, error_code, callback_arg);
} /* chimera_writev_complete */

/* Completion callback for chimera_writerv (evpl_iovec variant) */
static void
chimera_writerv_complete(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_client_request *request       = private_data;
    struct chimera_client_thread  *client_thread = request->thread;
    struct evpl                   *evpl          = client_thread->vfs_thread->evpl;
    chimera_write_callback_t       callback      = request->writerv.callback;
    void                          *callback_arg  = request->writerv.private_data;

    if (request->writerv.niov > 0 && request->writerv.iov[0].data != NULL) {
        evpl_iovecs_release(evpl, request->writerv.iov, request->writerv.niov);
    }

    chimera_client_request_free(client_thread, request);

    callback(client_thread, error_code, callback_arg);
} /* chimera_writerv_complete */

static void
chimera_write_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_client_request *request       = private_data;
    struct chimera_client_thread  *client_thread = request->thread;
    struct evpl                   *evpl          = client_thread->vfs_thread->evpl;
    chimera_write_callback_t       callback      = request->write.callback;
    void                          *callback_arg  = request->write.private_data;
    enum chimera_vfs_error         status        = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);

    if (request->write.niov > 0 && request->write.iov[0].data != NULL) {
        evpl_iovecs_release(evpl, request->write.iov, request->write.niov);
    }

    chimera_client_request_free(client_thread, request);

    callback(client_thread, status, callback_arg);
} /* chimera_write_sequence_complete */

static inline void
chimera_dispatch_write(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct evpl *evpl   = thread->vfs_thread->evpl;
    uint32_t     length = request->write.length;

    int          niov = evpl_iovec_alloc(evpl, length, 1, CHIMERA_CLIENT_IOV_MAX,
                                         0, request->write.iov);

    if (niov < 0) {
        chimera_write_callback_t callback     = request->write.callback;
        void                    *callback_arg = request->write.private_data;

        request->write.niov = 0;
        chimera_client_request_free(thread, request);
        callback(thread, CHIMERA_VFS_EIO, callback_arg);
        return;
    }

    request->write.niov = niov;

    /* Copy from source buffer to evpl_iovec */
    size_t      copied = 0;
    const char *p      = request->write.buf;

    for (int i = 0; copied < length; i++) {
        size_t chunk = request->write.iov[i].length;

        if (chunk > length - copied) {
            chunk = length - copied;
        }

        memcpy(request->write.iov[i].data, p + copied, chunk);
        copied += chunk;
    }

    request->compound = chimera_vfs_compound_alloc(thread->vfs_thread,
                                                   chimera_client_req_cred(request));

    /* Handle and payload are both the request's, borrowed for the sequence;
     * the payload is released on compound completion. */
    chimera_vfs_compound_add_puthandle(request->compound,
                                       request->write.handle,
                                       CHIMERA_VFS_OPEN_INFERRED);
    /* The handle stays on the op too -- see chimera_dispatch_read. */
    chimera_vfs_compound_add_write(request->compound,
                                   request->write.handle,
                                   request->write.offset,
                                   request->write.length,
                                   1,
                                   request->write.iov, niov,
                                   NULL);

    chimera_frontend_compound_submit(request->compound,
                                     chimera_write_sequence_complete, request);
} /* chimera_dispatch_write */

static void
chimera_writev_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    enum chimera_vfs_error status = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);
    chimera_writev_complete(status, 0, 0, NULL, NULL, private_data);
} // chimera_writev_sequence_complete

static void
chimera_writerv_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    enum chimera_vfs_error status = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);
    chimera_writerv_complete(status, 0, 0, NULL, NULL, private_data);
} // chimera_writerv_sequence_complete

/* Dispatch for chimera_writev - allocate evpl_iovec and copy from iovec */
static inline void
chimera_dispatch_writev(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct evpl        *evpl       = thread->vfs_thread->evpl;
    uint32_t            length     = request->writev.length;
    const struct iovec *src_iov    = request->writev.src_iov;
    int                 src_iovcnt = request->writev.src_iovcnt;

    int                 niov = evpl_iovec_alloc(evpl, length, 1, CHIMERA_CLIENT_IOV_MAX,
                                                0, request->writev.iov);

    if (niov < 0) {
        request->writev.niov = 0;
        chimera_writev_complete(CHIMERA_VFS_EIO, 0, 0,
                                NULL, NULL, request);
        return;
    }

    request->writev.niov = niov;

    /* Copy from source iovecs to evpl_iovec */
    int    dst_idx = 0;
    size_t dst_off = 0;

    for (int src_idx = 0; src_idx < src_iovcnt && dst_idx < niov; src_idx++) {
        size_t src_off = 0;

        while (src_off < src_iov[src_idx].iov_len && dst_idx < niov) {
            size_t src_avail = src_iov[src_idx].iov_len - src_off;
            size_t dst_avail = request->writev.iov[dst_idx].length - dst_off;
            size_t chunk     = src_avail < dst_avail ? src_avail : dst_avail;

            memcpy((char *) request->writev.iov[dst_idx].data + dst_off,
                   (char *) src_iov[src_idx].iov_base + src_off,
                   chunk);

            src_off += chunk;
            dst_off += chunk;

            if (dst_off >= request->writev.iov[dst_idx].length) {
                dst_idx++;
                dst_off = 0;
            }
        }
    }

    request->compound = chimera_vfs_compound_alloc(thread->vfs_thread,
                                                   chimera_client_req_cred(request));
    /* Input references stay owned by this request across rejected attempts. */
    chimera_vfs_compound_add_write(request->compound, request->writev.handle,
                                   request->writev.offset, request->writev.length, 1,
                                   request->writev.iov, request->writev.niov, NULL);
    chimera_frontend_compound_submit(request->compound,
                                     chimera_writev_sequence_complete, request);
} /* chimera_dispatch_writev */

/* Dispatch for chimera_writerv - evpl_iovec already provided */
static inline void
chimera_dispatch_writerv(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    request->compound = chimera_vfs_compound_alloc(thread->vfs_thread,
                                                   chimera_client_req_cred(request));
    /* Input references stay owned by this request across rejected attempts. */
    chimera_vfs_compound_add_write(request->compound, request->writerv.handle,
                                   request->writerv.offset, request->writerv.length, 1,
                                   request->writerv.iov, request->writerv.niov, NULL);
    chimera_frontend_compound_submit(request->compound,
                                     chimera_writerv_sequence_complete, request);
} /* chimera_dispatch_writerv */
