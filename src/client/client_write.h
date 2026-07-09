// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <string.h>

#include "client_internal.h"
#include "client_txn.h"

/*
 * Each write variant runs as one write transaction.  The evpl_iovec staging
 * buffers are allocated ONCE in the dispatch (before BeginTransaction) and
 * reused across every conflict replay -- the txn driver re-runs *_start, which
 * re-issues the write from the same iovecs -- and are released only in the
 * reply (after the durable commit or a terminal error).
 */

/* ---- chimera_write (buffer variant) ---- */

static void
chimera_write_op_complete(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    chimera_client_txn_finish(request->thread, request, error_code);
} /* chimera_write_op_complete */

static void
chimera_write_start(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_write(thread->vfs_thread,
                      chimera_client_req_cred(request), request->txn,
                      request->write.handle,
                      request->write.offset,
                      request->write.length,
                      1,
                      0,
                      0,
                      request->write.iov,
                      request->write.niov,
                      chimera_write_op_complete,
                      request);
} /* chimera_write_start */

static void
chimera_write_reply(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct evpl             *evpl         = thread->vfs_thread->evpl;
    chimera_write_callback_t callback     = request->write.callback;
    void                    *callback_arg = request->write.private_data;
    enum chimera_vfs_error   status       = request->txn_op_status;

    if (request->write.niov > 0 && request->write.iov[0].data != NULL) {
        evpl_iovecs_release(evpl, request->write.iov, request->write.niov);
    }

    chimera_client_request_free(thread, request);

    callback(thread, status, callback_arg);
} /* chimera_write_reply */

/* Dispatch for chimera_write - allocate evpl_iovec and copy from buffer */
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

    chimera_client_txn_run(thread, request,
                           request->write.handle->fh,
                           request->write.handle->fh_len,
                           CHIMERA_VFS_TXN_WRITE,
                           chimera_write_start, chimera_write_reply);
} /* chimera_dispatch_write */

/* ---- chimera_writev (struct iovec variant) ---- */

static void
chimera_writev_op_complete(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    chimera_client_txn_finish(request->thread, request, error_code);
} /* chimera_writev_op_complete */

static void
chimera_writev_start(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_write(thread->vfs_thread,
                      chimera_client_req_cred(request), request->txn,
                      request->writev.handle,
                      request->writev.offset,
                      request->writev.length,
                      1,
                      0,
                      0,
                      request->writev.iov,
                      request->writev.niov,
                      chimera_writev_op_complete,
                      request);
} /* chimera_writev_start */

static void
chimera_writev_reply(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct evpl             *evpl         = thread->vfs_thread->evpl;
    chimera_write_callback_t callback     = request->writev.callback;
    void                    *callback_arg = request->writev.private_data;
    enum chimera_vfs_error   status       = request->txn_op_status;

    if (request->writev.niov > 0 && request->writev.iov[0].data != NULL) {
        evpl_iovecs_release(evpl, request->writev.iov, request->writev.niov);
    }

    chimera_client_request_free(thread, request);

    callback(thread, status, callback_arg);
} /* chimera_writev_reply */

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
        chimera_write_callback_t callback     = request->writev.callback;
        void                    *callback_arg = request->writev.private_data;

        request->writev.niov = 0;
        chimera_client_request_free(thread, request);
        callback(thread, CHIMERA_VFS_EIO, callback_arg);
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

    chimera_client_txn_run(thread, request,
                           request->writev.handle->fh,
                           request->writev.handle->fh_len,
                           CHIMERA_VFS_TXN_WRITE,
                           chimera_writev_start, chimera_writev_reply);
} /* chimera_dispatch_writev */

/* ---- chimera_writerv (evpl_iovec variant) ---- */

static void
chimera_writerv_op_complete(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    chimera_client_txn_finish(request->thread, request, error_code);
} /* chimera_writerv_op_complete */

static void
chimera_writerv_start(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_write(thread->vfs_thread,
                      chimera_client_req_cred(request), request->txn,
                      request->writerv.handle,
                      request->writerv.offset,
                      request->writerv.length,
                      1,
                      0,
                      0,
                      request->writerv.iov,
                      request->writerv.niov,
                      chimera_writerv_op_complete,
                      request);
} /* chimera_writerv_start */

static void
chimera_writerv_reply(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct evpl             *evpl         = thread->vfs_thread->evpl;
    chimera_write_callback_t callback     = request->writerv.callback;
    void                    *callback_arg = request->writerv.private_data;
    enum chimera_vfs_error   status       = request->txn_op_status;

    if (request->writerv.niov > 0 && request->writerv.iov[0].data != NULL) {
        evpl_iovecs_release(evpl, request->writerv.iov, request->writerv.niov);
    }

    chimera_client_request_free(thread, request);

    callback(thread, status, callback_arg);
} /* chimera_writerv_reply */

/* Dispatch for chimera_writerv - evpl_iovec already provided */
static inline void
chimera_dispatch_writerv(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_client_txn_run(thread, request,
                           request->writerv.handle->fh,
                           request->writerv.handle->fh_len,
                           CHIMERA_VFS_TXN_WRITE,
                           chimera_writerv_start, chimera_writerv_reply);
} /* chimera_dispatch_writerv */
