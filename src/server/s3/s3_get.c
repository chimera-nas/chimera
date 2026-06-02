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

/* Bound on metadata-phase transaction conflict replays before failing. */
#define CHIMERA_S3_TXN_MAX_RETRIES 8

static void chimera_s3_get_begin(
    struct chimera_s3_request *request);

/* Final failure path: respond NoSuchKey. */
static void
chimera_s3_get_fail(struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread *thread = request->thread;

    request->status    = CHIMERA_S3_STATUS_NO_SUCH_KEY;
    request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(thread->evpl, request);
    }
} /* chimera_s3_get_fail */

/* EndTransaction(ABORT) completion after a wait-die conflict during the lookup:
 * replay the whole metadata phase (reusing the stable txn_ts), or give up. */
static void
chimera_s3_get_conflict_done(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_s3_request *request = private_data;

    (void) error_code;

    if (++request->txn_attempt > CHIMERA_S3_TXN_MAX_RETRIES) {
        chimera_s3_get_fail(request);
        return;
    }
    chimera_s3_get_begin(request);
} /* chimera_s3_get_conflict_done */

/* EndTransaction(ABORT) completion after a real lookup failure (NoSuchKey). */
static void
chimera_s3_get_notfound_done(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    (void) error_code;
    chimera_s3_get_fail(private_data);
} /* chimera_s3_get_notfound_done */

/* EndTransaction(COMMIT) completion: the metadata view is now consistent and
 * released.  Everything from here (headers, open, data) is autocommit -- so once
 * we emit output we never replay.  Uses the saved lookup attrs (request->txn_attr)
 * since the VFS attr pointer did not survive the async EndTransaction. */
static void
chimera_s3_get_committed(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct evpl                     *evpl    = thread->evpl;
    struct chimera_vfs_attrs        *attr    = &request->txn_attr;

    if (error_code == CHIMERA_VFS_ETXN_CONFLICT) {
        if (++request->txn_attempt > CHIMERA_S3_TXN_MAX_RETRIES) {
            chimera_s3_get_fail(request);
        } else {
            chimera_s3_get_begin(request);
        }
        return;
    }

    chimera_s3_attach_etag(request->http_request, attr);
    chimera_s3_attach_last_modified(request->http_request, attr);

    request->file_real_length = attr->va_size;

    if (request->file_length == 0) {
        request->file_length = request->file_real_length;
        request->file_left   = request->file_length;
    }

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }

    if (evpl_http_request_type(request->http_request) == EVPL_HTTP_REQUEST_TYPE_HEAD) {
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
    } else {
        /* Data phase runs autocommit (NULL txn): the object content streams to
         * the client and cannot be replayed. */
        chimera_vfs_open_fh(thread->vfs, &thread->shared->cred, NULL,
                            attr->va_fh,
                            attr->va_fh_len,
                            0,
                            chimera_s3_get_open_callback,
                            request);
    }
} /* chimera_s3_get_committed */

static void
chimera_s3_get_lookup_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;

    if (error_code == CHIMERA_VFS_ETXN_CONFLICT) {
        chimera_vfs_end_transaction(thread->vfs, &thread->shared->cred,
                                    request->txn, CHIMERA_VFS_TXN_ABORT,
                                    chimera_s3_get_conflict_done, request);
        return;
    }

    if (error_code) {
        chimera_vfs_end_transaction(thread->vfs, &thread->shared->cred,
                                    request->txn, CHIMERA_VFS_TXN_ABORT,
                                    chimera_s3_get_notfound_done, request);
        return;
    }

    chimera_s3_abort_if(!(attr->va_set_mask & CHIMERA_VFS_ATTR_FH), "get lookup callback: no fh");

    /* Preserve the lookup result across the async commit. */
    request->txn_attr = *attr;

    chimera_vfs_end_transaction(thread->vfs, &thread->shared->cred,
                                request->txn, CHIMERA_VFS_TXN_COMMIT_ASYNC,
                                chimera_s3_get_committed, request);
}  /* chimera_s3_get_lookup_callback */

static void
chimera_s3_get_began(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_transaction *txn,
    void                           *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_s3_get_fail(request);
        return;
    }

    request->txn = txn;     /* NULL for a non-transactional backend (autocommit) */

    chimera_vfs_lookup(thread->vfs, &thread->shared->cred, request->txn,
                       request->bucket_fh,
                       request->bucket_fhlen,
                       request->path,
                       request->path_len,
                       CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
                       CHIMERA_VFS_LOOKUP_FOLLOW,
                       chimera_s3_get_lookup_callback,
                       request);
} /* chimera_s3_get_began */

static void
chimera_s3_get_begin(struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread *thread = request->thread;

    chimera_vfs_begin_transaction(thread->vfs, &thread->shared->cred,
                                  request->bucket_fh, request->bucket_fhlen,
                                  CHIMERA_VFS_TXN_READ, request->txn_ts,
                                  chimera_s3_get_began, request);
} /* chimera_s3_get_begin */

void
chimera_s3_get(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    request->io_pending  = 0;
    request->txn_ts      = chimera_vfs_txn_alloc_ts(thread->vfs);
    request->txn_attempt = 0;

    chimera_s3_get_begin(request);
} /* chimera_s3_get */