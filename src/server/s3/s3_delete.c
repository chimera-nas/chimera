// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <time.h>
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "s3_internal.h"
#include "s3_compound.h"

/*
 * DeleteObject: parent lookup -> parent open -> remove_at, run as one
 * RETRYABLE WRITE compound via the shared driver (s3_compound.h).  The whole
 * chain is genuinely replayable: its inputs (the key path) live on the
 * request and nothing escapes before the commit -- the only output is the
 * 2xx/404 status, decided in the reply once the commit has settled.
 */

static void
chimera_s3_delete_remove_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    CHIMERA_S3_HOLD_REQUEST(private_data);
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;

    chimera_vfs_release(thread->vfs, request->dir_handle);
    request->dir_handle = NULL;

    if (error_code && error_code != CHIMERA_VFS_ECOMPOUND_CONFLICT &&
        error_code != CHIMERA_VFS_ECOMPOUND_EXHAUSTED) {
        request->status = CHIMERA_S3_STATUS_NO_SUCH_KEY;
    }

    chimera_s3_compound_finish(request, error_code);
} /* chimera_s3_delete_remove_callback */

static void
chimera_s3_delete_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    CHIMERA_S3_HOLD_REQUEST(private_data);
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;

    if (error_code) {
        request->status = CHIMERA_S3_STATUS_NO_SUCH_KEY;
        chimera_s3_compound_finish(request, error_code);
        return;
    }

    request->dir_handle = oh;

    chimera_s3_request_get(request);

    chimera_vfs_remove_at(thread->vfs, &thread->shared->cred, request->compound,
                          oh,
                          request->name,
                          request->name_len,
                          NULL,
                          0,
                          0,
                          0,
                          0,
                          NULL,
                          chimera_s3_delete_remove_callback,
                          request);

} /* chimera_s3_delete_open_callback */

static void
chimera_s3_delete_lookup_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    CHIMERA_S3_HOLD_REQUEST(private_data);
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;

    if (error_code) {
        request->status = CHIMERA_S3_STATUS_NO_SUCH_KEY;
        chimera_s3_compound_finish(request, error_code);
        return;
    }

    chimera_s3_abort_if(!(attr->va_set_mask & CHIMERA_VFS_ATTR_FH), "delete lookup callback: no fh");

    chimera_s3_request_get(request);

    chimera_vfs_open_fh(thread->vfs, &thread->shared->cred, request->compound,
                        attr->va_fh,
                        attr->va_fh_len,
                        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_s3_delete_open_callback,
                        request);
}  /* chimera_s3_delete_lookup_callback */

/* Compound start: run (or replay) the lookup -> open -> remove chain.  The
 * dirpath split is recomputed from request->path, which is stable across
 * replays. */
static void
chimera_s3_delete_start(struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread *thread  = request->thread;
    const char                      *dirpath = request->path;
    const char                      *slash;
    int                              dirpathlen;

    slash = rindex(request->path, '/');

    if (slash) {
        dirpathlen = slash - request->path;
    } else {
        dirpath    = "/";
        dirpathlen = 1;
    }

    chimera_s3_request_get(request);

    chimera_vfs_lookup(thread->vfs, &thread->shared->cred, request->compound,
                       request->bucket_fh,
                       request->bucket_fhlen,
                       dirpath,
                       dirpathlen,
                       CHIMERA_VFS_ATTR_FH,
                       CHIMERA_VFS_LOOKUP_FOLLOW,
                       chimera_s3_delete_lookup_callback,
                       request);
} /* chimera_s3_delete_start */

/* Compound reply: the commit (or abort) has settled; answer the request. */
static void
chimera_s3_delete_reply(struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread *thread = request->thread;
    struct evpl                     *evpl   = thread->evpl;

    if (request->compound_op_status != CHIMERA_VFS_OK) {
        chimera_s3_compound_map_error(request, request->compound_op_status);
        if (request->dir_handle) {
            chimera_vfs_release(thread->vfs, request->dir_handle);
            request->dir_handle = NULL;
        }
    }

    request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }
} /* chimera_s3_delete_reply */

void
chimera_s3_delete(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    const char *slash;

    slash = rindex(request->path, '/');

    if (slash) {
        request->name = slash + 1;

        while (*request->name == '/') {
            request->name++;
        }
    } else {
        request->name = request->path;
    }

    request->name_len = strlen(request->name);

    request->set_attr.va_req_mask = 0;
    request->set_attr.va_set_mask = 0;

    chimera_s3_compound_run(request,
                            request->bucket_fh, request->bucket_fhlen,
                            CHIMERA_VFS_COMPOUND_WRITE,
                            CHIMERA_VFS_COMPOUND_RETRYABLE,
                            chimera_s3_delete_start,
                            chimera_s3_delete_reply);
} /* chimera_s3_delete */
