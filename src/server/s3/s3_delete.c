// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <time.h>
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "s3_internal.h"

static void
chimera_s3_delete_remove_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct evpl                     *evpl    = thread->evpl;

    chimera_vfs_release(thread->vfs, request->dir_handle);

    if (error_code) {
        request->status = CHIMERA_S3_STATUS_NO_SUCH_KEY;
    }

    request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }

} /* chimera_s3_delete_remove_callback */

static void
chimera_s3_delete_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;

    if (error_code) {
        request->status    = CHIMERA_S3_STATUS_NO_SUCH_KEY;
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        chimera_vfs_release(thread->vfs, request->dir_handle);
        return;
    }

    request->dir_handle = oh;

    chimera_vfs_remove(thread->vfs,
                       oh,
                       request->name,
                       request->name_len,
                       0,
                       0,
                       chimera_s3_delete_remove_callback,
                       request);

} /* chimera_s3_put_create_callback */

static void
chimera_s3_get_lookup_callback(
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

    chimera_vfs_open(thread->vfs,
                     attr->va_fh,
                     attr->va_fh_len,
                     CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
                     chimera_s3_delete_open_callback,
                     request);
}  /* chimera_s3_get_lookup_callback */

void
chimera_s3_delete(
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


    chimera_vfs_lookup_path(thread->vfs,
                            request->bucket_fh,
                            request->bucket_fhlen,
                            dirpath,
                            dirpathlen,
                            CHIMERA_VFS_ATTR_FH,
                            CHIMERA_VFS_LOOKUP_FOLLOW,
                            chimera_s3_get_lookup_callback,
                            request);
} /* chimera_s3_get */