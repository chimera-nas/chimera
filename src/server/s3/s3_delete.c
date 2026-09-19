// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <time.h>
#include "vfs/vfs.h"
#include "vfs/vfs_compound.h"
#include "s3_internal.h"

/*
 * DeleteObject is idempotent (S3 API Reference, DeleteObject): deleting a key
 * that is not there succeeds with 204, and so does deleting one whose prefix
 * directory is not there.  Everything else keeps the status it earned.
 */
static enum chimera_s3_status
chimera_s3_delete_status(enum chimera_vfs_error error_code)
{
    if (error_code == CHIMERA_VFS_ENOENT || error_code == CHIMERA_VFS_ENOTDIR) {
        return CHIMERA_S3_STATUS_NO_CONTENT;
    }
    return chimera_s3_status_from_vfs(error_code, CHIMERA_S3_STATUS_NO_SUCH_KEY);
} /* chimera_s3_delete_status */

/*
 * PUTFH(bucket) -> LOOKUP_PATH(prefix directory) -> REMOVE(name).  Which op
 * failed says what the failure means: a prefix that is not there, or not a
 * directory, is a key that is not there (204); the remove itself reports a
 * missing name the same way and anything else as the failure it is.
 */
static void
chimera_s3_delete_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct evpl                     *evpl    = thread->evpl;
    enum chimera_vfs_error           error_code;
    uint32_t                         completed;

    error_code = chimera_vfs_compound_status(compound);
    completed  = chimera_vfs_compound_num_completed(compound);

    chimera_vfs_compound_free(compound);

    if (error_code == CHIMERA_VFS_OK) {
        /* 204 No Content for a key it removed, not 200. */
        request->status = CHIMERA_S3_STATUS_NO_CONTENT;
    } else if (completed < 3) {
        /* The prefix directory could not be resolved or opened. */
        request->status = chimera_s3_delete_status(error_code);
    } else if (error_code == CHIMERA_VFS_ENOENT) {
        /* DeleteObject is IDEMPOTENT (S3 API Reference, DeleteObject): a key
         * that was never there is a success, not a 404.  Only the not-found
         * errors take that path; a permission or I/O failure is still a
         * failure. */
        request->status = CHIMERA_S3_STATUS_NO_CONTENT;
    } else {
        request->status = chimera_s3_status_from_vfs(error_code, CHIMERA_S3_STATUS_NO_SUCH_KEY);
    }

    request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }
} /* chimera_s3_delete_sequence_complete */

void
chimera_s3_delete(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    struct chimera_vfs_compound *compound;
    const char                  *slash;
    const char                  *dirpath = request->path;
    int                          dirpathlen;

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

    request->set_attr.va_req_mask = 0;
    request->set_attr.va_set_mask = 0;

    compound = chimera_vfs_compound_alloc(thread->vfs, &request->cred);

    chimera_vfs_compound_add_putfh(compound, request->bucket_fh,
                                   request->bucket_fhlen);
    chimera_vfs_compound_add_lookup_path(compound, dirpath, dirpathlen,
                                         CHIMERA_VFS_ATTR_FH,
                                         CHIMERA_VFS_LOOKUP_FOLLOW);
    chimera_vfs_compound_add_remove(compound, request->name, request->name_len,
                                    0, 0, 0);

    chimera_s3_request_get(request);

    chimera_vfs_compound_submit(compound, chimera_s3_delete_sequence_complete,
                                request);
} /* chimera_s3_delete */
