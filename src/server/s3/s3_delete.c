// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "s3_compound.h"

/* A trailing-slash key denotes the adapter's empty leaf, not its parent
 * directory. Missing markers are idempotent and never remove descendants. */
static void
chimera_s3_delete_marker_done(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    if (*status == CHIMERA_VFS_ENOENT || *status == CHIMERA_VFS_ENOTDIR) {
        *status = CHIMERA_VFS_OK;
    }
} /* chimera_s3_delete_marker_done */

static void
chimera_s3_delete_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{

    struct chimera_s3_request *request = private_data;
    enum chimera_vfs_error     status  = chimera_vfs_compound_status(compound);

    request->status = CHIMERA_S3_STATUS_NO_CONTENT;
    if (status != CHIMERA_VFS_OK) {
        request->status = chimera_s3_compound_error(compound, request, CHIMERA_S3_STATUS_NO_SUCH_KEY);
        if (request->status == CHIMERA_S3_STATUS_NO_SUCH_KEY &&
            (status == CHIMERA_VFS_ENOENT || status == CHIMERA_VFS_ENOTDIR)) {
            request->status = CHIMERA_S3_STATUS_NO_CONTENT;
        }
    }
    chimera_vfs_compound_free(compound);
    request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(request->thread->evpl, request);
    }
    chimera_s3_request_drop(private_data);
} /* chimera_s3_delete_complete */

void
chimera_s3_delete(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    struct chimera_vfs_compound *compound = chimera_s3_compound_alloc(request);

    if (request->path_len && request->path[request->path_len - 1] == '/') {
        int index = chimera_vfs_compound_add_remove_at_path(compound,
                                                            request->path, request->path_len - 1, "", 0,
                                                            CHIMERA_VFS_REMOVE_ISNOTDIR);
        chimera_vfs_compound_set_op_callbacks(compound, index, NULL,
                                              chimera_s3_delete_marker_done, NULL);
    } else {
        chimera_vfs_compound_add_remove_path(compound, request->path,
                                             request->path_len, 0);
    }
    chimera_s3_request_get(request);
    chimera_frontend_compound_submit(compound, chimera_s3_delete_complete, request);
} /* chimera_s3_delete */
