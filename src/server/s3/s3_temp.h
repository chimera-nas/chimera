// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "s3_compound.h"

/* A failed upload may leave a named scratch file on backends without
 * CREATE_UNLINKED. Cleanup outlives the HTTP request, so its credentials and
 * operation inputs belong to this independent compound. It is best effort:
 * rollback or a successful rename may already have removed the scratch name. */
struct chimera_s3_temp_cleanup {
    struct chimera_vfs_cred cred;
};

static void
chimera_s3_temp_cleanup_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    chimera_vfs_compound_free(compound);
    free(private_data);
} // chimera_s3_temp_cleanup_complete

static inline void
chimera_s3_remove_temp(
    struct chimera_s3_request *request,
    const char                *directory,
    int                        directory_len,
    const char                *name,
    int                        name_len)
{
    struct chimera_s3_temp_cleanup  *ctx;
    struct chimera_vfs_compound     *compound;
    struct chimera_server_s3_shared *shared = request->thread->shared;

    if (name_len <= 0) {
        return;
    }
    ctx = calloc(1, sizeof(*ctx));
    if (!ctx) {
        return;
    }
    ctx->cred = request->cred;
    compound  = chimera_vfs_compound_alloc(request->thread->vfs, &ctx->cred);
    if (request->bucket_path) {
        chimera_vfs_compound_add_putfh(compound, shared->root_fh, shared->root_fh_len);
        chimera_vfs_compound_add_lookup_path(compound, request->bucket_path,
                                             strlen(request->bucket_path),
                                             CHIMERA_VFS_ATTR_FH, CHIMERA_VFS_LOOKUP_FOLLOW);
    } else {
        chimera_vfs_compound_add_putfh(compound, request->bucket_fh, request->bucket_fhlen);
    }
    chimera_vfs_compound_add_lookup_path(compound, directory, directory_len,
                                         CHIMERA_VFS_ATTR_FH, CHIMERA_VFS_LOOKUP_FOLLOW);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY
                                          , 0);
    chimera_vfs_compound_add_remove(compound, name, name_len, 0, 0, 0);
    chimera_frontend_compound_submit(compound, chimera_s3_temp_cleanup_complete, ctx);
} // chimera_s3_remove_temp
