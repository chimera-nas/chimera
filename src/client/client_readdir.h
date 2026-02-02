// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

static int
chimera_readdir_entry_callback(
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg)
{
    struct chimera_client_request *request       = arg;
    struct chimera_client_thread  *client_thread = request->thread;
    chimera_readdir_callback_t     callback      = request->readdir.callback;
    void                          *callback_arg  = request->readdir.private_data;
    struct chimera_dirent          dirent;

    dirent.ino     = inum;
    dirent.cookie  = cookie;
    dirent.namelen = namelen < 255 ? namelen : 255;
    memcpy(dirent.name, name, dirent.namelen);
    dirent.name[dirent.namelen] = '\0';

    return callback(client_thread, &dirent, callback_arg);
} /* chimera_readdir_entry_callback */

static void
chimera_readdir_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        cookie,
    uint64_t                        verifier,
    uint32_t                        eof,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data)
{
    struct chimera_client_request *request       = private_data;
    struct chimera_client_thread  *client_thread = request->thread;
    chimera_readdir_complete_t     complete      = request->readdir.complete;
    void                          *callback_arg  = request->readdir.private_data;

    chimera_client_request_free(client_thread, request);

    complete(client_thread, error_code, cookie, eof, callback_arg);
} /* chimera_readdir_complete */ /* chimera_readdir_complete */

static inline void
chimera_dispatch_readdir(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_readdir(thread->vfs_thread,
                        &thread->client->cred,
                        request->readdir.handle,
                        0,  // attr_mask for entries
                        0,  // dir_attr_mask
                        request->readdir.cookie,
                        0,  // verifier
                        CHIMERA_VFS_READDIR_EMIT_DOT,
                        chimera_readdir_entry_callback,
                        chimera_readdir_complete,
                        request);
} /* chimera_dispatch_readdir */
