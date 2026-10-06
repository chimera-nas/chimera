// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once
#include "client_internal.h"

static void
chimera_readdir_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_client_request *request  = private_data;
    struct chimera_client_thread  *thread   = request->thread;
    chimera_readdir_callback_t     callback = request->readdir.callback;
    chimera_readdir_complete_t     complete = request->readdir.complete;
    void                          *arg      = request->readdir.private_data;
    enum chimera_vfs_error         status   = chimera_vfs_compound_status(compound);
    uint64_t                       cookie   = request->readdir.cookie;
    uint32_t                       eof      = 0;

    if (status == CHIMERA_VFS_OK) {
        const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound,
                                                                           chimera_vfs_compound_num_ops(compound) - 1);
        uint32_t                              delivered = 0;
        for (uint32_t i = 0; i < op->num_entries; i++) {
            const struct chimera_vfs_compound_dirent *entry = &op->entries[i];
            struct chimera_dirent                     dirent;
            dirent.ino     = entry->inum;
            dirent.cookie  = entry->cookie;
            dirent.namelen = entry->name_len < 255 ? entry->name_len : 255;
            memcpy(dirent.name, entry->name, dirent.namelen);
            dirent.name[dirent.namelen] = 0;
            /* Callback observes accepted entries only. A nonzero return means
             * this entry was delivered; resume after it, not after the staged
             * suffix that the application has never seen. */
            cookie = entry->cookie;
            delivered++;
            if (callback(thread, &dirent, arg)) {
                break;
            }
        }
        eof = delivered == op->num_entries && op->eof;
        if (!op->num_entries) {
            cookie = op->r_cookie;
        }
    }
    chimera_vfs_compound_free(compound);
    chimera_client_request_free(thread, request);
    complete(thread, status, cookie, eof, arg);
} // chimera_readdir_sequence_complete

static inline void
chimera_dispatch_readdir(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    request->compound = chimera_vfs_compound_alloc(thread->vfs_thread,
                                                   chimera_client_req_cred(request));
    chimera_vfs_compound_add_puthandle(request->compound, request->readdir.handle,
                                       CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY);
    int index = chimera_vfs_compound_add_readdir(request->compound, request->readdir.cookie, 0, 0, 0,
                                                 CHIMERA_VFS_COMPOUND_READDIR_MAX_ENTRIES, 0, 0);
    if (index >= 0) {
        chimera_vfs_compound_op_set_handle(request->compound, index, request->readdir.handle);
        chimera_vfs_compound_op_args(request->compound, index)->readdir_flags =
            CHIMERA_VFS_READDIR_EMIT_DOT;
    }
    chimera_frontend_compound_submit(request->compound, chimera_readdir_sequence_complete, request);
} // chimera_dispatch_readdir
