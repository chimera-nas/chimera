// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

static void
chimera_lock_complete(
    enum chimera_vfs_error error_code,
    uint32_t               conflict_type,
    uint64_t               conflict_offset,
    uint64_t               conflict_length,
    pid_t                  conflict_pid,
    void                  *private_data)
{
    struct chimera_client_request *request        = private_data;
    struct chimera_client_thread  *client_thread  = request->thread;
    chimera_lock_callback_t        callback       = request->lock.callback;
    void                          *callback_arg   = request->lock.private_data;
    int                            heap_allocated = request->heap_allocated;

    if (heap_allocated) {
        chimera_client_request_free(client_thread, request);
    } else {
        request->lock.r_conflict_type   = conflict_type;
        request->lock.r_conflict_offset = conflict_offset;
        request->lock.r_conflict_length = conflict_length;
        request->lock.r_conflict_pid    = conflict_pid;
    }

    callback(client_thread, error_code,
             conflict_type, conflict_offset, conflict_length, conflict_pid,
             callback_arg);
} /* chimera_lock_complete */

static inline void
chimera_dispatch_lock(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_lock(
        thread->vfs_thread,
        &thread->client->cred,
        request->lock.handle,
        request->lock.whence,
        request->lock.offset,
        request->lock.length,
        request->lock.lock_type,
        request->lock.flags,
        chimera_lock_complete,
        request);
} /* chimera_dispatch_lock */
