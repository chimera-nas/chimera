// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

static void
chimera_write_complete(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_client_request *request       = private_data;
    struct chimera_client_thread  *client_thread = request->thread;
    chimera_write_callback_t       callback      = request->write.callback;
    void                          *callback_arg  = request->write.private_data;

    chimera_client_request_free(client_thread, request);

    callback(client_thread, error_code, callback_arg);
} /* chimera_write_complete */

static inline void
chimera_dispatch_write(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_write(thread->vfs_thread,
                      request->write.handle,
                      request->write.offset,
                      request->write.length,
                      1,
                      0,
                      0,
                      request->write.iov,
                      request->write.niov,
                      chimera_write_complete,
                      request);
} /* chimera_dispatch_write */
