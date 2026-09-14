// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "vfs/vfs_procs.h"

static void
chimera_write_same_complete(
    enum chimera_vfs_error    error_code,
    uint64_t                  count,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_client_request *request        = private_data;
    struct chimera_client_thread  *client_thread  = request->thread;
    chimera_write_same_callback_t  callback       = request->write_same.callback;
    void                          *callback_arg   = request->write_same.private_data;
    int                            heap_allocated = request->heap_allocated;

    (void) sync;
    (void) pre_attr;
    (void) post_attr;

    if (heap_allocated) {
        chimera_client_request_free(client_thread, request);
    }

    callback(client_thread, error_code, count, callback_arg);
} /* chimera_write_same_complete */

static void
chimera_write_same_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                status;
    uint32_t                              written = 0, committed = 0;

    status = chimera_vfs_compound_status(compound);

    if (status == CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(compound,
                                     chimera_vfs_compound_num_ops(compound) - 1);
        written   = op->written;
        committed = op->committed;
    }

    chimera_vfs_compound_free(compound);

    chimera_write_same_complete(status, written, committed, NULL, NULL,
                                private_data);
} /* chimera_write_same_sequence_complete */

static inline void
chimera_dispatch_write_same(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    request->compound = chimera_vfs_compound_alloc(thread->vfs_thread,
                                                   chimera_client_req_cred(request));

    /* Handle and pattern are both the caller's, borrowed for the sequence. */
    chimera_vfs_compound_add_puthandle(request->compound,
                                       request->write_same.handle,
                                       CHIMERA_VFS_OPEN_INFERRED);
    chimera_vfs_compound_add_write_same(request->compound,
                                        NULL,
                                        request->write_same.offset,
                                        request->write_same.block_size,
                                        request->write_same.block_count,
                                        request->write_same.pattern,
                                        request->write_same.pattern_len,
                                        request->write_same.reloff_pattern,
                                        CHIMERA_VFS_WRITE_FILESYNC,
                                        0, 0);

    chimera_vfs_compound_submit(request->compound,
                                chimera_write_same_sequence_complete, request);
} /* chimera_dispatch_write_same */
