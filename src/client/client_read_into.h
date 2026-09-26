// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once
#include "client_internal.h"

static void
chimera_read_into_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_client_request        *request  = private_data;
    struct chimera_client_thread         *thread   = request->thread;
    chimera_read_into_callback_t          callback = request->read_into.callback;
    void                                 *arg      = request->read_into.private_data;
    enum chimera_vfs_error                status   = chimera_vfs_compound_status(compound);
    const struct chimera_vfs_compound_op *op       = chimera_vfs_compound_op(compound,
                                                                             chimera_vfs_compound_num_ops(compound) - 1)
    ;
    uint32_t                              count = 0, eof = 0;

    if (status == CHIMERA_VFS_OK) {
        size_t capacity = 0;
        for (int i = 0; i < request->read_into.dest_niov; i++) {
            capacity += request->read_into.dest_iov[i].length;
        }
        if (op->read_len > capacity) {
            status = CHIMERA_VFS_EIO;
        } else {
            int    src = 0, dst = 0;
            size_t soff = 0, doff = 0, remaining = op->read_len;
            /* The destination is borrowed until completion, but is written
             * only after acceptance. Rejected attempts cannot expose bytes. */
            while (remaining) {
                size_t n     = op->iov[src].length - soff;
                size_t avail = request->read_into.dest_iov[dst].length - doff;
                if (n > avail) {
                    n = avail;
                }
                if (n > remaining) {
                    n = remaining;
                }
                if (n) {
                    memcpy((char *) request->read_into.dest_iov[dst].data + doff,
                           (char *) op->iov[src].data + soff, n);
                }
                remaining -= n; soff += n; doff += n;
                if (soff == op->iov[src].length) {
                    src++; soff = 0;
                }
                if (doff == request->read_into.dest_iov[dst].length) {
                    dst++; doff = 0;
                }
            }
            count = op->read_len;
            eof   = op->eof_read;
        }
    }
    request->read_into.result_count = count;
    request->read_into.result_eof   = eof;
    chimera_vfs_compound_free(compound);
    chimera_client_request_free(thread, request);
    callback(thread, status, count, eof, arg);
} // chimera_read_into_sequence_complete

static inline void
chimera_dispatch_read_into(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    request->compound = chimera_vfs_compound_alloc(thread->vfs_thread,
                                                   chimera_client_req_cred(request));
    chimera_vfs_compound_add_read(request->compound, request->read_into.handle,
                                  request->read_into.offset, request->read_into.length, request->read_into.iov,
                                  CHIMERA_CLIENT_IOV_MAX, NULL);
    chimera_frontend_compound_submit(request->compound, chimera_read_into_sequence_complete, request);
} // chimera_dispatch_read_into
