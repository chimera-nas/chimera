// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>

#include "posix_internal.h"
#include "../client/client_copy_range.h"

struct chimera_posix_copy_range_state {
    struct chimera_posix_completion comp;
    uint64_t                        bytes_copied;
};

static void
chimera_posix_copy_file_range_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    uint64_t                      bytes_copied,
    void                         *private_data)
{
    struct chimera_posix_copy_range_state *st = private_data;

    st->bytes_copied = bytes_copied;
    chimera_posix_complete(&st->comp, status);
} /* chimera_posix_copy_file_range_callback */

static void
chimera_posix_copy_file_range_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_copy_range(thread, request);
} /* chimera_posix_copy_file_range_exec */

SYMBOL_EXPORT ssize_t
chimera_posix_copy_file_range(
    int          fd_in,
    off_t       *off_in,
    int          fd_out,
    off_t       *off_out,
    size_t       len,
    unsigned int flags)
{
    struct chimera_posix_client          *posix  = chimera_posix_get_global();
    struct chimera_posix_worker          *worker = chimera_posix_choose_worker(posix);
    struct chimera_posix_fd_entry        *in_entry;
    struct chimera_posix_fd_entry        *out_entry;
    struct chimera_client_request         req;
    struct chimera_posix_copy_range_state st;
    off_t                                 src_off, dst_off;

    if (flags != 0) {
        errno = EINVAL;
        return -1;
    }

    if (len == 0) {
        return 0;
    }

    in_entry = chimera_posix_fd_acquire(posix, fd_in, 0);
    if (!in_entry) {
        return -1;
    }

    out_entry = chimera_posix_fd_acquire(posix, fd_out, 0);
    if (!out_entry) {
        chimera_posix_fd_release(in_entry, 0);
        return -1;
    }

    src_off = off_in ? *off_in : (off_t) in_entry->offset;
    dst_off = off_out ? *off_out : (off_t) out_entry->offset;

    chimera_posix_completion_init(&st.comp, &req);
    st.bytes_copied = 0;

    req.opcode                  = CHIMERA_CLIENT_OP_COPY_RANGE;
    req.copy_range.src_handle   = in_entry->handle;
    req.copy_range.dst_handle   = out_entry->handle;
    req.copy_range.src_offset   = (uint64_t) src_off;
    req.copy_range.dst_offset   = (uint64_t) dst_off;
    req.copy_range.length       = len;
    req.copy_range.r_length     = 0;
    req.copy_range.callback     = chimera_posix_copy_file_range_callback;
    req.copy_range.private_data = &st;

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_copy_file_range_exec);

    int err = chimera_posix_wait(&st.comp);

    if (!err) {
        if (off_in) {
            *off_in = src_off + (off_t) st.bytes_copied;
        } else {
            in_entry->offset = (uint64_t) (src_off + (off_t) st.bytes_copied);
        }
        if (off_out) {
            *off_out = dst_off + (off_t) st.bytes_copied;
        } else {
            out_entry->offset = (uint64_t) (dst_off + (off_t) st.bytes_copied);
        }
    }

    chimera_posix_fd_release(out_entry, 0);
    chimera_posix_fd_release(in_entry, 0);
    chimera_posix_completion_destroy(&st.comp);

    if (err) {
        errno = err;
        return -1;
    }

    return (ssize_t) st.bytes_copied;
} /* chimera_posix_copy_file_range */
