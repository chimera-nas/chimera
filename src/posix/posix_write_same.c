// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>

#include "posix_internal.h"
#include "../client/client_write_same.h"

struct chimera_posix_write_same_state {
    struct chimera_posix_completion comp;
    uint64_t                        bytes_written;
};

static void
chimera_posix_write_same_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    uint64_t                      bytes_written,
    void                         *private_data)
{
    struct chimera_posix_write_same_state *st = private_data;

    (void) thread;
    st->bytes_written = bytes_written;
    chimera_posix_complete(&st->comp, status);
} /* chimera_posix_write_same_callback */

static void
chimera_posix_write_same_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_write_same(thread, request);
} /* chimera_posix_write_same_exec */

SYMBOL_EXPORT ssize_t
chimera_posix_write_same(
    int         fd,
    off_t       offset,
    uint32_t    block_size,
    uint64_t    block_count,
    const void *pattern,
    uint32_t    pattern_len,
    uint32_t    reloff_pattern)
{
    struct chimera_posix_client          *posix  = chimera_posix_get_global();
    struct chimera_posix_worker          *worker = chimera_posix_choose_worker(posix);
    struct chimera_posix_fd_entry        *entry;
    struct chimera_client_request         req;
    struct chimera_posix_write_same_state st;

    entry = chimera_posix_fd_acquire(posix, fd, 0);
    if (!entry) {
        return -1;
    }

    chimera_posix_completion_init(&st.comp, &req);
    st.bytes_written = 0;

    req.opcode                    = CHIMERA_CLIENT_OP_WRITE_SAME;
    req.write_same.handle         = entry->handle;
    req.write_same.offset         = (uint64_t) offset;
    req.write_same.block_size     = block_size;
    req.write_same.block_count    = block_count;
    req.write_same.pattern        = pattern;
    req.write_same.pattern_len    = pattern_len;
    req.write_same.reloff_pattern = reloff_pattern;
    req.write_same.callback       = chimera_posix_write_same_callback;
    req.write_same.private_data   = &st;

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_write_same_exec);

    int err = chimera_posix_wait(&st.comp);

    chimera_posix_fd_release(entry, 0);
    chimera_posix_completion_destroy(&st.comp);

    if (err) {
        errno = err;
        return -1;
    }

    return (ssize_t) st.bytes_written;
} /* chimera_posix_write_same */
