// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: Unlicense

/* Test-only preload at the executor's internal READ entrance. Fragment the
* backend reply before the compound stores or fingerprints its result. This
* deliberately exercises backend output; frontend callbacks remain pure. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif /* ifndef _GNU_SOURCE */
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#undef NDEBUG
#include <assert.h>

#include "vfs/vfs_compound.h"
#include "vfs/vfs_internal_procs.h"

struct vector_read {
    struct evpl                *evpl;
    struct evpl_iovec          *iov;
    int                         capacity;
    uint32_t                    count;
    chimera_vfs_read_callback_t callback;
    void                       *private_data;
};

static void
fragmented_read(
    enum chimera_vfs_error    error,
    uint32_t                  count,
    uint32_t                  eof,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct vector_read         *read     = private_data;
    chimera_vfs_read_callback_t callback = read->callback;
    void                       *caller   = read->private_data;

    if (error == CHIMERA_VFS_OK && count == read->count) {
        struct evpl_iovec source;
        unsigned          pieces = (count + 3) / 4;

        assert(niov == 1 && read->capacity >= (int) pieces);
        evpl_iovec_move(&source, &iov[0]);
        for (unsigned i = 0; i < pieces; i++) {
            unsigned length = count - 4 * i;

            evpl_iovec_clone_segment(&read->iov[i], &source, 4 * i, length < 4 ? length : 4);
        }
        evpl_iovec_release(read->evpl, &source);
        iov  = read->iov;
        niov = (int) pieces;
        assert(fprintf(stdout, "NFS4_VECTOR_FRAGMENT bytes=%u pieces=%u\n", count, pieces) > 0);
        assert(fflush(stdout) == 0);
    }
    free(read);
    callback(error, count, eof, iov, niov, attr, caller);
} /* fragmented_read */

__attribute__((visibility("default"))) void
chimera_vfs_read_view(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    uint64_t                          offset,
    uint32_t                          count,
    struct evpl_iovec                *iov,
    int                               niov,
    uint64_t                          attr_mask,
    const struct chimera_vfs_io_view *view,
    chimera_vfs_read_callback_t       callback,
    void                             *private_data)
{
    typedef void (*read_fn)(
        struct chimera_vfs_thread *,
        const struct chimera_vfs_cred *,
        struct chimera_vfs_open_handle *,
        uint64_t,
        uint32_t,
        struct evpl_iovec *,
        int,
        uint64_t,
        const struct chimera_vfs_io_view *,
        chimera_vfs_read_callback_t,
        void *);
    read_fn next = (read_fn) dlsym(RTLD_NEXT, "chimera_vfs_read_view");

    assert(next);
    if (count > 0 && count <= 1024) {
        struct vector_read *read = malloc(sizeof(*read));

        assert(read);
        *read = (struct vector_read) {
            .evpl     = thread->evpl, .iov = iov, .capacity = niov, .count = count,
            .callback = callback, .private_data = private_data,
        };
        next(thread, cred, handle, offset, count, iov, niov, attr_mask,
             view, fragmented_read, read);
    } else {
        next(thread, cred, handle, offset, count, iov, niov, attr_mask,
             view, callback, private_data);
    }
} /* chimera_vfs_read_view */
