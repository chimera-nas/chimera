// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: Unlicense

/* Test-only rejection. Reads may be rejected after execution; writes are
 * vetoed before dispatch on rejected attempts, so no backend rollback is faked. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif /* ifndef _GNU_SOURCE */
#include <dlfcn.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#undef NDEBUG
#include <assert.h>
#include "vfs/vfs_compound.h"

static atomic_int reject_next;
static atomic_int operation_error_next;
static atomic_int attempts;
static void       (*observer)(
    void *);
static void      *observer_arg;

__attribute__((visibility("default"))) void
chimera_test_reject_finishes(int count)
{
    assert(count > 0);
    assert(!atomic_exchange(&reject_next, count));
    atomic_store(&attempts, 0);
} /* chimera_test_reject_finishes */

__attribute__((visibility("default"))) void
chimera_test_operation_eagain(void)
{
    chimera_test_reject_finishes(1);
    atomic_store(&operation_error_next, 1);
} /* chimera_test_operation_eagain */

__attribute__((visibility("default"))) int
chimera_test_finish_attempts(void)
{
    return atomic_load(&attempts);
} /* chimera_test_finish_attempts */

__attribute__((visibility("default"))) void
chimera_test_finish_observer(
    void ( *fn )(void *),
    void   *arg)
{
    observer     = fn;
    observer_arg = arg;
} /* chimera_test_finish_observer */

struct rejection {
    chimera_vfs_compound_callback_t callback;
    void                           *private_data;
    unsigned int                    rejects, seen;
    int                             operation_error, write_index;
    uint64_t                        payload_hash;
};

static uint64_t
payload_hash(const struct chimera_vfs_compound_op *op)
{
    uint64_t hash = 1469598103934665603ULL;

    for (int i = 0; i < op->w_niov; i++) {
        const unsigned char *data = op->w_iov[i].data;
        for (size_t j = 0; j < op->w_iov[i].length; j++) {
            hash = (hash ^ data[j]) * 1099511628211ULL;
        }
    }
    return hash;
} /* payload_hash */

static void
prepare_write(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct rejection *ctx = private_data;

    assert(payload_hash(chimera_vfs_compound_op(compound, index)) == ctx->payload_hash);
    if (ctx->seen < ctx->rejects) {
        *status = CHIMERA_VFS_EAGAIN;
    }
} /* prepare_write */

static void
reject_finish(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct rejection      *ctx    = private_data;
    enum chimera_vfs_error status = CHIMERA_VFS_OK;

    if (observer) {
        observer(observer_arg);
    }
    if (ctx->seen++ < ctx->rejects && !ctx->operation_error) {
        status = CHIMERA_VFS_EAGAIN;
    }
    atomic_store(&attempts, ctx->seen);
    chimera_vfs_compound_finish_result(compound, status);
} /* reject_finish */

static void
complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct rejection               *ctx      = private_data;
    chimera_vfs_compound_callback_t callback = ctx->callback;
    void                           *arg      = ctx->private_data;

    /* The wrapper persists across automatic retries. The frontend's retry
     * budget is eight, so its final rejected attempt must release this fixture. */
    if (chimera_vfs_compound_finish_status(compound) != CHIMERA_VFS_EAGAIN || ctx->seen == 9) {
        free(ctx);
    }
    callback(compound, arg);
} /* complete */

__attribute__((visibility("default"))) void
chimera_vfs_compound_submit(
    struct chimera_vfs_compound    *compound,
    chimera_vfs_compound_callback_t callback,
    void                           *private_data)
{
    typedef void (*submit_fn)(
        struct chimera_vfs_compound *,
        chimera_vfs_compound_callback_t,
        void *);
    submit_fn         next  = (submit_fn) dlsym(RTLD_NEXT, "chimera_vfs_compound_submit");
    int               count = atomic_exchange(&reject_next, 0);
    assert(next);
    if (!count) {
        next(compound, callback, private_data);
        return;
    }
    struct rejection *ctx = calloc(1, sizeof(*ctx));
    assert(ctx);
    ctx->callback        = callback;
    ctx->private_data    = private_data;
    ctx->rejects         = count;
    ctx->operation_error = atomic_exchange(&operation_error_next, 0);
    ctx->write_index     = -1;
    int               operations = 0;
    for (uint32_t i = 0; i < chimera_vfs_compound_num_ops(compound); i++) {
        const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, i);
        switch (op->type) {
            case CHIMERA_VFS_COMPOUND_OP_WRITE:
                assert(ctx->write_index == -1);
                ctx->write_index  = i;
                ctx->payload_hash = payload_hash(op);
                chimera_vfs_compound_set_op_callbacks(compound, i, prepare_write, NULL, ctx);
                operations++;
                break;
            case CHIMERA_VFS_COMPOUND_OP_READ:
            case CHIMERA_VFS_COMPOUND_OP_READDIR:
                operations++;
                break;
            case CHIMERA_VFS_COMPOUND_OP_PUTHANDLE:
            case CHIMERA_VFS_COMPOUND_OP_PUTFH:
                break;
            default:
                abort();
        } /* switch */
    }
    assert(operations == 1);
    assert(!ctx->operation_error || ctx->write_index >= 0);
    chimera_vfs_compound_set_finish_handler(compound, reject_finish, ctx);
    next(compound, complete, ctx);
} /* chimera_vfs_compound_submit */
