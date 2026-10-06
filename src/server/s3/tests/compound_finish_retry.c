// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: Unlicense

/* Wire fault injection without backend rollback. Read-only attempts may be
 * rejected at finish. Mutating attempts are vetoed before their first mutation
 * and then rejected at finish, retaining input payload and accepted progress. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif /* ifndef _GNU_SOURCE */
#include <dlfcn.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#undef NDEBUG
#include <assert.h>
#include "vfs/vfs_compound.h"

struct attempt {
    chimera_vfs_compound_callback_t    callback;
    void                              *private_data;
    unsigned int                       seen, rejected, vetoed, veto_index;
    int                                target, veto_mutations, blocked;
    chimera_vfs_compound_op_callback_t first_prepare;
    void                              *first_private;
};

static int
op_read_only(const struct chimera_vfs_compound_op *op)
{
    switch (op->type) {
        case CHIMERA_VFS_COMPOUND_OP_OPEN:
        case CHIMERA_VFS_COMPOUND_OP_OPEN_CURRENT:
        case CHIMERA_VFS_COMPOUND_OP_OPEN_PATH:
            if (op->open_flags & (CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_TRUNCATE) ||
                op->set_attr.va_set_mask) {
                return 0;
            }
            break;
        case CHIMERA_VFS_COMPOUND_OP_PUTFH:
        case CHIMERA_VFS_COMPOUND_OP_PUTHANDLE:
        case CHIMERA_VFS_COMPOUND_OP_GETFH:
        case CHIMERA_VFS_COMPOUND_OP_GETHANDLE:
        case CHIMERA_VFS_COMPOUND_OP_LOOKUP:
        case CHIMERA_VFS_COMPOUND_OP_LOOKUP_PATH:
        case CHIMERA_VFS_COMPOUND_OP_GETATTR:
        case CHIMERA_VFS_COMPOUND_OP_GETXATTR:
        case CHIMERA_VFS_COMPOUND_OP_LISTXATTRS:
        case CHIMERA_VFS_COMPOUND_OP_READ:
        case CHIMERA_VFS_COMPOUND_OP_READDIR:
        case CHIMERA_VFS_COMPOUND_OP_FIND:
            break;
        default:
            return 0;
    } /* switch */
    return 1;
} /* op_read_only */

static int
read_only(struct chimera_vfs_compound *compound)
{
    if (chimera_vfs_compound_execution_status(compound) != CHIMERA_VFS_OK) {
        return 0;
    }
    for (uint32_t i = 0; i < chimera_vfs_compound_num_ops(compound); i++) {
        if (!op_read_only(chimera_vfs_compound_op(compound, i))) {
            return 0;
        }
    }
    return 1;
} /* read_only */

static void
first_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct attempt *ctx = private_data;

    if (ctx->first_prepare) {
        ctx->first_prepare(compound, index, status, ctx->first_private);
    }
    if (*status == CHIMERA_VFS_OK && ctx->seen < (unsigned int) ctx->target) {
        ctx->blocked    = 1;
        ctx->veto_index = index;
        *status         = CHIMERA_VFS_EAGAIN;
    }
} /* first_prepare */

static void
gate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct attempt *ctx = private_data;

    if (*status || ctx->seen >= (unsigned int) ctx->target ||
        index + 1 >= chimera_vfs_compound_num_ops(compound) ||
        op_read_only(chimera_vfs_compound_op(compound, index + 1))) {
        return;
    }
    /* A dynamic suffix may have appeared in this operation's callback. Stop
     * before its first mutation, only if the executed prefix was read-only. */
    for (uint32_t i = 0; i <= index; i++) {
        if (!op_read_only(chimera_vfs_compound_op(compound, i))) {
            return;
        }
    }
    ctx->blocked    = 1;
    ctx->veto_index = index + 1;
    *status         = CHIMERA_VFS_EAGAIN;
} /* gate */

static void
finish(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct attempt        *ctx    = private_data;
    enum chimera_vfs_error status = CHIMERA_VFS_OK;

    ctx->seen++;
    if (ctx->blocked) {
        assert(chimera_vfs_compound_execution_status(compound) == CHIMERA_VFS_EAGAIN);
        for (uint32_t i = 0; i < ctx->veto_index; i++) {
            assert(op_read_only(chimera_vfs_compound_op(compound, i)));
        }
        ctx->vetoed++;
    }
    if ((ctx->blocked || read_only(compound)) && ctx->seen <= (unsigned int) ctx->target) {
        ctx->rejected++;
        status = CHIMERA_VFS_EAGAIN;
    }
    ctx->blocked = 0;
    chimera_vfs_compound_finish_result(compound, status);
} /* finish */

static void
complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct attempt                 *ctx      = private_data;
    chimera_vfs_compound_callback_t callback = ctx->callback;
    void                           *arg      = ctx->private_data;

    if (chimera_vfs_compound_finish_status(compound) != CHIMERA_VFS_EAGAIN || ctx->seen == 9) {
        if (ctx->rejected) {
            const char *path = getenv("CHIMERA_S3_FINISH_LOG");
            char        line[128];
            int         n = snprintf(line, sizeof(line), "%u %u %d %u\n", ctx->seen, ctx->rejected,
                                     chimera_vfs_compound_finish_status(compound), ctx->vetoed);
            int         fd = open(path, O_WRONLY | O_APPEND | O_CREAT | O_CLOEXEC, 0600);
            assert(fd >= 0);
            assert(write(fd, line, n) == n);
            close(fd);
        }
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
    submit_fn       next = (submit_fn) dlsym(RTLD_NEXT, "chimera_vfs_compound_submit");
    struct attempt *ctx  = calloc(1, sizeof(*ctx));
    assert(next && ctx);
    ctx->callback     = callback;
    ctx->private_data = private_data;
    const char     *count = getenv("CHIMERA_S3_FINISH_REJECTIONS");
    ctx->target         = count ? atoi(count) : 2;
    ctx->veto_mutations = getenv("CHIMERA_S3_RETRY_VETO_MUTATIONS") != NULL;
    if (ctx->veto_mutations) {
        chimera_vfs_compound_set_gate(compound, gate, ctx);
        if (chimera_vfs_compound_num_ops(compound)) {
            const struct chimera_vfs_compound_op *first = chimera_vfs_compound_op(compound, 0);
            if (!op_read_only(first)) {
                ctx->first_prepare = first->prepare;
                ctx->first_private = first->prepare_private;
                chimera_vfs_compound_set_op_prepare(compound, 0, first_prepare, ctx);
            }
        }
    }
    chimera_vfs_compound_set_finish_handler(compound, finish, ctx);
    next(compound, complete, ctx);
} /* chimera_vfs_compound_submit */
