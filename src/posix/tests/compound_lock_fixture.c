// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: Unlicense
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif /* ifndef _GNU_SOURCE */
#include <dlfcn.h>
#include "common/thread.h"
#include <stdlib.h>
#include <string.h>
#undef NDEBUG
#include <assert.h>
#include "vfs/vfs_compound.h"
#include "vfs/vfs_lock.h"

/* Deterministic barrier after a blocking lock has entered the executor.
 * Tests hold a conflicting owner until after close, so this cannot be a grant. */
static evpl_mutex_t                          mutex = EVPL_MUTEX_INITIALIZER;
static evpl_cond_t                           cond  = PTHREAD_COND_INITIALIZER;
static unsigned int                          submissions;
static __thread struct chimera_vfs_compound *waiting_compound;
static unsigned int                          reject_next, finish_attempts;
static const void                           *watch_next;
static size_t                                watch_length;

__attribute__((visibility("default"))) void
chimera_test_reject_lock_finishes(
    unsigned int count,
    const void  *watch,
    size_t       length)
{
    evpl_mutex_lock(&mutex);
    assert(!reject_next);
    reject_next     = count;
    finish_attempts = 0;
    watch_next      = watch;
    watch_length    = length;
    evpl_mutex_unlock(&mutex);
} /* chimera_test_reject_lock_finishes */

__attribute__((visibility("default"))) unsigned int
chimera_test_lock_finish_attempts(void)
{
    evpl_mutex_lock(&mutex);
    unsigned int result = finish_attempts;
    evpl_mutex_unlock(&mutex);
    return result;
} /* chimera_test_lock_finish_attempts */

struct finish_rejection {
    chimera_vfs_compound_callback_t callback;
    void                           *private_data;
    unsigned int                    count, seen;
    const void                     *watch;
    void                           *snapshot;
    size_t                          length;
};

static void
reject_finish(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct finish_rejection *ctx = private_data;

    if (ctx->length) {
        assert(!memcmp(ctx->watch, ctx->snapshot, ctx->length));
    }
    ctx->seen++;
    evpl_mutex_lock(&mutex);
    finish_attempts = ctx->seen;
    evpl_mutex_unlock(&mutex);
    chimera_vfs_compound_finish_result(compound,
                                       ctx->seen <= ctx->count ? CHIMERA_VFS_EAGAIN : CHIMERA_VFS_OK);
} /* reject_finish */

static void
reject_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct finish_rejection        *ctx      = private_data;
    chimera_vfs_compound_callback_t callback = ctx->callback;
    void                           *arg      = ctx->private_data;

    if (chimera_vfs_compound_finish_status(compound) != CHIMERA_VFS_EAGAIN || ctx->seen == 9 ||
        chimera_vfs_compound_op(compound, 0)->nonretryable) {
        free(ctx->snapshot);
        free(ctx);
    }
    callback(compound, arg);
} /* reject_complete */


__attribute__((visibility("default"))) unsigned int
chimera_test_lock_submission_count(void)
{
    evpl_mutex_lock(&mutex);
    unsigned int result = submissions;
    evpl_mutex_unlock(&mutex);
    return result;
} /* chimera_test_lock_submission_count */

__attribute__((visibility("default"))) void
chimera_test_wait_lock_submission(unsigned int previous)
{
    evpl_mutex_lock(&mutex);
    while (submissions == previous) {
        evpl_cond_wait(&cond, &mutex);
    }
    evpl_mutex_unlock(&mutex);
} /* chimera_test_wait_lock_submission */

__attribute__((visibility("default"))) int
chimera_vfs_compound_add_lock_change(
    struct chimera_vfs_compound           *compound,
    struct chimera_vfs_lock_domain        *domain,
    struct chimera_vfs_open_handle        *handle,
    const struct chimera_vfs_lock_request *request)
{
    typedef int (*add_fn)(
        struct chimera_vfs_compound *,
        struct chimera_vfs_lock_domain *,
        struct chimera_vfs_open_handle *,
        const struct chimera_vfs_lock_request *);
    add_fn next = (add_fn) dlsym(RTLD_NEXT, "chimera_vfs_compound_add_lock_change");
    assert(next);
    if (request->wait && request->owner.owner_lo == 0xb2) {
        waiting_compound = compound;
    }
    return next(compound, domain, handle, request);
} /* chimera_vfs_compound_add_lock_change */

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
    submit_fn                next = (submit_fn) dlsym(RTLD_NEXT, "chimera_vfs_compound_submit");
    assert(next);
    int                      waiting = waiting_compound == compound;
    if (waiting) {
        waiting_compound = NULL;
    }
    struct finish_rejection *reject = NULL;
    evpl_mutex_lock(&mutex);
    if (reject_next) {
        assert(chimera_vfs_compound_num_ops(compound) == 1);
        int type = chimera_vfs_compound_op(compound, 0)->type;
        assert(type == CHIMERA_VFS_COMPOUND_OP_LOCK_TEST || type == CHIMERA_VFS_COMPOUND_OP_LOCK_CHANGE ||
               type == CHIMERA_VFS_COMPOUND_OP_LOCK_RELEASE_OWNER);
        reject = calloc(1, sizeof(*reject));
        assert(reject);
        reject->count  = reject_next;
        reject->watch  = watch_next;
        reject->length = watch_length;
        if (watch_length) {
            reject->snapshot = malloc(watch_length);
            assert(reject->snapshot);
            memcpy(reject->snapshot, watch_next, watch_length);
        }
        reject->callback     = callback;
        reject->private_data = private_data;
        reject_next          = 0;
    }
    evpl_mutex_unlock(&mutex);
    if (reject) {
        chimera_vfs_compound_set_finish_handler(compound, reject_finish, reject);
        next(compound, reject_complete, reject);
    } else {
        next(compound, callback, private_data);
    }
    if (waiting) {
        evpl_mutex_lock(&mutex);
        submissions++;
        evpl_cond_broadcast(&cond);
        evpl_mutex_unlock(&mutex);
    }
} /* chimera_vfs_compound_submit */
