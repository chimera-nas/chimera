// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#include <assert.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "vfs_claim_journal_attempt.h"
#include "common/macros.h"

struct chimera_vfs_claim_range_attempt {
    struct chimera_vfs_thread                  *thread;
    struct chimera_vfs_claim_journal           *journal;
    struct chimera_vfs_claim_owner             *owner;
    const struct chimera_vfs_claim_exact_range *ranges;
    uint32_t                                    count, delay_us, timeout_ms;
    uint64_t                                    start_ms;
    bool                                        unlock, wait, active, timer_active, notified;
    struct evpl_timer                           timer;
    struct chimera_vfs_claim_batch_result       result;
    void                                        (*on_wait)(
        void *);
    bool                                        (*is_canceled)(
        void *);
    void                                        (*complete)(
        const struct chimera_vfs_claim_batch_result *,
        void *);
    void                                       *private_data;
};

static uint64_t
monotonic_ms(void)
{
    struct timespec now;

    clock_gettime(CLOCK_MONOTONIC, &now);
    return (uint64_t) now.tv_sec * 1000 + now.tv_nsec / 1000000;
} /* monotonic_ms */

static void
range_done(struct chimera_vfs_claim_range_attempt *a)
{
    a->active = false;
    if (a->timer_active) {
        evpl_remove_timer(a->thread->evpl, &a->timer);
        a->timer_active = false;
    }
    if (a->owner) {
        chimera_vfs_claim_owner_put(a->owner);
        a->owner = NULL;
    }
    /* Completion can free the compound and this helper. */
    a->complete(&a->result, a->private_data);
} /* range_done */

static void range_poll(
    struct chimera_vfs_claim_range_attempt *a);

static void
range_timer(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct chimera_vfs_claim_range_attempt *a =
        (void *) ((char *) timer - offsetof(struct chimera_vfs_claim_range_attempt, timer));

    (void) evpl;
    a->timer_active = false;
    range_poll(a);
} /* range_timer */

static void
range_poll(struct chimera_vfs_claim_range_attempt *a)
{
    if (a->is_canceled && a->is_canceled(a->private_data)) {
        a->result.status = CHIMERA_VFS_EINTR;
        range_done(a);
        return;
    }
    if (a->unlock) {
        chimera_vfs_claim_journal_unlock(a->journal, a->owner, a->ranges, a->count, &a->result);
    } else {
        chimera_vfs_claim_journal_acquire(a->journal, a->owner, a->ranges, a->count, &a->result);
    }
    /* EBUSY is an owner-retirement fence or a removal reservation, not a
     * byte conflict. Waiting on it could create a cross-journal edit cycle. */
    bool retry = !a->unlock && a->wait && !a->result.self_conflict &&
        a->result.status == CHIMERA_VFS_EAGAIN;
    if (retry && a->timeout_ms && monotonic_ms() - a->start_ms >= a->timeout_ms) {
        a->result.status = CHIMERA_VFS_EAGAIN;
        retry            = false;
    }
    if (!retry) {
        range_done(a);
        return;
    }
    chimera_vfs_claim_journal_unbind_idle(a->journal, a->owner);
    a->timer_active = true;
    evpl_add_oneshot_timer(a->thread->evpl, &a->timer, range_timer, a->delay_us);
    if (a->delay_us < 100000) {
        a->delay_us *= 2;
        if (a->delay_us > 100000) {
            a->delay_us = 100000;
        }
    }
    if (!a->notified && a->on_wait) {
        a->notified = true;
        /* Timer is already armed: on_wait may cancel and free the helper. */
        a->on_wait(a->private_data);
    }
} /* range_poll */

SYMBOL_EXPORT struct chimera_vfs_claim_range_attempt *
chimera_vfs_claim_range_attempt_alloc(struct chimera_vfs_thread *thread)
{
    struct chimera_vfs_claim_range_attempt *a = calloc(1, sizeof(*a));

    if (a) {
        a->thread = thread;
    }
    return a;
} /* chimera_vfs_claim_range_attempt_alloc */

SYMBOL_EXPORT void
chimera_vfs_claim_range_attempt_execute(
    struct chimera_vfs_claim_range_attempt *a,
    struct chimera_vfs_claim_journal *journal,
    struct chimera_vfs_claim_owner *owner,
    const struct chimera_vfs_claim_exact_range *ranges,
    uint32_t count,
    bool unlock,
    bool wait,
    uint32_t timeout_ms,
    void ( *on_wait )(void *),
    bool ( *is_canceled )(void *),
    void ( *complete )(const struct chimera_vfs_claim_batch_result *, void *),
    void *private_data)
{
    assert(!a->active && !a->timer_active);
    a->journal = journal;
    a->owner   = owner;
    if (owner) {
        chimera_vfs_claim_owner_ref(owner);
    }
    a->ranges       = ranges;
    a->count        = count;
    a->unlock       = unlock;
    a->wait         = wait;
    a->timeout_ms   = timeout_ms;
    a->on_wait      = on_wait;
    a->is_canceled  = is_canceled;
    a->complete     = complete;
    a->private_data = private_data;
    a->start_ms     = monotonic_ms();
    a->delay_us     = 1000;
    a->active       = true;
    a->notified     = false;
    memset(&a->result, 0, sizeof(a->result));
    range_poll(a);
} /* chimera_vfs_claim_range_attempt_execute */

SYMBOL_EXPORT bool
chimera_vfs_claim_range_attempt_cancel(struct chimera_vfs_claim_range_attempt *a)
{
    if (!a || !a->active) {
        return false;
    }
    a->result.status  = CHIMERA_VFS_EINTR;
    a->result.applied = 0;
    range_done(a);
    return true;
} /* chimera_vfs_claim_range_attempt_cancel */

SYMBOL_EXPORT void
chimera_vfs_claim_range_attempt_free(struct chimera_vfs_claim_range_attempt *a)
{
    if (!a) {
        return;
    }
    assert(!a->active && !a->timer_active && !a->owner);
    free(a);
} /* chimera_vfs_claim_range_attempt_free */
