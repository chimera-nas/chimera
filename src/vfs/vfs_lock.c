// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

#include <assert.h>
#include <fcntl.h>
#include <stddef.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include "vfs_lock.h"
#include "vfs_claim.h"
#include "vfs_claim_internal.h"
#include "sdk/vfs_log.h"
#include "vfs_internal_procs.h"
#include "common/macros.h"

struct lock_range {
    struct lock_range       *next;
    struct chimera_vfs_claim claim;
};

struct lock_owner_file {
    struct lock_owner_file          *next;
    struct chimera_claim_owner       owner;
    uint8_t                          fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                         fh_len;
    uint64_t                         hash, generation;
    struct chimera_vfs_file_state   *file;
    struct lock_range               *ranges;
    struct chimera_vfs_lock_attempt *busy;
    unsigned                         retiring;
    bool                             projected, backend_dirty, backend_only;
};

struct chimera_vfs_lock_domain {
    evpl_mutex_t                     lock;
    struct chimera_vfs              *vfs;
    struct lock_owner_file          *owners;
    struct chimera_vfs_lock_attempt *attempts;
    bool                             shutdown;
};

enum lock_phase { LOCK_NEW, LOCK_STARTING, LOCK_ACQUIRING, LOCK_ADMITTED,
                  LOCK_BACKEND, LOCK_BACKEND_DONE, LOCK_READY, LOCK_COMPLETE };

struct chimera_vfs_lock_attempt {
    struct chimera_vfs_lock_attempt   *next;
    struct chimera_vfs_lock_domain    *domain;
    struct lock_owner_file            *bucket;
    struct chimera_vfs_thread         *thread;
    struct chimera_vfs_open_handle    *handle;
    struct chimera_vfs_lock_request    request;
    struct chimera_vfs_claim           reservation;
    struct chimera_vfs_pending_acquire ticket;
    struct chimera_vfs_claim_conflict  conflict;
    uint32_t                           pid;
    struct lock_range                 *replacement;
    struct chimera_vfs_claim         **previous, **published;
    uint32_t                           nprevious, npublished;
    struct evpl_doorbell               bell;
    struct evpl_timer                  timer;
    atomic_bool                        acquired;
    enum chimera_vfs_claim_result      acquire_result;
    enum chimera_vfs_error             backend_status;
    enum lock_phase                    phase;
    bool                               test, release, started, canceled, timer_active, projected, backend_changed;
    bool                               reservation_held, reservation_retired, callback_done, finalized;
    void                               (*complete)(
        enum chimera_vfs_error,
        void *);
    void                              *private_data;
};

static void lock_resume(
    struct evpl          *evpl,
    struct evpl_doorbell *bell);

static struct lock_owner_file *
lock_find(
    struct chimera_vfs_lock_domain   *domain,
    const uint8_t                    *fh,
    uint32_t                          fh_len,
    const struct chimera_claim_owner *owner)
{
    for (struct lock_owner_file *b = domain->owners; b; b = b->next) {
        if (b->fh_len == fh_len && !memcmp(b->fh, fh, fh_len) &&
            chimera_claim_owner_equal(&b->owner, owner)) {
            return b;
        }
    }
    return NULL;
} /* lock_find */

static void
lock_wake(
    struct chimera_vfs_lock_domain *domain,
    struct lock_owner_file         *bucket)
{
    for (struct chimera_vfs_lock_attempt *a = domain->attempts; a; a = a->next) {
        if (a->bucket == bucket && a->started && !a->callback_done) {
            evpl_ring_doorbell(&a->bell);
        }
    }
} /* lock_wake */

SYMBOL_EXPORT struct chimera_vfs_lock_domain *
chimera_vfs_lock_domain_create(struct chimera_vfs *vfs)
{
    struct chimera_vfs_lock_domain *domain = calloc(1, sizeof(*domain));

    if (domain) {
        evpl_mutex_init(&domain->lock, NULL);
        domain->vfs = vfs;
    }
    return domain;
} /* chimera_vfs_lock_domain_create */

SYMBOL_EXPORT uint64_t
chimera_vfs_lock_domain_admit(
    struct chimera_vfs_lock_domain   *domain,
    struct chimera_vfs_open_handle   *handle,
    const struct chimera_claim_owner *owner)
{
    if (!domain || !handle || !owner) {
        return 0;
    }
    evpl_mutex_lock(&domain->lock);
    if (domain->shutdown) {
        evpl_mutex_unlock(&domain->lock);
        return 0;
    }
    struct lock_owner_file *b = lock_find(domain, handle->fh, handle->fh_len, owner);
    if (!b) {
        b = calloc(1, sizeof(*b));
        if (!b) {
            evpl_mutex_unlock(&domain->lock);
            return 0;
        }
        b->owner  = *owner;
        b->fh_len = handle->fh_len;
        memcpy(b->fh, handle->fh, b->fh_len);
        b->hash       = handle->fh_hash;
        b->generation = 1;
        b->projected  = owner->proto == CHIMERA_CLAIM_PROTO_POSIX &&
            handle->vfs_module && (handle->vfs_module->capabilities & CHIMERA_VFS_CAP_CLAIM_RANGE);
        b->file = chimera_vfs_state_get(domain->vfs->vfs_state, b->fh, b->fh_len, b->hash, true);
        if (!b->file) {
            free(b);
            evpl_mutex_unlock(&domain->lock);
            return 0;
        }
        b->next        = domain->owners;
        domain->owners = b;
    }
    if (!b->file) {
        b->file = chimera_vfs_state_get(domain->vfs->vfs_state, b->fh, b->fh_len, b->hash, true);
        if (!b->file) {
            evpl_mutex_unlock(&domain->lock);
            return 0;
        }
    }
    uint64_t generation = b->generation;
    evpl_mutex_unlock(&domain->lock);
    return generation;
} /* chimera_vfs_lock_domain_admit */

static void
lock_ranges_free(struct lock_range *ranges)
{
    while (ranges) {
        struct lock_range *next = ranges->next;
        free(ranges);
        ranges = next;
    }
} /* lock_ranges_free */

/* Retirement only detaches claims under the domain mutex. Pumping runs after
 * dropping it: arbiter grant callbacks take this mutex to keep the attempt
 * alive until its worker doorbell has been rung. Local-only ticket cancel
 * cannot invoke a completion callback inline. */
static void
lock_retire_locked(
    struct chimera_vfs_lock_domain *domain,
    struct lock_owner_file         *b)
{
    b->generation++;
    if (b->projected) {
        b->retiring++;
    }
    for (struct chimera_vfs_lock_attempt *a = domain->attempts; a; a = a->next) {
        if (a->bucket != b || a->release || a->finalized) {
            continue;
        }
        a->canceled = true;
        if (a->phase == LOCK_ACQUIRING && !atomic_load(&a->acquired) &&
            chimera_vfs_claim_cancel(domain->vfs->vfs_state, &a->ticket)) {
            a->acquire_result = CHIMERA_CLAIM_DENIED;
            atomic_store(&a->acquired, true);
        }
        if (a->reservation_held ||
            (a->phase == LOCK_ACQUIRING && atomic_load(&a->acquired) &&
             a->acquire_result == CHIMERA_CLAIM_GRANTED && !a->reservation_retired)) {
            struct chimera_vfs_claim *claim = &a->reservation;
            chimera_vfs_claim_range_retire(b->file, &claim, 1);
            a->reservation_held    = false;
            a->reservation_retired = true;
        }
        if (a->started && !a->callback_done) {
            evpl_ring_doorbell(&a->bell);
        }
    }
    struct lock_range *ranges = b->ranges;
    b->ranges = NULL;
    while (ranges) {
        struct lock_range        *next  = ranges->next;
        struct chimera_vfs_claim *claim = &ranges->claim;
        chimera_vfs_claim_range_retire(b->file, &claim, 1);
        free(ranges);
        ranges = next;
    }
    /* A finish-pending local attempt must not prevent close/new admissions.
     * Its acceptance checks generation before touching its old snapshot. */
    if (b->busy && b->busy->phase != LOCK_BACKEND) {
        b->busy = NULL;
    }
    lock_wake(domain, b);
} /* lock_retire_locked */

SYMBOL_EXPORT void
chimera_vfs_lock_domain_retire(
    struct chimera_vfs_lock_domain   *domain,
    const uint8_t                    *fh,
    uint32_t                          fh_len,
    const struct chimera_claim_owner *owner)
{
    if (!domain) {
        return;
    }
    evpl_mutex_lock(&domain->lock);
    struct lock_owner_file        *b = lock_find(domain, fh, fh_len, owner);
    if (b) {
        lock_retire_locked(domain, b);
    }
    struct chimera_vfs_file_state *file = b && b->file ?
        chimera_vfs_state_get(domain->vfs->vfs_state, b->fh, b->fh_len, b->hash, false) : NULL;
    evpl_mutex_unlock(&domain->lock);
    if (file) {
        chimera_vfs_claim_replacement_complete(file);
        chimera_vfs_state_put(domain->vfs->vfs_state, file);
    }
} /* chimera_vfs_lock_domain_retire */

SYMBOL_EXPORT void
chimera_vfs_lock_domain_shutdown(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_lock_domain *domain)
{
    (void) thread;
    if (!domain) {
        return;
    }
    evpl_mutex_lock(&domain->lock);
    if (!domain->shutdown) {
        domain->shutdown = true;
        for (struct lock_owner_file *b = domain->owners; b; b = b->next) {
            lock_retire_locked(domain, b);
        }
    }
    evpl_mutex_unlock(&domain->lock);
    for (struct lock_owner_file *b = domain->owners; b; b = b->next) {
        evpl_mutex_lock(&domain->lock);
        struct chimera_vfs_file_state *file = b->file ?
            chimera_vfs_state_get(domain->vfs->vfs_state, b->fh, b->fh_len, b->hash, false) : NULL;
        evpl_mutex_unlock(&domain->lock);
        if (file) {
            chimera_vfs_claim_replacement_complete(file);
            chimera_vfs_state_put(domain->vfs->vfs_state, file);
        }
    }
} /* chimera_vfs_lock_domain_shutdown */

SYMBOL_EXPORT void
chimera_vfs_lock_domain_destroy(struct chimera_vfs_lock_domain *domain)
{
    if (!domain) {
        return;
    }
    assert(!domain->attempts);
    for (struct lock_owner_file *b = domain->owners, *next; b; b = next) {
        next = b->next;
        assert(!b->ranges);
        if (b->backend_dirty) {
            /* Shutdown has already returned the cleanup failure. The backend
             * owner anchor remains responsible until backend teardown. */
            chimera_vfs_error("lock domain destroyed with unresolved backend owner %llu:%llu on file %llu",
                              (unsigned long long) b->owner.owner_hi,
                              (unsigned long long) b->owner.owner_lo,
                              (unsigned long long) b->hash);
        }
        if (b->file) {
            chimera_vfs_state_put(domain->vfs->vfs_state, b->file);
        }
        free(b);
    }
    evpl_mutex_destroy(&domain->lock);
    free(domain);
} /* chimera_vfs_lock_domain_destroy */

struct chimera_vfs_lock_attempt *
chimera_vfs_lock_attempt_alloc(
    struct chimera_vfs_thread             *thread,
    struct chimera_vfs_lock_domain        *domain,
    struct chimera_vfs_open_handle        *handle,
    const struct chimera_vfs_lock_request *request,
    bool                                   test,
    bool                                   release)
{
    struct chimera_vfs_lock_attempt *a = calloc(1, sizeof(*a));

    if (!a || !domain || !handle || !request) {
        free(a);
        return NULL;
    }
    a->thread  = thread;
    a->domain  = domain;
    a->handle  = handle;
    a->request = *request;
    a->test    = test;
    a->release = release;
    evpl_mutex_lock(&domain->lock);
    a->bucket = lock_find(domain, handle->fh, handle->fh_len, &request->owner);
    if (!a->bucket && !release) {
        evpl_mutex_unlock(&domain->lock);
        free(a);
        return NULL;
    }
    if (a->bucket && !a->bucket->file) {
        a->bucket->file = chimera_vfs_state_get(domain->vfs->vfs_state,
                                                a->bucket->fh, a->bucket->fh_len, a->bucket->hash, true);
        if (!a->bucket->file) {
            evpl_mutex_unlock(&domain->lock);
            free(a);
            return NULL;
        }
    }
    a->projected     = a->bucket && request->project_backend && a->bucket->projected;
    a->next          = domain->attempts;
    domain->attempts = a;
    evpl_add_doorbell(thread->evpl, &a->bell, lock_resume);
    evpl_mutex_unlock(&domain->lock);
    return a;
} /* chimera_vfs_lock_attempt_alloc */

static void
lock_journal_clear(struct chimera_vfs_lock_attempt *a)
{
    lock_ranges_free(a->replacement);
    a->replacement = NULL;
    free(a->previous);
    free(a->published);
    a->previous  = a->published = NULL;
    a->nprevious = a->npublished = 0;
} /* lock_journal_clear */

static bool
lock_add_piece(
    struct chimera_vfs_lock_attempt *a,
    uint64_t                         offset,
    uint64_t                         length,
    bool                             exclusive)
{
    struct lock_range *piece = calloc(1, sizeof(*piece));

    if (!piece) {
        return false;
    }
    chimera_vfs_claim_init_range(&piece->claim, exclusive, false, offset, length, &a->request.owner);
    piece->claim.local_only       = 1;
    piece->next                   = a->replacement;
    a->replacement                = piece;
    a->published[a->npublished++] = &piece->claim;
    return true;
} /* lock_add_piece */

/* All allocation precedes legacy backend mutation or optimistic finish. The
 * old committed list stays linked until acceptance; a provisional reservation
 * supplies only newly required coverage. */
static bool
lock_journal_build(struct chimera_vfs_lock_attempt *a)
{
    struct lock_owner_file *b     = a->bucket;
    uint32_t                count = 0;

    for (struct lock_range *r = b->ranges; r; r = r->next) {
        count++;
    }
    a->previous  = calloc(count + 1, sizeof(*a->previous));
    a->published = calloc(2 * count + 1, sizeof(*a->published));
    if (!a->previous || !a->published) {
        lock_journal_clear(a);
        return false;
    }
    uint64_t start = a->request.offset;
    uint64_t end   = a->request.length == UINT64_MAX ? UINT64_MAX : start + a->request.length;
    for (struct lock_range *r = b->ranges; r; r = r->next) {
        struct chimera_vfs_claim *c         = &r->claim;
        uint64_t                  stop      = c->length == UINT64_MAX ? UINT64_MAX : c->offset + c->length;
        bool                      exclusive = !!(c->used & CHIMERA_CLAIM_LW);
        a->previous[a->nprevious++] = c;
        if (stop <= start || c->offset >= end) {
            if (!lock_add_piece(a, c->offset, c->length, exclusive)) {
                goto failed;
            }
        } else {
            if (c->offset < start && !lock_add_piece(a, c->offset, start - c->offset, exclusive)) {
                goto failed;
            }
            if (stop > end && !lock_add_piece(a, end, stop == UINT64_MAX ? UINT64_MAX : stop - end, exclusive)) {
                goto failed;
            }
        }
    }
    if (a->reservation_held) {
        a->previous[a->nprevious++] = &a->reservation;
    }
    if (a->request.type != CHIMERA_VFS_LOCK_UNLOCK &&
        !lock_add_piece(a, start, a->request.length, a->request.type == CHIMERA_VFS_LOCK_WRITE)) {
        goto failed;
    }
    return true;
 failed:
    lock_journal_clear(a);
    return false;
} /* lock_journal_build */

static void
lock_complete(
    struct chimera_vfs_lock_attempt *a,
    enum chimera_vfs_error           status)
{
    evpl_mutex_lock(&a->domain->lock);
    if (a->release && a->bucket && status == CHIMERA_VFS_OK) {
        a->bucket->backend_dirty = false;
        a->bucket->backend_only  = false;
        if (a->bucket->retiring) {
            a->bucket->retiring--;
        }
        if (a->bucket->busy == a) {
            a->bucket->busy = NULL;
        }
        a->finalized = true;
        lock_wake(a->domain, a->bucket);
    }
    a->callback_done = true;
    a->phase         = status == CHIMERA_VFS_OK ? LOCK_READY : LOCK_COMPLETE;
    evpl_mutex_unlock(&a->domain->lock);
    a->complete(status, a->private_data);
} /* lock_complete */

static void
lock_acquired(
    enum chimera_vfs_claim_result            result,
    struct chimera_vfs_claim                *granted,
    const struct chimera_vfs_claim_conflict *conflict,
    void                                    *private_data)
{
    struct chimera_vfs_lock_attempt *a = private_data;

    (void) granted;
    evpl_mutex_lock(&a->domain->lock);
    a->acquire_result = result;
    if (conflict) {
        a->conflict = *conflict;
    }
    atomic_store(&a->acquired, true);
    evpl_ring_doorbell(&a->bell);
    evpl_mutex_unlock(&a->domain->lock);
} /* lock_acquired */

static void
lock_timer(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct chimera_vfs_lock_attempt *a = (void *) ((char *) timer - offsetof(struct chimera_vfs_lock_attempt, timer));

    (void) evpl;
    a->timer_active = false;
    evpl_ring_doorbell(&a->bell);
} /* lock_timer */

static void
lock_backend_result(
    enum chimera_vfs_error                     status,
    uint8_t                                    granted,
    uint64_t                                   token,
    const struct chimera_claim_range_conflict *conflict,
    void                                      *private_data)
{
    struct chimera_vfs_lock_attempt *a = private_data;

    (void) token; /* Whole-owner/geometry releases own backend token retirement. */
    if (status == CHIMERA_VFS_OK && !a->test && !a->release &&
        a->request.type != CHIMERA_VFS_LOCK_UNLOCK && !granted) {
        status = CHIMERA_VFS_EAGAIN;
    }
    if (conflict && conflict->type != CHIMERA_VFS_LOCK_UNLOCK) {
        a->conflict.construct = CHIMERA_CONSTRUCT_LOCK_ADVISORY;
        a->conflict.used      = conflict->type == CHIMERA_VFS_LOCK_WRITE ? CHIMERA_CLAIM_LW : CHIMERA_CLAIM_LR;
        a->conflict.offset    = conflict->offset;
        a->conflict.length    = conflict->length;
        a->pid                = conflict->pid;
    }
    evpl_mutex_lock(&a->domain->lock);
    if (status == CHIMERA_VFS_ENOTSUP) {
        /* Some multiplexed modules (notably the NFSv4 arm of NFS) advertise
         * RANGE for other mounts but explicitly decline projection here. */
        a->projected             = false;
        a->bucket->projected     = false;
        a->bucket->backend_dirty = false;
        a->bucket->retiring      = 0;
        if (a->request.whence == SEEK_SET || a->release) {
            status = CHIMERA_VFS_OK;
        }
    }
    a->backend_status  = status;
    a->backend_changed = a->projected && status == CHIMERA_VFS_OK && !a->test;
    if (a->backend_changed && !a->release) {
        a->bucket->backend_dirty = true;
    }
    a->phase = LOCK_BACKEND_DONE;
    evpl_ring_doorbell(&a->bell);
    evpl_mutex_unlock(&a->domain->lock);
} /* lock_backend_result */

static void
lock_backend_released(
    enum chimera_vfs_error status,
    void                  *private_data)
{
    lock_backend_result(status, 0, 0, NULL, private_data);
} /* lock_backend_released */

static void
lock_backend_dispatch(void *private_data)
{
    struct chimera_vfs_lock_attempt *a = private_data;
    struct lock_owner_file          *b = a->bucket;

    if (a->release || a->request.type == CHIMERA_VFS_LOCK_UNLOCK) {
        chimera_vfs_claim_release_backend(a->thread, b->fh, b->fh_len, b->hash,
                                          CHIMERA_VFS_CLAIM_KLASS_RANGE, 0, 0,
                                          a->release ? SEEK_SET : a->request.whence,
                                          a->release ? 0 : a->request.offset,
                                          a->release ? UINT64_MAX : a->request.length,
                                          &b->owner, lock_backend_released, a);
    } else {
        chimera_vfs_claim_acquire_backend(a->thread, b->fh, b->fh_len, b->hash,
                                          CHIMERA_VFS_CLAIM_KLASS_RANGE, 0, 0, a->request.type == CHIMERA_VFS_LOCK_WRITE
                                          ,
                                          a->test ? CHIMERA_VFS_CLAIM_TEST : CHIMERA_VFS_CLAIM_REPLACE, a->request.
                                          whence,
                                          a->request.offset, a->request.length, &b->owner, 0, NULL, NULL,
                                          lock_backend_result, a);
    }
} /* lock_backend_dispatch */

static void
lock_resume(
    struct evpl          *evpl,
    struct evpl_doorbell *bell)
{
    struct chimera_vfs_lock_attempt *a     = (void *) ((char *) bell - offsetof(struct chimera_vfs_lock_attempt, bell));
    struct chimera_vfs_lock_domain  *d     = a->domain;
    struct lock_owner_file          *b     = a->bucket;
    enum chimera_vfs_error           error = CHIMERA_VFS_OK;

    (void) evpl;
    evpl_mutex_lock(&d->lock);
    if (!a->started || a->callback_done || a->timer_active || a->phase == LOCK_BACKEND) {
        evpl_mutex_unlock(&d->lock);
        return;
    }
    if (a->phase == LOCK_ACQUIRING) {
        if (a->canceled && !atomic_load(&a->acquired) &&
            chimera_vfs_claim_cancel(d->vfs->vfs_state, &a->ticket)) {
            a->acquire_result = CHIMERA_CLAIM_DENIED;
            atomic_store(&a->acquired, true);
        }
        if (!atomic_load(&a->acquired)) {
            if (a->canceled) {
                /* The arbiter pump may temporarily own a dequeued ticket
                 * and requeue it after cancel loses that race. Retry on the
                 * home worker until cancel or its grant callback owns it. */
                a->timer_active = true;
                evpl_add_oneshot_timer(a->thread->evpl, &a->timer, lock_timer, 10000);
            }
            evpl_mutex_unlock(&d->lock);
            return;
        }
        a->reservation_held = a->acquire_result == CHIMERA_CLAIM_GRANTED && !a->reservation_retired;
        if (!a->reservation_held) {
            error = a->canceled ? CHIMERA_VFS_EINTR : CHIMERA_VFS_EAGAIN;
            goto done;
        }
        a->phase = LOCK_ADMITTED;
    }
    if (!a->release && (a->canceled || d->shutdown || a->request.generation != b->generation)) {
        error = CHIMERA_VFS_EINTR;
        goto done;
    }
    if (a->phase == LOCK_BACKEND_DONE) {
        if (a->backend_status == CHIMERA_VFS_EAGAIN && a->request.wait && !a->test && !a->release) {
            /* Nonblocking backend tries leave the owner's unlock/close lane
             * free while another process holds the range. */
            if (b->busy == a) {
                b->busy = NULL;
            }
            lock_journal_clear(a);
            a->phase        = LOCK_ADMITTED;
            a->timer_active = true;
            evpl_add_oneshot_timer(a->thread->evpl, &a->timer, lock_timer, 10000);
            lock_wake(d, b);
            evpl_mutex_unlock(&d->lock);
            return;
        }
        error = a->backend_status;
        goto done;
    }
    if (a->phase == LOCK_NEW) {
        a->projected = a->projected && b->projected;
        if (a->request.whence == SEEK_END && !a->projected) {
            error = CHIMERA_VFS_ENOTSUP;
            goto done;
        }
        if (a->test) {
            struct chimera_vfs_claim probe;
            chimera_vfs_claim_init_range(&probe, a->request.type == CHIMERA_VFS_LOCK_WRITE,
                                         false, a->request.offset, a->request.length, &a->request.owner);
            if (a->request.whence == SEEK_SET && !b->backend_only &&
                chimera_vfs_claim_test_locks(b->file, &probe, &a->conflict) != CHIMERA_CLAIM_GRANTED) {
                if (a->conflict.owner.proto == CHIMERA_CLAIM_PROTO_POSIX) {
                    a->pid = (uint32_t) a->conflict.owner.owner_lo;
                }
                goto done;
            }
            if (!a->projected) {
                goto done;
            }
            a->phase = LOCK_ADMITTED;
        } else if (a->release || a->request.type == CHIMERA_VFS_LOCK_UNLOCK || a->request.whence == SEEK_END || b->
                   backend_only) {
            a->phase = LOCK_ADMITTED;
        } else {
            /* Track the attempt before admission. Publication never operates
            * on a callback-granted node still awaiting frontend tracking. */
            chimera_vfs_claim_init_range(&a->reservation, a->request.type == CHIMERA_VFS_LOCK_WRITE,
                                         false, a->request.offset, a->request.length, &a->request.owner);
            a->reservation.local_only  = 1;
            a->reservation.provisional = 1;
            a->phase                   = LOCK_STARTING;
            evpl_mutex_unlock(&d->lock);
            chimera_vfs_claim_acquire(a->thread, d->vfs->vfs_state, b->file,
                                      &a->reservation, &a->ticket, true, a->request.wait, lock_acquired, NULL, a);
            /* A release may race the arbiter's initial try/enqueue window. */
            chimera_vfs_claim_pump_pending(d->vfs->vfs_state, b->file);
            evpl_mutex_lock(&d->lock);
            a->phase = LOCK_ACQUIRING;
            evpl_ring_doorbell(&a->bell);
            evpl_mutex_unlock(&d->lock);
            return;
        }
    }
    if (b->busy && b->busy != a) {
        evpl_mutex_unlock(&d->lock);
        return;
    }
    if (b->retiring && !a->release) {
        evpl_mutex_unlock(&d->lock);
        return;
    }
    if (!a->test) {
        b->busy = a;
        if (!a->release && !b->backend_only && a->request.whence == SEEK_SET && !a->previous && !lock_journal_build(a))
        {
            error = CHIMERA_VFS_ENOSPC;
            goto done;
        }
    }
    if (a->projected) {
        if (!a->test && !a->release) {
            b->backend_dirty = true;
        }
        a->phase = LOCK_BACKEND;
        evpl_mutex_unlock(&d->lock);
        chimera_vfs_claim_backend_flush_releases(a->thread, d->vfs->vfs_state, b->file,
                                                 lock_backend_dispatch, a);
        return;
    }
 done:
    evpl_mutex_unlock(&d->lock);
    lock_complete(a, error);
} /* lock_resume */

void
chimera_vfs_lock_attempt_execute(
    struct chimera_vfs_lock_attempt *a,
    bool finish_adapter,
    void ( *complete )(enum chimera_vfs_error, void *),
    void *private_data)
{
    a->complete     = complete;
    a->private_data = private_data;
    evpl_mutex_lock(&a->domain->lock);
    a->started = true;
    evpl_mutex_unlock(&a->domain->lock);
    if (!a->bucket) {
        a->finalized = true;
        lock_complete(a, CHIMERA_VFS_OK);
        return;
    }
    /* A projected mutation cannot enter an optimistic finish arrangement.
     * The caller learns that before any irreversible backend side effect. */
    if (a->projected && !a->test && !a->release && finish_adapter) {
        lock_complete(a, CHIMERA_VFS_ENOTSUP);
        return;
    }
    if ((a->request.whence != SEEK_SET && a->request.whence != SEEK_END) ||
        (a->request.whence == SEEK_END && !a->projected) ||
        a->request.type > CHIMERA_VFS_LOCK_UNLOCK ||
        (a->test && a->request.type == CHIMERA_VFS_LOCK_UNLOCK) ||
        (!a->release && a->request.whence == SEEK_SET &&
         (!a->request.length || (a->request.length != UINT64_MAX &&
                                 a->request.offset > UINT64_MAX - a->request.length)))) {
        lock_complete(a, a->request.whence == SEEK_END && !a->projected ? CHIMERA_VFS_ENOTSUP : CHIMERA_VFS_EINVAL);
        return;
    }
    evpl_ring_doorbell(&a->bell);
} /* chimera_vfs_lock_attempt_execute */

enum chimera_vfs_error
chimera_vfs_lock_attempt_accept(struct chimera_vfs_lock_attempt *a)
{
    if (a->finalized || a->phase != LOCK_READY) {
        return CHIMERA_VFS_OK;
    }
    struct chimera_vfs_lock_domain *d = a->domain;
    struct lock_owner_file         *b = a->bucket;
    evpl_mutex_lock(&d->lock);
    if (!a->release && (a->canceled || d->shutdown || a->request.generation != b->generation)) {
        evpl_mutex_unlock(&d->lock);
        return CHIMERA_VFS_EINTR;
    }
    if (!a->test && !a->release && a->request.whence == SEEK_END) {
        /* Backend results do not expose atomic resolved END geometry. Move
         * this owner wholly to backend-only arbitration so old absolute
         * local intervals cannot falsely survive an END unlock/downgrade. */
        b->backend_only = true;
        struct lock_range *r = b->ranges;
        b->ranges = NULL;
        while (r) {
            struct lock_range        *next  = r->next;
            struct chimera_vfs_claim *claim = &r->claim;
            chimera_vfs_claim_range_retire(b->file, &claim, 1);
            free(r);
            r = next;
        }
    }
    if (!a->test && a->previous) {
        struct lock_range *old = b->ranges;
        chimera_vfs_claim_range_publish(b->file, a->previous, a->nprevious, a->published, a->npublished);
        a->reservation_held = false;
        b->ranges           = a->replacement;
        a->replacement      = NULL;
        lock_ranges_free(old);
    }
    if (a->release) {
        b->backend_dirty = false;
        if (b->retiring) {
            b->retiring--;
        }
    }
    a->finalized = true;
    if (b->busy == a) {
        b->busy = NULL;
    }
    lock_wake(d, b);
    evpl_mutex_unlock(&d->lock);
    chimera_vfs_claim_replacement_complete(b->file);
    return CHIMERA_VFS_OK;
} /* chimera_vfs_lock_attempt_accept */

void
chimera_vfs_lock_attempt_result(
    struct chimera_vfs_lock_attempt   *a,
    struct chimera_vfs_claim_conflict *conflict,
    uint32_t                          *pid)
{
    *conflict = a->conflict;
    *pid      = a->pid;
} /* chimera_vfs_lock_attempt_result */

bool
chimera_vfs_lock_attempt_cancel(struct chimera_vfs_lock_attempt *a)
{
    evpl_mutex_lock(&a->domain->lock);
    if (a->release || a->finalized || a->backend_changed || (a->projected && a->phase == LOCK_BACKEND)) {
        evpl_mutex_unlock(&a->domain->lock);
        return false;
    }
    a->canceled = true;
    if (a->phase == LOCK_ACQUIRING && !atomic_load(&a->acquired) &&
        chimera_vfs_claim_cancel(a->domain->vfs->vfs_state, &a->ticket)) {
        a->acquire_result = CHIMERA_CLAIM_DENIED;
        atomic_store(&a->acquired, true);
    }
    if (a->started && !a->callback_done) {
        evpl_ring_doorbell(&a->bell);
    }
    evpl_mutex_unlock(&a->domain->lock);
    return true;
} /* chimera_vfs_lock_attempt_cancel */

void
chimera_vfs_lock_attempt_reset(struct chimera_vfs_lock_attempt *a)
{
    struct chimera_vfs_lock_domain *d = a->domain;

    evpl_mutex_lock(&d->lock);
    assert(a->phase != LOCK_ACQUIRING && a->phase != LOCK_STARTING && a->phase != LOCK_BACKEND);
    if (a->timer_active) {
        evpl_remove_timer(a->thread->evpl, &a->timer);
        a->timer_active = false;
    }
    bool                            wake = a->reservation_held;
    if (a->reservation_held) {
        struct chimera_vfs_claim *claim = &a->reservation;
        chimera_vfs_claim_range_retire(a->bucket->file, &claim, 1);
        a->reservation_held = false;
    }
    if (a->bucket && a->bucket->busy == a) {
        a->bucket->busy = NULL;
    }
    lock_journal_clear(a);
    a->phase               = LOCK_NEW;
    a->started             = a->callback_done = a->finalized = false;
    a->backend_changed     = false;
    a->reservation_retired = false;
    atomic_store(&a->acquired, false);
    memset(&a->conflict, 0, sizeof(a->conflict));
    a->pid = 0;
    lock_wake(d, a->bucket);
    evpl_mutex_unlock(&d->lock);
    if (wake) {
        chimera_vfs_claim_replacement_complete(a->bucket->file);
    }
} /* chimera_vfs_lock_attempt_reset */

void
chimera_vfs_lock_attempt_free(struct chimera_vfs_lock_attempt *a)
{
    if (!a) {
        return;
    }
    chimera_vfs_lock_attempt_reset(a);
    evpl_mutex_lock(&a->domain->lock);
    struct chimera_vfs_lock_attempt **p = &a->domain->attempts;
    while (*p != a) {
        p = &(*p)->next;
    }
    *p = a->next;
    struct lock_owner_file           *b    = a->bucket;
    bool                              held = false;
    for (struct chimera_vfs_lock_attempt *other = a->domain->attempts; other; other = other->next) {
        held |= other->bucket == b;
    }
    if (b && !held && !b->ranges && !b->backend_dirty && !b->retiring && b->file) {
        chimera_vfs_state_put(a->domain->vfs->vfs_state, b->file);
        b->file = NULL;
    }
    evpl_mutex_unlock(&a->domain->lock);
    evpl_remove_doorbell(a->thread->evpl, &a->bell);
    free(a);
} /* chimera_vfs_lock_attempt_free */

bool
chimera_vfs_lock_attempt_retryable(struct chimera_vfs_lock_attempt *a)
{
    return !a->release && (!a->projected || a->test);
} /* chimera_vfs_lock_attempt_retryable */

struct lock_shutdown {
    struct chimera_vfs_lock_domain  *domain;
    struct chimera_vfs_thread       *thread;
    struct lock_owner_file          *next;
    struct chimera_vfs_open_handle   handle;
    struct chimera_vfs_lock_attempt *attempt;
    enum chimera_vfs_error           status;
    unsigned                         retries;
    void                             (*callback)(
        enum chimera_vfs_error,
        void *);
    void                            *private_data;
};

static void lock_shutdown_next(
    struct lock_shutdown *shutdown);

static void
lock_shutdown_done(
    enum chimera_vfs_error status,
    void                  *private_data)
{
    struct lock_shutdown *shutdown = private_data;

    if (status != CHIMERA_VFS_OK && shutdown->retries++ < 2) {
        /* Full-owner unlock is idempotent, including an uncertain RPC reply.
         * Retry cleanup without hiding a persistent backend failure. */
        chimera_vfs_lock_attempt_reset(shutdown->attempt);
        chimera_vfs_lock_attempt_execute(shutdown->attempt, false, lock_shutdown_done, shutdown);
        return;
    }
    shutdown->retries = 0;
    if (status != CHIMERA_VFS_OK && shutdown->status == CHIMERA_VFS_OK) {
        shutdown->status = status;
    }
    chimera_vfs_lock_attempt_free(shutdown->attempt);
    shutdown->attempt = NULL;
    lock_shutdown_next(shutdown);
} /* lock_shutdown_done */

static void
lock_shutdown_next(struct lock_shutdown *shutdown)
{
    struct lock_owner_file *b = shutdown->next;

    while (b && !b->projected) {
        b = b->next;
    }
    if (!b) {
        void                   (*callback)(
            enum chimera_vfs_error,
            void *) = shutdown->callback;
        enum chimera_vfs_error status       = shutdown->status;
        void                  *private_data = shutdown->private_data;
        free(shutdown);
        callback(status, private_data);
        return;
    }
    shutdown->next = b->next;
    memcpy(shutdown->handle.fh, b->fh, b->fh_len);
    shutdown->handle.fh_len  = b->fh_len;
    shutdown->handle.fh_hash = b->hash;
    struct chimera_vfs_lock_request request = { .owner = b->owner,
                                                .type  = CHIMERA_VFS_LOCK_UNLOCK, .whence
                                                       =
                                                        SEEK_SET,
                                                .length = UINT64_MAX,             .project_backend
                                                        = true };
    shutdown->attempt = chimera_vfs_lock_attempt_alloc(shutdown->thread, shutdown->domain,
                                                       &shutdown->handle, &request, false, true);
    if (!shutdown->attempt) {
        shutdown->status = CHIMERA_VFS_ENOSPC;
        lock_shutdown_next(shutdown);
        return;
    }
    chimera_vfs_lock_attempt_execute(shutdown->attempt, false, lock_shutdown_done, shutdown);
} /* lock_shutdown_next */

SYMBOL_EXPORT void
chimera_vfs_lock_domain_shutdown_async(
    struct chimera_vfs_thread *thread,
    struct chimera_vfs_lock_domain *domain,
    void ( *callback )(enum chimera_vfs_error, void *),
    void *private_data)
{
    chimera_vfs_lock_domain_shutdown(thread, domain);
    struct lock_shutdown *shutdown = calloc(1, sizeof(*shutdown));
    if (!shutdown) {
        callback(CHIMERA_VFS_ENOSPC, private_data);
        return;
    }
    shutdown->thread       = thread;
    shutdown->domain       = domain;
    shutdown->next         = domain->owners;
    shutdown->callback     = callback;
    shutdown->private_data = private_data;
    lock_shutdown_next(shutdown);
} /* chimera_vfs_lock_domain_shutdown_async */
