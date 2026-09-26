// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#include <assert.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include "vfs_claim_access.h"
#include "vfs_claim_internal.h"
#include "common/macros.h"

SYMBOL_EXPORT bool
chimera_vfs_claim_access_fence_acquire(
    struct chimera_vfs_claim_access_fence *fence,
    struct chimera_vfs_file_state         *file,
    const void                            *cookie)
{
    if (!fence || !file || !cookie) {
        return false;
    }
    if (fence->file) {
        return fence->file == file && fence->cookie == cookie;
    }
    struct chimera_vfs_file_state *pin = chimera_vfs_state_get(
        file->state, file->fh, file->fh_len, file->fh_hash, false);
    if (!pin) {
        return false;
    }
    evpl_mutex_lock(&file->lock);
    if ((file->access_fence_cookie && file->access_fence_cookie != cookie) ||
        file->access_fence_refs == UINT32_MAX) {
        evpl_mutex_unlock(&file->lock);
        chimera_vfs_state_put(file->state, pin);
        return false;
    }
    file->access_fence_cookie = cookie;
    file->access_fence_refs++;
    fence->file   = pin;
    fence->cookie = cookie;
    evpl_mutex_unlock(&file->lock);
    return true;
} /* chimera_vfs_claim_access_fence_acquire */

SYMBOL_EXPORT void
chimera_vfs_claim_access_fence_release(struct chimera_vfs_claim_access_fence *fence)
{
    if (!fence || !fence->file) {
        return;
    }
    struct chimera_vfs_file_state *file = fence->file;
    evpl_mutex_lock(&file->lock);
    assert(file->access_fence_cookie == fence->cookie && file->access_fence_refs);
    bool                           last = --file->access_fence_refs == 0;
    if (last) {
        file->access_fence_cookie = NULL;
    }
    fence->file   = NULL;
    fence->cookie = NULL;
    evpl_mutex_unlock(&file->lock);
    if (last) {
        chimera_vfs_claim_pump_pending(file->state, file);
    }
    chimera_vfs_state_put(file->state, file);
} /* chimera_vfs_claim_access_fence_release */

struct chimera_vfs_claim_access_owner {
    evpl_mutex_t                             lock;
    atomic_uint                              refs;
    struct chimera_vfs_file_state           *file;
    struct chimera_vfs_claim                 claim;
    struct chimera_vfs_claim_access_journal *busy;
    bool                                     retiring, retired;
};

enum access_phase { ACCESS_ACTIVE, ACCESS_SEALED, ACCESS_PUBLISHED, ACCESS_COMPLETE };
struct access_change {
    struct chimera_vfs_claim_access_owner *owner;
    uint8_t                                used, denied;
    bool                                   retire;
};
struct chimera_vfs_claim_access_journal {
    struct access_change *changes;
    uint32_t              count, capacity;
    enum access_phase phase;
};

SYMBOL_EXPORT struct chimera_vfs_claim_access_owner *
chimera_vfs_claim_access_owner_alloc(
    struct chimera_vfs_file_state  *file,
    const struct chimera_vfs_claim *template_claim)
{
    if (!file || !template_claim || template_claim->file ||
        template_claim->klass != CHIMERA_CLAIM_CLASS_ACCESS) {
        return NULL;
    }
    struct chimera_vfs_claim_access_owner *owner = calloc(1, sizeof(*owner));
    if (!owner) {
        return NULL;
    }
    owner->file = chimera_vfs_state_get(file->state, file->fh, file->fh_len, file->fh_hash, false);
    if (!owner->file) {
        free(owner); return NULL;
    }
    evpl_mutex_init(&owner->lock, NULL);
    atomic_init(&owner->refs, 1);
    owner->claim                    = *template_claim;
    owner->claim.next               = owner->claim.prev = NULL;
    owner->claim.admit_excluded     = NULL;
    owner->claim.admit_num_excluded = 0;
    owner->claim.admission_cookie   = NULL;
    return owner;
} /* chimera_vfs_claim_access_owner_alloc */

SYMBOL_EXPORT struct chimera_vfs_claim *
chimera_vfs_claim_access_owner_claim(struct chimera_vfs_claim_access_owner *owner)
{
    return owner ? &owner->claim : NULL;
} /* chimera_vfs_claim_access_owner_claim */

SYMBOL_EXPORT void
chimera_vfs_claim_access_owner_ref(struct chimera_vfs_claim_access_owner *owner)
{
    atomic_fetch_add(&owner->refs, 1);
} /* chimera_vfs_claim_access_owner_ref */

SYMBOL_EXPORT void
chimera_vfs_claim_access_owner_put(struct chimera_vfs_claim_access_owner *owner)
{
    if (atomic_fetch_sub(&owner->refs, 1) != 1) {
        return;
    }
    assert(owner->retired && !owner->busy && !owner->claim.file);
    chimera_vfs_state_put(owner->file->state, owner->file);
    evpl_mutex_destroy(&owner->lock);
    free(owner);
} /* chimera_vfs_claim_access_owner_put */

/* Caller holds the edit mutex. The owner, not a frontend embedded field,
 * keeps claim storage alive even after an external cutoff drops its ref. */
static void
access_unlink(struct chimera_vfs_claim_access_owner *owner)
{
    evpl_mutex_lock(&owner->file->lock);
    if (owner->claim.file) {
        assert(owner->claim.file == owner->file);
        chimera_vfs_claim_unlink_locked(owner->file, &owner->claim);
    }
    owner->retired = true;
    evpl_mutex_unlock(&owner->file->lock);
} /* access_unlink */

SYMBOL_EXPORT void
chimera_vfs_claim_access_owner_retire(struct chimera_vfs_claim_access_owner *owner)
{
    if (!owner) {
        return;
    }
    evpl_mutex_lock(&owner->lock);
    owner->retiring = true;
    bool drain = !owner->busy && !owner->retired;
    if (drain) {
        access_unlink(owner);
    }
    evpl_mutex_unlock(&owner->lock);
    if (drain) {
        chimera_vfs_claim_replacement_complete(owner->file);
    }
} /* chimera_vfs_claim_access_owner_retire */

SYMBOL_EXPORT bool
chimera_vfs_claim_access_owner_is_retired(struct chimera_vfs_claim_access_owner *owner)
{
    if (!owner) {
        return true;
    }
    evpl_mutex_lock(&owner->lock);
    bool retired = owner->retired && !owner->busy;
    evpl_mutex_unlock(&owner->lock);
    return retired;
} /* chimera_vfs_claim_access_owner_is_retired */

SYMBOL_EXPORT struct chimera_vfs_claim_access_journal *
chimera_vfs_claim_access_journal_alloc(uint32_t capacity)
{
    if (!capacity) {
        return NULL;
    }
    struct chimera_vfs_claim_access_journal *j = calloc(1, sizeof(*j));
    if (!j) {
        return NULL;
    }
    j->changes = calloc(capacity, sizeof(*j->changes));
    if (!j->changes) {
        free(j); return NULL;
    }
    j->capacity = capacity;
    return j;
} /* chimera_vfs_claim_access_journal_alloc */

static int
access_latest(
    const struct chimera_vfs_claim_access_journal *j,
    const struct chimera_vfs_claim_access_owner   *owner)
{
    for (uint32_t i = j->count; i > 0; i--) {
        if (j->changes[i - 1].owner == owner) {
            return (int) i - 1;
        }
    }
    return -1;
} /* access_latest */

static enum chimera_vfs_error
access_stage(
    struct chimera_vfs_claim_access_journal *j,
    struct chimera_vfs_claim_access_owner   *owner,
    bool                                     retire,
    uint8_t                                  used,
    uint8_t                                  denied)
{
    if (!j || !owner || j->phase != ACCESS_ACTIVE) {
        return CHIMERA_VFS_EINVAL;
    }
    evpl_mutex_lock(&owner->lock);
    enum chimera_vfs_error status = CHIMERA_VFS_OK;
    int latest = access_latest(j, owner);
    if (retire && latest >= 0 && j->changes[latest].retire) {
        /* Repeated close of this already-staged owner is idempotent. */
        goto out;
    }
    if (owner->retiring) {
        status = CHIMERA_VFS_EINTR;
    } else if (owner->busy && owner->busy != j) {
        status = CHIMERA_VFS_EBUSY;
    } else if (!retire && latest >= 0 && j->changes[latest].retire) {
        status = CHIMERA_VFS_EINVAL;
    } else if (j->count == j->capacity) {
        status = CHIMERA_VFS_ENOSPC;
    } else {
        if (!retire) {
            evpl_mutex_lock(&owner->file->lock);
            uint8_t prior_used   = latest >= 0 ? j->changes[latest].used : owner->claim.used;
            uint8_t prior_denied = latest >= 0 ? j->changes[latest].denied : owner->claim.denied;
            bool valid           = owner->claim.file == owner->file &&
                !(used & (uint8_t) ~prior_used) && !(denied & (uint8_t) ~prior_denied);
            evpl_mutex_unlock(&owner->file->lock);
            if (!valid) {
                status = CHIMERA_VFS_EINVAL;
                goto out;
            }
        }
        owner->busy = j;
        chimera_vfs_claim_access_owner_ref(owner);
        j->changes[j->count++] = (struct access_change) {
            .owner = owner, .used = used, .denied = denied, .retire = retire,
        };
    }
 out:
    evpl_mutex_unlock(&owner->lock);
    return status;
} /* access_stage */

SYMBOL_EXPORT enum chimera_vfs_error
chimera_vfs_claim_access_journal_retire(
    struct chimera_vfs_claim_access_journal *j,
    struct chimera_vfs_claim_access_owner   *owner)
{
    return access_stage(j, owner, true, 0, 0);
} /* chimera_vfs_claim_access_journal_retire */

SYMBOL_EXPORT enum chimera_vfs_error
chimera_vfs_claim_access_journal_narrow(
    struct chimera_vfs_claim_access_journal *j,
    struct chimera_vfs_claim_access_owner   *owner,
    uint8_t                                  used,
    uint8_t                                  denied)
{
    return access_stage(j, owner, false, used, denied);
} /* chimera_vfs_claim_access_journal_narrow */

SYMBOL_EXPORT uint32_t
chimera_vfs_claim_access_journal_excluded(
    const struct chimera_vfs_claim_access_journal *j,
    const struct chimera_vfs_file_state           *file,
    const struct chimera_vfs_claim               **out,
    uint32_t                                       capacity)
{
    uint32_t count = 0;

    for (uint32_t i = 0; i < j->count; i++) {
        struct chimera_vfs_claim_access_owner *owner = j->changes[i].owner;
        if (owner->file != file || access_latest(j, owner) != (int) i) {
            continue;
        }
        if (out && count < capacity) {
            out[count] = &owner->claim;
        }
        count++;
    }
    return count;
} /* chimera_vfs_claim_access_journal_excluded */

SYMBOL_EXPORT enum chimera_vfs_claim_result
chimera_vfs_claim_access_journal_test(
    const struct chimera_vfs_claim_access_journal *j,
    const struct chimera_vfs_file_state           *file,
    const struct chimera_vfs_claim                *probe,
    struct chimera_vfs_claim_conflict             *conflict)
{
    assert(probe);
    if (conflict) {
        memset(conflict, 0, sizeof(*conflict));
    }
    if (!j) {
        return CHIMERA_CLAIM_GRANTED;
    }
    for (uint32_t i = 0; i < j->count; i++) {
        const struct access_change            *change = &j->changes[i];
        struct chimera_vfs_claim_access_owner *owner  = change->owner;
        if (owner->file != file || change->retire || access_latest(j, owner) != (int) i) {
            continue;
        }
        evpl_mutex_lock(&owner->lock);
        /* Failed producer cleanup/external cutoff removes this private row,
         * but its public storage remains pinned until journal drainage. */
        if (owner->retiring) {
            evpl_mutex_unlock(&owner->lock);
            continue;
        }
        evpl_mutex_lock(&owner->file->lock);
        struct chimera_vfs_claim view = owner->claim;
        evpl_mutex_unlock(&owner->file->lock);
        view.used    = view.advertised = change->used;
        view.denied  = change->denied;
        bool blocked = chimera_vfs_claim_conflicts(&view, probe);
        if (blocked && conflict) {
            chimera_vfs_claim_conflict_fill(&view, conflict);
        }
        evpl_mutex_unlock(&owner->lock);
        if (blocked) {
            return CHIMERA_CLAIM_DENIED;
        }
    }
    return CHIMERA_CLAIM_GRANTED;
} /* chimera_vfs_claim_access_journal_test */

SYMBOL_EXPORT void
chimera_vfs_claim_access_journal_seal(struct chimera_vfs_claim_access_journal *j)
{
    assert(j->phase == ACCESS_ACTIVE);
    j->phase = ACCESS_SEALED;
} /* chimera_vfs_claim_access_journal_seal */

SYMBOL_EXPORT void
chimera_vfs_claim_access_journal_publish(struct chimera_vfs_claim_access_journal *j)
{
    assert(j->phase == ACCESS_SEALED);
    for (uint32_t i = 0; i < j->count; i++) {
        struct access_change                  *change = &j->changes[i];
        struct chimera_vfs_claim_access_owner *owner  = change->owner;
        if (access_latest(j, owner) != (int) i) {
            continue;
        }
        evpl_mutex_lock(&owner->lock);
        assert(owner->busy == j);
        if (change->retire || owner->retiring) {
            owner->retiring = true;
            access_unlink(owner);
        } else {
            evpl_mutex_lock(&owner->file->lock);
            assert(owner->claim.file == owner->file);
            assert(!(change->used & (uint8_t) ~owner->claim.used));
            assert(!(change->denied & (uint8_t) ~owner->claim.denied));
            owner->claim.used   = owner->claim.advertised = change->used;
            owner->claim.denied = change->denied;
            evpl_mutex_unlock(&owner->file->lock);
        }
        evpl_mutex_unlock(&owner->lock);
    }
    j->phase = ACCESS_PUBLISHED;
} /* chimera_vfs_claim_access_journal_publish */

static void
access_release_owners_to(
    struct chimera_vfs_claim_access_journal *j,
    uint32_t                                 checkpoint)
{
    while (j->count > checkpoint) {
        struct chimera_vfs_claim_access_owner *owner = j->changes[--j->count].owner;
        evpl_mutex_lock(&owner->lock);
        assert(owner->busy == j);
        bool                                   last = access_latest(j, owner) < 0;
        if (last) {
            if (owner->retiring && !owner->retired) {
                access_unlink(owner);
            }
            owner->busy = NULL;
        }
        evpl_mutex_unlock(&owner->lock);
        if (last) {
            chimera_vfs_claim_replacement_complete(owner->file);
        }
        chimera_vfs_claim_access_owner_put(owner);
    }
} /* access_release_owners_to */

SYMBOL_EXPORT uint32_t
chimera_vfs_claim_access_journal_checkpoint(const struct chimera_vfs_claim_access_journal *j)
{
    assert(j && j->phase == ACCESS_ACTIVE);
    return j->count;
} /* chimera_vfs_claim_access_journal_checkpoint */

SYMBOL_EXPORT void
chimera_vfs_claim_access_journal_rewind(
    struct chimera_vfs_claim_access_journal *j,
    uint32_t                                 checkpoint)
{
    assert(j && j->phase == ACCESS_ACTIVE && checkpoint <= j->count);
    access_release_owners_to(j, checkpoint);
} /* chimera_vfs_claim_access_journal_rewind */

SYMBOL_EXPORT void
chimera_vfs_claim_access_journal_complete(struct chimera_vfs_claim_access_journal *j)
{
    assert(j->phase == ACCESS_PUBLISHED);
    j->phase = ACCESS_COMPLETE;
    access_release_owners_to(j, 0);
} /* chimera_vfs_claim_access_journal_complete */

SYMBOL_EXPORT void
chimera_vfs_claim_access_journal_reset(struct chimera_vfs_claim_access_journal *j)
{
    assert(j->phase == ACCESS_ACTIVE || j->phase == ACCESS_SEALED || j->phase == ACCESS_COMPLETE);
    access_release_owners_to(j, 0);
    j->phase = ACCESS_ACTIVE;
} /* chimera_vfs_claim_access_journal_reset */

SYMBOL_EXPORT void
chimera_vfs_claim_access_journal_free(struct chimera_vfs_claim_access_journal *j)
{
    if (!j) {
        return;
    }
    chimera_vfs_claim_access_journal_reset(j);
    free(j->changes);
    free(j);
} /* chimera_vfs_claim_access_journal_free */
