// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

#include <assert.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include "vfs_claim_journal.h"
#include "vfs_claim_internal.h"
#include "common/macros.h"

struct exact_record {
    struct exact_record *next;
    struct chimera_vfs_claim_exact_range range;
    /* Only [0, UINT64_MAX) needs two extents to avoid VFS's EOF sentinel. */
    struct chimera_vfs_claim claims[2];
    unsigned num_claims, refs;
    bool in_owner_list, published;
    struct chimera_vfs_claim_journal *created_by, *removed_by;
};

struct chimera_vfs_claim_owner {
    pthread_mutex_t lock;
    atomic_uint refs;
    struct chimera_vfs_file_state *file;
    struct chimera_claim_owner identity;
    struct chimera_vfs_open_handle *handle_anchor;
    bool zero_point;
    struct exact_record *records;
    uint32_t pins;
    struct chimera_vfs_claim_journal *retire_journal;
    bool retiring, retired;
    void (*retire_complete)(void *);
    void *retire_private;
};

struct exact_change {
    struct chimera_vfs_claim_owner *owner;
    struct exact_record *record;
    bool add;
};

struct retire_owner {
    struct chimera_vfs_claim_owner *owner;
    uint32_t first, count;
};

enum journal_phase { JOURNAL_ACTIVE, JOURNAL_SEALED, JOURNAL_PUBLISHED, JOURNAL_COMPLETE };
struct chimera_vfs_claim_journal {
    struct chimera_vfs_claim_owner **owners;
    struct retire_owner *retire_owners;
    struct exact_change *changes;
    const struct chimera_vfs_claim **excluded;
    uint32_t num_owners, max_owners, num_changes, max_changes;
    uint32_t num_retire_owners, retire_changes, initial_max_changes;
    enum journal_phase phase;
};

static bool
exact_overlap(const struct chimera_vfs_claim_exact_range *a,
              const struct chimera_vfs_claim_exact_range *b, bool zero_point)
{
    return (zero_point || (a->length && b->length)) &&
        (__uint128_t) a->offset < (__uint128_t) b->offset + b->length &&
        (__uint128_t) b->offset < (__uint128_t) a->offset + a->length;
}

/* Record refs and owner list membership are guarded by owner->lock. Every
 * journal delta retains a ref until complete/reset, independently of public
 * linkage: another journal may remove a newly published row before its creator
 * has finished frontend publication. */
static bool
record_removed(const struct chimera_vfs_claim_journal *j, const struct exact_record *record)
{
    return record->removed_by == j;
}

static void
record_put(struct exact_record *record)
{
    assert(record->refs);
    if (!--record->refs) {
        assert(!record->in_owner_list);
        free(record);
    }
}

static void
record_owner_unlink(struct chimera_vfs_claim_owner *owner, struct exact_record *record)
{
    if (!record->in_owner_list) return;
    struct exact_record **link = &owner->records;
    while (*link && *link != record) link = &(*link)->next;
    assert(*link == record);
    *link = record->next;
    record->next = NULL;
    record->in_owner_list = false;
    record_put(record);
}

static void
record_unlink_claims(struct exact_record *record)
{
    for (unsigned i = 0; i < record->num_claims; i++) {
        if (record->claims[i].file) {
            chimera_vfs_claim_unlink_locked(record->claims[i].file, &record->claims[i]);
        }
    }
}

/* Caller holds both owner and file locks. */
static void
owner_drain(struct chimera_vfs_claim_owner *owner)
{
    struct exact_record *record = owner->records;
    owner->records = NULL;
    while (record) {
        struct exact_record *next = record->next;
        record_unlink_claims(record);
        record->in_owner_list = false;
        record_put(record);
        record = next;
    }
    owner->retired = true;
}

SYMBOL_EXPORT struct chimera_vfs_claim_owner *
chimera_vfs_claim_owner_create(struct chimera_vfs_file_state *file,
                               const struct chimera_claim_owner *identity)
{
    if (!file || !file->state || !identity || identity->proto != CHIMERA_CLAIM_PROTO_SMB2) {
        return NULL;
    }
    struct chimera_vfs_claim_owner *owner = calloc(1, sizeof(*owner));
    if (!owner) {
        return NULL;
    }
    owner->file = chimera_vfs_state_get(file->state, file->fh, file->fh_len, file->fh_hash, false);
    if (!owner->file) {
        free(owner);
        return NULL;
    }
    pthread_mutex_init(&owner->lock, NULL);
    atomic_init(&owner->refs, 1);
    owner->identity = *identity;
    return owner;
}

SYMBOL_EXPORT struct chimera_vfs_claim_owner *
chimera_vfs_claim_owner_create_compat(struct chimera_vfs_file_state *file,
                                      const struct chimera_claim_actor *actor, bool zero_point)
{
    if (!actor) return NULL;
    struct chimera_vfs_claim_owner *owner = chimera_vfs_claim_owner_create(file, &actor->owner);
    if (owner) {
        owner->handle_anchor = actor->op_handle;
        owner->zero_point = zero_point;
    }
    return owner;
}

SYMBOL_EXPORT void
chimera_vfs_claim_owner_ref(struct chimera_vfs_claim_owner *owner)
{
    atomic_fetch_add(&owner->refs, 1);
}

SYMBOL_EXPORT void
chimera_vfs_claim_owner_put(struct chimera_vfs_claim_owner *owner)
{
    if (atomic_fetch_sub(&owner->refs, 1) != 1) {
        return;
    }
    assert(owner->retired && !owner->pins && !owner->records);
    chimera_vfs_state_put(owner->file->state, owner->file);
    pthread_mutex_destroy(&owner->lock);
    free(owner);
}

SYMBOL_EXPORT bool
chimera_vfs_claim_owner_has_locks(struct chimera_vfs_claim_owner *owner)
{
    if (!owner) return false;
    pthread_mutex_lock(&owner->lock);
    bool held = false;
    for (struct exact_record *record = owner->records; record; record = record->next) {
        if (record->published) { held = true; break; }
    }
    pthread_mutex_unlock(&owner->lock);
    return held;
}

SYMBOL_EXPORT bool
chimera_vfs_claim_owner_is_retired(struct chimera_vfs_claim_owner *owner)
{
    if (!owner) return true;
    pthread_mutex_lock(&owner->lock);
    bool retired = owner->retired && !owner->pins;
    pthread_mutex_unlock(&owner->lock);
    return retired;
}

SYMBOL_EXPORT bool
chimera_vfs_claim_owner_retire(struct chimera_vfs_claim_owner *owner,
                              void (*complete)(void *), void *private_data)
{
    pthread_mutex_lock(&owner->lock);
    if (owner->retiring) {
        pthread_mutex_unlock(&owner->lock);
        return false;
    }
    owner->retiring = true;
    owner->retire_complete = complete;
    owner->retire_private = private_data;
    if (owner->pins) {
        pthread_mutex_unlock(&owner->lock);
        return true;
    }
    pthread_mutex_lock(&owner->file->lock);
    owner_drain(owner);
    pthread_mutex_unlock(&owner->file->lock);
    owner->retire_complete = NULL;
    pthread_mutex_unlock(&owner->lock);
    chimera_vfs_claim_replacement_complete(owner->file);
    if (complete) {
        complete(private_data);
    }
    return true;
}

SYMBOL_EXPORT struct chimera_vfs_claim_journal *
chimera_vfs_claim_journal_alloc(uint32_t max_owners, uint32_t max_changes)
{
    if (!max_owners || !max_changes || max_changes > UINT32_MAX / 2) {
        return NULL;
    }
    struct chimera_vfs_claim_journal *j = calloc(1, sizeof(*j));
    if (!j) {
        return NULL;
    }
    j->owners = calloc(max_owners, sizeof(*j->owners));
    j->retire_owners = calloc(max_owners, sizeof(*j->retire_owners));
    j->changes = calloc(max_changes, sizeof(*j->changes));
    j->excluded = calloc((size_t) max_changes * 2, sizeof(*j->excluded));
    if (!j->owners || !j->retire_owners || !j->changes || !j->excluded) {
        free(j->owners); free(j->retire_owners); free(j->changes); free(j->excluded); free(j);
        return NULL;
    }
    j->max_owners = max_owners;
    j->max_changes = max_changes;
    j->initial_max_changes = max_changes;
    return j;
}

SYMBOL_EXPORT enum chimera_vfs_error
chimera_vfs_claim_journal_bind(struct chimera_vfs_claim_journal *j,
                              struct chimera_vfs_claim_owner *owner)
{
    if (!j || !owner || j->phase != JOURNAL_ACTIVE) return CHIMERA_VFS_EINVAL;
    bool bound = false;
    for (uint32_t i = 0; i < j->num_owners; i++) bound |= j->owners[i] == owner;
    pthread_mutex_lock(&owner->lock);
    enum chimera_vfs_error status = CHIMERA_VFS_OK;
    if (owner->retiring || owner->retire_journal == j) {
        status = CHIMERA_VFS_EINTR;
    } else if (owner->retire_journal) {
        status = CHIMERA_VFS_EBUSY;
    } else if (!bound) {
        if (j->num_owners == j->max_owners) {
            status = CHIMERA_VFS_ENOSPC;
        } else {
            owner->pins++;
            chimera_vfs_claim_owner_ref(owner);
            j->owners[j->num_owners++] = owner;
        }
    }
    pthread_mutex_unlock(&owner->lock);
    return status;
}

static void
journal_release_owner(struct chimera_vfs_claim_owner *owner)
{
    void (*complete)(void *) = NULL;
    void *private_data = NULL;
    pthread_mutex_lock(&owner->lock);
    assert(owner->pins);
    owner->pins--;
    if (!owner->pins && owner->retiring) {
        pthread_mutex_lock(&owner->file->lock);
        owner_drain(owner);
        pthread_mutex_unlock(&owner->file->lock);
        complete = owner->retire_complete;
        private_data = owner->retire_private;
        owner->retire_complete = NULL;
    }
    pthread_mutex_unlock(&owner->lock);
    chimera_vfs_claim_replacement_complete(owner->file);
    if (complete) complete(private_data);
    chimera_vfs_claim_owner_put(owner);
}

SYMBOL_EXPORT bool
chimera_vfs_claim_journal_unbind_idle(struct chimera_vfs_claim_journal *j,
                                     struct chimera_vfs_claim_owner *owner)
{
    if (!j || !owner || j->phase != JOURNAL_ACTIVE) return false;
    for (uint32_t i = 0; i < j->num_retire_owners; i++) {
        if (j->retire_owners[i].owner == owner) return false;
    }
    for (uint32_t i = 0; i < j->num_changes; i++) {
        if (j->changes[i].owner == owner) return false;
    }
    uint32_t slot = 0;
    while (slot < j->num_owners && j->owners[slot] != owner) slot++;
    if (slot == j->num_owners) return false;
    memmove(&j->owners[slot], &j->owners[slot + 1],
        (j->num_owners - slot - 1) * sizeof(*j->owners));
    j->num_owners--;
    journal_release_owner(owner);
    return true;
}

static void
batch_init(struct chimera_vfs_claim_batch_result *result, uint32_t count)
{
    memset(result, 0, sizeof(*result));
    result->status = CHIMERA_VFS_OK;
    result->admission = CHIMERA_CLAIM_GRANTED;
    result->failed = count;
}

/* Retirement may snapshot many accepted records even in a one-op compound.
 * Extra snapshot capacity is bounded independently from ordinary op deltas. */
static bool
journal_retire_capacity(struct chimera_vfs_claim_journal *j, uint32_t count)
{
    if (count > CHIMERA_VFS_CLAIM_RETIRE_MAX_RECORDS - j->retire_changes ||
        j->retire_changes > UINT32_MAX / 2 - j->initial_max_changes ||
        count > UINT32_MAX / 2 - j->initial_max_changes - j->retire_changes) return false;
    uint32_t capacity = j->initial_max_changes + j->retire_changes + count;
    if (capacity <= j->max_changes) return true;
    struct exact_change *changes = calloc(capacity, sizeof(*changes));
    const struct chimera_vfs_claim **excluded = calloc((size_t) capacity * 2, sizeof(*excluded));
    if (!changes || !excluded) { free(changes); free(excluded); return false; }
    memcpy(changes, j->changes, j->num_changes * sizeof(*changes));
    free(j->changes);
    free(j->excluded);
    j->changes = changes;
    j->excluded = excluded;
    j->max_changes = capacity;
    return true;
}

SYMBOL_EXPORT enum chimera_vfs_error
chimera_vfs_claim_journal_retire_owner(struct chimera_vfs_claim_journal *j,
                                      struct chimera_vfs_claim_owner *owner)
{
    if (!j || !owner || j->phase != JOURNAL_ACTIVE) return CHIMERA_VFS_EINVAL;
    for (uint32_t i = 0; i < j->num_retire_owners; i++) {
        if (j->retire_owners[i].owner == owner) return CHIMERA_VFS_OK;
    }
    enum chimera_vfs_error status = chimera_vfs_claim_journal_bind(j, owner);
    if (status != CHIMERA_VFS_OK) return status;
    pthread_mutex_lock(&owner->lock);
    uint32_t count = 0;
    if (owner->retiring) {
        status = CHIMERA_VFS_EINTR;
    } else if (owner->pins != 1 || owner->retire_journal) {
        /* Whole-owner removal conflicts with every other active editor, but
         * must never wait holding another owner's retirement reservation. */
        status = CHIMERA_VFS_EBUSY;
    } else {
        for (struct exact_record *r = owner->records; r; r = r->next) {
            assert(!r->removed_by || r->removed_by == j);
            if (!r->removed_by && ++count > CHIMERA_VFS_CLAIM_RETIRE_MAX_RECORDS) break;
        }
        if (!journal_retire_capacity(j, count)) status = CHIMERA_VFS_ENOSPC;
    }
    if (status == CHIMERA_VFS_OK) {
        owner->retire_journal = j;
        j->retire_owners[j->num_retire_owners++] = (struct retire_owner) {
            .owner = owner, .first = j->num_changes, .count = count };
        j->retire_changes += count;
        for (struct exact_record *r = owner->records; r; r = r->next) {
            if (r->removed_by) continue;
            r->removed_by = j;
            r->refs++;
            j->changes[j->num_changes++] = (struct exact_change) { .owner = owner, .record = r };
        }
    }
    pthread_mutex_unlock(&owner->lock);
    if (status != CHIMERA_VFS_OK) chimera_vfs_claim_journal_unbind_idle(j, owner);
    return status;
}

SYMBOL_EXPORT uint32_t
chimera_vfs_claim_journal_retire_checkpoint(const struct chimera_vfs_claim_journal *j)
{
    assert(j && j->phase == JOURNAL_ACTIVE);
    return j->num_retire_owners;
}

SYMBOL_EXPORT void
chimera_vfs_claim_journal_retire_rewind(struct chimera_vfs_claim_journal *j, uint32_t checkpoint)
{
    assert(j && j->phase == JOURNAL_ACTIVE && checkpoint <= j->num_retire_owners);
    while (j->num_retire_owners > checkpoint) {
        struct retire_owner retired = j->retire_owners[--j->num_retire_owners];
        struct chimera_vfs_claim_owner *owner = retired.owner;
        assert(j->num_changes == retired.first + retired.count);
        pthread_mutex_lock(&owner->lock);
        assert(owner->retire_journal == j);
        while (j->num_changes > retired.first) {
            struct exact_change *change = &j->changes[--j->num_changes];
            assert(!change->add && change->owner == owner && change->record->removed_by == j);
            change->record->removed_by = NULL;
            record_put(change->record);
        }
        owner->retire_journal = NULL;
        j->retire_changes -= retired.count;
        pthread_mutex_unlock(&owner->lock);
        chimera_vfs_claim_journal_unbind_idle(j, owner);
    }
}

static void
record_init(struct exact_record *record, struct chimera_vfs_claim_owner *owner,
            const struct chimera_vfs_claim_exact_range *range)
{
    record->range = *range;
    if (!range->length && !owner->zero_point) {
        return;
    }
    record->num_claims = range->offset == 0 && range->length == UINT64_MAX ? 2 : 1;
    for (unsigned i = 0; i < record->num_claims; i++) {
        uint64_t offset = range->offset, length = range->length;
        if (record->num_claims == 2) {
            offset = i ? UINT64_MAX - 1 : 0;
            length = i ? 1 : UINT64_MAX - 1;
        }
        chimera_vfs_claim_init_range(&record->claims[i], range->exclusive, true,
                                     offset, length, &owner->identity);
        record->claims[i].op_handle = owner->handle_anchor;
        record->claims[i].provisional = 1;
        record->claims[i].local_only = 1;
    }
}

static bool
same_owner_conflict(struct chimera_vfs_claim_journal *j,
                    struct chimera_vfs_claim_owner *owner,
                    const struct chimera_vfs_claim_exact_range *range)
{
    if (!range->exclusive) {
        return false;
    }
    for (struct exact_record *r = owner->records; r; r = r->next) {
        if (!record_removed(j, r) && exact_overlap(&r->range, range, owner->zero_point)) {
            return true;
        }
    }
    return false;
}

/* Local immediate admission, no recall, courtesy reclaim, projection or pump.
 * Frontends must coordinate cache breaks explicitly before this operation. */
static enum chimera_vfs_claim_result
record_reserve(struct chimera_vfs_claim_journal *j, struct chimera_vfs_claim_owner *owner,
               struct exact_record *record, struct chimera_vfs_claim_conflict *conflict)
{
    struct chimera_vfs_file_state *file = owner->file;
    enum chimera_vfs_claim_result result = CHIMERA_CLAIM_GRANTED;
    uint32_t excluded = 0;
    for (uint32_t i = 0; i < j->num_changes; i++) {
        struct exact_change *change = &j->changes[i];
        if (!change->add && change->owner->file == file) {
            for (unsigned k = 0; k < change->record->num_claims; k++) {
                j->excluded[excluded++] = &change->record->claims[k];
            }
        }
    }
    pthread_mutex_lock(&file->lock);
    if (record->num_claims) {
        for (struct chimera_vfs_claim *cache = file->claims[CHIMERA_CLAIM_CLASS_CACHE];
             cache; cache = cache->next) {
            if (cache->construct != CHIMERA_CONSTRUCT_IMPLICIT &&
                chimera_vfs_claim_advertised(cache) &&
                !chimera_claim_owner_equal(&cache->owner, &owner->identity) &&
                !chimera_claim_owner_same_key(&cache->owner, &owner->identity)) {
                chimera_vfs_claim_conflict_fill(cache, conflict);
                result = CHIMERA_CLAIM_BREAKING;
                break;
            }
        }
    }
    for (unsigned i = 0; result == CHIMERA_CLAIM_GRANTED && i < record->num_claims; i++) {
        struct chimera_vfs_claim *claim = &record->claims[i], *blocker = NULL;
        claim->admit_excluded = j->excluded;
        claim->admit_num_excluded = excluded;
        result = chimera_vfs_claim_admit_locked(file, claim, &blocker);
        claim->admit_excluded = NULL;
        claim->admit_num_excluded = 0;
        if (result == CHIMERA_CLAIM_GRANTED) {
            chimera_vfs_claim_link_locked(file, claim);
        } else if (blocker) {
            chimera_vfs_claim_conflict_fill(blocker, conflict);
        }
    }
    if (result != CHIMERA_CLAIM_GRANTED) {
        record_unlink_claims(record);
    }
    pthread_mutex_unlock(&file->lock);
    return result;
}

/* Drop only this acquire batch, preserving earlier journal commands. */
static void
batch_rollback(struct chimera_vfs_claim_journal *j, uint32_t first)
{
    while (j->num_changes > first) {
        struct exact_change *change = &j->changes[--j->num_changes];
        assert(change->add);
        pthread_mutex_lock(&change->owner->file->lock);
        record_unlink_claims(change->record);
        pthread_mutex_unlock(&change->owner->file->lock);
        record_owner_unlink(change->owner, change->record);
        record_put(change->record);
    }
}

SYMBOL_EXPORT void
chimera_vfs_claim_journal_acquire(struct chimera_vfs_claim_journal *j,
                                 struct chimera_vfs_claim_owner *owner,
                                 const struct chimera_vfs_claim_exact_range *ranges, uint32_t count,
                                 struct chimera_vfs_claim_batch_result *result)
{
    batch_init(result, count);
    result->status = chimera_vfs_claim_journal_bind(j, owner);
    if (result->status != CHIMERA_VFS_OK || (!ranges && count)) {
        if (result->status == CHIMERA_VFS_OK) result->status = CHIMERA_VFS_EINVAL;
        result->failed = 0;
        return;
    }
    if (count > j->max_changes - j->num_changes) {
        result->status = CHIMERA_VFS_ENOSPC;
        result->failed = 0;
        return;
    }
    /* Validate the entire acquisition request before changing the overlay. */
    for (uint32_t i = 0; i < count; i++) {
        if ((__uint128_t) ranges[i].offset + ranges[i].length > ((__uint128_t) 1 << 64)) {
            result->status = CHIMERA_VFS_EINVAL; result->failed = i; return;
        }
        for (uint32_t k = 0; k < i; k++) {
            if (exact_overlap(&ranges[i], &ranges[k], owner->zero_point)) {
                result->status = CHIMERA_VFS_EINVAL; result->failed = i; return;
            }
        }
    }
    uint32_t first = j->num_changes;
    pthread_mutex_lock(&owner->lock);
    if (owner->retiring || owner->retire_journal) {
        result->status = owner->retiring || owner->retire_journal == j ?
            CHIMERA_VFS_EINTR : CHIMERA_VFS_EBUSY;
        result->failed = 0;
        pthread_mutex_unlock(&owner->lock);
        return;
    }
    for (uint32_t i = 0; i < count; i++) {
        struct exact_record *record = NULL;
        if (same_owner_conflict(j, owner, &ranges[i])) {
            result->status = CHIMERA_VFS_EAGAIN;
            result->admission = CHIMERA_CLAIM_DENIED;
            result->self_conflict = true;
        } else if (!(record = calloc(1, sizeof(*record)))) {
            result->status = CHIMERA_VFS_ENOSPC;
        } else {
            record_init(record, owner, &ranges[i]);
            result->admission = record_reserve(j, owner, record, &result->conflict);
            if (result->admission != CHIMERA_CLAIM_GRANTED) {
                result->status = CHIMERA_VFS_EAGAIN;
                free(record);
                record = NULL;
            }
        }
        if (result->status != CHIMERA_VFS_OK) {
            result->failed = i;
            batch_rollback(j, first);
            pthread_mutex_unlock(&owner->lock);
            return;
        }
        record->refs = 2; /* owner list + creating journal */
        record->in_owner_list = true;
        record->created_by = j;
        struct exact_record **tail = &owner->records;
        while (*tail) tail = &(*tail)->next;
        *tail = record;
        j->changes[j->num_changes++] = (struct exact_change) { .owner = owner, .record = record, .add = true };
    }
    pthread_mutex_unlock(&owner->lock);
    result->applied = count;
}

SYMBOL_EXPORT void
chimera_vfs_claim_journal_unlock(struct chimera_vfs_claim_journal *j,
                                struct chimera_vfs_claim_owner *owner,
                                const struct chimera_vfs_claim_exact_range *ranges, uint32_t count,
                                struct chimera_vfs_claim_batch_result *result)
{
    batch_init(result, count);
    result->status = chimera_vfs_claim_journal_bind(j, owner);
    if (result->status != CHIMERA_VFS_OK || (!ranges && count)) {
        if (result->status == CHIMERA_VFS_OK) result->status = CHIMERA_VFS_EINVAL;
        result->failed = 0;
        return;
    }
    pthread_mutex_lock(&owner->lock);
    if (owner->retiring || owner->retire_journal) {
        result->status = owner->retiring || owner->retire_journal == j ?
            CHIMERA_VFS_EINTR : CHIMERA_VFS_EBUSY;
        result->failed = 0;
        pthread_mutex_unlock(&owner->lock);
        return;
    }
    for (uint32_t i = 0; i < count; i++) {
        struct exact_record *match = NULL;
        if (j->num_changes == j->max_changes) {
            result->status = CHIMERA_VFS_ENOSPC;
        } else {
            bool contended = false;
            for (struct exact_record *r = owner->records; r; r = r->next) {
                if (r->range.offset != ranges[i].offset || r->range.length != ranges[i].length ||
                    (!r->published && r->created_by != j)) continue;
                if (!r->removed_by) {
                    match = r;
                    break;
                }
                contended |= r->removed_by != j;
            }
            /* A peer's tentative removal can still abort. Report a command
             * conflict, never "absent" or a wait retaining our other edits. */
            if (!match) result->status = contended ? CHIMERA_VFS_EBUSY : CHIMERA_VFS_ENOENT;
        }
        if (result->status != CHIMERA_VFS_OK) {
            result->failed = i;
            pthread_mutex_unlock(&owner->lock);
            return;
        }
        match->removed_by = j;
        match->refs++; /* removing journal, retained through frontend publish */
        j->changes[j->num_changes++] = (struct exact_change) { .owner = owner, .record = match };
        result->applied++;
    }
    pthread_mutex_unlock(&owner->lock);
}

SYMBOL_EXPORT bool
chimera_vfs_claim_journal_io_denied(struct chimera_vfs_claim_journal *j,
                                   struct chimera_vfs_file_state *file,
                                   uint64_t offset, uint64_t length, bool write,
                                   const struct chimera_claim_actor *actor)
{
    bool denied = false;
    if (!file || (!write && !length)) return false;
    pthread_mutex_lock(&file->lock);
    for (struct chimera_vfs_claim *claim = file->claims[CHIMERA_CLAIM_CLASS_RANGE]; claim; claim = claim->next) {
        bool excluded = false;
        for (uint32_t i = 0; i < j->num_changes && !excluded; i++) {
            struct exact_change *change = &j->changes[i];
            if (!change->add) {
                for (unsigned k = 0; k < change->record->num_claims; k++) {
                    excluded |= claim == &change->record->claims[k];
                }
            }
        }
        /* Claim geometry retains the legacy EOF sentinel. I/O geometry is
         * finite, and retains the existing interior-point zero-WRITE test. */
        __uint128_t end = claim->length == UINT64_MAX ? ((__uint128_t) 1 << 64) :
            (__uint128_t) claim->offset + claim->length;
        bool overlap = (__uint128_t) claim->offset < (__uint128_t) offset + length &&
            (__uint128_t) offset < end;
        if (excluded || claim->break_state == CHIMERA_CLAIM_BREAK_REVOKED || !overlap) continue;
        bool self = actor && (chimera_claim_owner_equal(&claim->owner, &actor->owner) ||
            chimera_claim_owner_same_key(&claim->owner, &actor->owner) ||
            (actor->op_handle && actor->op_handle == claim->op_handle));
        if ((claim->used & CHIMERA_CLAIM_LW) ? !self : write) { denied = true; break; }
    }
    pthread_mutex_unlock(&file->lock);
    return denied;
}

SYMBOL_EXPORT uint32_t
chimera_vfs_claim_journal_excluded(const struct chimera_vfs_claim_journal *j,
                                  const struct chimera_vfs_file_state *file,
                                  const struct chimera_vfs_claim **out, uint32_t capacity)
{
    uint32_t count = 0;
    for (uint32_t i = 0; i < j->num_changes; i++) {
        if (!j->changes[i].add && j->changes[i].owner->file == file) {
            count += j->changes[i].record->num_claims;
        }
    }
    if (out && capacity >= count) {
        uint32_t index = 0;
        for (uint32_t i = 0; i < j->num_changes; i++) {
            if (!j->changes[i].add && j->changes[i].owner->file == file) {
                for (unsigned k = 0; k < j->changes[i].record->num_claims; k++) {
                    out[index++] = &j->changes[i].record->claims[k];
                }
            }
        }
    }
    return count;
}

SYMBOL_EXPORT void
chimera_vfs_claim_journal_seal(struct chimera_vfs_claim_journal *j)
{
    assert(j->phase == JOURNAL_ACTIVE);
    j->phase = JOURNAL_SEALED;
}

SYMBOL_EXPORT void
chimera_vfs_claim_journal_publish(struct chimera_vfs_claim_journal *j)
{
    assert(j->phase == JOURNAL_SEALED);
    for (uint32_t b = 0; b < j->num_owners; b++) {
        struct chimera_vfs_claim_owner *owner = j->owners[b];
        pthread_mutex_lock(&owner->lock);
        assert(owner->pins);
        if (owner->retire_journal == j) owner->retiring = true;
        pthread_mutex_lock(&owner->file->lock);
        for (uint32_t i = 0; i < j->num_changes; i++) {
            struct exact_change *change = &j->changes[i];
            if (change->owner != owner || change->add) continue;
            assert(change->record->removed_by == j);
            record_unlink_claims(change->record);
            record_owner_unlink(owner, change->record);
        }
        for (uint32_t i = 0; i < j->num_changes; i++) {
            struct exact_change *change = &j->changes[i];
            if (change->owner != owner || !change->add) continue;
            change->record->created_by = NULL;
            if (record_removed(j, change->record)) continue;
            for (unsigned k = 0; k < change->record->num_claims; k++) {
                assert(change->record->claims[k].file == owner->file);
                change->record->claims[k].provisional = 0;
            }
            change->record->published = true;
        }
        pthread_mutex_unlock(&owner->file->lock);
        pthread_mutex_unlock(&owner->lock);
    }
    /* Keep delta refs until complete: newly accepted rows can already be
     * removed by another journal before this frontend finishes publication. */
    j->phase = JOURNAL_PUBLISHED;
}

static void
journal_release_owners(struct chimera_vfs_claim_journal *j)
{
    for (uint32_t b = 0; b < j->num_retire_owners; b++) {
        struct chimera_vfs_claim_owner *owner = j->retire_owners[b].owner;
        pthread_mutex_lock(&owner->lock);
        assert(owner->retire_journal == j);
        owner->retire_journal = NULL;
        pthread_mutex_unlock(&owner->lock);
    }
    j->num_retire_owners = 0;
    j->retire_changes = 0;
    for (uint32_t b = 0; b < j->num_owners; b++) journal_release_owner(j->owners[b]);
    j->num_owners = 0;
}

static void
journal_release_changes(struct chimera_vfs_claim_journal *j, bool abort)
{
    for (uint32_t i = 0; i < j->num_changes; i++) {
        struct exact_change *change = &j->changes[i];
        struct chimera_vfs_claim_owner *owner = change->owner;
        struct exact_record *record = change->record;
        pthread_mutex_lock(&owner->lock);
        if (abort && change->add) {
            pthread_mutex_lock(&owner->file->lock);
            record_unlink_claims(record);
            record_owner_unlink(owner, record);
            pthread_mutex_unlock(&owner->file->lock);
        }
        if (!change->add) {
            assert(record->removed_by == j);
            record->removed_by = NULL;
        }
        record_put(record);
        pthread_mutex_unlock(&owner->lock);
    }
    j->num_changes = 0;
}

SYMBOL_EXPORT void
chimera_vfs_claim_journal_complete(struct chimera_vfs_claim_journal *j)
{
    assert(j->phase == JOURNAL_PUBLISHED);
    j->phase = JOURNAL_COMPLETE;
    journal_release_changes(j, false);
    journal_release_owners(j);
}

SYMBOL_EXPORT void
chimera_vfs_claim_journal_reset(struct chimera_vfs_claim_journal *j)
{
    assert(j->phase == JOURNAL_ACTIVE || j->phase == JOURNAL_SEALED || j->phase == JOURNAL_COMPLETE);
    journal_release_changes(j, true);
    journal_release_owners(j);
    j->phase = JOURNAL_ACTIVE;
}

SYMBOL_EXPORT void
chimera_vfs_claim_journal_free(struct chimera_vfs_claim_journal *j)
{
    if (!j) return;
    assert(j->phase != JOURNAL_PUBLISHED);
    chimera_vfs_claim_journal_reset(j);
    free(j->owners); free(j->retire_owners); free(j->changes); free(j->excluded); free(j);
}
