// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#include <stdio.h>
#include <string.h>
#undef NDEBUG
#include <assert.h>
#include "vfs/vfs_claim_access.h"
#include "vfs/vfs_internal.h"
#include "common/logging.h"

/* A reusable test barrier on the same native primitives as the arbiter. */
struct test_barrier {
    evpl_mutex_t mutex;
    evpl_cond_t  cond;
    unsigned int count, waiting, generation;
};

static int
test_barrier_init(
    struct test_barrier *barrier,
    const void          *attr,
    unsigned int         count)
{
    (void) attr;
    memset(barrier, 0, sizeof(*barrier));
    barrier->count = count;
    assert(!evpl_mutex_init(&barrier->mutex, NULL));
    assert(!evpl_cond_init(&barrier->cond, NULL));
    return 0;
} /* test_barrier_init */

static void
test_barrier_wait(struct test_barrier *barrier)
{
    evpl_mutex_lock(&barrier->mutex);
    unsigned int generation = barrier->generation;
    if (++barrier->waiting == barrier->count) {
        barrier->waiting = 0;
        barrier->generation++;
        evpl_cond_broadcast(&barrier->cond);
    } else {
        while (generation == barrier->generation) {
            evpl_cond_wait(&barrier->cond, &barrier->mutex);
        }
    }
    evpl_mutex_unlock(&barrier->mutex);
} /* test_barrier_wait */

static void
test_barrier_destroy(struct test_barrier *barrier)
{
    evpl_cond_destroy(&barrier->cond);
    evpl_mutex_destroy(&barrier->mutex);
} /* test_barrier_destroy */

static struct chimera_vfs_claim_access_owner *
make_owner(
    struct chimera_vfs_file_state *file,
    unsigned                       id)
{
    struct chimera_claim_owner             identity = { .proto             = CHIMERA_CLAIM_PROTO_SMB2,
                                                        .client_key        = id,                      .owner_lo = id };
    struct chimera_vfs_claim               template;

    chimera_vfs_claim_init_smb_open(&template, CHIMERA_CLAIM_R, CHIMERA_CLAIM_W, &identity);
    struct chimera_vfs_claim_access_owner *owner = chimera_vfs_claim_access_owner_alloc(file, &template);
    assert(owner);
    assert(chimera_vfs_claim_access_owner_claim(owner) != &template);
    assert(chimera_vfs_claim_try_acquire(file->state, file,
                                         chimera_vfs_claim_access_owner_claim(owner), NULL) == CHIMERA_CLAIM_GRANTED);
    return owner;
} /* make_owner */

static enum chimera_vfs_claim_result
probe(
    struct chimera_vfs_file_state           *file,
    struct chimera_vfs_claim_access_journal *j)
{
    struct chimera_claim_owner      identity = { .proto      = CHIMERA_CLAIM_PROTO_SMB2,
                                                 .client_key = 900,                     .owner_lo= 900 };
    struct chimera_vfs_claim        request;
    const struct chimera_vfs_claim *excluded[2];

    chimera_vfs_claim_init_smb_open(&request, CHIMERA_CLAIM_W, 0, &identity);
    if (j) {
        request.admit_num_excluded = chimera_vfs_claim_access_journal_excluded(j, file, excluded, 2);
        assert(request.admit_num_excluded <= 2);
        request.admit_excluded = excluded;
    }
    return chimera_vfs_claim_test(file, &request, NULL);
} /* probe */

struct fence_completion {
    unsigned calls;
    enum chimera_vfs_claim_result result;
    bool     fenced;
};

static void
fence_complete(
    enum chimera_vfs_claim_result            result,
    struct chimera_vfs_claim                *claim,
    const struct chimera_vfs_claim_conflict *conflict,
    void                                    *private)
{
    struct fence_completion *test = private;

    test->calls++;
    test->result = result;
    test->fenced = conflict && conflict->admission_fenced;
    assert((claim != NULL) == (result == CHIMERA_CLAIM_GRANTED));
} /* fence_complete */

static void
check_admission_fence(struct chimera_vfs_file_state *file)
{
    struct chimera_vfs_state             *state    = file->state;
    struct chimera_claim_owner            identity = { .proto      = CHIMERA_CLAIM_PROTO_SMB2,
                                                       .client_key = 910,                     .owner_lo = 910 };
    struct chimera_vfs_claim              blocker, waiting, fresh, own, inert;
    struct chimera_vfs_pending_acquire    ticket = { 0 }, fresh_ticket = { 0 };
    struct fence_completion               queued = { 0 }, immediate = { 0 };
    struct chimera_vfs_claim_conflict     conflict;
    struct chimera_vfs_claim_access_fence first = { 0 }, same = { 0 }, other = { 0 };
    int                                   cookie, other_cookie;

    chimera_vfs_claim_init_smb_open(&blocker, CHIMERA_CLAIM_R, CHIMERA_CLAIM_W, &identity);
    assert(chimera_vfs_claim_try_acquire(state, file, &blocker, NULL) == CHIMERA_CLAIM_GRANTED);
    identity.owner_lo++;
    chimera_vfs_claim_init_smb_open(&waiting, CHIMERA_CLAIM_W, 0, &identity);
    chimera_vfs_claim_acquire(NULL, state, file, &waiting, &ticket, true, true,
                              fence_complete, NULL, &queued);
    assert(ticket.queued && !queued.calls);
    assert(chimera_vfs_claim_access_fence_acquire(&first, file, &cookie));
    assert(chimera_vfs_claim_access_fence_acquire(&first, file, &cookie));
    assert(!chimera_vfs_claim_access_fence_acquire(&other, file, &other_cookie));
    assert(!other.file);
    assert(chimera_vfs_claim_access_fence_acquire(&same, file, &cookie));
    chimera_vfs_claim_release(state, file, &blocker); /* Pump cannot insert waiting. */
    assert(ticket.queued && !queued.calls && !waiting.file);

    chimera_vfs_claim_init_smb_open(&inert, 0, 0, &identity);
    assert(chimera_vfs_claim_test(file, &inert, NULL) == CHIMERA_CLAIM_GRANTED);
    assert(chimera_vfs_claim_try_acquire(state, file, &inert, &conflict) == CHIMERA_CLAIM_DENIED);
    assert(conflict.admission_fenced && !inert.file && !conflict.policy_tag);
    chimera_vfs_claim_init_smb_open(&fresh, CHIMERA_CLAIM_R, 0, &identity);
    chimera_vfs_claim_acquire(NULL, state, file, &fresh, &fresh_ticket, true, true,
                              fence_complete, NULL, &immediate);
    assert(immediate.calls == 1 && immediate.result == CHIMERA_CLAIM_DENIED && immediate.fenced);
    assert(!fresh_ticket.queued && !fresh.file);
    chimera_vfs_claim_init_smb_open(&own, CHIMERA_CLAIM_R, 0, &identity);
    own.admission_cookie = &cookie;
    assert(chimera_vfs_claim_try_acquire(state, file, &own, NULL) == CHIMERA_CLAIM_GRANTED);
    own.admission_cookie = NULL;
    chimera_vfs_claim_release(state, file, &own);
    assert(ticket.queued && !queued.calls);
    chimera_vfs_claim_access_fence_release(&first);
    assert(ticket.queued && !queued.calls); /* Matching second holder still fences. */
    chimera_vfs_claim_access_fence_release(&first); /* Idempotent release. */
    chimera_vfs_claim_access_fence_release(&same);
    assert(queued.calls == 1 && queued.result == CHIMERA_CLAIM_GRANTED && !queued.fenced);
    assert(!ticket.queued && waiting.file == file);
    chimera_vfs_claim_release(state, file, &waiting);
    assert(!file->access_fence_cookie && !file->access_fence_refs);
    assert(chimera_vfs_claim_access_fence_acquire(&other, file, &other_cookie));
    chimera_vfs_claim_access_fence_release(&other);
} /* check_admission_fence */

static void
check_revoke_storage_anchor(struct chimera_vfs_file_state *file)
{
    struct chimera_claim_owner               identity = { .proto      = CHIMERA_CLAIM_PROTO_SMB2,
                                                          .client_key = 920,                     .owner_lo = 920 };
    struct chimera_vfs_claim                 cache, access;
    struct chimera_vfs_claim_grant          *grant;
    int                                      member;

    chimera_vfs_claim_init_rqls(&cache, CHIMERA_CLAIM_CR, &identity);
    assert(chimera_vfs_claim_grant_acquire(file->state, file, &cache, 0, 1,
                                           CHIMERA_CLAIM_GRANT_EXACT, &member, NULL, &grant, NULL) ==
           CHIMERA_CLAIM_GRANTED);
    chimera_vfs_claim_init_smb_open(&access, CHIMERA_CLAIM_R, 0, &identity);
    access.own_cache = grant;
    struct chimera_vfs_claim_access_owner   *owner = chimera_vfs_claim_access_owner_alloc(file, &access);
    assert(owner);
    assert(chimera_vfs_claim_try_acquire(file->state, file,
                                         chimera_vfs_claim_access_owner_claim(owner), NULL) == CHIMERA_CLAIM_GRANTED);
    struct chimera_vfs_claim_access_journal *journal = chimera_vfs_claim_access_journal_alloc(1);
    assert(journal && chimera_vfs_claim_access_journal_retire(journal, owner) == CHIMERA_VFS_OK);
    struct chimera_vfs_file_state           *file_pin = chimera_vfs_state_get(file->state,
                                                                              file->fh, file->fh_len, file->fh_hash,
                                                                              false);
    assert(file_pin == file);
    evpl_mutex_lock(&file->lock);
    struct chimera_vfs_claim_grant          *grant_pin = chimera_vfs_claim_pin_grant(&grant->claim);
    assert(grant_pin == grant && grant->refcount == 2 && grant->members == &member);
    assert(grant->claim.used == CHIMERA_CLAIM_CR && grant->epoch == 1);
    assert(!chimera_vfs_claim_pin_grant(chimera_vfs_claim_access_owner_claim(owner)));
    evpl_mutex_unlock(&file->lock);
    chimera_vfs_claim_access_owner_retire(owner);
    chimera_vfs_claim_grant_revoke_empty(grant);
    assert(grant->claim.used == CHIMERA_CLAIM_CR); /* A live member keeps its rights. */
    evpl_mutex_lock(&file->lock);
    grant->members = NULL;
    evpl_mutex_unlock(&file->lock);
    chimera_vfs_claim_grant_revoke_empty(grant);
    assert(!grant->claim.used && !grant->claim.advertised && grant->refcount == 2);
    /* The protocol drops its grant ref while a CLOSE snapshot still pins it.
     * ACCESS own_cache remains valid through the journal's final cleanup. */
    chimera_vfs_claim_grant_release(file->state, grant, true);
    assert(grant_pin->refcount == 1);
    assert(chimera_vfs_claim_access_owner_claim(owner)->own_cache == grant_pin);
    assert(!chimera_vfs_claim_access_owner_is_retired(owner));
    chimera_vfs_claim_grant_revoke_empty(grant); /* Idempotent rights cutoff. */
    chimera_vfs_claim_access_journal_free(journal);
    assert(chimera_vfs_claim_access_owner_is_retired(owner));
    chimera_vfs_claim_access_owner_put(owner);
    chimera_vfs_claim_grant_release(file->state, grant_pin, true);
    chimera_vfs_state_put(file->state, file_pin);
} /* check_revoke_storage_anchor */

struct member_probe { struct member_probe *next; };
struct member_join {
    struct chimera_vfs_file_state  *file;
    struct chimera_vfs_claim        template;
    struct chimera_vfs_claim_grant *grant;
    struct member_probe             member;
    struct test_barrier             attached, finish;
    bool                            acquire;
};
static void *
join_member(void *private_data)
{
    struct member_join *join = private_data;

    if (join->acquire) {
        assert(chimera_vfs_claim_grant_acquire_member(join->file->state, join->file,
                                                      &join->template, 0, 1, 900, CHIMERA_CLAIM_GRANT_EXACT,
                                                      &join->member, &join->member.next, &join->grant, NULL) ==
               CHIMERA_CLAIM_GRANTED);
    } else {
        join->grant = chimera_vfs_claim_grant_coalesce_member(join->file,
                                                              &join->template.owner, join->template.used, 0, &join->
                                                              member, &join->member.next);
        assert(join->grant);
    }
    /* Simulate CREATE preemption immediately after acquisition. CLOSE on the
     * other thread must see its member, without a subsequent frontend call. */
    test_barrier_wait(&join->attached);
    test_barrier_wait(&join->finish);
    return NULL;
} /* join_member */
static void
check_atomic_member_attach(struct chimera_vfs_file_state *file)
{
    struct chimera_claim_owner identity = { .proto      = CHIMERA_CLAIM_PROTO_SMB2,
                                            .client_key = 921,                     .owner_lo = 921 };

    for (unsigned int acquire = 0; acquire < 2; acquire++) {
        struct member_probe             first = { 0 };
        struct member_join              join  = { .file = file, .acquire = acquire };
        struct chimera_vfs_claim_grant *grant;
        chimera_vfs_claim_init_rqls(&join.template, CHIMERA_CLAIM_CR | CHIMERA_CLAIM_H, &identity);
        assert(chimera_vfs_claim_grant_acquire_member(file->state, file, &join.template,
                                                      0, 1, 43, CHIMERA_CLAIM_GRANT_EXACT, &first, &first.next, &grant,
                                                      NULL) == CHIMERA_CLAIM_GRANTED);
        assert(grant->members == &first && !first.next && grant->epoch == 43);
        assert(!test_barrier_init(&join.attached, NULL, 2));
        assert(!test_barrier_init(&join.finish, NULL, 2));
        evpl_native_thread_t            worker;
        assert(!evpl_native_thread_create(&worker, NULL, join_member, &join));
        test_barrier_wait(&join.attached);
        evpl_mutex_lock(&file->lock);
        assert(join.grant == grant && grant->members == &join.member && join.member.next == &first);
        assert(grant->epoch == 43); /* Coalescing must ignore requester epoch 900. */
        join.member.next = NULL; /* Accepted removal of the original member. */
        evpl_mutex_unlock(&file->lock);
        chimera_vfs_claim_grant_revoke_empty(grant);
        assert(grant->claim.used == (CHIMERA_CLAIM_CR | CHIMERA_CLAIM_H));
        chimera_vfs_claim_grant_release(file->state, grant, true);
        assert(join.grant->refcount == 1);
        test_barrier_wait(&join.finish);
        assert(!evpl_native_thread_join(worker, NULL));
        evpl_mutex_lock(&file->lock);
        grant->members = NULL;
        evpl_mutex_unlock(&file->lock);
        chimera_vfs_claim_grant_revoke_empty(grant);
        assert(!grant->claim.used);
        chimera_vfs_claim_grant_release(file->state, grant, true);
        test_barrier_destroy(&join.attached);
        test_barrier_destroy(&join.finish);
    }
} /* check_atomic_member_attach */

static void
unexpected_oplock_break(
    struct chimera_vfs_claim *claim,
    uint8_t                   needed,
    void                     *private_data)
{
    (void) claim; (void) needed; (void) private_data;
    assert(!"opportunistic publication must never recall a peer");
} /* unexpected_oplock_break */
static void
check_prepared_oplock(struct chimera_vfs_file_state *file)
{
    struct chimera_claim_owner      identity = { .proto      = CHIMERA_CLAIM_PROTO_SMB2,
                                                 .client_key = 922,                     .owner_lo = 922 };
    struct chimera_claim_owner      other = { .proto      = CHIMERA_CLAIM_PROTO_SMB2,
                                              .client_key = 923,                     .owner_lo = 923 };
    struct chimera_vfs_claim        template, peer;
    struct member_probe             member    = { 0 };
    struct chimera_vfs_claim_grant *candidate = calloc(1, sizeof(*candidate));

    assert(candidate);
    chimera_vfs_claim_init_oplock(&template, CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW | CHIMERA_CLAIM_H, &identity);
    template.break_cb = unexpected_oplock_break;
    chimera_vfs_claim_init_oplock(&peer, CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW, &other);
    peer.break_cb = unexpected_oplock_break;
    assert(chimera_vfs_claim_try_acquire(file->state, file, &peer, NULL) == CHIMERA_CLAIM_GRANTED);
    evpl_mutex_lock(&file->lock);
    assert(!chimera_vfs_claim_grant_publish_oplock_locked(file, candidate, &template, &member));
    assert(!candidate->refcount && !candidate->claim.file && peer.break_state == CHIMERA_CLAIM_BREAK_IDLE);
    evpl_mutex_unlock(&file->lock);
    chimera_vfs_claim_release(file->state, file, &peer);
    chimera_vfs_claim_init_oplock(&peer, CHIMERA_CLAIM_CR, &other);
    peer.break_cb = unexpected_oplock_break;
    assert(chimera_vfs_claim_try_acquire(file->state, file, &peer, NULL) == CHIMERA_CLAIM_GRANTED);
    evpl_mutex_lock(&file->lock);
    assert(chimera_vfs_claim_grant_publish_oplock_locked(file, candidate, &template, &member));
    assert(candidate->claim.used == CHIMERA_CLAIM_CR);
    assert(candidate->claim.construct == CHIMERA_CONSTRUCT_OPLOCK_II && candidate->members == &member);
    assert(peer.break_state == CHIMERA_CLAIM_BREAK_IDLE);
    evpl_mutex_unlock(&file->lock);
    chimera_vfs_claim_grant_release(file->state, candidate, true);
    chimera_vfs_claim_release(file->state, file, &peer);
    candidate = calloc(1, sizeof(*candidate));
    assert(candidate);
    evpl_mutex_lock(&file->lock);
    assert(chimera_vfs_claim_grant_publish_oplock_locked(file, candidate, &template, &member));
    assert(candidate->claim.used == template.used && candidate->claim.construct == CHIMERA_CONSTRUCT_OPLOCK_BATCH);
    evpl_mutex_unlock(&file->lock);
    chimera_vfs_claim_grant_release(file->state, candidate, true);
} /* check_prepared_oplock */

struct first_grant_join {
    struct chimera_vfs_file_state  *file;
    struct chimera_vfs_claim        template;
    struct member_probe             member;
    struct chimera_vfs_claim_grant *grant;
    struct test_barrier            *start;
    uint16_t                        epoch;
};
static void *
first_grant_member(void *private_data)
{
    struct first_grant_join *join = private_data;

    test_barrier_wait(join->start);
    assert(chimera_vfs_claim_grant_acquire_member(join->file->state, join->file,
                                                  &join->template, 0, 1, join->epoch, CHIMERA_CLAIM_GRANT_EXACT,
                                                  &join->member, &join->member.next, &join->grant, NULL) ==
           CHIMERA_CLAIM_GRANTED);
    return NULL;
} /* first_grant_member */
static void
check_first_grant_race(struct chimera_vfs_file_state *file)
{
    struct chimera_claim_owner identity = { .proto      = CHIMERA_CLAIM_PROTO_SMB2,
                                            .client_key = 925,                     .owner_lo = 925, .key = { 0x25 } };

    for (unsigned int run = 0; run < 32; run++) {
        struct test_barrier     start;
        assert(!test_barrier_init(&start, NULL, 3));
        struct first_grant_join joins[2] = {
            { .file = file, .start = &start, .epoch = 43  },
            { .file = file, .start = &start, .epoch = 903 },
        };
        evpl_native_thread_t    workers[2];
        for (unsigned int i = 0; i < 2; i++) {
            chimera_vfs_claim_init_rqls(&joins[i].template, CHIMERA_CLAIM_CR, &identity);
            assert(!evpl_native_thread_create(&workers[i], NULL, first_grant_member, &joins[i]));
        }
        test_barrier_wait(&start);
        for (unsigned int i = 0; i < 2; i++) {
            assert(!evpl_native_thread_join(workers[i], NULL));
        }
        struct chimera_vfs_claim_grant *grant = joins[0].grant;
        assert(grant == joins[1].grant && file->grants == grant && !grant->grant_next);
        assert(file->claims[CHIMERA_CLAIM_CLASS_CACHE] == &grant->claim && !grant->claim.next);
        assert(grant->refcount == 2 && (grant->epoch == 43 || grant->epoch == 903));
        struct member_probe            *head = grant->members;
        assert((head == &joins[0].member && head->next == &joins[1].member) ||
               (head == &joins[1].member && head->next == &joins[0].member));
        assert(!head->next->next);
        grant->members = NULL;
        chimera_vfs_claim_grant_release(file->state, grant, true);
        chimera_vfs_claim_grant_release(file->state, grant, true);
        test_barrier_destroy(&start);
    }
} /* check_first_grant_race */
static void
seeded_epoch_break(
    struct chimera_vfs_claim *claim,
    uint8_t                   needed,
    void                     *private_data)
{
    unsigned int *calls = private_data;

    (void) needed;
    assert(claim->grant->epoch == 44); /* Seed 43 plus the first v2 break. */
    assert(claim->file->grants == claim->grant); /* No half-published grant. */
    (*calls)++;
} /* seeded_epoch_break */
static void
check_prepared_read_lease(
    struct chimera_vfs_file_state *file,
    bool                           directory)
{
    struct chimera_claim_owner      identity = { .proto      = CHIMERA_CLAIM_PROTO_SMB2,
                                                 .client_key = 926,                     .owner_lo = 926, .key = { 0x26 }
    };
    struct chimera_vfs_claim        template;
    struct member_probe             first = { 0 }, second = { 0 };
    struct chimera_vfs_claim_grant *candidate = calloc(1, sizeof(*candidate));
    struct chimera_vfs_claim_grant *spare = calloc(1, sizeof(*spare)), *grant;
    unsigned int                    calls = 0;

    assert(candidate && spare);
    if (directory) {
        chimera_vfs_claim_init_dir_lease(&template, CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW, &identity);
        assert(template.used == CHIMERA_CLAIM_CR);
    } else {
        chimera_vfs_claim_init_rqls(&template, CHIMERA_CLAIM_CR, &identity);
    }
    template.break_cb   = seeded_epoch_break;
    template.cb_private = &calls;
    evpl_mutex_lock(&file->lock);
    grant = chimera_vfs_claim_grant_publish_rqls_locked(file, candidate, &template,
                                                        1, 43, &first, &first.next);
    assert(grant == candidate && grant->epoch == 43 && grant->is_v2);
    assert(grant->claim.construct == (directory ? CHIMERA_CONSTRUCT_DIR_LEASE : CHIMERA_CONSTRUCT_RQLS));
    template.used = template.advertised = 0;
    assert(chimera_vfs_claim_grant_publish_rqls_locked(file, spare, &template,
                                                       0, 900, &second, &second.next) == grant);
    assert(grant->epoch == 43 && grant->is_v2 && grant->claim.used == CHIMERA_CLAIM_CR);
    assert(grant->members == &second && second.next == &first && grant->refcount == 2);
    evpl_mutex_unlock(&file->lock);
    struct chimera_claim_actor actor = { .owner
                                             = { .
                                                 proto
                                                     =
                                                         CHIMERA_CLAIM_PROTO_SMB2,
                                                 .
                                                 client_key
                                                     =
                                                         927,
                                                 .
                                                 owner_lo
                                                     =
                                                         927 } };
    chimera_vfs_claim_invalidate(file->state, file->fh, file->fh_len, file->fh_hash,
                                 directory ? CHIMERA_TRIGGER_DIR_CONTENT : CHIMERA_TRIGGER_WRITE, &actor, 0);
    assert(calls == 1);
    grant->members = NULL;
    chimera_vfs_claim_grant_release(file->state, grant, true);
    chimera_vfs_claim_grant_release(file->state, grant, true);
    free(spare);

    /* The ordinary/legacy acquire path must seed before the first break too. */
    chimera_vfs_claim_init_rqls(&template, CHIMERA_CLAIM_CR, &identity);
    template.break_cb = seeded_epoch_break; template.cb_private = &calls;
    assert(chimera_vfs_claim_grant_acquire_member(file->state, file, &template,
                                                  0, 1, 43, CHIMERA_CLAIM_GRANT_EXACT, &first, &first.next,
                                                  &grant, NULL) == CHIMERA_CLAIM_GRANTED);
    chimera_vfs_claim_invalidate(file->state, file->fh, file->fh_len, file->fh_hash,
                                 CHIMERA_TRIGGER_WRITE, &actor, 0);
    assert(calls == 2);
    grant->members = NULL;
    chimera_vfs_claim_grant_release(file->state, grant, true);
} /* check_prepared_read_lease */

static enum chimera_vfs_claim_result
narrow_probe(
    struct chimera_vfs_file_state           *file,
    struct chimera_vfs_claim_access_journal *journal,
    uint8_t                                  used,
    uint8_t                                  denied)
{
    struct chimera_claim_owner      identity = { .proto      = CHIMERA_CLAIM_PROTO_SMB2,
                                                 .client_key = 999,                     .owner_lo= 999 };
    struct chimera_vfs_claim        claim;
    const struct chimera_vfs_claim *excluded[8];

    chimera_vfs_claim_init_smb_open(&claim, used, denied, &identity);
    if (journal) {
        enum chimera_vfs_claim_result result = chimera_vfs_claim_access_journal_test(journal, file, &claim, NULL);
        if (result != CHIMERA_CLAIM_GRANTED) {
            return result;
        }
        claim.admit_num_excluded = chimera_vfs_claim_access_journal_excluded(journal, file, excluded, 8);
        assert(claim.admit_num_excluded <= 8);
        claim.admit_excluded = excluded;
    }
    return chimera_vfs_claim_test(file, &claim, NULL);
} /* narrow_probe */

static void
check_private_narrow(struct chimera_vfs_file_state *file)
{
    struct chimera_claim_owner               identity = { .proto      = CHIMERA_CLAIM_PROTO_SMB2,
                                                          .client_key = 930,                     .owner_lo = 930 };
    struct chimera_vfs_claim                 claim;

    chimera_vfs_claim_init_smb_open(&claim, CHIMERA_CLAIM_R | CHIMERA_CLAIM_W, CHIMERA_CLAIM_D, &identity);
    struct chimera_vfs_claim_access_owner   *owner = chimera_vfs_claim_access_owner_alloc(file, &claim);
    assert(owner);
    assert(chimera_vfs_claim_try_acquire(file->state, file,
                                         chimera_vfs_claim_access_owner_claim(owner), NULL) == CHIMERA_CLAIM_GRANTED);
    struct chimera_vfs_claim_access_journal *journal = chimera_vfs_claim_access_journal_alloc(8);
    struct chimera_vfs_claim_access_journal *other   = chimera_vfs_claim_access_journal_alloc(1);
    assert(journal && other);
    assert(chimera_vfs_claim_access_journal_narrow(journal, owner, CHIMERA_CLAIM_R, 0) == CHIMERA_VFS_OK);
    assert(chimera_vfs_claim_access_journal_narrow(other, owner, CHIMERA_CLAIM_R, 0) == CHIMERA_VFS_EBUSY);
    /* Both conflict directions matter: dropping transient W does not drop R. */
    assert(narrow_probe(file, NULL, CHIMERA_CLAIM_R, CHIMERA_CLAIM_W) == CHIMERA_CLAIM_DENIED);
    assert(narrow_probe(file, journal, CHIMERA_CLAIM_R, CHIMERA_CLAIM_W) == CHIMERA_CLAIM_GRANTED);
    assert(narrow_probe(file, journal, CHIMERA_CLAIM_W, CHIMERA_CLAIM_R) == CHIMERA_CLAIM_DENIED);
    assert(narrow_probe(file, NULL, CHIMERA_CLAIM_D, 0) == CHIMERA_CLAIM_DENIED);
    assert(narrow_probe(file, journal, CHIMERA_CLAIM_D, 0) == CHIMERA_CLAIM_GRANTED);
    uint32_t first = chimera_vfs_claim_access_journal_checkpoint(journal);
    assert(chimera_vfs_claim_access_journal_narrow(journal, owner, 0, 0) == CHIMERA_VFS_OK);
    assert(narrow_probe(file, journal, CHIMERA_CLAIM_W, CHIMERA_CLAIM_R) == CHIMERA_CLAIM_GRANTED);
    assert(chimera_vfs_claim_access_journal_narrow(journal, owner, CHIMERA_CLAIM_R, 0) == CHIMERA_VFS_EINVAL);
    chimera_vfs_claim_access_journal_rewind(journal, first);
    assert(narrow_probe(file, journal, CHIMERA_CLAIM_W, CHIMERA_CLAIM_R) == CHIMERA_CLAIM_DENIED);
    assert(chimera_vfs_claim_access_journal_retire(journal, owner) == CHIMERA_VFS_OK);
    assert(narrow_probe(file, journal, CHIMERA_CLAIM_W, CHIMERA_CLAIM_R) == CHIMERA_CLAIM_GRANTED);
    assert(chimera_vfs_claim_access_journal_narrow(journal, owner, 0, 0) == CHIMERA_VFS_EINVAL);
    chimera_vfs_claim_access_journal_rewind(journal, first);
    assert(narrow_probe(file, journal, CHIMERA_CLAIM_W, CHIMERA_CLAIM_R) == CHIMERA_CLAIM_DENIED);
    chimera_vfs_claim_access_journal_seal(journal);
    chimera_vfs_claim_access_journal_reset(journal); /* Rejected finish restores conservative rights. */
    assert(narrow_probe(file, journal, CHIMERA_CLAIM_R, CHIMERA_CLAIM_W) == CHIMERA_CLAIM_DENIED);
    assert(!chimera_vfs_claim_access_owner_is_retired(owner));

    assert(chimera_vfs_claim_access_journal_narrow(journal, owner, CHIMERA_CLAIM_R, 0) == CHIMERA_VFS_OK);
    struct chimera_vfs_claim           waiter;
    struct chimera_vfs_pending_acquire ticket     = { 0 };
    struct fence_completion            completion = { 0 };
    identity.owner_lo++;
    chimera_vfs_claim_init_smb_open(&waiter, CHIMERA_CLAIM_R, CHIMERA_CLAIM_W, &identity);
    chimera_vfs_claim_acquire(NULL, file->state, file, &waiter, &ticket, true, true,
                              fence_complete, NULL, &completion);
    assert(ticket.queued && !completion.calls);
    chimera_vfs_claim_access_journal_seal(journal);
    chimera_vfs_claim_access_journal_publish(journal);
    assert(narrow_probe(file, NULL, CHIMERA_CLAIM_R, CHIMERA_CLAIM_W) == CHIMERA_CLAIM_GRANTED);
    assert(!completion.calls); /* Accepted protocol publication precedes waiter wakeup. */
    chimera_vfs_claim_access_journal_complete(journal);
    assert(completion.calls == 1 && completion.result == CHIMERA_CLAIM_GRANTED);
    chimera_vfs_claim_release(file->state, file, &waiter);
    assert(!chimera_vfs_claim_access_owner_is_retired(owner));
    chimera_vfs_claim_access_journal_reset(journal);

    /* Failed producer cleanup cuts off a narrowed row privately, retaining its
     * public claim and storage until the journal drains. */
    assert(chimera_vfs_claim_access_journal_narrow(journal, owner, CHIMERA_CLAIM_R, 0) == CHIMERA_VFS_OK);
    chimera_vfs_claim_access_owner_retire(owner);
    assert(narrow_probe(file, journal, CHIMERA_CLAIM_W, CHIMERA_CLAIM_R) == CHIMERA_CLAIM_GRANTED);
    assert(narrow_probe(file, NULL, CHIMERA_CLAIM_W, CHIMERA_CLAIM_R) == CHIMERA_CLAIM_DENIED);
    chimera_vfs_claim_access_journal_reset(journal);
    assert(chimera_vfs_claim_access_owner_is_retired(owner));
    chimera_vfs_claim_access_owner_put(owner);
    chimera_vfs_claim_access_journal_free(journal);
    chimera_vfs_claim_access_journal_free(other);
} /* check_private_narrow */

int
main(void)
{
    ChimeraLogLevel = CHIMERA_LOG_INFO;
    chimera_log_init();
    struct chimera_vfs_state                *state = chimera_vfs_state_init();
    uint8_t                                  fh[]  = { 0xa1, 0xa2 };
    struct chimera_vfs_file_state           *file  = chimera_vfs_state_get(state, fh, sizeof(fh),
                                                                           chimera_vfs_hash(fh, sizeof(fh)), true);
    struct chimera_vfs_claim_access_owner   *a    = make_owner(file, 1);
    struct chimera_vfs_claim_access_journal *j    = chimera_vfs_claim_access_journal_alloc(1);
    struct chimera_vfs_claim_access_journal *peer = chimera_vfs_claim_access_journal_alloc(1);
    assert(j && peer && probe(file, NULL) == CHIMERA_CLAIM_DENIED);
    assert(chimera_vfs_claim_access_journal_retire(j, a) == CHIMERA_VFS_OK);
    assert(chimera_vfs_claim_access_journal_retire(peer, a) == CHIMERA_VFS_EBUSY);
    assert(probe(file, j) == CHIMERA_CLAIM_GRANTED);
    assert(probe(file, NULL) == CHIMERA_CLAIM_DENIED);
    chimera_vfs_claim_access_journal_seal(j);
    /* Rejection restores the public open, with no temporary public hole. */
    chimera_vfs_claim_access_journal_reset(j);
    assert(probe(file, j) == CHIMERA_CLAIM_DENIED);
    assert(!chimera_vfs_claim_access_owner_is_retired(a));
    assert(chimera_vfs_claim_access_journal_retire(j, a) == CHIMERA_VFS_OK);
    chimera_vfs_claim_access_journal_seal(j);
    chimera_vfs_claim_access_journal_publish(j);
    assert(probe(file, NULL) == CHIMERA_CLAIM_GRANTED);
    assert(!chimera_vfs_claim_access_owner_is_retired(a)); /* accepted frontend still publishes */
    chimera_vfs_claim_access_journal_complete(j);
    assert(chimera_vfs_claim_access_owner_is_retired(a));
    chimera_vfs_claim_access_owner_put(a);
    chimera_vfs_claim_access_journal_reset(j);

    /* Independent external close requests cutoff immediately, but cannot
     * invalidate a journal's admission view until accepted/reset drain. */
    a = make_owner(file, 2);
    assert(chimera_vfs_claim_access_journal_retire(j, a) == CHIMERA_VFS_OK);
    chimera_vfs_claim_access_owner_retire(a);
    assert(chimera_vfs_claim_access_journal_retire(peer, a) == CHIMERA_VFS_EINTR);
    assert(!chimera_vfs_claim_access_owner_is_retired(a));
    assert(probe(file, NULL) == CHIMERA_CLAIM_DENIED);
    chimera_vfs_claim_access_journal_reset(j);
    assert(chimera_vfs_claim_access_owner_is_retired(a));
    assert(probe(file, NULL) == CHIMERA_CLAIM_GRANTED);
    chimera_vfs_claim_access_owner_put(a);

    /* A pinned token owns storage after the frontend drops its retired ref. */
    a = make_owner(file, 3);
    assert(chimera_vfs_claim_access_journal_retire(j, a) == CHIMERA_VFS_OK);
    chimera_vfs_claim_access_owner_retire(a);
    chimera_vfs_claim_access_owner_put(a);
    assert(probe(file, j) == CHIMERA_CLAIM_GRANTED);
    assert(probe(file, NULL) == CHIMERA_CLAIM_DENIED);
    chimera_vfs_claim_access_journal_seal(j);
    chimera_vfs_claim_access_journal_publish(j);
    chimera_vfs_claim_access_journal_complete(j);
    assert(probe(file, NULL) == CHIMERA_CLAIM_GRANTED);

    check_private_narrow(file);
    check_admission_fence(file);
    check_revoke_storage_anchor(file);
    check_atomic_member_attach(file);
    check_prepared_oplock(file);
    check_first_grant_race(file);
    check_prepared_read_lease(file, false);
    check_prepared_read_lease(file, true);
    chimera_vfs_claim_access_journal_free(j);
    chimera_vfs_claim_access_journal_free(peer);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
    fprintf(stderr, "vfs_claim_access_test: all checks passed\n");
    return 0;
} /* main */
