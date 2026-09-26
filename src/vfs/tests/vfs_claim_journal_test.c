// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#include <stdio.h>
#include <string.h>
#ifndef _WIN32
#include <unistd.h>
#include <signal.h>
#include <sys/resource.h>
#include <sys/wait.h>
#endif /* ifndef _WIN32 */
#undef NDEBUG
#include <assert.h>
#include "vfs/vfs_claim_journal.h"
#include "vfs/vfs_internal.h"
#include "common/logging.h"

static struct chimera_vfs_claim_owner *
make_owner(
    struct chimera_vfs_file_state *file,
    unsigned                       id)
{
    struct chimera_claim_owner      identity = { .proto = CHIMERA_CLAIM_PROTO_SMB2,
                                                 .client_key = id,                      .owner_lo = id };
    struct chimera_vfs_claim_owner *owner = chimera_vfs_claim_owner_create(file, &identity);

    assert(owner);
    return owner;
} /* make_owner */

static enum chimera_vfs_claim_result
probe(
    struct chimera_vfs_file_state *file,
    uint64_t                       offset,
    uint64_t                       length,
    bool                           getlk)
{
    struct chimera_claim_owner identity = { .proto      = CHIMERA_CLAIM_PROTO_NLM,
                                            .client_key = 99,                     .owner_lo= 99 };
    struct chimera_vfs_claim   claim;

    chimera_vfs_claim_init_range(&claim, true, false, offset, length, &identity);
    return getlk ? chimera_vfs_claim_test_locks(file, &claim, NULL) :
           chimera_vfs_claim_test(file, &claim, NULL);
} /* probe */

static void
accept_journal(struct chimera_vfs_claim_journal *j)
{
    chimera_vfs_claim_journal_seal(j);
    chimera_vfs_claim_journal_publish(j);
    chimera_vfs_claim_journal_complete(j);
    chimera_vfs_claim_journal_reset(j);
} /* accept */

static void
retired(void *private)
{
    unsigned *calls = private;

    (*calls)++;
} /* retired */

static void
unused_break(
    struct chimera_vfs_claim *claim,
    uint8_t                   mode,
    void                     *private)
{
    (void) claim; (void) mode;
    unsigned *calls = private;
    (*calls)++;
} /* unused_break */

#ifndef _WIN32
/* A boolean range-end return used to truncate all nonzero ends to 1,
 * bypassing the publication coverage proof for ranges starting above zero. */
static void
unreserved_publication_child(void)
{
    struct rlimit                  limit = { 0, 0 };

    setrlimit(RLIMIT_CORE, &limit);
    struct chimera_vfs_state      *state = chimera_vfs_state_init();
    uint8_t                        fh[]  = { 0xee };
    struct chimera_vfs_file_state *file  = chimera_vfs_state_get(state, fh, sizeof(fh),
                                                                 chimera_vfs_hash(fh, sizeof(fh)), true);
    struct chimera_claim_owner     identity = { .proto      = CHIMERA_CLAIM_PROTO_NLM,
                                                .client_key = 1,                      .owner_lo = 1 };
    struct chimera_vfs_claim       held, widened;
    chimera_vfs_claim_init_range(&held, true, false, 100, 100, &identity);
    assert(chimera_vfs_claim_try_acquire(state, file, &held, NULL) == CHIMERA_CLAIM_GRANTED);
    chimera_vfs_claim_init_range(&widened, true, false, 100, 101, &identity);
    struct chimera_vfs_claim      *old[] = { &held }, *replacement[] = { &widened };
    chimera_vfs_claim_range_publish(file, old, 1, replacement, 1);
} /* unreserved_publication_child */

static void
check_unreserved_publication_rejected(const char *program)
{
    pid_t pid = fork();

    assert(pid >= 0);
    if (!pid) {
        execlp(program, program, "--unreserved-publication", (char *) NULL);
        _exit(127);
    }
    int   status;
    assert(waitpid(pid, &status, 0) == pid);
    assert(WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT);
} /* check_unreserved_publication_rejected */

#endif /* ifndef _WIN32 */

static void
check_owner_retirement(struct chimera_vfs_file_state *file)
{
    struct chimera_vfs_claim_owner       *a = make_owner(file, 10), *b = make_owner(file, 11);
    struct chimera_vfs_claim_journal     *j    = chimera_vfs_claim_journal_alloc(2, 3);
    struct chimera_vfs_claim_journal     *peer = chimera_vfs_claim_journal_alloc(2, 3);
    struct chimera_vfs_claim_batch_result result;
    struct chimera_vfs_claim_exact_range  ranges[] = {
        { .offset = 400, .length = 10, .exclusive = true },
        { .offset = 420, .length = 10, .exclusive = true },
        { .offset = 440, .length = 10, .exclusive = true },
        { .offset = 460, .length = 10, .exclusive = true },
    };

    assert(j && peer);
    chimera_vfs_claim_journal_acquire(j, a, ranges, 3, &result);
    assert(result.status == CHIMERA_VFS_OK);
    accept_journal(j);
    chimera_vfs_claim_journal_acquire(j, a, &ranges[3], 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    chimera_vfs_claim_journal_unlock(j, a, &ranges[0], 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    assert(chimera_vfs_claim_journal_bind(peer, a) == CHIMERA_VFS_OK);
    assert(chimera_vfs_claim_journal_retire_owner(j, a) == CHIMERA_VFS_EBUSY);
    assert(chimera_vfs_claim_journal_excluded(j, file, NULL, 0) == 1);
    chimera_vfs_claim_journal_reset(peer);

    uint32_t checkpoint = chimera_vfs_claim_journal_retire_checkpoint(j);
    assert(chimera_vfs_claim_journal_retire_owner(j, a) == CHIMERA_VFS_OK);
    assert(chimera_vfs_claim_journal_retire_owner(j, a) == CHIMERA_VFS_OK);
    assert(chimera_vfs_claim_journal_excluded(j, file, NULL, 0) == 4);
    assert(!chimera_vfs_claim_journal_io_denied(j, file, 420, 1, true, NULL));
    assert(probe(file, 420, 1, false) == CHIMERA_CLAIM_DENIED);
    chimera_vfs_claim_journal_acquire(peer, a, &ranges[3], 1, &result);
    assert(result.status == CHIMERA_VFS_EBUSY);
    assert(chimera_vfs_claim_journal_bind(j, a) == CHIMERA_VFS_EINTR);
    /* Failed later ACCESS admission rewinds only retirement, preserving the
     * earlier successful LOCK and UNLOCK commands. */
    chimera_vfs_claim_journal_retire_rewind(j, checkpoint);
    assert(chimera_vfs_claim_journal_excluded(j, file, NULL, 0) == 1);
    assert(chimera_vfs_claim_journal_bind(peer, a) == CHIMERA_VFS_OK);
    chimera_vfs_claim_journal_reset(peer);
    assert(chimera_vfs_claim_journal_retire_owner(j, a) == CHIMERA_VFS_OK);
    chimera_vfs_claim_journal_acquire(j, b, &ranges[1], 1, &result);
    assert(result.status == CHIMERA_VFS_OK); /* CLOSE -> new owner's LOCK */
    chimera_vfs_claim_journal_reset(j);
    assert(chimera_vfs_claim_owner_has_locks(a));
    assert(!chimera_vfs_claim_owner_has_locks(b));
    assert(probe(file, 460, 1, false) == CHIMERA_CLAIM_GRANTED);
    assert(probe(file, 400, 1, false) == CHIMERA_CLAIM_DENIED);

    assert(chimera_vfs_claim_journal_retire_owner(j, a) == CHIMERA_VFS_OK);
    /* An unrelated owner remains available even while A is fenced. */
    chimera_vfs_claim_journal_acquire(peer, b, &ranges[3], 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    accept_journal(peer);
    chimera_vfs_claim_journal_seal(j);
    chimera_vfs_claim_journal_publish(j);
    assert(!chimera_vfs_claim_owner_has_locks(a));
    assert(!chimera_vfs_claim_owner_is_retired(a));
    assert(chimera_vfs_claim_journal_bind(peer, a) == CHIMERA_VFS_EINTR);
    chimera_vfs_claim_journal_complete(j);
    assert(chimera_vfs_claim_owner_is_retired(a));
    chimera_vfs_claim_journal_reset(j);
    chimera_vfs_claim_owner_put(a);

    /* Rejected retirement honors an independent external CLOSE cutoff. */
    assert(chimera_vfs_claim_journal_retire_owner(j, b) == CHIMERA_VFS_OK);
    unsigned calls = 0;
    assert(chimera_vfs_claim_owner_retire(b, retired, &calls));
    assert(!calls);
    chimera_vfs_claim_journal_retire_rewind(j, 0);
    assert(calls == 1 && chimera_vfs_claim_owner_is_retired(b));
    chimera_vfs_claim_owner_put(b);
    chimera_vfs_claim_journal_free(j);
    chimera_vfs_claim_journal_free(peer);
    assert(!file->claims[CHIMERA_CLAIM_CLASS_RANGE]);

    /* Empty owners still keep the fence and lifetime pin until acceptance. */
    a = make_owner(file, 12);
    j = chimera_vfs_claim_journal_alloc(1, 1);
    assert(chimera_vfs_claim_journal_retire_owner(j, a) == CHIMERA_VFS_OK);
    assert(!chimera_vfs_claim_journal_unbind_idle(j, a));
    accept_journal(j);
    assert(chimera_vfs_claim_owner_is_retired(a));
    chimera_vfs_claim_owner_put(a);
    chimera_vfs_claim_journal_free(j);
} /* check_owner_retirement */

int
main(
    int    argc,
    char **argv)
{
    ChimeraLogLevel = CHIMERA_LOG_INFO;
    chimera_log_init();
#ifndef _WIN32
    if (argc == 2 && !strcmp(argv[1], "--unreserved-publication")) {
        unreserved_publication_child();
        return 0;
    }
    check_unreserved_publication_rejected(argv[0]);
#else  /* ifndef _WIN32 */
    (void) argc; (void) argv;
#endif /* ifndef _WIN32 */
    struct chimera_vfs_state             *state = chimera_vfs_state_init();
    uint8_t                               fh[]  = { 0xaa, 0xbb, 0xcc };
    struct chimera_vfs_file_state        *file  = chimera_vfs_state_get(state, fh, sizeof(fh),
                                                                        chimera_vfs_hash(fh, sizeof(fh)), true);
    assert(file);
    struct chimera_vfs_claim_owner       *a = make_owner(file, 1), *b = make_owner(file, 2);
    struct chimera_vfs_claim_journal     *j    = chimera_vfs_claim_journal_alloc(2, 32);
    struct chimera_vfs_claim_journal     *peer = chimera_vfs_claim_journal_alloc(2, 32);
    struct chimera_vfs_claim_batch_result result;
    struct chimera_vfs_claim_exact_range  range = { .offset = 10, .length = 10, .exclusive = false };
    assert(j && peer);

    /* Private acquisition blocks admission but is not an accepted GETLK row. */
    chimera_vfs_claim_journal_acquire(j, a, &range, 1, &result);
    assert(result.status == CHIMERA_VFS_OK && result.applied == 1);
    assert(probe(file, 10, 1, false) == CHIMERA_CLAIM_DENIED);
    assert(probe(file, 10, 1, true) == CHIMERA_CLAIM_GRANTED);
    assert(chimera_vfs_claim_journal_bind(peer, a) == CHIMERA_VFS_OK);
    chimera_vfs_claim_journal_reset(peer);
    assert(chimera_vfs_claim_journal_io_denied(j, file, 10, 1, true, NULL));
    assert(!chimera_vfs_claim_journal_io_denied(j, file, 10, 1, false, NULL));
    chimera_vfs_claim_journal_reset(j);
    assert(probe(file, 10, 1, false) == CHIMERA_CLAIM_GRANTED);

    /* Independently stacked identical shared acquisitions survive one exact
     * unlock; a missing second element preserves the first successful unlock. */
    chimera_vfs_claim_journal_acquire(j, a, &range, 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    chimera_vfs_claim_journal_acquire(j, a, &range, 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    accept_journal(j);
    assert(probe(file, 10, 1, true) == CHIMERA_CLAIM_DENIED);
    chimera_vfs_claim_journal_unlock(j, a, &range, 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    accept_journal(j);
    assert(probe(file, 10, 1, true) == CHIMERA_CLAIM_DENIED);
    struct chimera_vfs_claim_exact_range unlocks[] = { range, { .offset = 999, .length = 1 } };
    chimera_vfs_claim_journal_unlock(j, a, unlocks, 2, &result);
    assert(result.status == CHIMERA_VFS_ENOENT && result.applied == 1 && result.failed == 1);
    assert(!chimera_vfs_claim_journal_io_denied(j, file, 10, 1, true, NULL));
    assert(probe(file, 10, 1, false) == CHIMERA_CLAIM_DENIED); /* peers see old state */
    accept_journal(j);
    assert(probe(file, 10, 1, false) == CHIMERA_CLAIM_GRANTED);

    /* Whole-attempt rejection restores a tentative unlock without losing the
     * accepted exact record. A later same-attempt owner sees the private hole. */
    range.exclusive = true;
    chimera_vfs_claim_journal_acquire(j, a, &range, 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    accept_journal(j);
    chimera_vfs_claim_journal_unlock(j, a, &range, 1, &result);
    chimera_vfs_claim_journal_acquire(j, b, &range, 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    chimera_vfs_claim_journal_seal(j);
    chimera_vfs_claim_journal_reset(j);
    chimera_vfs_claim_journal_unlock(j, a, &range, 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    accept_journal(j);
    assert(probe(file, 10, 1, false) == CHIMERA_CLAIM_GRANTED);

    /* Same-owner exclusive conflicts, shared stacks on exclusive. */
    chimera_vfs_claim_journal_acquire(j, a, &range, 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    chimera_vfs_claim_journal_acquire(j, a, &range, 1, &result);
    assert(result.status == CHIMERA_VFS_EAGAIN && result.applied == 0);
    range.exclusive = false;
    chimera_vfs_claim_journal_acquire(j, a, &range, 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    chimera_vfs_claim_journal_reset(j);

    /* Acquisition batch is atomic on a later conflict; earlier independent
     * journal commands remain and only the batch prefix is discarded. */
    struct chimera_vfs_claim_exact_range blocker = { .offset = 100, .length = 10, .exclusive = true };
    chimera_vfs_claim_journal_acquire(peer, b, &blocker, 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    accept_journal(peer);
    chimera_vfs_claim_journal_acquire(j, a, &range, 1, &result);
    struct chimera_vfs_claim_exact_range batch[] = {
        { .offset = 50, .length = 10, .exclusive = true }, blocker,
    };
    chimera_vfs_claim_journal_acquire(j, a, batch, 2, &result);
    assert(result.status == CHIMERA_VFS_EAGAIN && result.failed == 1 && result.applied == 0);
    assert(probe(file, 50, 1, false) == CHIMERA_CLAIM_GRANTED);
    assert(probe(file, 10, 1, false) == CHIMERA_CLAIM_DENIED);
    chimera_vfs_claim_journal_reset(j);
    chimera_vfs_claim_journal_unlock(peer, b, &blocker, 1, &result);
    accept_journal(peer);

    /* Raw UINT64_MAX is finite at offset zero. It must not accidentally
     * reserve the final byte, while [1,1+MAX) does own that final byte. */
    struct chimera_vfs_claim_exact_range huge = { .offset = 0, .length = UINT64_MAX, .exclusive = true };
    chimera_vfs_claim_journal_acquire(j, a, &huge, 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    assert(probe(file, UINT64_MAX - 1, 1, false) == CHIMERA_CLAIM_DENIED);
    assert(probe(file, UINT64_MAX, 1, false) == CHIMERA_CLAIM_GRANTED);
    accept_journal(j);
    chimera_vfs_claim_journal_unlock(j, a, &huge, 1, &result);
    accept_journal(j);
    huge.offset = 1;
    chimera_vfs_claim_journal_acquire(j, a, &huge, 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    assert(probe(file, UINT64_MAX, 1, false) == CHIMERA_CLAIM_DENIED);
    chimera_vfs_claim_journal_reset(j);
    huge.offset = 2;
    chimera_vfs_claim_journal_acquire(j, a, &huge, 1, &result);
    assert(result.status == CHIMERA_VFS_EINVAL && result.applied == 0);
    chimera_vfs_claim_journal_reset(j);

    /* Zero-byte acquisitions are distinct exact records, with no byte claim. */
    struct chimera_vfs_claim_exact_range zero = { .offset = 5, .length = 0, .exclusive = true };
    chimera_vfs_claim_journal_acquire(j, a, &zero, 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    chimera_vfs_claim_journal_acquire(j, a, &zero, 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    accept_journal(j);
    assert(!file->claims[CHIMERA_CLAIM_CLASS_RANGE]);
    chimera_vfs_claim_journal_unlock(j, a, &zero, 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    chimera_vfs_claim_journal_unlock(j, a, &zero, 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    chimera_vfs_claim_journal_unlock(j, a, &zero, 1, &result);
    assert(result.status == CHIMERA_VFS_ENOENT);
    accept_journal(j);

    /* Explicit SMB compatibility policy preserves the legacy point geometry
     * without changing default zero-record or POSIX/NFS sentinel semantics. */
    struct chimera_claim_actor           compat_actor = { .owner = {
                                                              .proto    = CHIMERA_CLAIM_PROTO_SMB2, .client_key = 3,
                                                              .owner_lo = 3
                                                          } };
    struct chimera_vfs_claim_owner      *compat = chimera_vfs_claim_owner_create_compat(file, &compat_actor, true);
    assert(compat);
    struct chimera_vfs_claim_exact_range containing = { .offset = 0, .length = 10, .exclusive = true };
    chimera_vfs_claim_journal_acquire(peer, b, &containing, 1, &result);
    accept_journal(peer);
    chimera_vfs_claim_journal_acquire(j, compat, &zero, 1, &result);
    assert(result.status == CHIMERA_VFS_EAGAIN); /* offset 5 strictly inside */
    chimera_vfs_claim_journal_reset(j);
    zero.offset = 0; /* the boundary point does not overlap */
    chimera_vfs_claim_journal_acquire(j, compat, &zero, 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    chimera_vfs_claim_journal_acquire(j, compat, &zero, 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    accept_journal(j);
    assert(chimera_vfs_claim_owner_has_locks(compat));
    chimera_vfs_claim_journal_unlock(j, compat, &zero, 1, &result);
    chimera_vfs_claim_journal_unlock(j, compat, &zero, 1, &result);
    accept_journal(j);
    assert(!chimera_vfs_claim_owner_has_locks(compat));
    chimera_vfs_claim_journal_unlock(peer, b, &containing, 1, &result);
    accept_journal(peer);
    chimera_vfs_claim_journal_acquire(j, compat, &containing, 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    zero.offset = 5;
    chimera_vfs_claim_journal_acquire(j, compat, &zero, 1, &result);
    assert(result.status == CHIMERA_VFS_EAGAIN); /* own exclusive point rule */
    chimera_vfs_claim_journal_reset(j);
    assert(chimera_vfs_claim_owner_retire(compat, NULL, NULL));
    chimera_vfs_claim_owner_put(compat);

    /* Two attempts acquire owners in opposite order without an owner-lane
     * dependency cycle. All four byte ranges are disjoint. */
    struct chimera_vfs_claim_exact_range disjoint[] = {
        { .offset = 200, .length = 10, .exclusive = true },
        { .offset = 220, .length = 10, .exclusive = true },
        { .offset = 240, .length = 10, .exclusive = true },
        { .offset = 260, .length = 10, .exclusive = true },
    };
    chimera_vfs_claim_journal_acquire(j, a, &disjoint[0], 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    chimera_vfs_claim_journal_acquire(peer, b, &disjoint[1], 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    chimera_vfs_claim_journal_acquire(j, b, &disjoint[2], 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    chimera_vfs_claim_journal_acquire(peer, a, &disjoint[3], 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    accept_journal(j);
    accept_journal(peer);
    chimera_vfs_claim_journal_unlock(j, a, &disjoint[0], 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    chimera_vfs_claim_journal_unlock(peer, b, &disjoint[1], 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    chimera_vfs_claim_journal_unlock(j, b, &disjoint[2], 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    chimera_vfs_claim_journal_unlock(peer, a, &disjoint[3], 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    accept_journal(j);
    accept_journal(peer);

    /* Tentative rows owned by a different attempt cannot be unlocked, but
     * do block conflicting acquisitions, including on the same owner. */
    chimera_vfs_claim_journal_acquire(j, a, &disjoint[0], 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    chimera_vfs_claim_journal_unlock(peer, a, &disjoint[0], 1, &result);
    assert(result.status == CHIMERA_VFS_ENOENT);
    chimera_vfs_claim_journal_acquire(peer, a, &disjoint[0], 1, &result);
    assert(result.status == CHIMERA_VFS_EAGAIN && result.self_conflict);
    chimera_vfs_claim_journal_reset(j);
    chimera_vfs_claim_journal_acquire(peer, a, &disjoint[0], 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    chimera_vfs_claim_journal_reset(peer);

    /* Preserve oldest-record selection and multiplicity across concurrent
     * exact removals. A failed competing removal is EBUSY, not missing: the
     * first remover may abort. The failed command retains no removal claim. */
    range.exclusive = false;
    chimera_vfs_claim_journal_acquire(j, a, &range, 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    accept_journal(j);
    struct chimera_vfs_claim       *oldest = file->claims[CHIMERA_CLAIM_CLASS_RANGE];
    assert(oldest);
    chimera_vfs_claim_journal_acquire(j, a, &range, 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    accept_journal(j);
    chimera_vfs_claim_journal_unlock(j, a, &range, 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    const struct chimera_vfs_claim *removed[2];
    assert(chimera_vfs_claim_journal_excluded(j, file, removed, 2) == 1);
    assert(removed[0] == oldest);
    chimera_vfs_claim_journal_unlock(peer, a, &range, 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    assert(chimera_vfs_claim_journal_excluded(peer, file, removed, 2) == 1);
    assert(removed[0] != oldest);
    chimera_vfs_claim_journal_unlock(peer, a, &range, 1, &result);
    assert(result.status == CHIMERA_VFS_EBUSY && result.applied == 0);
    chimera_vfs_claim_journal_reset(j); /* oldest is available again */
    chimera_vfs_claim_journal_unlock(peer, a, &range, 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    assert(chimera_vfs_claim_journal_excluded(peer, file, removed, 2) == 2);
    assert(removed[1] == oldest);
    accept_journal(peer);
    assert(!chimera_vfs_claim_owner_has_locks(a));

    /* A genuinely conflicting removal never waits on another journal while
     * retaining its own prefix, avoiding a hidden cross-record wait cycle. */
    chimera_vfs_claim_journal_acquire(j, a, &disjoint[0], 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    chimera_vfs_claim_journal_acquire(j, b, &disjoint[1], 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    accept_journal(j);
    chimera_vfs_claim_journal_unlock(j, a, &disjoint[0], 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    chimera_vfs_claim_journal_unlock(peer, b, &disjoint[1], 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    chimera_vfs_claim_journal_unlock(j, b, &disjoint[1], 1, &result);
    assert(result.status == CHIMERA_VFS_EBUSY && result.applied == 0);
    chimera_vfs_claim_journal_unlock(peer, a, &disjoint[0], 1, &result);
    assert(result.status == CHIMERA_VFS_EBUSY && result.applied == 0);
    accept_journal(j);
    accept_journal(peer);
    assert(!chimera_vfs_claim_owner_has_locks(a));
    assert(!chimera_vfs_claim_owner_has_locks(b));

    /* Published rows can be consumed by a second journal before the first
     * frontend finishes publication. Both journals retain safe record refs. */
    chimera_vfs_claim_journal_acquire(j, a, &range, 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    chimera_vfs_claim_journal_seal(j);
    chimera_vfs_claim_journal_publish(j);
    chimera_vfs_claim_journal_unlock(peer, a, &range, 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    accept_journal(peer);
    assert(!chimera_vfs_claim_owner_has_locks(a));
    chimera_vfs_claim_journal_complete(j);
    chimera_vfs_claim_journal_reset(j);

    /* No cache-break side effects from tentative lock execution. */
    struct chimera_claim_owner cache_owner = { .proto      = CHIMERA_CLAIM_PROTO_NFSV4,
                                               .client_key = 44,                       .owner_lo = 44 };
    struct chimera_vfs_claim   cache;
    unsigned                   breaks = 0;
    chimera_vfs_claim_init_delegation(&cache, false, &cache_owner);
    cache.break_cb   = unused_break;
    cache.cb_private = &breaks;
    assert(chimera_vfs_claim_try_acquire(state, file, &cache, NULL) == CHIMERA_CLAIM_GRANTED);
    chimera_vfs_claim_journal_acquire(j, a, &range, 1, &result);
    assert(result.status == CHIMERA_VFS_EAGAIN && result.admission == CHIMERA_CLAIM_BREAKING);
    assert(!breaks);
    chimera_vfs_claim_journal_reset(j);
    chimera_vfs_claim_release(state, file, &cache);

    /* Sealed retirement cannot veto publish. No CLOSE completion before both
     * publication and the caller's protocol-publication barrier complete. */
    unsigned closes = 0;
    chimera_vfs_claim_journal_acquire(j, a, &range, 1, &result);
    assert(result.status == CHIMERA_VFS_OK);
    chimera_vfs_claim_journal_seal(j);
    assert(chimera_vfs_claim_owner_retire(a, retired, &closes));
    assert(!chimera_vfs_claim_owner_retire(a, retired, &closes));
    assert(!closes);
    assert(chimera_vfs_claim_journal_bind(peer, a) == CHIMERA_VFS_EINTR);
    chimera_vfs_claim_journal_publish(j);
    assert(!closes && probe(file, 10, 1, false) == CHIMERA_CLAIM_DENIED);
    chimera_vfs_claim_journal_complete(j);
    assert(closes == 1 && probe(file, 10, 1, false) == CHIMERA_CLAIM_GRANTED);
    chimera_vfs_claim_owner_put(a);
    chimera_vfs_claim_journal_reset(j);

    /* Aborting after a retirement cutoff also drains old accepted claims. */
    chimera_vfs_claim_journal_acquire(peer, b, &range, 1, &result);
    accept_journal(peer);
    assert(chimera_vfs_claim_journal_bind(j, b) == CHIMERA_VFS_OK);
    assert(chimera_vfs_claim_journal_bind(peer, b) == CHIMERA_VFS_OK);
    assert(chimera_vfs_claim_owner_retire(b, retired, &closes));
    assert(closes == 1);
    chimera_vfs_claim_journal_reset(j);
    assert(closes == 1 && !chimera_vfs_claim_owner_is_retired(b));
    assert(probe(file, 10, 1, false) == CHIMERA_CLAIM_DENIED);
    chimera_vfs_claim_journal_reset(peer);
    assert(closes == 2 && probe(file, 10, 1, false) == CHIMERA_CLAIM_GRANTED);
    chimera_vfs_claim_owner_put(b);

    chimera_vfs_claim_journal_free(j);
    chimera_vfs_claim_journal_free(peer);
    check_owner_retirement(file);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
    fprintf(stderr, "vfs_claim_journal_test: all checks passed\n");
    return 0;
} /* main */
