// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Unit tests for the unified VFS claim core (vfs_claim.{h,c}).
 *
 * These cover the admission predicate and break-orchestration skeleton in
 * isolation — no protocol stack, no real VFS instance.  The predicate is
 * pure logic, so we exercise every (existing-claim, new-claim) pair
 * across the three claim classes, plus same-owner coalescing and the
 * claim-break ack / revoke paths.
 *
 * What is intentionally NOT covered here:
 *   - integration with NLM/NFSv4 LOCK and SMB2 LOCK
 *   - integration with SMB CREATE share_access and NFSv4 OPEN
 *   - break notification packets (CB_RECALL, OPLOCK_BREAK)
 *
 * Those are exercised by the protocol suites that use this layer.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#undef NDEBUG
#include <assert.h>

#include "vfs/vfs_claim.h"
#include "vfs/vfs_claim_internal.h"
#include "vfs/vfs_internal.h"
#include "common/logging.h"

static int passed = 0;
static int failed = 0;

#define PASS(name) do { fprintf(stderr, "  PASS: %s\n", (name)); passed++; } while (0)
#define FAIL(name) do { fprintf(stderr, "  FAIL: %s\n", (name)); failed++; } while (0)
#define CHECK(cond, name)         \
        do {                          \
            if (cond) { PASS(name); } \
            else { FAIL(name); }      \
        } while (0)

/* Test-side scaffolding ----------------------------------------------- */

static void
make_fh(
    uint8_t out[CHIMERA_VFS_FH_SIZE],
    uint8_t tag)
{
    memset(out, 0, CHIMERA_VFS_FH_SIZE);
    out[0]                       = 0xAA;
    out[CHIMERA_VFS_FH_SIZE - 1] = tag;
} /* make_fh */

static struct chimera_vfs_file_state *
get_file(
    struct chimera_vfs_state *state,
    uint8_t                   tag)
{
    uint8_t  fh[CHIMERA_VFS_FH_SIZE];
    uint64_t fh_hash;

    make_fh(fh, tag);
    fh_hash = chimera_vfs_hash(fh, sizeof(fh));
    return chimera_vfs_state_get(state, fh, sizeof(fh), fh_hash, true);
} /* get_file */

static void
init_owner(
    struct chimera_claim_owner *owner,
    uint8_t                     proto,
    uint64_t                    client,
    uint64_t                    owner_id)
{
    memset(owner, 0, sizeof(*owner));
    owner->proto      = proto;
    owner->client_key = client;
    owner->owner_lo   = owner_id;
} /* init_owner */

/* Pack a 128-bit SMB LeaseKey into owner->key (the KEY circle). */
static void
set_owner_key(
    struct chimera_claim_owner *owner,
    uint64_t                    lo,
    uint64_t                    hi)
{
    memcpy(owner->key, &lo, 8);
    memcpy(owner->key + 8, &hi, 8);
} /* set_owner_key */

/* Break callback that just counts invocations and records the claim. */
struct break_recorder {
    int                       fired;
    struct chimera_vfs_claim *last_claim;
    uint8_t                   last_needed_mode;
};

static void
recording_break_cb(
    struct chimera_vfs_claim *claim,
    uint8_t                   needed_mode,
    void                     *priv)
{
    struct break_recorder *r = priv;

    r->fired++;
    r->last_claim       = claim;
    r->last_needed_mode = needed_mode;
} /* recording_break_cb */

/* Test 1: init + destroy --------------------------------------------- */
static void
test_init_destroy(void)
{
    struct chimera_vfs_state *state;

    fprintf(stderr, "\ntest_init_destroy\n");

    state = chimera_vfs_state_init();
    CHECK(state != NULL, "init returns non-null");

    chimera_vfs_state_destroy(state);
    PASS("destroy (no crash)");
} /* test_init_destroy */

/* Test 2: file-state lookup is refcounted and create/get coalesces --- */
static void
test_file_state_lookup(void)
{
    struct chimera_vfs_state      *state;
    struct chimera_vfs_file_state *f1, *f2, *f3;

    fprintf(stderr, "\ntest_file_state_lookup\n");

    state = chimera_vfs_state_init();

    f1 = get_file(state, 1);
    CHECK(f1 != NULL, "get_or_create returns non-null");
    assert(f1 != NULL);

    f2 = get_file(state, 1);
    CHECK(f2 == f1, "same FH returns same state (coalesced)");
    CHECK(f1->refcount == 2, "refcount bumped on second get");

    f3 = get_file(state, 2);
    CHECK(f3 != f1, "different FH returns different state");

    chimera_vfs_state_put(state, f1);
    chimera_vfs_state_put(state, f2);
    /* After both puts, f1's refcount should be zero and it should be
     * freed — we can't probe it directly, but a fresh get must allocate
     * a new state object. */
    f1 = get_file(state, 1);
    CHECK(f1 != NULL, "fresh get after release returns new state");
    assert(f1 != NULL);
    CHECK(f1->refcount == 1, "fresh state has refcount 1");

    chimera_vfs_state_put(state, f1);
    chimera_vfs_state_put(state, f3);

    chimera_vfs_state_destroy(state);
} /* test_file_state_lookup */

/* Test 3: range vs range — fcntl conflict rules ---------------------- */
static void
test_range_vs_range(void)
{
    struct chimera_vfs_state         *state;
    struct chimera_vfs_file_state    *file;
    struct chimera_vfs_claim          a, b, c;
    struct chimera_claim_owner        owner;
    enum chimera_vfs_claim_result     r;
    struct chimera_vfs_claim_conflict conflict;

    fprintf(stderr, "\ntest_range_vs_range\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    /* Owner A takes a shared read lock on [0, 100). */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_NFSV4, 0xA, 1);
    chimera_vfs_claim_init_range(&a, false, false, 0, 100, &owner);

    r = chimera_vfs_claim_try_acquire(state, file, &a, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "first read lock granted");

    /* Owner B takes a non-overlapping read lock — granted. */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_NFSV4, 0xB, 2);
    chimera_vfs_claim_init_range(&b, false, false, 200, 100, &owner);

    r = chimera_vfs_claim_try_acquire(state, file, &b, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "non-overlapping lock granted");

    /* Owner C tries an overlapping write — denied by A.  The conflict
     * arrives BY VALUE now: assert the holder's identity/geometry, not a
     * pointer. */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_NFSV4, 0xC, 3);
    chimera_vfs_claim_init_range(&c, true, false, 50, 100, &owner);

    r = chimera_vfs_claim_try_acquire(state, file, &c, &conflict);
    CHECK(r == CHIMERA_CLAIM_DENIED, "overlapping write denied");
    CHECK(conflict.owner.client_key == 0xA && conflict.owner.owner_lo == 1 &&
          conflict.offset == 0 && conflict.length == 100,
          "conflict reports first existing holder by value");

    /* Owner C tries an overlapping read — read+read coexist. */
    chimera_vfs_claim_init_range(&c, false, false, 50, 100, &owner);
    r = chimera_vfs_claim_try_acquire(state, file, &c, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "overlapping read with read granted");

    chimera_vfs_claim_release(state, file, &a);
    chimera_vfs_claim_release(state, file, &b);
    chimera_vfs_claim_release(state, file, &c);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_range_vs_range */

/* Test 4: same-owner coalescing on range locks ----------------------- */
static void
test_range_same_owner_coalesces(void)
{
    struct chimera_vfs_state         *state;
    struct chimera_vfs_file_state    *file;
    struct chimera_vfs_claim          a, b;
    struct chimera_claim_owner        owner;
    enum chimera_vfs_claim_result     r;
    struct chimera_vfs_claim_conflict conflict;

    fprintf(stderr, "\ntest_range_same_owner_coalesces\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    init_owner(&owner, CHIMERA_CLAIM_PROTO_NFSV4, 0xA, 1);
    chimera_vfs_claim_init_range(&a, true, false, 0, 100, &owner);

    r = chimera_vfs_claim_try_acquire(state, file, &a, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "first write lock granted");

    /* Same owner taking an overlapping write — must not self-conflict. */
    chimera_vfs_claim_init_range(&b, true, false, 50, 100, &owner);

    r = chimera_vfs_claim_try_acquire(state, file, &b, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "same-owner overlapping write granted (coalesces)");

    chimera_vfs_claim_release(state, file, &a);
    chimera_vfs_claim_release(state, file, &b);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_range_same_owner_coalesces */

/* Test 5: range length==UINT64_MAX means to EOF ---------------------- */
static void
test_range_to_eof(void)
{
    struct chimera_vfs_state         *state;
    struct chimera_vfs_file_state    *file;
    struct chimera_vfs_claim          a, b;
    struct chimera_claim_owner        owner;
    enum chimera_vfs_claim_result     r;
    struct chimera_vfs_claim_conflict conflict;

    fprintf(stderr, "\ntest_range_to_eof\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    /* Existing W lock from [1000, EOF). */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_NLM, 0xA, 1);
    chimera_vfs_claim_init_range(&a, true, false, 1000, UINT64_MAX, &owner);

    r = chimera_vfs_claim_try_acquire(state, file, &a, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "to-EOF write granted");

    /* A new write at [2000, 100) overlaps with the to-EOF range. */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_NLM, 0xB, 2);
    chimera_vfs_claim_init_range(&b, true, false, 2000, 100, &owner);

    r = chimera_vfs_claim_try_acquire(state, file, &b, &conflict);
    CHECK(r == CHIMERA_CLAIM_DENIED, "overlap with to-EOF range denied");

    /* A new write at [0, 500) does NOT overlap [1000, EOF). */
    chimera_vfs_claim_init_range(&b, true, false, 0, 500, &owner);
    r = chimera_vfs_claim_try_acquire(state, file, &b, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "non-overlap before to-EOF range granted");

    chimera_vfs_claim_release(state, file, &a);
    chimera_vfs_claim_release(state, file, &b);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_range_to_eof */

/* Test 5b: length==0 is a genuine zero-byte range in the claim
 * vocabulary (UINT64_MAX, not 0, is to-EOF).  A zero-length point
 * strictly inside another owner's exclusive range still collides (SMB
 * zero-byte lock semantics), while a zero-length range outside it — and
 * two zero-length ranges at the same offset — coexist. ---------------- */
// CLAIMTODO: zero-length geometry asserted from chimera_vfs_claim_range_overlap's half-open math (interior point conflicts, equal-offset zero pairs do not); no old-core test existed to compare against.
static void
test_range_zero_length(void)
{
    struct chimera_vfs_state         *state;
    struct chimera_vfs_file_state    *file;
    struct chimera_vfs_claim          a, zl_in, zl_out, zl_out2;
    struct chimera_claim_owner        owner;
    enum chimera_vfs_claim_result     r;
    struct chimera_vfs_claim_conflict conflict;

    fprintf(stderr, "\ntest_range_zero_length\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    /* Existing W lock from [1000, EOF). */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_NLM, 0xA, 1);
    chimera_vfs_claim_init_range(&a, true, false, 1000, UINT64_MAX, &owner);

    r = chimera_vfs_claim_try_acquire(state, file, &a, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "to-EOF write granted");

    /* Zero-length exclusive lock at an interior point of [1000, EOF). */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_NLM, 0xB, 2);
    chimera_vfs_claim_init_range(&zl_in, true, false, 2000, 0, &owner);

    r = chimera_vfs_claim_try_acquire(state, file, &zl_in, &conflict);
    CHECK(r == CHIMERA_CLAIM_DENIED, "zero-length point inside range denied");

    /* Zero-length exclusive lock before the range coexists. */
    chimera_vfs_claim_init_range(&zl_out, true, false, 500, 0, &owner);
    r = chimera_vfs_claim_try_acquire(state, file, &zl_out, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "zero-length point outside range granted");

    /* A second owner's zero-length lock at the SAME offset also coexists:
     * two empty ranges never overlap. */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_NLM, 0xC, 3);
    chimera_vfs_claim_init_range(&zl_out2, true, false, 500, 0, &owner);
    r = chimera_vfs_claim_try_acquire(state, file, &zl_out2, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "equal-offset zero-length pair coexists");

    chimera_vfs_claim_release(state, file, &a);
    chimera_vfs_claim_release(state, file, &zl_out);
    chimera_vfs_claim_release(state, file, &zl_out2);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_range_zero_length */

/* Test 6: share-mode conflict — same matrix as SMB sharemode --------- */
static void
test_share_vs_share(void)
{
    struct chimera_vfs_state         *state;
    struct chimera_vfs_file_state    *file;
    struct chimera_vfs_claim          a, b, c;
    struct chimera_claim_owner        owner;
    enum chimera_vfs_claim_result     r;
    struct chimera_vfs_claim_conflict conflict;

    fprintf(stderr, "\ntest_share_vs_share\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    /* Holder A: wants R, denies W (FILE_READ_DATA + !FILE_SHARE_WRITE). */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_SMB2, 0xA, 1);
    chimera_vfs_claim_init_smb_open(&a, CHIMERA_CLAIM_R, CHIMERA_CLAIM_W,
                                    &owner);

    r = chimera_vfs_claim_try_acquire(state, file, &a, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "first share R deny-W granted");

    /* Probe B: wants W — A denies W, so denied. */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_SMB2, 0xB, 2);
    chimera_vfs_claim_init_smb_open(&b, CHIMERA_CLAIM_W, 0, &owner);

    r = chimera_vfs_claim_try_acquire(state, file, &b, &conflict);
    CHECK(r == CHIMERA_CLAIM_DENIED, "share W denied by existing deny-W");

    /* Probe B: wants R only — no conflict. */
    chimera_vfs_claim_init_smb_open(&b, CHIMERA_CLAIM_R, 0, &owner);
    r = chimera_vfs_claim_try_acquire(state, file, &b, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "second share R coexists");

    /* Probe C: wants R and denies R — should conflict with a holder that
     * has R (both A and B do). */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_SMB2, 0xC, 3);
    chimera_vfs_claim_init_smb_open(&c, CHIMERA_CLAIM_R, CHIMERA_CLAIM_R,
                                    &owner);

    r = chimera_vfs_claim_try_acquire(state, file, &c, &conflict);
    CHECK(r == CHIMERA_CLAIM_DENIED,
          "share probe denying R denied because another holder has R");
    CHECK((conflict.used & CHIMERA_CLAIM_R) != 0,
          "conflict names an R-using holder");

    chimera_vfs_claim_release(state, file, &a);
    chimera_vfs_claim_release(state, file, &b);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_share_vs_share */

/* Test 7: caching claims — CW is exclusive, CR can be shared across owners */
static void
test_caching_lease_basics(void)
{
    struct chimera_vfs_state         *state;
    struct chimera_vfs_file_state    *file;
    struct chimera_vfs_claim          a, b;
    struct chimera_claim_owner        owner;
    enum chimera_vfs_claim_result     r;
    struct chimera_vfs_claim_conflict conflict;

    fprintf(stderr, "\ntest_caching_lease_basics\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    /* Client A holds R-cache (a LEVEL_II oplock). */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_SMB2, 0xA, 1);
    chimera_vfs_claim_init_oplock(&a, CHIMERA_CLAIM_CR, &owner);

    r = chimera_vfs_claim_try_acquire(state, file, &a, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "first R-cache granted");

    /* Client B requests R-cache — should also be granted (CR is shared
     * across different owners; this is SMB2 Level2 oplock semantics). */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_SMB2, 0xB, 2);
    chimera_vfs_claim_init_oplock(&b, CHIMERA_CLAIM_CR, &owner);

    r = chimera_vfs_claim_try_acquire(state, file, &b, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED,
          "second R-cache from different owner coexists (Level2 semantics)");

    chimera_vfs_claim_release(state, file, &a);
    chimera_vfs_claim_release(state, file, &b);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_caching_lease_basics */

/* Test 8: caching CW-claim forces break of CR-cache on other client --- */
static void
test_caching_w_breaks_r(void)
{
    struct chimera_vfs_state         *state;
    struct chimera_vfs_file_state    *file;
    struct chimera_vfs_claim          a, b;
    struct chimera_claim_owner        owner;
    enum chimera_vfs_claim_result     r;
    struct chimera_vfs_claim_conflict conflict;
    struct break_recorder             rec = { 0 };

    fprintf(stderr, "\ntest_caching_w_breaks_r\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    /* Client A holds R-cache, with a registered break callback. */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_SMB2, 0xA, 1);
    chimera_vfs_claim_init_oplock(&a, CHIMERA_CLAIM_CR, &owner);
    a.break_cb   = recording_break_cb;
    a.cb_private = &rec;

    r = chimera_vfs_claim_try_acquire(state, file, &a, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "R-cache granted");

    /* Client B requests W-cache.  This must initiate a break on A. */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_SMB2, 0xB, 2);
    chimera_vfs_claim_init_oplock(&b, CHIMERA_CLAIM_CW, &owner);

    r = chimera_vfs_claim_try_acquire(state, file, &b, &conflict);
    CHECK(r == CHIMERA_CLAIM_BREAKING, "W-cache request triggers break");
    CHECK(conflict.owner.client_key == 0xA &&
          conflict.used == CHIMERA_CLAIM_CR,
          "conflict reports the existing R-cache holder by value");
    CHECK(rec.fired == 1, "break callback fired exactly once");
    CHECK(rec.last_claim == &a, "break callback received correct claim");
    CHECK(a.break_state == CHIMERA_CLAIM_BREAK_BREAKING, "claim marked BREAKING");

    /* Client A acks the break by downgrading to nothing. */
    chimera_vfs_claim_ack(&a, 0);
    CHECK(a.break_state == CHIMERA_CLAIM_BREAK_ACKED, "claim moved to ACKED");
    CHECK(a.used == 0, "claim mode downgraded");

    /* B can retry now.  In real code the protocol would release the acked
     * claim before retrying; we mimic that here. */
    chimera_vfs_claim_release(state, file, &a);

    r = chimera_vfs_claim_try_acquire(state, file, &b, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "W-cache granted after break ack");

    chimera_vfs_claim_release(state, file, &b);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_caching_w_breaks_r */

/* Test 9: SMB lease-key coalescing — same client_key+LeaseKey doesn't break */
static void
test_smb_lease_key_coalesces(void)
{
    struct chimera_vfs_state         *state;
    struct chimera_vfs_file_state    *file;
    struct chimera_vfs_claim          a, b;
    struct chimera_claim_owner        owner;
    enum chimera_vfs_claim_result     r;
    struct chimera_vfs_claim_conflict conflict;
    struct break_recorder             rec = { 0 };

    fprintf(stderr, "\ntest_smb_lease_key_coalesces\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    /* SMB client opens the file with a lease key (client_key=client_guid,
     * the 128-bit LeaseKey in owner.key — the KEY circle). */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_SMB2, 0xC11E1700, 1);
    owner.owner_hi = 0xDEAD;
    set_owner_key(&owner, 1, 0xDEAD);
    chimera_vfs_claim_init_rqls(&a,
                                CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW |
                                CHIMERA_CLAIM_H,
                                &owner);
    a.break_cb   = recording_break_cb;
    a.cb_private = &rec;

    r = chimera_vfs_claim_try_acquire(state, file, &a, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "RWH lease granted on first open");

    /* Same client opens the same file again with the same lease_key —
     * Samba's locking.tdb rule says this is the same lease holder, no
     * break required. */
    chimera_vfs_claim_init_rqls(&b,
                                CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW |
                                CHIMERA_CLAIM_H,
                                &owner);

    r = chimera_vfs_claim_try_acquire(state, file, &b, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED,
          "second open with same lease_key coalesces without break");
    CHECK(rec.fired == 0, "no break invoked for same-owner reopen");

    chimera_vfs_claim_release(state, file, &a);
    chimera_vfs_claim_release(state, file, &b);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_smb_lease_key_coalesces */

/* Test 10: caching W-claim must be broken by another client's W request,
* and revoke (timeout-equivalent) lets the new request through ------- */
static void
test_caching_break_revoke(void)
{
    struct chimera_vfs_state         *state;
    struct chimera_vfs_file_state    *file;
    struct chimera_vfs_claim          a, b;
    struct chimera_claim_owner        owner;
    enum chimera_vfs_claim_result     r;
    struct chimera_vfs_claim_conflict conflict;
    struct break_recorder             rec = { 0 };

    fprintf(stderr, "\ntest_caching_break_revoke\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    /* Client A holds a write delegation. */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_NFSV4, 0xA, 1);
    chimera_vfs_claim_init_delegation(&a, true, &owner);
    a.break_cb   = recording_break_cb;
    a.cb_private = &rec;

    r = chimera_vfs_claim_try_acquire(state, file, &a, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "W delegation granted");

    /* Different NFSv4 client wants W — break A. */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_NFSV4, 0xB, 2);
    chimera_vfs_claim_init_delegation(&b, true, &owner);

    r = chimera_vfs_claim_try_acquire(state, file, &b, &conflict);
    CHECK(r == CHIMERA_CLAIM_BREAKING, "second W delegation initiates break");
    CHECK(rec.fired == 1, "break_cb fired once");

    /* Client A doesn't respond — simulate revoke (forcible expiry). */
    chimera_vfs_claim_revoke(&a);
    CHECK(a.break_state == CHIMERA_CLAIM_BREAK_REVOKED, "claim moved to REVOKED");
    CHECK(a.used == 0, "revoked claim has empty mode");

    chimera_vfs_claim_release(state, file, &a);
    r = chimera_vfs_claim_try_acquire(state, file, &b, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "new W granted after revoke");

    chimera_vfs_claim_release(state, file, &b);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_caching_break_revoke */

/* Test 11: range-lock probe that would clash with a caching W-claim on
 * another client triggers a break instead of immediate denial -------- */
static void
test_range_breaks_caching(void)
{
    struct chimera_vfs_state         *state;
    struct chimera_vfs_file_state    *file;
    struct chimera_vfs_claim          cache, range;
    struct chimera_claim_owner        owner;
    enum chimera_vfs_claim_result     r;
    struct chimera_vfs_claim_conflict conflict;
    struct break_recorder             rec = { 0 };

    fprintf(stderr, "\ntest_range_breaks_caching\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    /* Client A: W-caching lease. */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_SMB2, 0xA, 1);
    chimera_vfs_claim_init_rqls(&cache, CHIMERA_CLAIM_CW, &owner);
    cache.break_cb   = recording_break_cb;
    cache.cb_private = &rec;

    r = chimera_vfs_claim_try_acquire(state, file, &cache, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "W-cache granted");

    /* Client B asks for a byte-range read lock — must break A's W-cache
     * because reads on B may see writes that A has cached. */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_NLM, 0xB, 2);
    chimera_vfs_claim_init_range(&range, false, false, 0, 100, &owner);

    r = chimera_vfs_claim_try_acquire(state, file, &range, &conflict);
    CHECK(r == CHIMERA_CLAIM_BREAKING,
          "range read lock against other-client W-cache triggers break");
    CHECK(rec.fired == 1, "break_cb fired on caching holder");
    CHECK(conflict.owner.client_key == 0xA &&
          conflict.construct == CHIMERA_CONSTRUCT_RQLS,
          "conflict reports the caching holder");
    CHECK(conflict.offset == 0 && conflict.length == UINT64_MAX,
          "whole-file conflict reports offset 0 / length UINT64_MAX");

    chimera_vfs_claim_release(state, file, &cache);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_range_breaks_caching */

/* Test 11b: an NFSv4 client that holds a caching (delegation) claim may take a
 * byte-range lock on the same file under a DIFFERENT lock-owner without
 * conflicting with -- or recalling -- its own delegation (RFC 8881 §10.2: a
 * delegation is per-client).  A range lock from a DIFFERENT NFSv4 client must
 * still break the delegation. ------------------------------------------------ */
static void
test_nfs4_lock_vs_own_delegation(void)
{
    struct chimera_vfs_state         *state;
    struct chimera_vfs_file_state    *file;
    struct chimera_vfs_claim          deleg, self_lock, other_lock;
    struct chimera_claim_owner        owner;
    enum chimera_vfs_claim_result     r;
    struct chimera_vfs_claim_conflict conflict;
    struct break_recorder             rec = { 0 };

    fprintf(stderr, "\ntest_nfs4_lock_vs_own_delegation\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    /* Client 0xA holds a WRITE delegation.  A delegation is keyed by the
     * file-handle hash, so its owner_lo differs from any lock. */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_NFSV4, 0xA, 0xF11E);
    chimera_vfs_claim_init_delegation(&deleg, true, &owner);
    deleg.break_cb   = recording_break_cb;
    deleg.cb_private = &rec;

    r = chimera_vfs_claim_try_acquire(state, file, &deleg, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "write delegation granted");

    /* The SAME client takes a write byte-range lock under a different
     * lock-owner -- granted outright, and the delegation is NOT recalled. */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_NFSV4, 0xA, 0x10C0);
    chimera_vfs_claim_init_range(&self_lock, true, false, 0, 100, &owner);

    r = chimera_vfs_claim_try_acquire(state, file, &self_lock, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED,
          "same-client lock on own delegated file granted");
    CHECK(rec.fired == 0, "own delegation not recalled by the client's lock");

    /* Drop the client's own range lock so the next probe is evaluated purely
     * against the delegation (otherwise range-vs-range would deny it first). */
    chimera_vfs_claim_release(state, file, &self_lock);

    /* A DIFFERENT client's lock still breaks the delegation. */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_NFSV4, 0xB, 0x20C0);
    chimera_vfs_claim_init_range(&other_lock, true, false, 0, 100, &owner);

    r = chimera_vfs_claim_try_acquire(state, file, &other_lock, &conflict);
    CHECK(r == CHIMERA_CLAIM_BREAKING,
          "other-client lock breaks the delegation");
    CHECK(rec.fired == 1, "delegation recalled for the other client");
    CHECK(conflict.owner.client_key == 0xA &&
          conflict.construct == CHIMERA_CONSTRUCT_DELEG_W,
          "conflict reports the delegation holder");

    chimera_vfs_claim_release(state, file, &deleg);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_nfs4_lock_vs_own_delegation */

/* Acquire-callback recorder for the ticketed-API tests.  The conflict
 * argument is only valid inside the callback, so it is copied by value. */
struct acquire_recorder {
    int                               fired;
    enum chimera_vfs_claim_result last_result;
    struct chimera_vfs_claim         *last_granted;
    int                               has_conflict;
    struct chimera_vfs_claim_conflict last_conflict;
};

static void
recording_acquire_cb(
    enum chimera_vfs_claim_result            result,
    struct chimera_vfs_claim                *granted,
    const struct chimera_vfs_claim_conflict *conflict,
    void                                    *priv)
{
    struct acquire_recorder *r = priv;

    r->fired++;
    r->last_result  = result;
    r->last_granted = granted;
    r->has_conflict = (conflict != NULL);
    if (conflict) {
        r->last_conflict = *conflict;
    }
} /* recording_acquire_cb */

/* Test 12: ticketed acquire wait=false fires cb synchronously --------- */
static void
test_async_acquire_immediate(void)
{
    struct chimera_vfs_state          *state;
    struct chimera_vfs_file_state     *file;
    struct chimera_vfs_claim           claim;
    struct chimera_claim_owner         owner;
    struct chimera_vfs_pending_acquire ticket;
    struct acquire_recorder            rec = { 0 };

    fprintf(stderr, "\ntest_async_acquire_immediate\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    init_owner(&owner, CHIMERA_CLAIM_PROTO_NFSV4, 0xA, 1);
    chimera_vfs_claim_init_range(&claim, true, false, 0, 100, &owner);

    chimera_vfs_claim_acquire(NULL, state, file, &claim, &ticket, false, false,
                              recording_acquire_cb, NULL, &rec);
    CHECK(rec.fired == 1, "cb fires synchronously on first acquire");
    CHECK(rec.last_result == CHIMERA_CLAIM_GRANTED, "result is GRANTED");
    CHECK(rec.last_granted == &claim, "granted claim points to inserted claim");

    chimera_vfs_claim_release(state, file, &claim);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_async_acquire_immediate */

/* Test 13: ticketed acquire wait=true queues on BREAKING, fires after ack */
static void
test_async_acquire_wait_then_ack(void)
{
    struct chimera_vfs_state          *state;
    struct chimera_vfs_file_state     *file;
    struct chimera_vfs_claim           cache, range;
    struct chimera_claim_owner         owner;
    struct chimera_vfs_pending_acquire ticket;
    struct acquire_recorder            rec  = { 0 };
    struct break_recorder              brec = { 0 };

    fprintf(stderr, "\ntest_async_acquire_wait_then_ack\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    /* Existing breakable W-cache on client A. */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_SMB2, 0xA, 1);
    chimera_vfs_claim_init_rqls(&cache, CHIMERA_CLAIM_CW, &owner);
    cache.break_cb   = recording_break_cb;
    cache.cb_private = &brec;
    chimera_vfs_claim_try_acquire(state, file, &cache, NULL);

    /* Client B's range read lock — must wait on the W-cache break. */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_NLM, 0xB, 2);
    chimera_vfs_claim_init_range(&range, false, false, 0, 100, &owner);

    chimera_vfs_claim_acquire(NULL, state, file, &range, &ticket, true, false,
                              recording_acquire_cb, NULL, &rec);
    CHECK(rec.fired == 0, "wait acquire does not fire cb yet");
    CHECK(brec.fired == 1, "break_cb fired on conflicting holder");
    CHECK(ticket.queued == true, "ticket is queued");

    /* Client A acks with full release.  Pump runs and retries our ticket. */
    chimera_vfs_claim_ack(&cache, 0);

    CHECK(rec.fired == 1, "cb fires once ack completes");
    CHECK(rec.last_result == CHIMERA_CLAIM_GRANTED, "retry granted");
    CHECK(ticket.queued == false, "ticket is dequeued");

    chimera_vfs_claim_release(state, file, &cache);
    chimera_vfs_claim_release(state, file, &range);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_async_acquire_wait_then_ack */

/* Test 14: cancel removes a queued ticket and suppresses cb -------- */
static void
test_async_acquire_cancel(void)
{
    struct chimera_vfs_state          *state;
    struct chimera_vfs_file_state     *file;
    struct chimera_vfs_claim           cache, range;
    struct chimera_claim_owner         owner;
    struct chimera_vfs_pending_acquire ticket;
    struct acquire_recorder            rec  = { 0 };
    struct break_recorder              brec = { 0 };
    bool                               cancelled;

    fprintf(stderr, "\ntest_async_acquire_cancel\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    init_owner(&owner, CHIMERA_CLAIM_PROTO_SMB2, 0xA, 1);
    chimera_vfs_claim_init_rqls(&cache, CHIMERA_CLAIM_CW, &owner);
    cache.break_cb   = recording_break_cb;
    cache.cb_private = &brec;
    chimera_vfs_claim_try_acquire(state, file, &cache, NULL);

    init_owner(&owner, CHIMERA_CLAIM_PROTO_NLM, 0xB, 2);
    chimera_vfs_claim_init_range(&range, false, false, 0, 100, &owner);

    chimera_vfs_claim_acquire(NULL, state, file, &range, &ticket, true, false,
                              recording_acquire_cb, NULL, &rec);
    CHECK(ticket.queued == true, "ticket queued while waiting");

    /* Caller changes its mind (e.g., NLM CANCEL). */
    cancelled = chimera_vfs_claim_cancel(state, &ticket);
    CHECK(cancelled == true, "cancel reports the ticket was dequeued");
    CHECK(ticket.queued == false, "ticket no longer queued after cancel");
    CHECK(rec.fired == 0, "cb does not fire for cancelled acquire");

    /* Acking the conflict no longer fires the cancelled ticket. */
    chimera_vfs_claim_ack(&cache, 0);
    chimera_vfs_claim_release(state, file, &cache);
    CHECK(rec.fired == 0, "cancelled ticket stays silent after ack");

    /* Cancelling again is a no-op (returns false). */
    cancelled = chimera_vfs_claim_cancel(state, &ticket);
    CHECK(cancelled == false, "second cancel returns false");

    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_async_acquire_cancel */

/* Test 14b: the backend RANGE confirm lane ------------------------- */

/* A projectable RANGE grant does not complete when the pump runs: its
 * ticket rides the service work FIFO awaiting the backend confirm.  Both
 * points at which a cancel can still claim such a ticket are tested here,
 * and BOTH must answer without blocking -- chimera_vfs_claim_cancel used
 * to spin until the confirm's callback returned, which deadlocked any
 * caller holding a lock that callback also takes (NLM client teardown
 * cancelling under nlm_state.mutex against the confirm's NLM4_GRANTED
 * callback).  There is no backend here: forcing lease_capable is what puts
 * the core on the projecting path, which is otherwise reachable only with
 * a real CAP_LEASE module attached. */
static void
test_backend_confirm_lane_cancel(void)
{
    struct chimera_vfs_state          *state;
    struct chimera_vfs_file_state     *file;
    struct chimera_vfs_claim           held, want;
    struct chimera_claim_owner         owner;
    struct chimera_vfs_pending_acquire ticket;
    struct chimera_vfs_bl_range_op     op;
    struct acquire_recorder            rec = { 0 };
    bool                               cancelled;

    fprintf(stderr, "\ntest_backend_confirm_lane_cancel\n");

    state = chimera_vfs_state_init();
    /* Pretend a CAP_LEASE backend is attached (short-circuits the lazy
     * module probe, which needs a real vfs). */
    state->lease_probed  = 1;
    state->lease_capable = 1;

    file = get_file(state, 2);

    /* A holds [0,100) exclusively; B blocks behind it. */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_NLM, 0xC, 1);
    chimera_vfs_claim_init_range(&held, true, false, 0, 100, &owner);
    chimera_vfs_claim_try_acquire(state, file, &held, NULL);

    init_owner(&owner, CHIMERA_CLAIM_PROTO_NLM, 0xD, 2);
    chimera_vfs_claim_init_range(&want, true, false, 0, 100, &owner);
    chimera_vfs_claim_acquire(NULL, state, file, &want, &ticket, true, true,
                              recording_acquire_cb, NULL, &rec);
    CHECK(ticket.queued == true, "blocking range ticket queues");

    /* Releasing A pumps B.  B is granted locally but, being projectable,
     * is handed to the service lane instead of completing. */
    chimera_vfs_claim_release(state, file, &held);
    CHECK(rec.fired == 0, "pump defers a projectable grant to the lane");
    CHECK(state->work_head != NULL, "ticket is queued on the work FIFO");

    /* Still on the FIFO: the cancel yanks it outright. */
    cancelled = chimera_vfs_claim_cancel(state, &ticket);
    CHECK(cancelled == true, "cancel claims a ticket queued for confirm");
    CHECK(state->work_head == NULL, "cancel removed the FIFO entry");
    CHECK(rec.fired == 0, "cancelled ticket never fires its callback");

    /* Already dispatched: the confirm is in flight and its op is linked,
     * so the cancel claims the op instead and the completion will skip the
     * callback.  It must decide immediately rather than wait the confirm
     * out. */
    memset(&op, 0, sizeof(op));
    op.state               = state;
    op.file                = file;
    op.ticket              = &ticket;
    op.serial_lane         = true;
    state->confirm_head    = &op;
    state->work_confirming = true;

    cancelled = chimera_vfs_claim_cancel(state, &ticket);
    CHECK(cancelled == true, "cancel claims a confirm already in flight");
    CHECK(op.cancelled == true, "the in-flight confirm is marked cancelled");

    /* Once the completion has unlinked its op -- which it does before
     * invoking the callback -- the callback owns the ticket and the cancel
     * must say so instead of blocking. */
    state->confirm_head    = NULL;
    state->work_confirming = false;

    cancelled = chimera_vfs_claim_cancel(state, &ticket);
    CHECK(cancelled == false, "cancel yields once the callback owns the ticket");

    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_backend_confirm_lane_cancel */

/* Test 15: release pumps pending queue ----------------------------- */
static void
test_release_pumps_pending(void)
{
    struct chimera_vfs_state          *state;
    struct chimera_vfs_file_state     *file;
    struct chimera_vfs_claim           cache, range;
    struct chimera_claim_owner         owner;
    struct chimera_vfs_pending_acquire ticket;
    struct acquire_recorder            rec  = { 0 };
    struct break_recorder              brec = { 0 };

    fprintf(stderr, "\ntest_release_pumps_pending\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    init_owner(&owner, CHIMERA_CLAIM_PROTO_SMB2, 0xA, 1);
    chimera_vfs_claim_init_rqls(&cache, CHIMERA_CLAIM_CW, &owner);
    cache.break_cb   = recording_break_cb;
    cache.cb_private = &brec;
    chimera_vfs_claim_try_acquire(state, file, &cache, NULL);

    init_owner(&owner, CHIMERA_CLAIM_PROTO_NLM, 0xB, 2);
    chimera_vfs_claim_init_range(&range, false, false, 0, 100, &owner);

    chimera_vfs_claim_acquire(NULL, state, file, &range, &ticket, true, false,
                              recording_acquire_cb, NULL, &rec);
    CHECK(rec.fired == 0, "wait acquire queued");

    /* If the holder protocol skips ack and just releases (e.g., on
     * close), the release path should still pump the pending queue. */
    chimera_vfs_claim_release(state, file, &cache);
    CHECK(rec.fired == 1, "release alone pumps pending queue");
    CHECK(rec.last_result == CHIMERA_CLAIM_GRANTED, "pending granted");

    chimera_vfs_claim_release(state, file, &range);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_release_pumps_pending */

/* Test 16: chimera_vfs_claim_test exposes conflict without inserting */
static void
test_lease_test(void)
{
    struct chimera_vfs_state         *state;
    struct chimera_vfs_file_state    *file;
    struct chimera_vfs_claim          held, probe;
    struct chimera_claim_owner        owner;
    struct chimera_vfs_claim_conflict conflict;
    enum chimera_vfs_claim_result     r;

    fprintf(stderr, "\ntest_lease_test\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    init_owner(&owner, CHIMERA_CLAIM_PROTO_NFSV4, 0xA, 1);
    chimera_vfs_claim_init_range(&held, true, false, 0, 100, &owner);
    chimera_vfs_claim_try_acquire(state, file, &held, NULL);

    /* Probe overlapping write from another owner — should DENY. */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_NFSV4, 0xB, 2);
    chimera_vfs_claim_init_range(&probe, true, false, 50, 100, &owner);

    r = chimera_vfs_claim_test(file, &probe, &conflict);
    CHECK(r == CHIMERA_CLAIM_DENIED, "test reports conflict");
    CHECK(conflict.owner.client_key == 0xA && conflict.owner.owner_lo == 1 &&
          conflict.offset == 0 && conflict.length == 100,
          "test names the holder by value");

    /* Probe is NOT inserted — file should still have only one claim. */
    CHECK(file->claims[CHIMERA_CLAIM_CLASS_RANGE] == &held &&
          held.next == NULL,
          "test does not insert the probe");

    chimera_vfs_claim_release(state, file, &held);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_lease_test */

/* Test 17: a breakable ACCESS holder (chimera's implicit I/O claim) is
 * recalled rather than hard-denied when a conflicting client open or
 * delegation arrives. ------------------------------------------------- */
static void
test_breakable_share_recall(void)
{
    struct chimera_vfs_state         *state;
    struct chimera_vfs_file_state    *file;
    struct chimera_vfs_claim          impl, deny, deleg;
    struct chimera_claim_owner        owner;
    enum chimera_vfs_claim_result     r;
    struct chimera_vfs_claim_conflict conflict;
    struct break_recorder             rec = { 0 };

    fprintf(stderr, "\ntest_breakable_share_recall\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    /* Chimera's implicit claim: construct IMPLICIT, deny-nothing ACCESS
     * claim using W, breakable.  Hand-assembled the way vfs_claim_io.c
     * builds it — there is deliberately no public constructor for the
     * internal claim. */
    // CLAIMTODO: no init_* constructor exists for CHIMERA_CONSTRUCT_IMPLICIT; mirroring vfs_claim_io.c's field assembly is the closest faithful stand-in for the old PROTO_INTERNAL share lease.
    memset(&impl, 0, sizeof(impl));
    impl.construct  = CHIMERA_CONSTRUCT_IMPLICIT;
    impl.klass      = CHIMERA_CLAIM_CLASS_ACCESS;
    impl.used       = CHIMERA_CLAIM_W;
    impl.advertised = CHIMERA_CLAIM_W;
    impl.length     = UINT64_MAX;
    init_owner(&owner, CHIMERA_CLAIM_PROTO_INTERNAL, 0, 1);
    impl.owner      = owner;
    impl.break_cb   = recording_break_cb;
    impl.cb_private = &rec;

    r = chimera_vfs_claim_try_acquire(state, file, &impl, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "implicit deny=0 claim granted");

    /* A real client open that denies write conflicts with the implicit W
     * user — but because the holder is breakable, it is recalled, not
     * denied. */
    // CLAIMTODO: suspected core gap — contended_floor() never strips a displaced ACCESS holder's data bits (floor == used), so begin_break no-ops and try_acquire can livelock here; asserting the intended old-core behavior (BREAKING + one break_cb).
    init_owner(&owner, CHIMERA_CLAIM_PROTO_SMB2, 0xB, 2);
    chimera_vfs_claim_init_smb_open(&deny, CHIMERA_CLAIM_W, CHIMERA_CLAIM_W,
                                    &owner);

    r = chimera_vfs_claim_try_acquire(state, file, &deny, &conflict);
    CHECK(r == CHIMERA_CLAIM_BREAKING,
          "client deny-W open recalls implicit claim (not denied)");
    CHECK(rec.fired == 1, "implicit claim break_cb fired once");

    /* An NFSv4 write-delegation grant likewise recalls the implicit claim
     * (CACHE-vs-ACCESS path). */
    rec.fired        = 0;
    impl.break_state = CHIMERA_CLAIM_BREAK_IDLE; /* make it recallable again */
    impl.advertised  = impl.used;                /* re-arm the dropped mode */

    init_owner(&owner, CHIMERA_CLAIM_PROTO_NFSV4, 0xC, 0);
    chimera_vfs_claim_init_delegation(&deleg, true, &owner);

    r = chimera_vfs_claim_try_acquire(state, file, &deleg, &conflict);
    CHECK(r == CHIMERA_CLAIM_BREAKING,
          "NFSv4 deleg grant recalls implicit claim (not denied)");
    CHECK(rec.fired == 1, "implicit claim break_cb fired for deleg conflict");

    chimera_vfs_claim_release(state, file, &impl);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_breakable_share_recall */

/* Test 18: a non-breakable ACCESS holder (an ordinary client open) still
* hard-denies a conflicting acquire. --------------------------------- */
static void
test_nonbreakable_share_denies(void)
{
    struct chimera_vfs_state         *state;
    struct chimera_vfs_file_state    *file;
    struct chimera_vfs_claim          a, b;
    struct chimera_claim_owner        owner;
    enum chimera_vfs_claim_result     r;
    struct chimera_vfs_claim_conflict conflict;

    fprintf(stderr, "\ntest_nonbreakable_share_denies\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    /* Ordinary client open, deny-W, NOT breakable. */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_SMB2, 0xA, 1);
    chimera_vfs_claim_init_smb_open(&a, CHIMERA_CLAIM_R, CHIMERA_CLAIM_W,
                                    &owner);

    r = chimera_vfs_claim_try_acquire(state, file, &a, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "non-breakable share granted");

    init_owner(&owner, CHIMERA_CLAIM_PROTO_SMB2, 0xB, 2);
    chimera_vfs_claim_init_smb_open(&b, CHIMERA_CLAIM_W, 0, &owner);

    r = chimera_vfs_claim_try_acquire(state, file, &b, &conflict);
    CHECK(r == CHIMERA_CLAIM_DENIED,
          "non-breakable deny-W share still denies");

    chimera_vfs_claim_release(state, file, &a);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_nonbreakable_share_denies */

/* Test 19: the share-conflict escape onto a handle-caching RqLs lease must
* never land on the ACQUIRER's own lease.  A second open under one LeaseKey
* presents that key on both opens, so the escape's RqLs arm -- which matches
* the blocking open's own cache grant via claim->own_cache -- would otherwise
* match the probe's own lease and break a handle cache the client is still
* using, parking the conflicting open behind its own ack.  A DIFFERENT key
* must still escape, or a conflicting open could never wait for a
* handle-lease holder to close. ---------------------------------------- */
static void
test_share_escape_skips_own_lease(void)
{
    struct chimera_vfs_state         *state;
    struct chimera_vfs_file_state    *file;
    struct chimera_vfs_claim_grant   *grant = NULL;
    struct chimera_claim_owner        lease_owner;
    struct chimera_claim_owner        owner;
    struct chimera_vfs_claim          template_claim;
    struct chimera_vfs_claim          holder, probe;
    enum chimera_vfs_claim_result     r;
    struct chimera_vfs_claim_conflict conflict;
    struct break_recorder             rec         = { 0 };
    int                               holder_open = 0; /* stands in for open_file */

    fprintf(stderr, "\ntest_share_escape_skips_own_lease\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    /* The holder's RqLs lease under LeaseKey 0x1111/0x2222: RWH, breakable,
     * keyed by lease key (owner.key), managed by a core-owned grant -- the
     * shape the escape's RqLs arm matches. */
    init_owner(&lease_owner, CHIMERA_CLAIM_PROTO_SMB2, 0xA, 0x1111);
    lease_owner.owner_hi = 0x2222;
    set_owner_key(&lease_owner, 0x1111, 0x2222);

    chimera_vfs_claim_init_rqls(&template_claim,
                                CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW |
                                CHIMERA_CLAIM_H,
                                &lease_owner);
    template_claim.break_cb   = recording_break_cb;
    template_claim.cb_private = &rec;

    r = chimera_vfs_claim_grant_acquire(state, file, &template_claim, 0,
                                        /* is_v2 */ 0,
                                        CHIMERA_CLAIM_GRANT_EXACT,
                                        /* member_seed */ NULL, NULL,
                                        &grant, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED && grant, "holder RWH lease granted");

    /* The holder's open: ShareAccess=0 (denies R and W), NOT breakable (an
     * ordinary client open), linked to the lease above via own_cache (the
     * holder-lite link that replaces the old own_lease_key stamp). */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_SMB2, 0xA, 1);
    owner.owner_hi = 1;
    chimera_vfs_claim_init_smb_open(&holder,
                                    CHIMERA_CLAIM_R | CHIMERA_CLAIM_W,
                                    CHIMERA_CLAIM_R | CHIMERA_CLAIM_W,
                                    &owner);
    holder.cb_private = &holder_open;
    holder.own_cache  = grant;

    r = chimera_vfs_claim_try_acquire(state, file, &holder, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "holder deny-all open granted");

    /* Same client, SAME LeaseKey, a second open that share-conflicts.  The
     * conflict is real and stands, but it must be answered without breaking
     * the client's own lease.  The acquirer's LeaseKey travels in its
     * owner.key (replacing the old break_skip stamp); the core's KEY circle
     * refuses the escape onto a same-key cache. */
    rec.fired = 0;
    init_owner(&owner, CHIMERA_CLAIM_PROTO_SMB2, 0xA, 2);
    owner.owner_hi = 2;
    set_owner_key(&owner, 0x1111, 0x2222);
    chimera_vfs_claim_init_smb_open(&probe, CHIMERA_CLAIM_R, 0, &owner);

    r = chimera_vfs_claim_try_acquire(state, file, &probe, &conflict);
    CHECK(r == CHIMERA_CLAIM_DENIED,
          "same-lease-key second open denied, not parked");
    CHECK(rec.fired == 0, "own handle lease not broken by its own opener");

    /* A different LeaseKey is a genuine peer: the escape applies, so the open
     * parks on the holder's HANDLE break instead of being denied outright. */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_SMB2, 0xA, 3);
    owner.owner_hi = 3;
    set_owner_key(&owner, 0x3333, 0x4444);
    chimera_vfs_claim_init_smb_open(&probe, CHIMERA_CLAIM_R, 0, &owner);

    r = chimera_vfs_claim_try_acquire(state, file, &probe, &conflict);
    CHECK(r == CHIMERA_CLAIM_BREAKING,
          "different-lease-key open still escapes onto the H break");
    CHECK(rec.fired == 1, "holder lease broken for a peer opener");
    CHECK((rec.last_needed_mode & CHIMERA_CLAIM_H) == 0 &&
          (rec.last_needed_mode &
           (CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW)) ==
          (CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW),
          "break drops H only, leaving read/write caching (RWH -> RW)");

    chimera_vfs_claim_release(state, file, &holder);
    chimera_vfs_claim_grant_release(state, grant, false);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_share_escape_skips_own_lease */

/* Test 20: MS-SMB2 sole-opener rule, legacy arm (smb2.oplock.batch10/13/14/16,
 * exclusive9): an exclusive/batch oplock request is hard-DENIED (never parked)
 * when ANOTHER client holds a real share reservation, and the create path's
 * cap steps it down to a shared read cache (LEVEL_II).  The requester's own
 * client's opens, inert attribute-only registrations, and parked
 * (disconnected-durable) holders never cap it (batch9/9a,
 * keep-disconnected-rh-*). ------------------------------------------------ */
static void
test_sole_opener_legacy_cap(void)
{
    struct chimera_vfs_state         *state;
    struct chimera_vfs_file_state    *file;
    struct chimera_vfs_claim          peer_open, own_open, probe, tmpl;
    struct chimera_claim_owner        owner;
    enum chimera_vfs_claim_result     r;
    struct chimera_vfs_claim_conflict conflict;
    uint8_t                           capped;

    fprintf(stderr, "\ntest_sole_opener_legacy_cap\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    /* Client 0xB holds a real (non-inert) share reservation, share-all. */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_SMB2, 0xB, 1);
    chimera_vfs_claim_init_smb_open(&peer_open,
                                    CHIMERA_CLAIM_R | CHIMERA_CLAIM_W, 0,
                                    &owner);
    r = chimera_vfs_claim_try_acquire(state, file, &peer_open, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "peer client open granted");

    /* Client 0xA requests an EXCLUSIVE oplock: DENIED outright by the mere
     * existence of the other client's open -- a hard cap, not a wait
     * (the old would_conflict arm returned DENIED over BREAKING). */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_SMB2, 0xA, 2);
    chimera_vfs_claim_init_oplock(&probe,
                                  CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW,
                                  &owner);
    r = chimera_vfs_claim_test(file, &probe, &conflict);
    CHECK(r == CHIMERA_CLAIM_DENIED,
          "exclusive oplock DENIED (not BREAKING) behind another client's open");
    CHECK(conflict.owner.client_key == 0xB &&
          conflict.construct == CHIMERA_CONSTRUCT_SMB_OPEN,
          "conflict names the blocking share reservation");

    /* The create path's cap walks BATCH down CW -> H -> CR: LEVEL_II. */
    chimera_vfs_claim_init_oplock(&tmpl,
                                  CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW |
                                  CHIMERA_CLAIM_H,
                                  &owner);
    capped = chimera_vfs_claim_grant_cap_mode(file, &tmpl, false);
    CHECK(capped == CHIMERA_CLAIM_CR,
          "batch request caps to CR (LEVEL_II) behind another client's open");

    /* A parked (disconnected durable) holder is courtesy-held and caps
     * nobody. */
    chimera_vfs_claim_park(&peer_open, true);
    r = chimera_vfs_claim_test(file, &probe, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED,
          "parked peer open does not cap the oplock");
    chimera_vfs_claim_park(&peer_open, false);

    chimera_vfs_claim_release(state, file, &peer_open);

    /* An inert (0,0) attribute-only registration is not a real opener
     * (smb2.oplock.batch9: attrs-only open keeps its full BATCH grantable
     * to a later prober -- here, does not cap the probe). */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_SMB2, 0xB, 3);
    chimera_vfs_claim_init_smb_open(&peer_open, 0, 0, &owner);
    r = chimera_vfs_claim_try_acquire(state, file, &peer_open, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "inert attrs-only open granted");
    capped = chimera_vfs_claim_grant_cap_mode(file, &tmpl, false);
    CHECK(capped == (CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW | CHIMERA_CLAIM_H),
          "inert attrs-only open does not cap a batch request");
    chimera_vfs_claim_release(state, file, &peer_open);

    /* The requesting client's OWN open never caps its own oplock. */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_SMB2, 0xA, 4);
    chimera_vfs_claim_init_smb_open(&own_open,
                                    CHIMERA_CLAIM_R | CHIMERA_CLAIM_W, 0,
                                    &owner);
    r = chimera_vfs_claim_try_acquire(state, file, &own_open, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "own client open granted");
    capped = chimera_vfs_claim_grant_cap_mode(file, &tmpl, false);
    CHECK(capped == (CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW | CHIMERA_CLAIM_H),
          "own client's open does not cap the batch request");
    chimera_vfs_claim_release(state, file, &own_open);

    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_sole_opener_legacy_cap */

/* Test 21: sole-opener rule, RqLs arm (smb2.lease.oplock): an RqLs lease's H
 * is capped by a NON-lease-backed open -- even the requesting client's own
 * legacy-oplock open -- while a lease-backed (keyed) open, its own included,
 * never caps it.  Only H is at stake here (CW is arbitrated by the
 * cache-vs-cache rows). --------------------------------------------------- */
static void
test_sole_opener_rqls_h_cap(void)
{
    struct chimera_vfs_state         *state;
    struct chimera_vfs_file_state    *file;
    struct chimera_vfs_claim          legacy_open, keyed_open, tmpl;
    struct chimera_claim_owner        owner, lease_owner;
    enum chimera_vfs_claim_result     r;
    struct chimera_vfs_claim_conflict conflict;
    uint8_t                           capped;

    fprintf(stderr, "\ntest_sole_opener_rqls_h_cap\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    init_owner(&lease_owner, CHIMERA_CLAIM_PROTO_SMB2, 0xA, 0x1111);
    lease_owner.owner_hi = 0x2222;
    set_owner_key(&lease_owner, 0x1111, 0x2222);
    chimera_vfs_claim_init_rqls(&tmpl,
                                CHIMERA_CLAIM_CR | CHIMERA_CLAIM_H,
                                &lease_owner);

    /* The SAME client holds a keyless (non-lease-backed) open -- the shape a
     * legacy s/x/b oplock open leaves behind.  Its mere presence caps the
     * lease's H to R, even same-client (smb2.lease.oplock loop 2). */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_SMB2, 0xA, 1);
    chimera_vfs_claim_init_smb_open(&legacy_open,
                                    CHIMERA_CLAIM_R | CHIMERA_CLAIM_W, 0,
                                    &owner);
    r = chimera_vfs_claim_try_acquire(state, file, &legacy_open, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "same-client keyless open granted");

    r = chimera_vfs_claim_test(file, &tmpl, &conflict);
    CHECK(r == CHIMERA_CLAIM_DENIED,
          "RqLs RH probe DENIED by same-client non-lease open");
    capped = chimera_vfs_claim_grant_cap_mode(file, &tmpl, false);
    CHECK(capped == CHIMERA_CLAIM_CR,
          "RqLs RH caps to R behind a same-client legacy open");

    chimera_vfs_claim_release(state, file, &legacy_open);

    /* A lease-backed open -- the LeaseKey stamped on its share owner -- never
     * caps a lease's H, its own key or a peer's (RH+RH coexistence). */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_SMB2, 0xB, 2);
    set_owner_key(&owner, 0x3333, 0x4444);
    chimera_vfs_claim_init_smb_open(&keyed_open,
                                    CHIMERA_CLAIM_R | CHIMERA_CLAIM_W, 0,
                                    &owner);
    r = chimera_vfs_claim_try_acquire(state, file, &keyed_open, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "peer lease-backed open granted");

    capped = chimera_vfs_claim_grant_cap_mode(file, &tmpl, false);
    CHECK(capped == (CHIMERA_CLAIM_CR | CHIMERA_CLAIM_H),
          "peer lease-backed open leaves the RqLs H intact");

    chimera_vfs_claim_release(state, file, &keyed_open);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_sole_opener_rqls_h_cap */

/* Test 22: the sole-opener denial overrides a breakable cache conflict: the
 * requester steps its mode down instead of waiting the opener out (the old
 * arm ran after -- and overrode -- has_breakable_conflict). --------------- */
static void
test_sole_opener_beats_breakable(void)
{
    struct chimera_vfs_state         *state;
    struct chimera_vfs_file_state    *file;
    struct chimera_vfs_claim          peer_open, peer_cache, probe;
    struct chimera_claim_owner        owner;
    enum chimera_vfs_claim_result     r;
    struct chimera_vfs_claim_conflict conflict;
    struct break_recorder             rec = { 0 };

    fprintf(stderr, "\ntest_sole_opener_beats_breakable\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    /* Client 0xB: a real open plus a breakable LEVEL_II read cache. */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_SMB2, 0xB, 1);
    chimera_vfs_claim_init_smb_open(&peer_open, CHIMERA_CLAIM_R, 0, &owner);
    r = chimera_vfs_claim_try_acquire(state, file, &peer_open, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "peer open granted");

    chimera_vfs_claim_init_oplock(&peer_cache, CHIMERA_CLAIM_CR, &owner);
    peer_cache.break_cb   = recording_break_cb;
    peer_cache.cb_private = &rec;
    r                     = chimera_vfs_claim_try_acquire(state, file, &peer_cache, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "peer read cache granted");

    /* Client 0xA requests BATCH: the CW-vs-CR conflict is breakable, but the
     * sole-opener rule must return a hard DENIED first -- and start no
     * break. */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_SMB2, 0xA, 2);
    chimera_vfs_claim_init_oplock(&probe,
                                  CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW |
                                  CHIMERA_CLAIM_H,
                                  &owner);
    r = chimera_vfs_claim_try_acquire(state, file, &probe, &conflict);
    CHECK(r == CHIMERA_CLAIM_DENIED,
          "sole-opener DENIED overrides the breakable cache conflict");
    CHECK(rec.fired == 0, "no break started for the capped request");
    CHECK(conflict.construct == CHIMERA_CONSTRUCT_SMB_OPEN,
          "conflict names the ACCESS blocker, not the cache");

    chimera_vfs_claim_release(state, file, &peer_cache);
    chimera_vfs_claim_release(state, file, &peer_open);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_sole_opener_beats_breakable */

/* Test 23: WRITE-trigger self-break of a legacy read cache
 * (smb2.oplock.batch1/batch6): a legacy oplock holder that no longer holds
 * the write cache -- a LEVEL_II grant, or an exclusive/batch already
 * downgraded to a read cache -- confers no write coherence, so its OWN write
 * breaks it to NONE.  An RqLs holder's own-key write, and an
 * exclusive/batch holder still holding CW, self-exempt. ------------------- */
static void
test_write_trigger_legacy_self_break(void)
{
    struct chimera_vfs_state         *state;
    struct chimera_vfs_file_state    *file;
    struct chimera_vfs_claim_grant   *grant = NULL;
    struct chimera_vfs_claim          tmpl;
    struct chimera_claim_owner        owner;
    struct chimera_claim_actor        actor;
    enum chimera_vfs_claim_result     r;
    struct chimera_vfs_claim_conflict conflict;
    struct break_recorder             rec = { 0 };

    fprintf(stderr, "\ntest_write_trigger_legacy_self_break\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    /* Client 0xA holds a BATCH oplock (owner = its open's file id). */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_SMB2, 0xA, 1);
    chimera_vfs_claim_init_oplock(&tmpl,
                                  CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW |
                                  CHIMERA_CLAIM_H,
                                  &owner);
    tmpl.break_cb   = recording_break_cb;
    tmpl.cb_private = &rec;
    r               = chimera_vfs_claim_grant_acquire(state, file, &tmpl, 0, 0,
                                                      CHIMERA_CLAIM_GRANT_EXACT,
                                                      NULL, NULL, &grant, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED && grant, "batch oplock granted");
    if (!grant) {
        /* CHECK records the failure; the analyzer (rightly) sees the
         * NULL-grant path reaching the derefs below -- stop here. */
        return;
    }

    memset(&actor, 0, sizeof(actor));
    actor.owner = owner;

    /* While the holder still owns CW it is coherent with its own write:
     * no self-break (nobreakself analogue for exclusive/batch). */
    chimera_vfs_claim_invalidate(state, file->fh, file->fh_len, file->fh_hash,
                                 CHIMERA_TRIGGER_WRITE, &actor, 0);
    CHECK(rec.fired == 0, "own write does not break a CW-holding batch");

    /* A conflicting open downgrades it to LEVEL_II (one-shot to CR). */
    {
        struct chimera_claim_actor opener = { 0 };

        init_owner(&opener.owner, CHIMERA_CLAIM_PROTO_SMB2, 0xB, 2);
        chimera_vfs_claim_invalidate(state, file->fh, file->fh_len,
                                     file->fh_hash,
                                     CHIMERA_TRIGGER_OPEN_H, &opener,
                                     CHIMERA_CLAIM_CR);
    }
    CHECK(rec.fired == 1 && rec.last_needed_mode == CHIMERA_CLAIM_CR,
          "conflicting open breaks batch to LEVEL_II in one notification");
    chimera_vfs_claim_ack(&grant->claim, CHIMERA_CLAIM_CR);
    CHECK(grant->claim.break_state == CHIMERA_CLAIM_BREAK_IDLE &&
          grant->claim.used == CHIMERA_CLAIM_CR,
          "downgraded holder settles IDLE at CR");

    /* Now the holder's OWN write must break its own read cache to NONE:
     * a downgraded batch is mode-scored (no CW), not construct-scored
     * (smb2.oplock.batch1 "writing should generate a self break"). */
    rec.fired = 0;
    chimera_vfs_claim_invalidate(state, file->fh, file->fh_len, file->fh_hash,
                                 CHIMERA_TRIGGER_WRITE, &actor, 0);
    CHECK(rec.fired == 1 && rec.last_needed_mode == 0,
          "own write self-breaks the downgraded legacy read cache to NONE");

    chimera_vfs_claim_ack(&grant->claim, 0);
    chimera_vfs_claim_grant_release(state, grant, false);

    /* Contrast: an RqLs holder's own-key write never self-breaks. */
    {
        struct chimera_vfs_claim_grant *lease_grant = NULL;
        struct chimera_claim_owner      lease_owner;

        rec.fired = 0;
        init_owner(&lease_owner, CHIMERA_CLAIM_PROTO_SMB2, 0xA, 0x1111);
        lease_owner.owner_hi = 0x2222;
        set_owner_key(&lease_owner, 0x1111, 0x2222);
        chimera_vfs_claim_init_rqls(&tmpl,
                                    CHIMERA_CLAIM_CR | CHIMERA_CLAIM_H,
                                    &lease_owner);
        tmpl.break_cb   = recording_break_cb;
        tmpl.cb_private = &rec;
        r               = chimera_vfs_claim_grant_acquire(state, file, &tmpl, 0, 0,
                                                          CHIMERA_CLAIM_GRANT_EXACT,
                                                          NULL, NULL, &lease_grant,
                                                          &conflict);
        CHECK(r == CHIMERA_CLAIM_GRANTED && lease_grant, "RH lease granted");

        memset(&actor, 0, sizeof(actor));
        actor.owner = lease_owner;
        chimera_vfs_claim_invalidate(state, file->fh, file->fh_len,
                                     file->fh_hash,
                                     CHIMERA_TRIGGER_WRITE, &actor, 0);
        CHECK(rec.fired == 0, "own-key write leaves the RqLs cache intact");

        chimera_vfs_claim_grant_release(state, lease_grant, false);
    }

    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_write_trigger_legacy_self_break */

/* Test 24: mid-break cascade deepening + one-epoch-per-break-event
 * (smb2.lease.breaking3 / v2_breaking3): a plain open starts an RWH -> RH
 * break (one notification, one epoch bump); a write/truncate arriving
 * MID-BREAK deepens the floor to NONE silently (no notification, no epoch);
 * the acks then walk RH -> R -> NONE, one notification per step, all at the
 * SAME epoch; a same-key coalesce mid-break never upgrades; an ACKED-at-0
 * lease re-arms on re-open with a fresh epoch. ---------------------------- */
static void
test_midbreak_deepen_one_epoch(void)
{
    struct chimera_vfs_state         *state;
    struct chimera_vfs_file_state    *file;
    struct chimera_vfs_claim_grant   *grant = NULL, *g2;
    struct chimera_vfs_claim          tmpl;
    struct chimera_claim_owner        lease_owner;
    struct chimera_claim_actor        opener = { 0 };
    enum chimera_vfs_claim_result     r;
    struct chimera_vfs_claim_conflict conflict;
    struct break_recorder             rec = { 0 };
    uint32_t                          epoch0;

    fprintf(stderr, "\ntest_midbreak_deepen_one_epoch\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    init_owner(&lease_owner, CHIMERA_CLAIM_PROTO_SMB2, 0xA, 0x1111);
    lease_owner.owner_hi = 0x2222;
    set_owner_key(&lease_owner, 0x1111, 0x2222);
    chimera_vfs_claim_init_rqls(&tmpl,
                                CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW |
                                CHIMERA_CLAIM_H,
                                &lease_owner);
    tmpl.break_cb   = recording_break_cb;
    tmpl.cb_private = &rec;

    r = chimera_vfs_claim_grant_acquire(state, file, &tmpl, 0, 1 /* v2 */,
                                        CHIMERA_CLAIM_GRANT_EXACT,
                                        NULL, NULL, &grant, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED && grant, "v2 RWH lease granted");
    epoch0 = grant->epoch;

    /* A plain conflicting open: RWH -> RH, ONE notification, epoch + 1. */
    init_owner(&opener.owner, CHIMERA_CLAIM_PROTO_SMB2, 0xB, 2);
    chimera_vfs_claim_invalidate(state, file->fh, file->fh_len, file->fh_hash,
                                 CHIMERA_TRIGGER_OPEN_W, &opener,
                                 CHIMERA_CLAIM_CR | CHIMERA_CLAIM_H);
    CHECK(rec.fired == 1 &&
          rec.last_needed_mode == (CHIMERA_CLAIM_CR | CHIMERA_CLAIM_H),
          "conflicting open asks RWH -> RH in one notification");
    CHECK(grant->epoch == epoch0 + 1, "break EVENT advances the epoch once");
    CHECK(grant->claim.used ==
          (CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW | CHIMERA_CLAIM_H),
          "raw used holds RWH until the ack");

    /* A same-key re-open mid-break must NOT upgrade or re-arm (the SMB layer
     * reports the current state + BREAK_IN_PROGRESS). */
    g2 = chimera_vfs_claim_grant_coalesce(file, &lease_owner,
                                          CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW |
                                          CHIMERA_CLAIM_H, 1);
    CHECK(g2 == grant, "same-key re-open coalesces onto the breaking grant");
    CHECK(grant->claim.break_state == CHIMERA_CLAIM_BREAK_BREAKING &&
          grant->epoch == epoch0 + 1,
          "mid-break coalesce neither upgrades nor bumps the epoch");
    chimera_vfs_claim_grant_release(state, grant, false);

    /* A truncating open / write arriving mid-break deepens the floor to NONE
     * with NO new notification and NO epoch bump (breaking3's OVERWRITE). */
    {
        struct chimera_claim_actor writer = { 0 };

        init_owner(&writer.owner, CHIMERA_CLAIM_PROTO_SMB2, 0xC, 3);
        chimera_vfs_claim_invalidate(state, file->fh, file->fh_len,
                                     file->fh_hash,
                                     CHIMERA_TRIGGER_WRITE, &writer, 0);
    }
    CHECK(rec.fired == 1, "mid-break write sends no new notification");
    CHECK(grant->epoch == epoch0 + 1, "mid-break write bumps no epoch");

    /* Ack RH: the deepened floor drives the cascade on, RH -> R, same
     * epoch. */
    chimera_vfs_claim_ack(&grant->claim, CHIMERA_CLAIM_CR | CHIMERA_CLAIM_H);
    CHECK(rec.fired == 2 && rec.last_needed_mode == CHIMERA_CLAIM_CR,
          "ack drives RH -> R (cascade continues to the deepened floor)");
    CHECK(grant->epoch == epoch0 + 1, "cascade step keeps the epoch");

    /* Ack R: R -> NONE, same epoch. */
    chimera_vfs_claim_ack(&grant->claim, CHIMERA_CLAIM_CR);
    CHECK(rec.fired == 3 && rec.last_needed_mode == 0,
          "ack drives R -> NONE");
    CHECK(grant->epoch == epoch0 + 1, "final step keeps the epoch");

    /* Final settle at NONE -> ACKED (inert). */
    chimera_vfs_claim_ack(&grant->claim, 0);
    CHECK(grant->claim.break_state == CHIMERA_CLAIM_BREAK_ACKED &&
          grant->claim.used == 0,
          "lease settles ACKED at NONE");

    /* A re-open under the same key re-arms the settled lease to the
     * requested mode with a fresh epoch (nobreakself re-open). */
    g2 = chimera_vfs_claim_grant_coalesce(file, &lease_owner,
                                          CHIMERA_CLAIM_CR, 1);
    CHECK(g2 == grant, "re-open coalesces onto the settled grant");
    CHECK(grant->claim.used == CHIMERA_CLAIM_CR &&
          grant->claim.break_state == CHIMERA_CLAIM_BREAK_IDLE,
          "ACKED-at-0 lease re-arms to the requested mode");
    CHECK(grant->epoch == epoch0 + 2, "re-arm advances the epoch");
    chimera_vfs_claim_grant_release(state, grant, false);

    chimera_vfs_claim_grant_release(state, grant, false);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_midbreak_deepen_one_epoch */

/* FUSE sync grant: DELEG_R-shaped admission, ADVERTISE_NEVER at
 * break-begin (a conflicting writer keeps waiting for the real ack, the
 * coherence=sync contract), CLIENT-circle self-exemption, and WRITE
 * trigger selection with same-client exemption. */
static void
test_fuse_grant_sync_semantics(void)
{
    struct chimera_vfs_state         *state;
    struct chimera_vfs_file_state    *file;
    struct chimera_vfs_claim          g, w, own;
    struct chimera_claim_owner        owner;
    struct chimera_claim_actor        actor;
    enum chimera_vfs_claim_result     r;
    struct chimera_vfs_claim_conflict conflict;
    struct break_recorder             rec = { 0 };

    fprintf(stderr, "\ntest_fuse_grant_sync_semantics\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    /* Mount 0xA holds the kernel read-cache grant. */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_FUSE, 0xA, 1);
    chimera_vfs_claim_init_fuse_grant(&g, &owner);
    g.break_cb   = recording_break_cb;
    g.cb_private = &rec;

    r = chimera_vfs_claim_try_acquire(state, file, &g, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "fuse grant granted");

    /* The mount's OWN write-mode open coexists (CLIENT circle). */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_FUSE, 0xA, 7);
    chimera_vfs_claim_init_nfs4_open(&own, CHIMERA_CLAIM_W, 0, &owner);
    r = chimera_vfs_claim_try_acquire(state, file, &own, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "own-mount W open self-exempts");
    chimera_vfs_claim_release(state, file, &own);

    /* A FOREIGN writer's open conflicts and starts the break... */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_NFSV4, 0xB, 2);
    chimera_vfs_claim_init_nfs4_open(&w, CHIMERA_CLAIM_W, 0, &owner);

    r = chimera_vfs_claim_try_acquire(state, file, &w, &conflict);
    CHECK(r == CHIMERA_CLAIM_BREAKING, "foreign W open starts the break");
    CHECK(rec.fired == 1 && rec.last_needed_mode == 0, "one all-or-nothing recall");

    /* ...and KEEPS conflicting until the ack lands: advertised survives
     * break-begin (ADVERTISE_NEVER), unlike an SMB holder. */
    r = chimera_vfs_claim_try_acquire(state, file, &w, &conflict);
    CHECK(r == CHIMERA_CLAIM_BREAKING,
          "writer still waits mid-break (advertised held)");

    chimera_vfs_claim_ack(&g, 0);

    r = chimera_vfs_claim_try_acquire(state, file, &w, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "writer proceeds after the ack");
    chimera_vfs_claim_release(state, file, &w);
    chimera_vfs_claim_release(state, file, &g);

    /* WRITE trigger: a fresh grant is invalidated by a foreign actor's
     * write but NOT by a same-client (same-mount) actor's write. */
    init_owner(&owner, CHIMERA_CLAIM_PROTO_FUSE, 0xA, 1);
    chimera_vfs_claim_init_fuse_grant(&g, &owner);
    g.break_cb   = recording_break_cb;
    g.cb_private = &rec;
    rec.fired    = 0;
    r            = chimera_vfs_claim_try_acquire(state, file, &g, &conflict);
    CHECK(r == CHIMERA_CLAIM_GRANTED, "fuse grant re-granted");

    memset(&actor, 0, sizeof(actor));
    init_owner(&actor.owner, CHIMERA_CLAIM_PROTO_FUSE, 0xA, 9);
    chimera_vfs_claim_invalidate(state, file->fh, file->fh_len,
                                 file->fh_hash,
                                 CHIMERA_TRIGGER_WRITE, &actor, 0);
    CHECK(rec.fired == 0, "same-mount write does not invalidate own grant");

    init_owner(&actor.owner, CHIMERA_CLAIM_PROTO_SMB2, 0xB, 2);
    chimera_vfs_claim_invalidate(state, file->fh, file->fh_len,
                                 file->fh_hash,
                                 CHIMERA_TRIGGER_WRITE, &actor, 0);
    CHECK(rec.fired == 1 && rec.last_needed_mode == 0,
          "foreign write invalidates the grant to 0");
    CHECK(g.advertised != 0,
          "advertised held until the kernel invalidation acks");

    chimera_vfs_claim_ack(&g, 0);
    CHECK(g.advertised == 0, "ack settles at 0");

    chimera_vfs_claim_release(state, file, &g);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_fuse_grant_sync_semantics */

/* Main ---------------------------------------------------------------- */
int
main(
    int    argc,
    char **argv)
{
    (void) argc;
    (void) argv;

    ChimeraLogLevel = CHIMERA_LOG_INFO;

    test_init_destroy();
    test_file_state_lookup();
    test_range_vs_range();
    test_range_same_owner_coalesces();
    test_range_to_eof();
    test_range_zero_length();
    test_share_vs_share();
    test_caching_lease_basics();
    test_caching_w_breaks_r();
    test_smb_lease_key_coalesces();
    test_caching_break_revoke();
    test_range_breaks_caching();
    test_nfs4_lock_vs_own_delegation();
    test_async_acquire_immediate();
    test_async_acquire_wait_then_ack();
    test_async_acquire_cancel();
    test_backend_confirm_lane_cancel();
    test_release_pumps_pending();
    test_lease_test();
    test_breakable_share_recall();
    test_nonbreakable_share_denies();
    test_share_escape_skips_own_lease();
    test_sole_opener_legacy_cap();
    test_sole_opener_rqls_h_cap();
    test_sole_opener_beats_breakable();
    test_write_trigger_legacy_self_break();
    test_midbreak_deepen_one_epoch();
    test_fuse_grant_sync_semantics();

    fprintf(stderr, "\n========================================\n");
    fprintf(stderr, "Results: %d passed, %d failed\n", passed, failed);
    fprintf(stderr, "========================================\n");

    return failed == 0 ? 0 : 1;
} /* main */
