// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Unit tests for the unified VFS lease/lock state (vfs_state.{h,c}).
 *
 * These cover the conflict matrix and break-orchestration skeleton in
 * isolation — no protocol stack, no real VFS instance.  The matrix is
 * pure logic, so we exercise every (existing-state, new-request) pair
 * across all three lease kinds, plus same-owner coalescing and the
 * lease-break ack / revoke paths.
 *
 * What is intentionally NOT covered here:
 *   - Stage B+ integration with NLM/NFSv4 LOCK and SMB2 LOCK
 *   - Stage C+ integration with SMB CREATE share_access and NFSv4 OPEN
 *   - Stage D+ break notification packets (CB_RECALL, OPLOCK_BREAK)
 *
 * Those land with the callers that use this layer in later stages.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#undef NDEBUG
#include <assert.h>

#include "vfs/vfs_state.h"
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
    struct chimera_vfs_lease_owner *owner,
    uint32_t                        protocol,
    uint64_t                        client,
    uint64_t                        owner_id)
{
    memset(owner, 0, sizeof(*owner));
    owner->protocol   = protocol;
    owner->client_key = client;
    owner->owner_lo   = owner_id;
} /* init_owner */

/* Break callback that just counts invocations and records the lease. */
struct break_recorder {
    int                       fired;
    struct chimera_vfs_lease *last_lease;
    uint8_t                   last_needed_mode;
};

static void
recording_break_cb(
    struct chimera_vfs_lease *lease,
    uint8_t                   needed_mode,
    void                     *priv)
{
    struct break_recorder *r = priv;

    r->fired++;
    r->last_lease       = lease;
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
    struct chimera_vfs_state      *state;
    struct chimera_vfs_file_state *file;
    struct chimera_vfs_lease       a, b, c;
    enum chimera_vfs_lease_result  r;
    struct chimera_vfs_lease      *conflict;

    fprintf(stderr, "\ntest_range_vs_range\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    /* Owner A takes a shared read lock on [0, 100). */
    memset(&a, 0, sizeof(a));
    a.kind         = CHIMERA_VFS_LEASE_RANGE;
    a.mode.granted = CHIMERA_VFS_LEASE_MODE_R;
    a.offset       = 0;
    a.length       = 100;
    init_owner(&a.owner, CHIMERA_VFS_LEASE_PROTO_NFSV4, 0xA, 1);

    r = chimera_vfs_state_try_insert(state, file, &a, &conflict);
    CHECK(r == CHIMERA_VFS_LEASE_GRANTED, "first read lock granted");

    /* Owner B takes a non-overlapping read lock — granted. */
    memset(&b, 0, sizeof(b));
    b.kind         = CHIMERA_VFS_LEASE_RANGE;
    b.mode.granted = CHIMERA_VFS_LEASE_MODE_R;
    b.offset       = 200;
    b.length       = 100;
    init_owner(&b.owner, CHIMERA_VFS_LEASE_PROTO_NFSV4, 0xB, 2);

    r = chimera_vfs_state_try_insert(state, file, &b, &conflict);
    CHECK(r == CHIMERA_VFS_LEASE_GRANTED, "non-overlapping lock granted");

    /* Owner C tries an overlapping write — denied by either A or B. */
    memset(&c, 0, sizeof(c));
    c.kind         = CHIMERA_VFS_LEASE_RANGE;
    c.mode.granted = CHIMERA_VFS_LEASE_MODE_W;
    c.offset       = 50;
    c.length       = 100;
    init_owner(&c.owner, CHIMERA_VFS_LEASE_PROTO_NFSV4, 0xC, 3);

    r = chimera_vfs_state_try_insert(state, file, &c, &conflict);
    CHECK(r == CHIMERA_VFS_LEASE_DENIED, "overlapping write denied");
    CHECK(conflict == &a, "conflict points to first existing holder");

    /* Owner C tries an overlapping read — read+read coexist. */
    c.mode.granted = CHIMERA_VFS_LEASE_MODE_R;
    r              = chimera_vfs_state_try_insert(state, file, &c, &conflict);
    CHECK(r == CHIMERA_VFS_LEASE_GRANTED, "overlapping read with read granted");

    chimera_vfs_state_remove(state, file, &a);
    chimera_vfs_state_remove(state, file, &b);
    chimera_vfs_state_remove(state, file, &c);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_range_vs_range */

/* Test 4: same-owner coalescing on range locks ----------------------- */
static void
test_range_same_owner_coalesces(void)
{
    struct chimera_vfs_state      *state;
    struct chimera_vfs_file_state *file;
    struct chimera_vfs_lease       a, b;
    enum chimera_vfs_lease_result  r;
    struct chimera_vfs_lease      *conflict;

    fprintf(stderr, "\ntest_range_same_owner_coalesces\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    memset(&a, 0, sizeof(a));
    a.kind         = CHIMERA_VFS_LEASE_RANGE;
    a.mode.granted = CHIMERA_VFS_LEASE_MODE_W;
    a.offset       = 0;
    a.length       = 100;
    init_owner(&a.owner, CHIMERA_VFS_LEASE_PROTO_NFSV4, 0xA, 1);

    r = chimera_vfs_state_try_insert(state, file, &a, &conflict);
    CHECK(r == CHIMERA_VFS_LEASE_GRANTED, "first write lock granted");

    /* Same owner taking an overlapping write — must not self-conflict. */
    memset(&b, 0, sizeof(b));
    b.kind         = CHIMERA_VFS_LEASE_RANGE;
    b.mode.granted = CHIMERA_VFS_LEASE_MODE_W;
    b.offset       = 50;
    b.length       = 100;
    init_owner(&b.owner, CHIMERA_VFS_LEASE_PROTO_NFSV4, 0xA, 1);

    r = chimera_vfs_state_try_insert(state, file, &b, &conflict);
    CHECK(r == CHIMERA_VFS_LEASE_GRANTED, "same-owner overlapping write granted (coalesces)");

    chimera_vfs_state_remove(state, file, &a);
    chimera_vfs_state_remove(state, file, &b);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_range_same_owner_coalesces */

/* Test 5: range length==0 means to EOF ------------------------------- */
static void
test_range_to_eof(void)
{
    struct chimera_vfs_state      *state;
    struct chimera_vfs_file_state *file;
    struct chimera_vfs_lease       a, b;
    enum chimera_vfs_lease_result  r;
    struct chimera_vfs_lease      *conflict;

    fprintf(stderr, "\ntest_range_to_eof\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    /* Existing W lock from [1000, EOF). */
    memset(&a, 0, sizeof(a));
    a.kind         = CHIMERA_VFS_LEASE_RANGE;
    a.mode.granted = CHIMERA_VFS_LEASE_MODE_W;
    a.offset       = 1000;
    a.length       = 0; /* to EOF */
    init_owner(&a.owner, CHIMERA_VFS_LEASE_PROTO_NLM, 0xA, 1);

    r = chimera_vfs_state_try_insert(state, file, &a, &conflict);
    CHECK(r == CHIMERA_VFS_LEASE_GRANTED, "to-EOF write granted");

    /* A new write at [2000, 100) overlaps with the to-EOF range. */
    memset(&b, 0, sizeof(b));
    b.kind         = CHIMERA_VFS_LEASE_RANGE;
    b.mode.granted = CHIMERA_VFS_LEASE_MODE_W;
    b.offset       = 2000;
    b.length       = 100;
    init_owner(&b.owner, CHIMERA_VFS_LEASE_PROTO_NLM, 0xB, 2);

    r = chimera_vfs_state_try_insert(state, file, &b, &conflict);
    CHECK(r == CHIMERA_VFS_LEASE_DENIED, "overlap with to-EOF range denied");

    /* A new write at [0, 500) does NOT overlap [1000, EOF). */
    b.offset = 0;
    b.length = 500;
    r        = chimera_vfs_state_try_insert(state, file, &b, &conflict);
    CHECK(r == CHIMERA_VFS_LEASE_GRANTED, "non-overlap before to-EOF range granted");

    chimera_vfs_state_remove(state, file, &a);
    chimera_vfs_state_remove(state, file, &b);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_range_to_eof */

/* Test 6: share-mode conflict — same matrix as SMB sharemode --------- */
static void
test_share_vs_share(void)
{
    struct chimera_vfs_state      *state;
    struct chimera_vfs_file_state *file;
    struct chimera_vfs_lease       a, b;
    enum chimera_vfs_lease_result  r;
    struct chimera_vfs_lease      *conflict;

    fprintf(stderr, "\ntest_share_vs_share\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    /* Holder A: wants R, denies W (FILE_READ_DATA + !FILE_SHARE_WRITE). */
    memset(&a, 0, sizeof(a));
    a.kind         = CHIMERA_VFS_LEASE_SHARE;
    a.mode.granted = CHIMERA_VFS_LEASE_MODE_R;
    a.mode.denied  = CHIMERA_VFS_LEASE_MODE_W;
    init_owner(&a.owner, CHIMERA_VFS_LEASE_PROTO_SMB2, 0xA, 1);

    r = chimera_vfs_state_try_insert(state, file, &a, &conflict);
    CHECK(r == CHIMERA_VFS_LEASE_GRANTED, "first share R deny-W granted");

    /* Probe B: wants W — A denies W, so denied. */
    memset(&b, 0, sizeof(b));
    b.kind         = CHIMERA_VFS_LEASE_SHARE;
    b.mode.granted = CHIMERA_VFS_LEASE_MODE_W;
    b.mode.denied  = 0;
    init_owner(&b.owner, CHIMERA_VFS_LEASE_PROTO_SMB2, 0xB, 2);

    r = chimera_vfs_state_try_insert(state, file, &b, &conflict);
    CHECK(r == CHIMERA_VFS_LEASE_DENIED, "share W denied by existing deny-W");

    /* Probe B: wants R only — no conflict. */
    b.mode.granted = CHIMERA_VFS_LEASE_MODE_R;
    r              = chimera_vfs_state_try_insert(state, file, &b, &conflict);
    CHECK(r == CHIMERA_VFS_LEASE_GRANTED, "second share R coexists");

    /* Probe C: wants R and denies R — should conflict with B (which has R). */
    struct chimera_vfs_lease c;
    memset(&c, 0, sizeof(c));
    c.kind         = CHIMERA_VFS_LEASE_SHARE;
    c.mode.granted = CHIMERA_VFS_LEASE_MODE_R;
    c.mode.denied  = CHIMERA_VFS_LEASE_MODE_R;
    init_owner(&c.owner, CHIMERA_VFS_LEASE_PROTO_SMB2, 0xC, 3);

    r = chimera_vfs_state_try_insert(state, file, &c, &conflict);
    CHECK(r == CHIMERA_VFS_LEASE_DENIED,
          "share probe denying R denied because another holder has R");

    chimera_vfs_state_remove(state, file, &a);
    chimera_vfs_state_remove(state, file, &b);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_share_vs_share */

/* Test 7: caching lease — W is exclusive, R can be shared across owners */
static void
test_caching_lease_basics(void)
{
    struct chimera_vfs_state      *state;
    struct chimera_vfs_file_state *file;
    struct chimera_vfs_lease       a, b;
    enum chimera_vfs_lease_result  r;
    struct chimera_vfs_lease      *conflict;

    fprintf(stderr, "\ntest_caching_lease_basics\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    /* Client A holds R-cache. */
    memset(&a, 0, sizeof(a));
    a.kind         = CHIMERA_VFS_LEASE_CACHING;
    a.mode.granted = CHIMERA_VFS_LEASE_MODE_R;
    init_owner(&a.owner, CHIMERA_VFS_LEASE_PROTO_SMB2, 0xA, 1);

    r = chimera_vfs_state_try_insert(state, file, &a, &conflict);
    CHECK(r == CHIMERA_VFS_LEASE_GRANTED, "first R-cache granted");

    /* Client B requests R-cache — should also be granted (R is shared
     * across different owners; this is SMB2 Level2 oplock semantics). */
    memset(&b, 0, sizeof(b));
    b.kind         = CHIMERA_VFS_LEASE_CACHING;
    b.mode.granted = CHIMERA_VFS_LEASE_MODE_R;
    init_owner(&b.owner, CHIMERA_VFS_LEASE_PROTO_SMB2, 0xB, 2);

    r = chimera_vfs_state_try_insert(state, file, &b, &conflict);
    CHECK(r == CHIMERA_VFS_LEASE_GRANTED,
          "second R-cache from different owner coexists (Level2 semantics)");

    chimera_vfs_state_remove(state, file, &a);
    chimera_vfs_state_remove(state, file, &b);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_caching_lease_basics */

/* Test 8: caching W-lease forces break of R-cache on other client ----- */
static void
test_caching_w_breaks_r(void)
{
    struct chimera_vfs_state      *state;
    struct chimera_vfs_file_state *file;
    struct chimera_vfs_lease       a, b;
    enum chimera_vfs_lease_result  r;
    struct chimera_vfs_lease      *conflict;
    struct break_recorder          rec = { 0 };

    fprintf(stderr, "\ntest_caching_w_breaks_r\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    /* Client A holds R-cache, with a registered break callback. */
    memset(&a, 0, sizeof(a));
    a.kind         = CHIMERA_VFS_LEASE_CACHING;
    a.mode.granted = CHIMERA_VFS_LEASE_MODE_R;
    init_owner(&a.owner, CHIMERA_VFS_LEASE_PROTO_SMB2, 0xA, 1);
    a.owner.break_cb   = recording_break_cb;
    a.owner.cb_private = &rec;

    r = chimera_vfs_state_try_insert(state, file, &a, &conflict);
    CHECK(r == CHIMERA_VFS_LEASE_GRANTED, "R-cache granted");

    /* Client B requests W-cache.  This must initiate a break on A. */
    memset(&b, 0, sizeof(b));
    b.kind         = CHIMERA_VFS_LEASE_CACHING;
    b.mode.granted = CHIMERA_VFS_LEASE_MODE_W;
    init_owner(&b.owner, CHIMERA_VFS_LEASE_PROTO_SMB2, 0xB, 2);

    r = chimera_vfs_state_try_insert(state, file, &b, &conflict);
    CHECK(r == CHIMERA_VFS_LEASE_BREAKING, "W-cache request triggers break");
    CHECK(conflict == &a, "conflict points to existing R-cache holder");
    CHECK(rec.fired == 1, "break callback fired exactly once");
    CHECK(rec.last_lease == &a, "break callback received correct lease");
    CHECK(a.break_state == CHIMERA_VFS_BREAK_BREAKING, "lease marked BREAKING");

    /* Client A acks the break by downgrading to nothing. */
    struct chimera_vfs_lease_mode none = { 0, 0 };
    chimera_vfs_lease_ack(&a, none);
    CHECK(a.break_state == CHIMERA_VFS_BREAK_ACKED, "lease moved to ACKED");
    CHECK(a.mode.granted == 0, "lease mode downgraded");

    /* B can retry now.  Since A still occupies the list with mode=0,
     * the caching_conflict test won't deny — A's W and H bits are
     * unset.  In real code the protocol would call state_remove on
     * the acked lease before retrying; we mimic that here. */
    chimera_vfs_state_remove(state, file, &a);

    r = chimera_vfs_state_try_insert(state, file, &b, &conflict);
    CHECK(r == CHIMERA_VFS_LEASE_GRANTED, "W-cache granted after break ack");

    chimera_vfs_state_remove(state, file, &b);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_caching_w_breaks_r */

/* Test 9: SMB lease-key coalescing — same client_key+owner doesn't break */
static void
test_smb_lease_key_coalesces(void)
{
    struct chimera_vfs_state      *state;
    struct chimera_vfs_file_state *file;
    struct chimera_vfs_lease       a, b;
    enum chimera_vfs_lease_result  r;
    struct chimera_vfs_lease      *conflict;
    struct break_recorder          rec = { 0 };

    fprintf(stderr, "\ntest_smb_lease_key_coalesces\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    /* SMB client opens file with a lease key (encoded as
     * client_key=client_guid, owner_lo|owner_hi = 128-bit lease_key). */
    memset(&a, 0, sizeof(a));
    a.kind         = CHIMERA_VFS_LEASE_CACHING;
    a.mode.granted = CHIMERA_VFS_LEASE_MODE_R |
        CHIMERA_VFS_LEASE_MODE_W |
        CHIMERA_VFS_LEASE_MODE_H;
    init_owner(&a.owner, CHIMERA_VFS_LEASE_PROTO_SMB2, 0xC11E1700, 1);
    a.owner.owner_hi   = 0xDEAD;
    a.owner.break_cb   = recording_break_cb;
    a.owner.cb_private = &rec;

    r = chimera_vfs_state_try_insert(state, file, &a, &conflict);
    CHECK(r == CHIMERA_VFS_LEASE_GRANTED, "RWH lease granted on first open");

    /* Same client opens the same file again with the same lease_key —
     * Samba's locking.tdb rule says this is the same lease holder, no
     * break required. */
    memset(&b, 0, sizeof(b));
    b.kind         = CHIMERA_VFS_LEASE_CACHING;
    b.mode.granted = CHIMERA_VFS_LEASE_MODE_R |
        CHIMERA_VFS_LEASE_MODE_W |
        CHIMERA_VFS_LEASE_MODE_H;
    init_owner(&b.owner, CHIMERA_VFS_LEASE_PROTO_SMB2, 0xC11E1700, 1);
    b.owner.owner_hi = 0xDEAD;

    r = chimera_vfs_state_try_insert(state, file, &b, &conflict);
    CHECK(r == CHIMERA_VFS_LEASE_GRANTED,
          "second open with same lease_key coalesces without break");
    CHECK(rec.fired == 0, "no break invoked for same-owner reopen");

    chimera_vfs_state_remove(state, file, &a);
    chimera_vfs_state_remove(state, file, &b);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_smb_lease_key_coalesces */

/* Test 10: caching W-lease must be broken by another client's W request,
* and revoke (timeout-equivalent) lets the new request through ------- */
static void
test_caching_break_revoke(void)
{
    struct chimera_vfs_state      *state;
    struct chimera_vfs_file_state *file;
    struct chimera_vfs_lease       a, b;
    enum chimera_vfs_lease_result  r;
    struct chimera_vfs_lease      *conflict;
    struct break_recorder          rec = { 0 };

    fprintf(stderr, "\ntest_caching_break_revoke\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    memset(&a, 0, sizeof(a));
    a.kind         = CHIMERA_VFS_LEASE_CACHING;
    a.mode.granted = CHIMERA_VFS_LEASE_MODE_W;
    init_owner(&a.owner, CHIMERA_VFS_LEASE_PROTO_NFSV4, 0xA, 1);
    a.owner.break_cb   = recording_break_cb;
    a.owner.cb_private = &rec;

    r = chimera_vfs_state_try_insert(state, file, &a, &conflict);
    CHECK(r == CHIMERA_VFS_LEASE_GRANTED, "W delegation granted");

    /* Different NFSv4 client wants W — break A. */
    memset(&b, 0, sizeof(b));
    b.kind         = CHIMERA_VFS_LEASE_CACHING;
    b.mode.granted = CHIMERA_VFS_LEASE_MODE_W;
    init_owner(&b.owner, CHIMERA_VFS_LEASE_PROTO_NFSV4, 0xB, 2);

    r = chimera_vfs_state_try_insert(state, file, &b, &conflict);
    CHECK(r == CHIMERA_VFS_LEASE_BREAKING, "second W delegation initiates break");
    CHECK(rec.fired == 1, "break_cb fired once");

    /* Client A doesn't respond — simulate revoke (forcible expiry). */
    chimera_vfs_lease_revoke(&a);
    CHECK(a.break_state == CHIMERA_VFS_BREAK_REVOKED, "lease moved to REVOKED");
    CHECK(a.mode.granted == 0, "revoked lease has empty mode");

    chimera_vfs_state_remove(state, file, &a);
    r = chimera_vfs_state_try_insert(state, file, &b, &conflict);
    CHECK(r == CHIMERA_VFS_LEASE_GRANTED, "new W granted after revoke");

    chimera_vfs_state_remove(state, file, &b);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_caching_break_revoke */

/* Test 11: range-lock probe that would clash with a caching W-lease on
 * another client triggers a break instead of immediate denial -------- */
static void
test_range_breaks_caching(void)
{
    struct chimera_vfs_state      *state;
    struct chimera_vfs_file_state *file;
    struct chimera_vfs_lease       cache, range;
    enum chimera_vfs_lease_result  r;
    struct chimera_vfs_lease      *conflict;
    struct break_recorder          rec = { 0 };

    fprintf(stderr, "\ntest_range_breaks_caching\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    /* Client A: W-caching lease. */
    memset(&cache, 0, sizeof(cache));
    cache.kind         = CHIMERA_VFS_LEASE_CACHING;
    cache.mode.granted = CHIMERA_VFS_LEASE_MODE_W;
    init_owner(&cache.owner, CHIMERA_VFS_LEASE_PROTO_SMB2, 0xA, 1);
    cache.owner.break_cb   = recording_break_cb;
    cache.owner.cb_private = &rec;

    r = chimera_vfs_state_try_insert(state, file, &cache, &conflict);
    CHECK(r == CHIMERA_VFS_LEASE_GRANTED, "W-cache granted");

    /* Client B asks for a byte-range read lock — must break A's W-cache
     * because reads on B may see writes that A has cached. */
    memset(&range, 0, sizeof(range));
    range.kind         = CHIMERA_VFS_LEASE_RANGE;
    range.mode.granted = CHIMERA_VFS_LEASE_MODE_R;
    range.offset       = 0;
    range.length       = 100;
    init_owner(&range.owner, CHIMERA_VFS_LEASE_PROTO_NLM, 0xB, 2);

    r = chimera_vfs_state_try_insert(state, file, &range, &conflict);
    CHECK(r == CHIMERA_VFS_LEASE_BREAKING,
          "range read lock against other-client W-cache triggers break");
    CHECK(rec.fired == 1, "break_cb fired on caching holder");
    CHECK(conflict == &cache, "conflict reports the caching holder");

    chimera_vfs_state_remove(state, file, &cache);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_range_breaks_caching */

/* Acquire-callback recorder for the async-API tests. */
struct acquire_recorder {
    int                       fired;
    enum chimera_vfs_lease_result last_result;
    struct chimera_vfs_lease *last_granted;
    struct chimera_vfs_lease *last_conflict;
};

static void
recording_acquire_cb(
    enum chimera_vfs_lease_result result,
    struct chimera_vfs_lease     *granted_lease,
    struct chimera_vfs_lease     *conflict,
    void                         *priv)
{
    struct acquire_recorder *r = priv;

    r->fired++;
    r->last_result   = result;
    r->last_granted  = granted_lease;
    r->last_conflict = conflict;
} /* recording_acquire_cb */

/* Test 12: async acquire wait=false fires cb synchronously --------- */
static void
test_async_acquire_immediate(void)
{
    struct chimera_vfs_state          *state;
    struct chimera_vfs_file_state     *file;
    struct chimera_vfs_lease           lease;
    struct chimera_vfs_pending_acquire ticket;
    struct acquire_recorder            rec = { 0 };

    fprintf(stderr, "\ntest_async_acquire_immediate\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    memset(&lease, 0, sizeof(lease));
    lease.kind         = CHIMERA_VFS_LEASE_RANGE;
    lease.mode.granted = CHIMERA_VFS_LEASE_MODE_W;
    lease.offset       = 0;
    lease.length       = 100;
    init_owner(&lease.owner, CHIMERA_VFS_LEASE_PROTO_NFSV4, 0xA, 1);

    chimera_vfs_lease_acquire(state, file, &lease, &ticket, false,
                              recording_acquire_cb, &rec);
    CHECK(rec.fired == 1, "cb fires synchronously on first acquire");
    CHECK(rec.last_result == CHIMERA_VFS_LEASE_GRANTED, "result is GRANTED");
    CHECK(rec.last_granted == &lease, "granted_lease points to inserted lease");

    chimera_vfs_state_remove(state, file, &lease);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_async_acquire_immediate */

/* Test 13: async acquire wait=true queues on BREAKING, fires after ack */
static void
test_async_acquire_wait_then_ack(void)
{
    struct chimera_vfs_state          *state;
    struct chimera_vfs_file_state     *file;
    struct chimera_vfs_lease           cache, range;
    struct chimera_vfs_pending_acquire ticket;
    struct acquire_recorder            rec  = { 0 };
    struct break_recorder              brec = { 0 };
    struct chimera_vfs_lease_mode      none = { 0, 0 };

    fprintf(stderr, "\ntest_async_acquire_wait_then_ack\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    /* Existing breakable W-cache on client A. */
    memset(&cache, 0, sizeof(cache));
    cache.kind         = CHIMERA_VFS_LEASE_CACHING;
    cache.mode.granted = CHIMERA_VFS_LEASE_MODE_W;
    init_owner(&cache.owner, CHIMERA_VFS_LEASE_PROTO_SMB2, 0xA, 1);
    cache.owner.break_cb   = recording_break_cb;
    cache.owner.cb_private = &brec;
    chimera_vfs_state_try_insert(state, file, &cache, NULL);

    /* Client B's range read lock — must wait on the W-cache break. */
    memset(&range, 0, sizeof(range));
    range.kind         = CHIMERA_VFS_LEASE_RANGE;
    range.mode.granted = CHIMERA_VFS_LEASE_MODE_R;
    range.offset       = 0;
    range.length       = 100;
    init_owner(&range.owner, CHIMERA_VFS_LEASE_PROTO_NLM, 0xB, 2);

    chimera_vfs_lease_acquire(state, file, &range, &ticket, true,
                              recording_acquire_cb, &rec);
    CHECK(rec.fired == 0, "wait acquire does not fire cb yet");
    CHECK(brec.fired == 1, "break_cb fired on conflicting holder");
    CHECK(ticket.queued == true, "ticket is queued");

    /* Client A acks with full release.  Pump runs and retries our ticket. */
    chimera_vfs_lease_ack(&cache, none);
    chimera_vfs_state_remove(state, file, &cache);

    CHECK(rec.fired == 1, "cb fires once ack/remove completes");
    CHECK(rec.last_result == CHIMERA_VFS_LEASE_GRANTED, "retry granted");
    CHECK(ticket.queued == false, "ticket is dequeued");

    chimera_vfs_state_remove(state, file, &range);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_async_acquire_wait_then_ack */

/* Test 14: cancel removes a queued ticket and suppresses cb -------- */
static void
test_async_acquire_cancel(void)
{
    struct chimera_vfs_state          *state;
    struct chimera_vfs_file_state     *file;
    struct chimera_vfs_lease           cache, range;
    struct chimera_vfs_pending_acquire ticket;
    struct acquire_recorder            rec  = { 0 };
    struct break_recorder              brec = { 0 };
    struct chimera_vfs_lease_mode      none = { 0, 0 };
    bool                               cancelled;

    fprintf(stderr, "\ntest_async_acquire_cancel\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    memset(&cache, 0, sizeof(cache));
    cache.kind         = CHIMERA_VFS_LEASE_CACHING;
    cache.mode.granted = CHIMERA_VFS_LEASE_MODE_W;
    init_owner(&cache.owner, CHIMERA_VFS_LEASE_PROTO_SMB2, 0xA, 1);
    cache.owner.break_cb   = recording_break_cb;
    cache.owner.cb_private = &brec;
    chimera_vfs_state_try_insert(state, file, &cache, NULL);

    memset(&range, 0, sizeof(range));
    range.kind         = CHIMERA_VFS_LEASE_RANGE;
    range.mode.granted = CHIMERA_VFS_LEASE_MODE_R;
    range.length       = 100;
    init_owner(&range.owner, CHIMERA_VFS_LEASE_PROTO_NLM, 0xB, 2);

    chimera_vfs_lease_acquire(state, file, &range, &ticket, true,
                              recording_acquire_cb, &rec);
    CHECK(ticket.queued == true, "ticket queued while waiting");

    /* Caller changes its mind (e.g., NLM CANCEL). */
    cancelled = chimera_vfs_lease_acquire_cancel(state, &ticket);
    CHECK(cancelled == true, "cancel reports the ticket was dequeued");
    CHECK(ticket.queued == false, "ticket no longer queued after cancel");
    CHECK(rec.fired == 0, "cb does not fire for cancelled acquire");

    /* Acking the conflict no longer fires the cancelled ticket. */
    chimera_vfs_lease_ack(&cache, none);
    chimera_vfs_state_remove(state, file, &cache);
    CHECK(rec.fired == 0, "cancelled ticket stays silent after ack");

    /* Cancelling again is a no-op (returns false). */
    cancelled = chimera_vfs_lease_acquire_cancel(state, &ticket);
    CHECK(cancelled == false, "second cancel returns false");

    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_async_acquire_cancel */

/* Test 15: release pumps pending queue ----------------------------- */
static void
test_release_pumps_pending(void)
{
    struct chimera_vfs_state          *state;
    struct chimera_vfs_file_state     *file;
    struct chimera_vfs_lease           cache, range;
    struct chimera_vfs_pending_acquire ticket;
    struct acquire_recorder            rec  = { 0 };
    struct break_recorder              brec = { 0 };

    fprintf(stderr, "\ntest_release_pumps_pending\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    memset(&cache, 0, sizeof(cache));
    cache.kind         = CHIMERA_VFS_LEASE_CACHING;
    cache.mode.granted = CHIMERA_VFS_LEASE_MODE_W;
    init_owner(&cache.owner, CHIMERA_VFS_LEASE_PROTO_SMB2, 0xA, 1);
    cache.owner.break_cb   = recording_break_cb;
    cache.owner.cb_private = &brec;
    chimera_vfs_state_try_insert(state, file, &cache, NULL);

    memset(&range, 0, sizeof(range));
    range.kind         = CHIMERA_VFS_LEASE_RANGE;
    range.mode.granted = CHIMERA_VFS_LEASE_MODE_R;
    range.length       = 100;
    init_owner(&range.owner, CHIMERA_VFS_LEASE_PROTO_NLM, 0xB, 2);

    chimera_vfs_lease_acquire(state, file, &range, &ticket, true,
                              recording_acquire_cb, &rec);
    CHECK(rec.fired == 0, "wait acquire queued");

    /* If the holder protocol skips ack and just releases (e.g., on
     * close), the release path should still pump the pending queue. */
    chimera_vfs_lease_release(state, file, &cache);
    CHECK(rec.fired == 1, "release alone pumps pending queue");
    CHECK(rec.last_result == CHIMERA_VFS_LEASE_GRANTED, "pending granted");

    chimera_vfs_state_remove(state, file, &range);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_release_pumps_pending */

/* Test 16: chimera_vfs_lease_test exposes conflict without inserting */
static void
test_lease_test(void)
{
    struct chimera_vfs_state      *state;
    struct chimera_vfs_file_state *file;
    struct chimera_vfs_lease       held, probe;
    struct chimera_vfs_lease      *conflict;
    enum chimera_vfs_lease_result  r;

    fprintf(stderr, "\ntest_lease_test\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    memset(&held, 0, sizeof(held));
    held.kind         = CHIMERA_VFS_LEASE_RANGE;
    held.mode.granted = CHIMERA_VFS_LEASE_MODE_W;
    held.offset       = 0;
    held.length       = 100;
    init_owner(&held.owner, CHIMERA_VFS_LEASE_PROTO_NFSV4, 0xA, 1);
    chimera_vfs_state_try_insert(state, file, &held, NULL);

    /* Probe overlapping write from another owner — should DENY. */
    memset(&probe, 0, sizeof(probe));
    probe.kind         = CHIMERA_VFS_LEASE_RANGE;
    probe.mode.granted = CHIMERA_VFS_LEASE_MODE_W;
    probe.offset       = 50;
    probe.length       = 100;
    init_owner(&probe.owner, CHIMERA_VFS_LEASE_PROTO_NFSV4, 0xB, 2);

    r = chimera_vfs_lease_test(file, &probe, &conflict);
    CHECK(r == CHIMERA_VFS_LEASE_DENIED, "test reports conflict");
    CHECK(conflict == &held, "test names the holder");

    /* Probe is NOT inserted — file should still have only one lease. */
    CHECK(file->range_locks == &held && held.next == NULL,
          "test does not insert the probe");

    chimera_vfs_state_remove(state, file, &held);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_lease_test */

/* Test 17: a breakable SHARE holder (chimera's implicit I/O lease) is
 * recalled rather than hard-denied when a conflicting client open or
 * delegation arrives. ------------------------------------------------- */
static void
test_breakable_share_recall(void)
{
    struct chimera_vfs_state      *state;
    struct chimera_vfs_file_state *file;
    struct chimera_vfs_lease       impl, deny, deleg;
    enum chimera_vfs_lease_result  r;
    struct chimera_vfs_lease      *conflict;
    struct break_recorder          rec = { 0 };

    fprintf(stderr, "\ntest_breakable_share_recall\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    /* Chimera's implicit lease: deny-nothing SHARE granting W, breakable. */
    memset(&impl, 0, sizeof(impl));
    impl.kind         = CHIMERA_VFS_LEASE_SHARE;
    impl.mode.granted = CHIMERA_VFS_LEASE_MODE_W;
    impl.mode.denied  = 0;
    init_owner(&impl.owner, CHIMERA_VFS_LEASE_PROTO_INTERNAL, 0, 1);
    impl.owner.break_cb   = recording_break_cb;
    impl.owner.cb_private = &rec;

    r = chimera_vfs_state_try_insert(state, file, &impl, &conflict);
    CHECK(r == CHIMERA_VFS_LEASE_GRANTED, "implicit deny=0 share granted");

    /* A real client open that denies write conflicts with the implicit W
     * share — but because the holder is breakable, it is recalled, not
     * denied. */
    memset(&deny, 0, sizeof(deny));
    deny.kind         = CHIMERA_VFS_LEASE_SHARE;
    deny.mode.granted = CHIMERA_VFS_LEASE_MODE_W;
    deny.mode.denied  = CHIMERA_VFS_LEASE_MODE_W;
    init_owner(&deny.owner, CHIMERA_VFS_LEASE_PROTO_SMB2, 0xB, 2);

    r = chimera_vfs_state_try_insert(state, file, &deny, &conflict);
    CHECK(r == CHIMERA_VFS_LEASE_BREAKING,
          "client deny-W open recalls implicit share (not denied)");
    CHECK(rec.fired == 1, "implicit share break_cb fired once");

    /* An NFSv4 write-delegation grant likewise recalls the implicit share
     * (CACHING-vs-SHARE path). */
    rec.fired        = 0;
    impl.break_state = CHIMERA_VFS_BREAK_IDLE; /* make it recallable again */

    memset(&deleg, 0, sizeof(deleg));
    deleg.kind         = CHIMERA_VFS_LEASE_CACHING;
    deleg.mode.granted = CHIMERA_VFS_LEASE_MODE_W;
    init_owner(&deleg.owner, CHIMERA_VFS_LEASE_PROTO_NFSV4, 0xC, 0);

    r = chimera_vfs_state_try_insert(state, file, &deleg, &conflict);
    CHECK(r == CHIMERA_VFS_LEASE_BREAKING,
          "NFSv4 deleg grant recalls implicit share (not denied)");
    CHECK(rec.fired == 1, "implicit share break_cb fired for deleg conflict");

    chimera_vfs_state_remove(state, file, &impl);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_breakable_share_recall */

/* Test 18: a non-breakable SHARE holder (an ordinary client open) still
 * hard-denies a conflicting acquire. --------------------------------- */
static void
test_nonbreakable_share_denies(void)
{
    struct chimera_vfs_state      *state;
    struct chimera_vfs_file_state *file;
    struct chimera_vfs_lease       a, b;
    enum chimera_vfs_lease_result  r;
    struct chimera_vfs_lease      *conflict;

    fprintf(stderr, "\ntest_nonbreakable_share_denies\n");

    state = chimera_vfs_state_init();
    file  = get_file(state, 1);

    /* Ordinary client open, deny-W, NOT breakable. */
    memset(&a, 0, sizeof(a));
    a.kind         = CHIMERA_VFS_LEASE_SHARE;
    a.mode.granted = CHIMERA_VFS_LEASE_MODE_R;
    a.mode.denied  = CHIMERA_VFS_LEASE_MODE_W;
    init_owner(&a.owner, CHIMERA_VFS_LEASE_PROTO_SMB2, 0xA, 1);

    r = chimera_vfs_state_try_insert(state, file, &a, &conflict);
    CHECK(r == CHIMERA_VFS_LEASE_GRANTED, "non-breakable share granted");

    memset(&b, 0, sizeof(b));
    b.kind         = CHIMERA_VFS_LEASE_SHARE;
    b.mode.granted = CHIMERA_VFS_LEASE_MODE_W;
    init_owner(&b.owner, CHIMERA_VFS_LEASE_PROTO_SMB2, 0xB, 2);

    r = chimera_vfs_state_try_insert(state, file, &b, &conflict);
    CHECK(r == CHIMERA_VFS_LEASE_DENIED,
          "non-breakable deny-W share still denies");

    chimera_vfs_state_remove(state, file, &a);
    chimera_vfs_state_put(state, file);
    chimera_vfs_state_destroy(state);
} /* test_nonbreakable_share_denies */

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
    test_share_vs_share();
    test_caching_lease_basics();
    test_caching_w_breaks_r();
    test_smb_lease_key_coalesces();
    test_caching_break_revoke();
    test_range_breaks_caching();
    test_async_acquire_immediate();
    test_async_acquire_wait_then_ack();
    test_async_acquire_cancel();
    test_release_pumps_pending();
    test_lease_test();
    test_breakable_share_recall();
    test_nonbreakable_share_denies();

    fprintf(stderr, "\n========================================\n");
    fprintf(stderr, "Results: %d passed, %d failed\n", passed, failed);
    fprintf(stderr, "========================================\n");

    return failed == 0 ? 0 : 1;
} /* main */
