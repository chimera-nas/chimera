/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * SMB2 oplock/lease ground-truth probe (ROADMAP-SMB.md Increment 1) -- the
 * SMB twin of nfs4_deleg_probe.c.  It drives the real chimera SMB server
 * (two in-process connections: a caching HOLDER and a conflicting OPENER)
 * through the oplock/lease grant -> break -> acknowledge lifecycle and pins
 * chimera's behavior BEFORE any generative model bakes an expectation.
 *
 * Conformance discipline (DEVIATIONS-SMB.md): every EXPECT() asserts the
 * MS-SMB2 / MS-FSA-required outcome and cites the clause.  Where the spec
 * leaves the server a choice (whether an oplock/lease is granted, and at
 * what level -- MS-SMB2 3.3.5.9), the granted value is RECORDED with NOTE()
 * and pinned as policy, never dressed up as a mandate.  A chimera divergence
 * from a mandate is a DEVIATIONS-SMB.md entry, not a weakened assertion.
 *
 * Sections:
 *   O1  grant           -- sole opener gets the requested oplock / lease
 *   O2  break-on-open   -- a conflicting open breaks a batch oplock (II) and
 *                          completes once the holder acks
 *   O3  break-on-write  -- no read cache survives a conflicting write
 *   O4  lease break     -- write-lease downgrade, epoch bump, ack idempotency
 *   O5  grant denial    -- exclusive/batch is refused while a peer open exists
 *   O6  parking         -- WHICH conflicting opens emit an async interim, and
 *                          whether the interim is a wait or just a marker
 *   O7  coalescing      -- two opens under one lease key share one grant
 *   O8  no-ack breaks   -- a read-only break settles at once; acking it is an
 *                          error, not a no-op
 *   O9  lease v1 vs v2  -- only a v2 lease versions its state with an epoch
 *   O10 force-level-2   -- the share flag caps every grant to a read cache
 *   O11 grant matrix    -- legacy oplock grant across DesiredAccess /
 *                          ShareAccess / disposition
 *   O12 peer-open cap   -- WHICH peer open caps a fresh grant: same client
 *                          (same conn / same ClientGuid), a distinct client,
 *                          and an attribute-only peer
 *
 * Each connection gets its own ClientGuid: chimera derives the lease owner's
 * client key from it, so a shared guid would make every connection ONE client
 * and silently disable cross-client arbitration (that is exactly how the
 * retired S-1 "deviation" arose -- see DEVIATIONS-SMB.md).
 */

#include "smb2_mbt_common.h"

static int nfail = 0;
static int ndev  = 0;

/* A spec-mandated assertion.  Fails the probe (and CI) on a violation. */
#define EXPECT(ok, ...)                              \
        do {                                         \
            if (ok) { printf("ok   - "); }           \
            else { printf("FAIL - "); nfail++; }  \
            printf(__VA_ARGS__);                     \
            printf("\n");                            \
        } while (0)

/* A ground-truth observation (server discretion / recorded behavior), not
 * pass/fail -- MS-SMB2 3.3.5.9 leaves the grant level to the server. */
#define NOTE(...)                                    \
        do { printf("note - "); printf(__VA_ARGS__); printf("\n"); } while (0)

/* A recorded, spec-cited DEVIATION: chimera contradicts a mandate.  Loud in
 * the output and counted, but does NOT fail CI -- it pins a known,
 * documented non-conformance (see DEVIATIONS-SMB.md) so the probe stays a
 * regression anchor.  The conformant branch (below each use) turns green when
 * chimera is fixed, retiring the deviation.  Never used to hide an
 * unanalyzed failure: every DEVIATION has a DEVIATIONS-SMB.md entry. */
#define DEVIATION(id, ...)                                       \
        do {                                                     \
            printf("DEVIATION %s - ", id); ndev++;               \
            printf(__VA_ARGS__);                                 \
            printf("\n");                                        \
        } while (0)

static const char *
oplock_name(uint8_t lvl)
{
    switch (lvl) {
        case SMB2_OPLOCK_LEVEL_NONE:      return "NONE";
        case SMB2_OPLOCK_LEVEL_II:        return "II";
        case SMB2_OPLOCK_LEVEL_EXCLUSIVE: return "EXCLUSIVE";
        case SMB2_OPLOCK_LEVEL_BATCH:     return "BATCH";
        case SMB2_OPLOCK_LEVEL_LEASE:     return "LEASE";
        default:                          return "?";
    } /* switch */
} /* oplock_name */

/* R/H/W lease-bit string (wire bits: R=1, H=2, W=4).  Rotates through a few
 * static buffers so two calls in one printf() do not alias. */
static const char *
lease_str(uint32_t s)
{
    static char bufs[4][8];
    static int  which = 0;
    char       *buf   = bufs[which++ & 3];
    int         n     = 0;

    if (s & SMB2_LEASE_READ) {
        buf[n++] = 'R';
    }
    if (s & SMB2_LEASE_HANDLE) {
        buf[n++] = 'H';
    }
    if (s & SMB2_LEASE_WRITE) {
        buf[n++] = 'W';
    }
    if (n == 0) {
        buf[n++] = '-';
    }
    buf[n] = '\0';
    return buf;
} /* lease_str */

/* Post `opener`'s conflicting open and drive the shared loop until it
 * completes, acknowledging every break the `holder` receives (accepting the
 * exact downgrade the server requested -- the conformant holder response).
 * Captured breaks are returned for assertion.  A hang is a harness/server
 * bug, not a modeled outcome, so it aborts. */
static uint32_t
conflicting_open(
    struct smb2_env              *env,
    struct smb2_conn             *opener,
    struct smb2_conn             *holder,
    const char                   *name,
    uint32_t                      disp,
    uint32_t                      access,
    uint32_t                      share,
    const struct smb2_oplock_req *req,
    struct smb2_create_out       *out,
    struct smb2_break            *breaks,
    int                          *nbreaks)
{
    int      n        = 0;
    uint64_t deadline = smb2c_now_ms() + SMB2C_HANG_MS;

    smb2_create_post(opener, name, disp, access, share, req);

    while (!opener->reply_ready) {
        smb2_pump(env);
        struct smb2_break b;
        while (smb2_conn_pop_break(holder, &b)) {
            if (breaks && n < 8) {
                breaks[n] = b;
            }
            n++;
            if (b.is_lease) {
                smb2_lease_break_ack(holder, b.lease_key, b.new_state);
            } else {
                smb2_oplock_break_ack(holder, b.file_id, b.oplock_level);
            }
        }
        if (opener->disconnected) {
            fprintf(stderr, "conflicting_open('%s'): connection dropped\n",
                    name);
            exit(3);
        }
        if (smb2c_now_ms() >= deadline) {
            /* Not a modeled outcome: the opener is parked on an event this
             * driver is not producing.  Fail loudly and name it rather than
             * spinning until ctest reports an opaque timeout. */
            smb2c_hang(opener, "a conflicting open to complete "
                       "(every break delivered so far was acknowledged)");
        }
    }
    smb2c_parse_create(opener, out);
    if (nbreaks) {
        *nbreaks = n;
    }
    return out->status;
} /* conflicting_open */

/* ---- O1: grant (discretion recorded, level consistency asserted) -------- */

static void
sec_o1(
    struct smb2_env  *env,
    struct smb2_conn *a)
{
    struct smb2_create_out o;
    struct smb2_oplock_req req;

    printf("\n== O1: oplock / lease grant to a sole opener ==\n");

    /* O1a: legacy BATCH oplock, sole opener.  MS-FSA 2.1.5.17.1: with no
     * conflicting opens the requested oplock is granted (whether oplocks are
     * supported at all is the config-gated discretion -- enabled here). */
    memset(&req, 0, sizeof(req));
    req.level = SMB2_OPLOCK_LEVEL_BATCH;
    smb2_create(a, "o1a", FILE_OPEN_IF, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                &req, &o);
    EXPECT(o.status == ST_SUCCESS, "O1a CREATE(batch) -> 0x%08x", o.status);
    NOTE("O1a granted oplock level = %s (0x%02x)", oplock_name(o.oplock),
         o.oplock);
    EXPECT(o.oplock == SMB2_OPLOCK_LEVEL_BATCH,
           "O1a sole opener granted BATCH (MS-FSA 2.1.5.17.1)");
    smb2_close(a, o.file_id);

    /* O1b: RqLs lease RWH, sole opener.  MS-SMB2 3.3.5.9.11: a lease open
     * reports OplockLevel LEASE and the granted state in the RqLs response;
     * with no conflict the full requested RWH is grantable. */
    memset(&req, 0, sizeof(req));
    req.is_lease     = 1;
    req.lease_key[0] = 0xA1;
    req.lease_state  = SMB2_LEASE_RWH;
    req.lease_epoch  = 1;
    smb2_create(a, "o1b", FILE_OPEN_IF, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                &req, &o);
    EXPECT(o.status == ST_SUCCESS, "O1b CREATE(lease RWH) -> 0x%08x", o.status);
    EXPECT(o.oplock == SMB2_OPLOCK_LEVEL_LEASE,
           "O1b lease open reports OplockLevel=LEASE (MS-SMB2 3.3.5.9.11)");
    EXPECT(o.has_lease, "O1b CREATE reply carries an RqLs response context");
    NOTE("O1b granted lease state = %s (0x%02x), epoch=%u",
         lease_str(o.lease_state), o.lease_state, o.lease_epoch);
    EXPECT(o.lease_state == SMB2_LEASE_RWH,
           "O1b sole opener granted RWH lease (MS-SMB2 3.3.5.9.11)");
    smb2_close(a, o.file_id);
} /* sec_o1 */

/* ---- O2: break-on-open (mandated) --------------------------------------- */

static void
sec_o2(
    struct smb2_env  *env,
    struct smb2_conn *a,
    struct smb2_conn *b)
{
    struct smb2_create_out oa, ob;
    struct smb2_oplock_req req;
    struct smb2_break      breaks[8];
    int                    nbreaks = 0;

    printf("\n== O2: conflicting open breaks a batch oplock ==\n");

    /* Holder A takes a batch oplock on a fresh file, sole opener. */
    memset(&req, 0, sizeof(req));
    req.level = SMB2_OPLOCK_LEVEL_BATCH;
    smb2_create(a, "o2", FILE_OPEN_IF, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                &req, &oa);
    EXPECT(oa.status == ST_SUCCESS && oa.oplock == SMB2_OPLOCK_LEVEL_BATCH,
           "O2 holder A granted BATCH");

    /* Opener B does a conflicting read open.  MS-SMB2 3.3.4.6 / MS-FSA
     * 2.1.5.1.2: the batch oplock MUST break; the holder is notified and the
     * opener is deferred (STATUS_PENDING) until the holder acks. */
    ob.status = conflicting_open(env, b, a, "o2", FILE_OPEN, FILE_READ_ACCESS,
                                 FILE_SHARE_RWD, NULL, &ob, breaks, &nbreaks);

    EXPECT(nbreaks >= 1, "O2 holder A received a break notification (%d)",
           nbreaks);
    if (nbreaks >= 1) {
        EXPECT(breaks[0].is_lease == 0,
               "O2 break is a legacy OPLOCK_BREAK (not a lease break)");
        NOTE("O2 server asked holder to break to %s (0x%02x)",
             oplock_name(breaks[0].oplock_level), breaks[0].oplock_level);
        /* A read open lets a read cache survive, so the batch oplock must
         * break to LEVEL_II, not NONE (MS-SMB2 3.3.4.6). */
        EXPECT(breaks[0].oplock_level == SMB2_OPLOCK_LEVEL_II,
               "O2 break-to-level for a read open is II (MS-SMB2 3.3.4.6)");
    }
    EXPECT(ob.status == ST_SUCCESS,
           "O2 opener B's CREATE completes after the break drains -> 0x%08x",
           ob.status);

    smb2_close(a, oa.file_id);
    smb2_close(b, ob.file_id);
} /* sec_o2 */

/* ---- O3: break-on-write invariant (mandated) ---------------------------- */

static void
sec_o3(
    struct smb2_env  *env,
    struct smb2_conn *a,
    struct smb2_conn *b)
{
    struct smb2_create_out oa, ob;
    struct smb2_oplock_req req;
    struct smb2_break      b0 = { 0 };   /* read only when got_break; zero-init
                                          * silences a -Wmaybe-uninitialized
                                          * false positive on gcc/amd64 */
    uint32_t               st, count = 0;
    const char             data[] = "coherent";
    int                    got_break;

    printf("\n== O3: a conflicting write leaves no live read cache ==\n");

    /* Writer B opens first with write access and NO caching request, so it
     * holds a writable handle but no cache to break. */
    memset(&req, 0, sizeof(req));
    smb2_create(b, "o3", FILE_OPEN_IF, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                NULL, &ob);
    EXPECT(ob.status == ST_SUCCESS, "O3 writer B opened (no cache)");

    /* Reader A then requests a level-II read cache.  Whether the server
     * grants a read cache while a writer is open is discretion (the grant is
     * capped to what is grantable without an immediate break -- MS-SMB2
     * 3.3.5.9.9); RECORD it. */
    memset(&req, 0, sizeof(req));
    req.level = SMB2_OPLOCK_LEVEL_II;
    smb2_create(a, "o3", FILE_OPEN, FILE_READ_ACCESS, FILE_SHARE_RWD,
                &req, &oa);
    EXPECT(oa.status == ST_SUCCESS, "O3 reader A opened");
    NOTE("O3 reader A granted oplock = %s while a writer is open",
         oplock_name(oa.oplock));

    /* B writes.  The mandated invariant (MS-FSA 2.1.5.3 / the SMB2 caching
     * model): a conflicting write must not leave a stale read cache alive --
     * so IF A holds a read cache, the write MUST break it away.  This is
     * asserted as an invariant, independent of the discretionary grant
     * timing above.  Drive B's write, collecting any break delivered to A. */
    got_break = 0;
    smb2_write_post(b, ob.file_id, 0, data, (uint32_t) sizeof(data) - 1);
    while (!b->reply_ready) {
        smb2_pump(env);
        struct smb2_break bx;
        while (smb2_conn_pop_break(a, &bx)) {
            got_break = 1;
            b0        = bx;
            if (bx.is_lease) {
                smb2_lease_break_ack(a, bx.lease_key, bx.new_state);
            } else {
                smb2_oplock_break_ack(a, bx.file_id, bx.oplock_level);
            }
        }
    }
    st    = g32(b->rbuf + 4, 8);
    count = (st == ST_SUCCESS) ? smb2c_write_count(b) : 0;
    EXPECT(st == ST_SUCCESS && count == sizeof(data) - 1,
           "O3 B's WRITE succeeds (count=%u)", count);

    int a_had_read_cache = (oa.oplock == SMB2_OPLOCK_LEVEL_II ||
                            oa.oplock == SMB2_OPLOCK_LEVEL_EXCLUSIVE ||
                            oa.oplock == SMB2_OPLOCK_LEVEL_BATCH);

    /* B's WRITE reply can land BEFORE the break notification reaches A: the
     * break travels through a cross-thread doorbell, so it is not ordered
     * against the reply.  Waiting only on the reply therefore leaves a break
     * sitting in A's queue that a LATER section pops as if it were its own --
     * which is exactly how this probe used to fail intermittently under load
     * (O4 would see O3's legacy OPLOCK_BREAK and report "break is not a
     * LEASE_BREAK").
     *
     * Settle causally instead of waiting on the clock: smb2_quiesce() drives
     * every server thread until a whole pass produces nothing new, so whatever
     * break A was owed has been sent -- and if none has arrived by then, none
     * is coming.  The previous form here slept out a five-second budget on
     * EVERY run, because the break had usually already been popped by the
     * write loop above and the "wait for nbreaks != 0" condition could then
     * never be satisfied. */
    if (a_had_read_cache) {
        struct smb2_break bz;

        smb2_quiesce(env);
        while (smb2_conn_pop_break(a, &bz)) {
            got_break = 1;
            b0        = bz;
            if (bz.is_lease) {
                smb2_lease_break_ack(a, bz.lease_key, bz.new_state);
            } else {
                smb2_oplock_break_ack(a, bz.file_id, bz.oplock_level);
            }
        }
    }
    EXPECT(smb2_conn_nbreaks(a) == 0 && smb2_conn_nbreaks(b) == 0,
           "O3 leaves no undelivered break behind (section isolation)");
    if (a_had_read_cache) {
        EXPECT(got_break,
               "O3 A's read cache is broken by B's write (MS-FSA 2.1.5.3)");
        if (got_break && !b0.is_lease) {
            EXPECT(b0.oplock_level == SMB2_OPLOCK_LEVEL_NONE,
                   "O3 break-on-write drives the read cache to NONE");
        }
    } else {
        NOTE("O3 chimera declined a read cache to A while a writer was open "
             "(conservative, spec-consistent); break-on-write not exercised");
    }

    smb2_close(a, oa.file_id);
    smb2_close(b, ob.file_id);
} /* sec_o3 */

/* ---- O4: lease break downgrade + epoch + ack idempotency (mandated) ------ */

static void
sec_o4(
    struct smb2_env  *env,
    struct smb2_conn *a,
    struct smb2_conn *b)
{
    struct smb2_create_out oa, ob;
    struct smb2_oplock_req req;
    struct smb2_break      breaks[8];
    int                    nbreaks = 0;
    uint32_t               st;

    printf("\n== O4: write-lease break (downgrade, epoch, ack idempotency) ==\n");

    /* Holder A takes a write lease (RWH), sole opener. */
    memset(&req, 0, sizeof(req));
    req.is_lease     = 1;
    req.lease_key[0] = 0xB4;
    req.lease_state  = SMB2_LEASE_RWH;
    req.lease_epoch  = 1;
    smb2_create(a, "o4", FILE_OPEN_IF, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                &req, &oa);
    EXPECT(oa.status == ST_SUCCESS && oa.has_lease &&
           oa.lease_state == SMB2_LEASE_RWH, "O4 holder A granted RWH lease");
    uint16_t               granted_epoch = oa.lease_epoch;

    /* Opener B does a conflicting read open with a DIFFERENT lease key.  The
     * write lease MUST break (MS-SMB2 3.3.4.7): a lease break notification to
     * A, downgrade a proper subset, W stripped, epoch bumped, ack required. */
    struct smb2_oplock_req breq;
    memset(&breq, 0, sizeof(breq));
    breq.is_lease     = 1;
    breq.lease_key[0] = 0xB5;
    breq.lease_state  = SMB2_LEASE_RH;
    breq.lease_epoch  = 1;
    conflicting_open(env, b, a, "o4", FILE_OPEN, FILE_READ_ACCESS,
                     FILE_SHARE_RWD, &breq, &ob, breaks, &nbreaks);

    EXPECT(nbreaks >= 1, "O4 holder A received a lease break (%d)", nbreaks);
    if (nbreaks >= 1) {
        struct smb2_break *lb = &breaks[0];
        EXPECT(lb->is_lease, "O4 break is a LEASE_BREAK");
        NOTE("O4 lease break: cur=%s new=%s epoch=%u ack_required=%d",
             lease_str(lb->cur_state), lease_str(lb->new_state),
             lb->new_epoch, lb->ack_required);
        EXPECT(lb->cur_state == SMB2_LEASE_RWH,
               "O4 CurrentLeaseState echoes the granted RWH (MS-SMB2 2.2.23.2)");
        EXPECT((lb->new_state & ~lb->cur_state) == 0,
               "O4 NewLeaseState is a subset of Current (a break only "
               "downgrades, MS-SMB2 3.3.4.7)");
        EXPECT((lb->new_state & SMB2_LEASE_WRITE) == 0,
               "O4 write caching is stripped by a conflicting read open "
               "(MS-FSA 2.1.5.x)");
        EXPECT(lb->new_epoch > granted_epoch,
               "O4 epoch advances on the break (v2, MS-SMB2 3.3.4.7): %u > %u",
               lb->new_epoch, granted_epoch);
        EXPECT(lb->ack_required,
               "O4 break stripping W is ack-required (MS-SMB2 2.2.23.2)");
    }
    EXPECT(ob.status == ST_SUCCESS, "O4 opener B completes after the break");

    /* Idempotency (MS-SMB2 3.3.5.22.2): a lease-break ack for a lease that is
     * no longer breaking MUST be rejected -- it is not a benign no-op.  The
     * conflicting_open above already acked the break, so a second ack now
     * targets a settled lease. */
    uint8_t settled_key[16] = { 0xB4 };
    st = smb2_lease_break_ack(a, settled_key, SMB2_LEASE_RH);
    NOTE("O4 second (redundant) lease-break ack -> 0x%08x", st);
    EXPECT(st != ST_SUCCESS,
           "O4 ack on an already-settled lease is rejected "
           "(MS-SMB2 3.3.5.22.2)");

    smb2_close(a, oa.file_id);
    smb2_close(b, ob.file_id);
} /* sec_o4 */

/* ---- O5: grant denial while a peer open exists (mandated) ---------------- */

static void
sec_o5(
    struct smb2_env  *env,
    struct smb2_conn *a,
    struct smb2_conn *b)
{
    struct smb2_create_out oa, ob;
    struct smb2_oplock_req req;

    printf("\n== O5: exclusive/batch is refused while a peer open exists ==\n");

    /* A holds a plain read open (no oplock), so there is a peer open but no
     * cache to break. */
    smb2_create(a, "o5", FILE_OPEN_IF, FILE_READ_ACCESS, FILE_SHARE_RWD,
                NULL, &oa);
    EXPECT(oa.status == ST_SUCCESS, "O5 peer A opened (no oplock)");

    /* B requests a BATCH oplock.  MS-FSA 2.1.5.17.1: an exclusive/batch
     * oplock requires sole access, so with A's open present it MUST NOT be
     * granted at that level; the server downgrades to at most LEVEL_II. */
    memset(&req, 0, sizeof(req));
    req.level = SMB2_OPLOCK_LEVEL_BATCH;
    /* B may itself trigger a break of A -- but A holds no cache, so this is a
     * plain (non-parking) open; use the driving helper regardless. */
    struct smb2_break breaks[8];
    int               nbr = 0;
    conflicting_open(env, b, a, "o5", FILE_OPEN, FILE_READ_ACCESS,
                     FILE_SHARE_RWD, &req, &ob, breaks, &nbr);
    EXPECT(ob.status == ST_SUCCESS, "O5 B's CREATE(batch request) succeeds");
    NOTE("O5 B granted oplock = %s while peer A is open", oplock_name(ob.oplock));
    /* MS-FSA 2.1.5.17.2: an exclusive/batch oplock is granted only when the
     * requesting Open is the sole Open on the stream.  With A's open present,
     * B must be refused (granted at most LEVEL_II). */
    if (ob.oplock == SMB2_OPLOCK_LEVEL_BATCH ||
        ob.oplock == SMB2_OPLOCK_LEVEL_EXCLUSIVE) {
        DEVIATION("S-1",
                  "batch/exclusive oplock (%s) granted to B while peer A is "
                  "open; MS-FSA 2.1.5.17.2 requires sole access. chimera caps "
                  "grants against peer CACHES, not peer OPENS (A holds no "
                  "cache). See DEVIATIONS-SMB.md.", oplock_name(ob.oplock));
    } else {
        EXPECT(1, "O5 exclusive/batch refused while a peer open exists "
               "(MS-FSA 2.1.5.17.2)");
    }

    smb2_close(a, oa.file_id);
    smb2_close(b, ob.file_id);
} /* sec_o5 */

/* ---- O6: when does a conflicting open actually PARK? -------------------- */

/* The model's ST_PENDING must name the same event the wire does.  On the wire a
 * CREATE parks (emits an async interim, then completes later on the SAME
 * request) only when its SHARE acquire comes back BREAKING -- i.e. only when it
 * both conflicts on share modes with, and is blocked behind, a handle-caching
 * holder that is mid-break (smb_proc_create.c: the CHIMERA_VFS_LEASE_BREAKING
 * arm of the share try_insert).  A conflicting open that breaks a peer's oplock
 * but has NO share conflict is answered immediately: the break is fired and the
 * open proceeds without waiting for the ack.
 *
 * That distinction is invisible to O2 (whose driver acks in the pump loop), and
 * it is exactly the fact a generated trace must get right, so pin it here by
 * settling the server WITHOUT acking and checking that no final response came.
 *
 * ---- how "no response arrives" is proved, and why not with a sleep --------
 *
 * This is the one assertion in the suite that is a negative, and a negative
 * needs a bound.  The bound used here is CAUSAL, not temporal: enumerate every
 * edge that can complete the parked CREATE, then show that none of them has
 * fired.  From the server source there are exactly two.
 *
 *   1. The holder's break settling.  O6a's caching wait is resumed by
 *      chimera_smb_create_resume_parked off the thread's lease_resume_doorbell
 *      (smb.c); O6b's share wait is resumed by the lease ticket that
 *      chimera_vfs_lease_acquire enqueued (smb_proc_create.c, the
 *      CHIMERA_VFS_LEASE_BREAKING arm of the share try_insert), which fires
 *      through chimera_smb_create_share_park_cb.  BOTH need the holder to ack
 *      or to close, and the probe deliberately does neither until after the
 *      assertion.  Neither is on a timer.
 *   2. chimera's own oplock-break deadline: smb_proc_create.c arms a one-shot
 *      timer for vfs_state->default_break_deadline_ms
 *      (CHIMERA_VFS_STATE_DEFAULT_BREAK_DEADLINE_MS = 30 000) which revokes the
 *      unacked holder and completes the open anyway.  That is the ONLY
 *      time-driven edge, it is a server constant rather than a property of the
 *      machine, and this section runs three orders of magnitude below it.
 *
 * smb2_quiesce() discharges (1): an SMB2 ECHO is completed synchronously on
 * the very thread that owns the parked request and would emit its completion,
 * so a returned ECHO proves that thread ran complete event-loop passes --
 * doorbells included -- with the CREATE still parked; and quiescing repeats
 * that over every connection until a whole pass produces nothing new, which is
 * what covers the cross-thread CHAIN (B's thread runs the CREATE, only then
 * does A's thread have a break to flush).  The verdict is then simply whether
 * b's count of non-barrier replies moved.
 *
 * What this replaces: a 5 s sleep per sub-section that asserted silence by
 * outlasting it.  That made the verdict a function of machine load (a slow
 * enough host could have completed the open inside the window and a fast
 * enough one could not), and it cost 10 s of the probe's 15 s runtime. */
static void
sec_o6(
    struct smb2_env  *env,
    struct smb2_conn *a,
    struct smb2_conn *b)
{
    struct smb2_create_out oa, ob;
    struct smb2_oplock_req req;
    struct smb2_break      bx;
    int                    got_reply, ninterim0, nreply0, mark;

    printf("\n== O6: which conflicting opens park (async interim)? ==\n");

    /* --- O6a: batch holder, share-compatible conflicting open --- */
    memset(&req, 0, sizeof(req));
    req.level = SMB2_OPLOCK_LEVEL_BATCH;
    smb2_create(a, "o6a", FILE_OPEN_IF, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                &req, &oa);
    EXPECT(oa.status == ST_SUCCESS && oa.oplock == SMB2_OPLOCK_LEVEL_BATCH,
           "O6a holder A granted BATCH");

    ninterim0 = b->ninterim;
    nreply0   = b->nreply_app;
    smb2_create_post(b, "o6a", FILE_OPEN, FILE_READ_ACCESS, FILE_SHARE_RWD,
                     NULL);
    /* Drive the whole server to quiescence (smb2_mbt_common.h): B's thread has
     * to run the CREATE before A's thread has a break to flush, so this is a
     * chain, and a fixed number of barriers would be a race.  The silence
     * verdict is read off b->nreply_app sampled BEFORE the CREATE was posted,
     * so it covers the entire span from the post to the end of the settle --
     * quiescing pumps the shared loop, and a completion landing anywhere in
     * there has to count. */
    smb2_quiesce(env);
    got_reply = (b->nreply_app > nreply0);
    NOTE("O6a share-compatible open: reply=%d interim=%d (A breaks pending=%d)",
         got_reply, b->ninterim - ninterim0, smb2_conn_nbreaks(a));
    EXPECT(smb2_conn_nbreaks(a) >= 1,
           "O6a the batch oplock is broken by the conflicting open "
           "(MS-FSA 2.1.5.1.2)");
    EXPECT(b->ninterim > ninterim0,
           "O6a the deferred open announces itself with an async interim "
           "STATUS_PENDING (MS-SMB2 3.3.5.9 pending-open)");
    EXPECT(!got_reply,
           "O6a and then WAITS: no final response arrives while the holder "
           "has not acknowledged");
    mark = b->nreply_app;
    while (smb2_conn_pop_break(a, &bx)) {
        smb2_oplock_break_ack(a, bx.file_id, bx.oplock_level);
    }
    got_reply = smb2c_pump_for_nreply(b, mark,
                                      "O6a's parked CREATE to complete after "
                                      "the holder's acknowledgment");
    EXPECT(got_reply, "O6a the parked open completes once the holder acks");
    if (got_reply) {
        smb2c_parse_create(b, &ob);
        NOTE("O6a deferred completion status = 0x%08x", ob.status);
        EXPECT(ob.status == ST_SUCCESS,
               "O6a share-compatible open succeeds after the break drains");
        if (ob.status == ST_SUCCESS) {
            smb2_close(b, ob.file_id);
        }
    }
    smb2_close(a, oa.file_id);

    /* --- O6b: batch holder that DENIES sharing.  The conflicting open must
     * still wait -- the holder may close its deferred handle and dissolve the
     * conflict -- but this park goes through the SHARE acquire rather than the
     * caching wait, and that path emits NO async interim. --- */
    memset(&req, 0, sizeof(req));
    req.level = SMB2_OPLOCK_LEVEL_BATCH;
    smb2_create(a, "o6b", FILE_OPEN_IF, FILE_ALL_ACCESS, 0 /* share NONE */,
                &req, &oa);
    EXPECT(oa.status == ST_SUCCESS && oa.oplock == SMB2_OPLOCK_LEVEL_BATCH,
           "O6b holder A granted BATCH with ShareAccess=NONE");

    ninterim0 = b->ninterim;
    nreply0   = b->nreply_app;
    smb2_create_post(b, "o6b", FILE_OPEN, FILE_READ_ACCESS, FILE_SHARE_RWD,
                     NULL);
    smb2_quiesce(env);
    got_reply = (b->nreply_app > nreply0);
    NOTE("O6b share-CONFLICTING open: reply=%d interim=%d (A breaks pending=%d)",
         got_reply, b->ninterim - ninterim0, smb2_conn_nbreaks(a));
    EXPECT(smb2_conn_nbreaks(a) >= 1,
           "O6b the batch holder is broken BEFORE the share check "
           "(MS-FSA 2.1.5.1: a batch holder may close and dissolve the "
           "conflict)");
    EXPECT(!got_reply,
           "O6b the share-conflicting open WAITS for the batch holder's "
           "answer rather than refusing immediately");
    /* Both waits now announce themselves.  This assertion used to pin the
     * opposite -- a silent share-acquire park was recorded as Q-3, on the
     * grounds that MS-SMB2 3.3.5.9's pending-open contract exists so a client
     * can correlate and CANCEL a deferred create rather than time out.  The
     * server since emits the interim on this path too, for that exact reason,
     * so the assertion flips to the mandate: a park of either kind is
     * announced. */
    EXPECT(b->ninterim > ninterim0,
           "O6b the share-acquire park emits an async interim "
           "(MS-SMB2 3.3.5.9)");
    /* Release it: the holder acks but keeps its handle, so the deferred
     * completion must be SHARING_VIOLATION. */
    mark = b->nreply_app;
    while (smb2_conn_pop_break(a, &bx)) {
        smb2_oplock_break_ack(a, bx.file_id, bx.oplock_level);
    }
    got_reply = smb2c_pump_for_nreply(b, mark,
                                      "O6b's parked CREATE to complete after "
                                      "the holder's acknowledgment");
    EXPECT(got_reply, "O6b the parked open completes once the holder acks");
    if (got_reply) {
        smb2c_parse_create(b, &ob);
        NOTE("O6b deferred completion status = 0x%08x", ob.status);
        EXPECT(ob.status == ST_SHARING_VIOLATION,
               "O6b holder acked but kept its handle -> SHARING_VIOLATION");
        if (ob.status == ST_SUCCESS) {
            smb2_close(b, ob.file_id);
        }
    }
    smb2_close(a, oa.file_id);
} /* sec_o6 */

/* ---- O7: same-lease-key coalescing --------------------------------------- */

/* MS-SMB2 3.3.5.9.8: two opens of one file under ONE lease key share a single
 * lease -- the second open joins the grant (it does not create a second one,
 * and it never downgrades the first).  The model must coalesce identically or
 * it will predict two independent grants, two epochs and two break
 * notifications where the wire has one of each. */
static void
sec_o7(
    struct smb2_env  *env,
    struct smb2_conn *a,
    struct smb2_conn *b)
{
    struct smb2_create_out o1, o2, ob;
    struct smb2_oplock_req req, breq;
    struct smb2_break      breaks[8];
    int                    nbreaks = 0;

    printf("\n== O7: two opens under one lease key coalesce ==\n");

    memset(&req, 0, sizeof(req));
    req.is_lease     = 1;
    req.lease_key[0] = 0xC7;
    req.lease_state  = SMB2_LEASE_RWH;
    req.lease_epoch  = 1;
    smb2_create(a, "o7", FILE_OPEN_IF, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                &req, &o1);
    EXPECT(o1.status == ST_SUCCESS && o1.lease_state == SMB2_LEASE_RWH,
           "O7 first open under key C7 granted RWH");

    /* Second open, SAME key, asking for LESS (R only). */
    memset(&req, 0, sizeof(req));
    req.is_lease     = 1;
    req.lease_key[0] = 0xC7;
    req.lease_state  = SMB2_LEASE_READ;
    req.lease_epoch  = 1;
    smb2_create(a, "o7", FILE_OPEN, FILE_READ_ACCESS, FILE_SHARE_RWD,
                &req, &o2);
    EXPECT(o2.status == ST_SUCCESS, "O7 second open under key C7 succeeds");
    NOTE("O7 second same-key open reports lease=%s epoch=%u (first: %s epoch=%u)",
         lease_str(o2.lease_state), o2.lease_epoch,
         lease_str(o1.lease_state), o1.lease_epoch);
    EXPECT(o2.lease_state == o1.lease_state,
           "O7 a same-key re-open joins the existing lease and does NOT "
           "reduce it (MS-SMB2 3.3.5.9.8)");
    EXPECT(o2.lease_epoch == o1.lease_epoch,
           "O7 coalesced opens share ONE epoch counter (MS-SMB2 3.3.5.9.11)");

    /* A conflicting open by another client must produce exactly ONE break for
     * the shared lease, not one per member open. */
    memset(&breq, 0, sizeof(breq));
    breq.is_lease     = 1;
    breq.lease_key[0] = 0xC8;
    breq.lease_state  = SMB2_LEASE_RH;
    breq.lease_epoch  = 1;
    conflicting_open(env, b, a, "o7", FILE_OPEN, FILE_READ_ACCESS,
                     FILE_SHARE_RWD, &breq, &ob, breaks, &nbreaks);
    NOTE("O7 conflicting open produced %d break notification(s) for the "
         "coalesced lease", nbreaks);
    EXPECT(nbreaks == 1,
           "O7 the coalesced lease breaks ONCE, not once per member open");
    EXPECT(ob.status == ST_SUCCESS, "O7 opener B completes");

    smb2_close(a, o1.file_id);
    smb2_close(a, o2.file_id);
    smb2_close(b, ob.file_id);
} /* sec_o7 */

/* ---- O8: which breaks require an acknowledgment? ------------------------- */

/* smb_proc_oplock_break.c: an ack is required only when the break strips WRITE
 * or HANDLE caching (lease), or when the holder held EXCLUSIVE/BATCH (legacy
 * oplock).  A break that removes only READ caching settles immediately and is
 * never acknowledged -- an ack for it is a protocol error.  A model that marks
 * every broken grant "awaiting ack" would generate acks the server rejects. */
static void
sec_o8(
    struct smb2_env  *env,
    struct smb2_conn *a,
    struct smb2_conn *b)
{
    struct smb2_create_out oa, ob;
    struct smb2_oplock_req req;
    struct smb2_break      bx;
    uint32_t               st;
    const char             data[] = "x";
    int                    got    = 0;

    printf("\n== O8: read-only breaks need no acknowledgment ==\n");

    /* A takes a LEVEL_II (read-cache-only) oplock as the sole opener. */
    memset(&req, 0, sizeof(req));
    req.level = SMB2_OPLOCK_LEVEL_II;
    smb2_create(a, "o8", FILE_OPEN_IF, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                &req, &oa);
    EXPECT(oa.status == ST_SUCCESS, "O8 holder A opened");
    NOTE("O8 A granted oplock = %s", oplock_name(oa.oplock));

    /* B opens for write and writes: A's read cache must be invalidated. */
    smb2_create(b, "o8", FILE_OPEN, FILE_ALL_ACCESS, FILE_SHARE_RWD, NULL, &ob);
    EXPECT(ob.status == ST_SUCCESS, "O8 writer B opened");

    smb2_write_post(b, ob.file_id, 0, data, 1);
    while (!b->reply_ready) {
        smb2_pump(env);
        while (smb2_conn_pop_break(a, &bx)) {
            got = 1;
            NOTE("O8 break to A: is_lease=%d level=%s ack_required=%d",
                 bx.is_lease, oplock_name(bx.oplock_level), bx.ack_required);
        }
    }
    st = g32(b->rbuf + 4, 8);
    EXPECT(st == ST_SUCCESS, "O8 B's WRITE succeeds -> 0x%08x", st);
    if (got && oa.oplock == SMB2_OPLOCK_LEVEL_II) {
        /* An unsolicited ack for a LEVEL_II break is a protocol error
         * (MS-SMB2 3.3.5.22.1); the server must reject it. */
        st = smb2_oplock_break_ack(a, oa.file_id, SMB2_OPLOCK_LEVEL_NONE);
        NOTE("O8 ack for a LEVEL_II (no-ack) break -> 0x%08x", st);
        EXPECT(st != ST_SUCCESS,
               "O8 acking a break that required no ack is rejected "
               "(MS-SMB2 3.3.5.22.1)");
    }

    smb2_close(a, oa.file_id);
    smb2_close(b, ob.file_id);
} /* sec_o8 */

/* ---- O9: lease v1 vs v2 (epoch versioning) ------------------------------- */

static void
sec_o9(
    struct smb2_env  *env,
    struct smb2_conn *a,
    struct smb2_conn *b)
{
    struct smb2_create_out oa, ob;
    struct smb2_oplock_req req, breq;
    struct smb2_break      breaks[8];
    int                    nbreaks = 0;

    printf("\n== O9: a v1 lease is not epoch-versioned ==\n");

    memset(&req, 0, sizeof(req));
    req.is_lease     = 1;
    req.force_v1     = 1;
    req.lease_key[0] = 0xD9;
    req.lease_state  = SMB2_LEASE_RWH;
    smb2_create(a, "o9", FILE_OPEN_IF, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                &req, &oa);
    EXPECT(oa.status == ST_SUCCESS && oa.has_lease,
           "O9 v1 RqLs open granted a lease");
    NOTE("O9 v1 grant: state=%s epoch=%u", lease_str(oa.lease_state),
         oa.lease_epoch);

    memset(&breq, 0, sizeof(breq));
    breq.is_lease     = 1;
    breq.force_v1     = 1;
    breq.lease_key[0] = 0xDA;
    breq.lease_state  = SMB2_LEASE_RH;
    conflicting_open(env, b, a, "o9", FILE_OPEN, FILE_READ_ACCESS,
                     FILE_SHARE_RWD, &breq, &ob, breaks, &nbreaks);
    EXPECT(nbreaks >= 1, "O9 the v1 write lease breaks (%d)", nbreaks);
    if (nbreaks >= 1) {
        NOTE("O9 v1 break: cur=%s new=%s epoch=%u",
             lease_str(breaks[0].cur_state), lease_str(breaks[0].new_state),
             breaks[0].new_epoch);
        EXPECT(breaks[0].new_epoch == 0,
               "O9 a v1 lease breaks with epoch 0 (only v2 versions state; "
               "MS-SMB2 2.2.23.2)");
    }
    EXPECT(ob.status == ST_SUCCESS, "O9 opener B completes");

    smb2_close(a, oa.file_id);
    smb2_close(b, ob.file_id);
} /* sec_o9 */

/* ---- O10: SMB2_SHAREFLAG_FORCE_LEVELII_OPLOCK --------------------------- */

/* Runs on its own server instance: the share flag is set at share-creation
 * time.  WPTS ShareForceLevel2 is 112 of the 156 green oplock cases, so this
 * is the single highest-volume oplock axis in the catalog. */
static void
sec_o10(void)
{
    struct smb2_env        env;
    struct smb2_env_opts   opts = { .oplocks      = 1, .leases = 1,
                                    .force_level2 = 1 };
    struct smb2_conn      *a;
    struct smb2_create_out o;
    struct smb2_oplock_req req;

    printf("\n== O10: a force-level-2 share caps every grant to a read cache ==\n");

    smb2_env_start_opts(&env, &opts);
    a = smb2_conn_open(&env);
    smb2_handshake(a);

    memset(&req, 0, sizeof(req));
    req.level = SMB2_OPLOCK_LEVEL_BATCH;
    smb2_create(a, "f1", FILE_OPEN_IF, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                &req, &o);
    EXPECT(o.status == ST_SUCCESS, "O10 CREATE(batch) on a force-level-2 share");
    NOTE("O10 batch request granted %s", oplock_name(o.oplock));
    EXPECT(o.oplock == SMB2_OPLOCK_LEVEL_II || o.oplock == SMB2_OPLOCK_LEVEL_NONE,
           "O10 a batch oplock is capped to at most LEVEL_II on a "
           "force-level-2 share (MS-SMB2 2.2.10)");
    smb2_close(a, o.file_id);

    memset(&req, 0, sizeof(req));
    req.is_lease     = 1;
    req.lease_key[0] = 0xF2;
    req.lease_state  = SMB2_LEASE_RWH;
    req.lease_epoch  = 1;
    smb2_create(a, "f2", FILE_OPEN_IF, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                &req, &o);
    EXPECT(o.status == ST_SUCCESS, "O10 CREATE(lease RWH) on a force-level-2 share");
    NOTE("O10 RWH lease request granted %s", lease_str(o.lease_state));
    EXPECT((o.lease_state & (SMB2_LEASE_WRITE | SMB2_LEASE_HANDLE)) == 0,
           "O10 write/handle caching is stripped on a force-level-2 share");
    smb2_close(a, o.file_id);

    smb2_env_stop(&env);
} /* sec_o10 */

/* ---- O11: which sole-opener requests actually get a legacy oplock? ------- */

/* A generated trace draws DesiredAccess / ShareAccess / CreateDisposition
 * independently of the oplock request, so the model must know the grant rule
 * for the whole matrix, not just for the FILE_ALL_ACCESS / share-everything
 * corner O1 pins.  Every open here is the SOLE open of a FRESH file, so
 * MS-FSA 2.1.5.17.1 says the requested oplock is grantable in every row; the
 * probe RECORDS what chimera returns (the grant level is discretion --
 * MS-SMB2 3.3.5.9) so the model can be pinned to it rather than to a guess. */
static void
sec_o11(
    struct smb2_env  *env,
    struct smb2_conn *a)
{
    static const struct {
        const char *label;
        uint32_t    disp;
        uint32_t    access;
        uint32_t    share;
    } rows[] = {
        { "OPEN_IF  RA|RD|WD  share RWD",  FILE_OPEN_IF,
          0x80u | 0x01u | 0x02u, FILE_SHARE_RWD },
        { "OPEN_IF  RA|WD     share RWD",  FILE_OPEN_IF,
          0x80u | 0x02u, FILE_SHARE_RWD },
        { "OPEN_IF  RA|RD     share RWD",  FILE_OPEN_IF,
          0x80u | 0x01u, FILE_SHARE_RWD },
        { "OPEN_IF  RA|RD|WD  share R",    FILE_OPEN_IF,
          0x80u | 0x01u | 0x02u, FILE_SHARE_READ },
        { "OPEN_IF  RA|RD|WD  share -",    FILE_OPEN_IF,
          0x80u | 0x01u | 0x02u, 0 },
        { "OVERWRITE_IF RA|WD share R",    FILE_OVERWRITE_IF,
          0x80u | 0x02u, FILE_SHARE_READ },
        { "OPEN_IF  RA|RD|WD|DEL sh RW",   FILE_OPEN_IF,
          0x80u | 0x01u | 0x02u | 0x00010000u,
          FILE_SHARE_READ | FILE_SHARE_WRITE },
        { "OPEN_IF  ALL_ACCESS share RWD", FILE_OPEN_IF,        FILE_ALL_ACCESS,
          FILE_SHARE_RWD },
    };
    int n = (int) (sizeof(rows) / sizeof(rows[0]));

    printf("\n== O11: legacy oplock grant across the request matrix ==\n");

    for (int i = 0; i < n; i++) {
        struct smb2_create_out o;
        struct smb2_oplock_req req;
        char                   name[32];

        snprintf(name, sizeof(name), "o11_%d", i);
        memset(&req, 0, sizeof(req));
        req.level = SMB2_OPLOCK_LEVEL_BATCH;
        smb2_create(a, name, rows[i].disp, rows[i].access, rows[i].share,
                    &req, &o);
        EXPECT(o.status == ST_SUCCESS, "O11[%d] %s -> 0x%08x", i, rows[i].label,
               o.status);
        if (o.status != ST_SUCCESS) {
            continue;
        }
        NOTE("O11[%d] %-30s BATCH request granted %s", i, rows[i].label,
             oplock_name(o.oplock));
        /* Whatever the level, a sole opener of a fresh file must not be
         * refused caching outright when oplocks are enabled: that would make
         * the whole break lifecycle unreachable for this request shape. */
        EXPECT(o.oplock != SMB2_OPLOCK_LEVEL_NONE,
               "O11[%d] a sole opener of a fresh file is granted SOME oplock "
               "(MS-FSA 2.1.5.17.1)", i);
        smb2_close(a, o.file_id);
    }
} /* sec_o11 */

/* ---- O12: WHICH peer open caps a fresh grant, and for whom? ------------- */

/* O5 establishes one cell of a matrix: a data open by a DISTINCT client caps a
 * batch request to LEVEL_II.  The generator needs the whole matrix, because a
 * generated trace draws the peer's client (session), the peer's DesiredAccess
 * (including the attribute-only profile) and the requested oplock/lease
 * independently.  Until that matrix was measured, the generator simply refused
 * to request any caching grant behind a peer open (the `grantAgreed` guard in
 * smb2.qnt), which suppressed exactly the WPTS Oplocks/Leases surface.
 *
 * The mandate being checked is MS-FSA 2.1.5.18.1 "Algorithm to Request an
 * Exclusive Oplock" (numbered 2.1.5.17.2 in the revision this suite's older
 * citations use):
 *
 *   "If Open.File.OpenList contains more than one Open whose Stream is the
 *    same as Open.Stream, and NO_OPLOCK is present in Open.Stream.Oplock.State
 *    -- the operation MUST be failed with Status set to
 *    STATUS_OPLOCK_NOT_GRANTED."
 *
 * Note what that clause does NOT say: it has no TargetOplockKey exemption and
 * no client-identity exemption.  The key exemptions in 2.1.5.18.1 apply only
 * to UPGRADING a stream that already holds an R / RH oplock; a fresh exclusive
 * grant on a stream with NO_OPLOCK is refused by the mere existence of a
 * second Open, whoever owns it.  (A same-lease-key second open never reaches
 * this algorithm at all: MS-SMB2 3.3.5.9.8 coalesces it onto the existing
 * lease above the FSA layer -- probe O7 / C-6.)
 *
 * WRITE_CACHING is the discriminator: BATCH (R|W|H), EXCLUSIVE (R|W) and an
 * RWH/RW lease are "exclusive oplock" requests routed through 2.1.5.18.1,
 * while LEVEL_II, an R lease and an RH lease are SHARED requests
 * (MS-FSA 2.1.5.18.2), which have no sole-open condition at all.
 *
 * Granting LESS than was asked for is server discretion (MS-SMB2 3.3.5.9), so
 * a row whose grant merely lands below the request is recorded as policy, not
 * failed.  Granting a W (or H, for legacy) cache while a peer Open exists is
 * the direction the spec forbids, and is reported as a DEVIATION. */

enum o12_peer {
    O12_P_NONE = 0,   /* sole opener (control row) */
    O12_P_SELF,       /* same client, SAME connection */
    O12_P_GUID,       /* same client, a second connection sharing ClientGuid */
    O12_P_OTHER,      /* a genuinely distinct client (distinct ClientGuid) */
    O12_P_OATTR, /* distinct client, ATTRIBUTE-ONLY (stat) open */
    O12_P_LEAS_O,/* distinct client holding an RH lease (other key) */
    O12_P_LEAS_S  /* SAME client holding an RH lease under another key */
};

enum o12_req {
    O12_R_BATCH = 0,       /* legacy BATCH   -> FSA R|W|H  (exclusive) */
    O12_R_EXCL,            /* legacy EXCLUSIVE -> FSA R|W  (exclusive) */
    O12_R_II,              /* legacy LEVEL_II  -> FSA LEVEL_TWO (shared) */
    O12_R_RWH,             /* RqLs lease RWH   -> FSA R|W|H (exclusive) */
    O12_R_RH,              /* RqLs lease RH    -> FSA R|H   (shared) */
    O12_R_BSTAT       /* BATCH requested by an ATTRIBUTE-ONLY open */
};

/* DesiredAccess of an attribute-only open: exactly what the model's ACCESS_H
 * profile puts on the wire (smb2_mbt_replay.c access_wire). */
#define O12_ATTR_ONLY 0x00000080u

static const char *
o12_peer_name(int p)
{
    switch (p) {
        case O12_P_NONE:       return "no peer open";
        case O12_P_SELF:       return "peer: same client, same conn";
        case O12_P_GUID:       return "peer: same client, 2nd conn";
        case O12_P_OTHER:      return "peer: OTHER client, data open";
        case O12_P_OATTR: return "peer: OTHER client, attr-only";
        case O12_P_LEAS_O: return "peer: OTHER client, RH lease";
        case O12_P_LEAS_S:  return "peer: same client, RH lease";
        default:                  return "?";
    } /* switch */
} /* o12_peer_name */

static const char *
o12_req_name(int r)
{
    switch (r) {
        case O12_R_BATCH:      return "BATCH";
        case O12_R_EXCL:       return "EXCLUSIVE";
        case O12_R_II:         return "LEVEL_II";
        case O12_R_RWH:        return "lease RWH";
        case O12_R_RH:         return "lease RH";
        case O12_R_BSTAT: return "BATCH (attr-only opener)";
        default:             return "?";
    } /* switch */
} /* o12_req_name */

/* Short aliases so the row table below stays inside the line limit; the
 * granted-level and lease-state constants are the wire values. */
#define O12_L_NONE  SMB2_OPLOCK_LEVEL_NONE
#define O12_L_II    SMB2_OPLOCK_LEVEL_II
#define O12_L_EXCL  SMB2_OPLOCK_LEVEL_EXCLUSIVE
#define O12_L_BATCH SMB2_OPLOCK_LEVEL_BATCH
#define O12_L_LEASE SMB2_OPLOCK_LEVEL_LEASE
#define O12_S_R     SMB2_LEASE_READ
#define O12_S_RH    SMB2_LEASE_RH
#define O12_S_RWH   SMB2_LEASE_RWH

static void
sec_o12(
    struct smb2_env  *env,
    struct smb2_conn *a,     /* the requester */
    struct smb2_conn *b,     /* a distinct client */
    struct smb2_conn *g)     /* a second connection of a's client */
{
    /* exp_opl / exp_lease pin the OBSERVED policy (discretion, MS-SMB2
     * 3.3.5.9): they are what chimera returns today, so the model can be
     * written against a measurement rather than a guess, and a silent change
     * of grant policy fails here first.
     *
     * The rows run in peer groups: no peer, same client on the same
     * connection, same client on a second connection (one ClientGuid), a
     * distinct client with a data open, a distinct client with an
     * attribute-only open, and finally peers holding an RH LEASE (one under
     * another client, one under this one).  Four results in the table are
     * worth reading twice, because each one corrected a guess made from the
     * server source:
     *
     *   - an RqLs lease IS capped (to R) by its own client's plain open, while
     *     a legacy oplock in the same position is not: the share-reservation
     *     cap exempts a peer open only when that peer is itself LEASE-backed.
     *     The legacy half of that asymmetry is DEVIATIONS-SMB.md S-3;
     *   - an attribute-only REQUESTER is not refused outright behind a peer
     *     open.  The strict "oplock-transparent" cap only bites when even a
     *     read cache would have to break someone, so with a peer that holds no
     *     cache the request lands at LEVEL_II like a data open;
     *   - behind a peer holding an RH lease the HANDLE bit survives (two RqLs
     *     leases coexist at R+H) while W is arbitrated by the
     *     caching-vs-caching path;
     *   - a LEGACY oplock request by a client that already holds an H lease on
     *     the file is granted NOTHING -- at LEVEL_II as well as at BATCH.
     *     Handle caching already owns the handle and the oplock must not
     *     recall the requester's own lease (smb_proc_create.c
     *     chimera_vfs_client_holds_handle_lease). */
    static const struct {
        int      peer;
        int      req;
        uint8_t  exp_opl;
        uint32_t exp_lease;   /* meaningful when exp_opl == LEASE */
    } rows[] = {
        { O12_P_NONE,   O12_R_BATCH,
          O12_L_BATCH, 0 },
        { O12_P_NONE,   O12_R_EXCL,
          O12_L_EXCL, 0 },
        { O12_P_NONE,   O12_R_II,
          O12_L_II, 0 },
        { O12_P_NONE,   O12_R_RWH,
          O12_L_LEASE, O12_S_RWH },
        { O12_P_NONE,   O12_R_RH,
          O12_L_LEASE, O12_S_RH },
        { O12_P_NONE,   O12_R_BSTAT,
          O12_L_BATCH, 0 },
        { O12_P_SELF,   O12_R_BATCH,
          O12_L_BATCH, 0 },
        { O12_P_SELF,   O12_R_EXCL,
          O12_L_EXCL, 0 },
        { O12_P_SELF,   O12_R_II,
          O12_L_II, 0 },
        { O12_P_SELF,   O12_R_RWH,
          O12_L_LEASE, O12_S_R },
        { O12_P_SELF,   O12_R_RH,
          O12_L_LEASE, O12_S_R },
        { O12_P_SELF,   O12_R_BSTAT,
          O12_L_BATCH, 0 },
        { O12_P_GUID,   O12_R_BATCH,
          O12_L_BATCH, 0 },
        { O12_P_GUID,   O12_R_RWH,
          O12_L_LEASE, O12_S_R },
        { O12_P_OTHER,  O12_R_BATCH,
          O12_L_II, 0 },
        { O12_P_OTHER,  O12_R_EXCL,
          O12_L_II, 0 },
        { O12_P_OTHER,  O12_R_II,
          O12_L_II, 0 },
        { O12_P_OTHER,  O12_R_RWH,
          O12_L_LEASE, O12_S_R },
        { O12_P_OTHER,  O12_R_RH,
          O12_L_LEASE, O12_S_R },
        { O12_P_OTHER,  O12_R_BSTAT,
          O12_L_II, 0 },
        { O12_P_OATTR,  O12_R_BATCH,
          O12_L_BATCH, 0 },
        { O12_P_OATTR,  O12_R_EXCL,
          O12_L_EXCL, 0 },
        { O12_P_OATTR,  O12_R_II,
          O12_L_II, 0 },
        { O12_P_OATTR,  O12_R_RWH,
          O12_L_LEASE, O12_S_RWH },
        { O12_P_OATTR,  O12_R_RH,
          O12_L_LEASE, O12_S_RH },
        { O12_P_OATTR,  O12_R_BSTAT,
          O12_L_BATCH, 0 },
        { O12_P_LEAS_O, O12_R_RWH,
          O12_L_LEASE, O12_S_RH },
        { O12_P_LEAS_O, O12_R_RH,
          O12_L_LEASE, O12_S_RH },
        { O12_P_LEAS_O, O12_R_BATCH,
          O12_L_II, 0 },
        { O12_P_LEAS_O, O12_R_II,
          O12_L_II, 0 },
        { O12_P_LEAS_S, O12_R_RWH,
          O12_L_LEASE, O12_S_RH },
        { O12_P_LEAS_S, O12_R_RH,
          O12_L_LEASE, O12_S_RH },
        { O12_P_LEAS_S, O12_R_BATCH,
          O12_L_NONE, 0 },
        { O12_P_LEAS_S, O12_R_II,
          O12_L_NONE, 0 },
    };
    int n = (int) (sizeof(rows) / sizeof(rows[0]));

    printf("\n== O12: which peer OPEN caps a fresh oplock/lease grant? ==\n");

    for (int i = 0; i < n; i++) {
        struct smb2_conn       *peer = NULL;
        struct smb2_create_out  po, o;
        struct smb2_oplock_req  req, peer_req;
        struct smb2_oplock_req *peer_reqp = NULL;
        struct smb2_break       breaks[8];
        int                     nbreaks     = 0;
        uint32_t                peer_access = FILE_READ_ACCESS;
        uint32_t                req_access  = FILE_READ_ACCESS;
        char                    name[32];
        int                     has_w;

        memset(&peer_req, 0, sizeof(peer_req));

        switch (rows[i].peer) {
            case O12_P_SELF:  peer = a; break;
            case O12_P_GUID:  peer = g; break;
            case O12_P_OTHER: peer = b; break;
            case O12_P_OATTR:
                peer        = b;
                peer_access = O12_ATTR_ONLY;
                break;
            case O12_P_LEAS_O:
            case O12_P_LEAS_S:
                peer = rows[i].peer == O12_P_LEAS_S
                    ? g : b;
                peer_req.is_lease     = 1;
                peer_req.lease_key[0] = (uint8_t) (0xE0 + i);
                peer_req.lease_state  = SMB2_LEASE_RH;
                peer_req.lease_epoch  = 1;
                peer_reqp             = &peer_req;
                break;
            default: break;
        } /* switch */

        snprintf(name, sizeof(name), "o12_%d", i);

        if (peer) {
            smb2_create(peer, name, FILE_OPEN_IF, peer_access,
                        FILE_SHARE_RWD, peer_reqp, &po);
            EXPECT(po.status == ST_SUCCESS,
                   "O12[%d] peer open (%s) -> 0x%08x", i,
                   o12_peer_name(rows[i].peer), po.status);
            if (po.status != ST_SUCCESS) {
                continue;
            }
            if (peer_reqp) {
                NOTE("O12[%d] peer holds lease %s (epoch %u)", i,
                     lease_str(po.lease_state), po.lease_epoch);
            }
        }

        memset(&req, 0, sizeof(req));
        switch (rows[i].req) {
            case O12_R_BATCH:
                req.level = SMB2_OPLOCK_LEVEL_BATCH;
                break;
            case O12_R_EXCL:
                req.level = SMB2_OPLOCK_LEVEL_EXCLUSIVE;
                break;
            case O12_R_II:
                req.level = SMB2_OPLOCK_LEVEL_II;
                break;
            case O12_R_RWH:
                req.is_lease     = 1;
                req.lease_key[0] = (uint8_t) (0xC0 + i);
                req.lease_state  = SMB2_LEASE_RWH;
                req.lease_epoch  = 1;
                break;
            case O12_R_RH:
                req.is_lease     = 1;
                req.lease_key[0] = (uint8_t) (0xC0 + i);
                req.lease_state  = SMB2_LEASE_RH;
                req.lease_epoch  = 1;
                break;
            case O12_R_BSTAT:
                req.level  = SMB2_OPLOCK_LEVEL_BATCH;
                req_access = O12_ATTR_ONLY;
                break;
            default: break;
        } /* switch */

        /* Drive through the shared helper so a row that unexpectedly parks
         * behind a break still completes (and the break is counted) instead of
         * hanging the probe.  No row here is expected to break anything: every
         * peer holds an open, never a cache. */
        conflicting_open(env, a, peer ? peer : a, name, FILE_OPEN_IF,
                         req_access, FILE_SHARE_RWD, &req, &o,
                         breaks, &nbreaks);
        EXPECT(o.status == ST_SUCCESS, "O12[%d] requester CREATE -> 0x%08x", i,
               o.status);
        if (o.status != ST_SUCCESS) {
            if (peer) {
                smb2_close(peer, po.file_id);
            }
            continue;
        }

        if (o.oplock == SMB2_OPLOCK_LEVEL_LEASE) {
            NOTE("O12[%d] %-32s %-24s -> LEASE %s (epoch %u)", i,
                 o12_peer_name(rows[i].peer), o12_req_name(rows[i].req),
                 lease_str(o.lease_state), o.lease_epoch);
        } else {
            NOTE("O12[%d] %-32s %-24s -> %s", i, o12_peer_name(rows[i].peer),
                 o12_req_name(rows[i].req), oplock_name(o.oplock));
        }

        if (peer_reqp) {
            NOTE("O12[%d] peer break notification(s): %d", i, nbreaks);
        } else {
            EXPECT(nbreaks == 0,
                   "O12[%d] a peer that holds no cache is not broken "
                   "(%d break(s))", i, nbreaks);
        }

        /* Policy pin (discretion: MS-SMB2 3.3.5.9 lets the server grant a
         * lesser level).  This is what chimera grants TODAY and what the model
         * encodes; it is not a spec mandate. */
        EXPECT(o.oplock == rows[i].exp_opl,
               "O12[%d] granted level %s, policy says %s "
               "(discretion, MS-SMB2 3.3.5.9)", i, oplock_name(o.oplock),
               oplock_name(rows[i].exp_opl));
        if (rows[i].exp_opl == SMB2_OPLOCK_LEVEL_LEASE &&
            o.oplock == SMB2_OPLOCK_LEVEL_LEASE) {
            EXPECT(o.lease_state == rows[i].exp_lease,
                   "O12[%d] granted lease %s, policy says %s "
                   "(discretion, MS-SMB2 3.3.5.9)", i,
                   lease_str(o.lease_state), lease_str(rows[i].exp_lease));
        }

        /* The MANDATE.  With a peer Open on the stream and no pre-existing
         * oplock, MS-FSA 2.1.5.18.1 refuses an exclusive request outright, so
         * no WRITE cache (legacy EXCLUSIVE/BATCH, or a lease carrying W) may be
         * handed out.  HANDLE caching rides along on the same clause for a
         * legacy BATCH oplock (BATCH == R|W|H is one exclusive request), while
         * an RH lease is a SHARED request (2.1.5.18.2) and is unconstrained
         * here. */
        has_w = (o.oplock == SMB2_OPLOCK_LEVEL_LEASE)
            ? ((o.lease_state & SMB2_LEASE_WRITE) != 0)
            : (o.oplock == SMB2_OPLOCK_LEVEL_EXCLUSIVE ||
               o.oplock == SMB2_OPLOCK_LEVEL_BATCH);

        if (rows[i].peer != O12_P_NONE) {
            if (has_w) {
                DEVIATION("S-3",
                          "O12[%d] %s: a %s request was granted a WRITE cache "
                          "(%s %s) while a peer Open is on the stream; "
                          "MS-FSA 2.1.5.18.1 refuses an exclusive oplock when "
                          "Open.File.OpenList holds more than one Open on the "
                          "stream -- that clause has no OplockKey and no "
                          "client-identity exemption. See DEVIATIONS-SMB.md.",
                          i, o12_peer_name(rows[i].peer),
                          o12_req_name(rows[i].req), oplock_name(o.oplock),
                          o.oplock == SMB2_OPLOCK_LEVEL_LEASE
                          ? lease_str(o.lease_state) : "");
            } else {
                EXPECT(1,
                       "O12[%d] no write cache is granted behind a peer Open "
                       "(MS-FSA 2.1.5.18.1)", i);
            }
        }

        smb2_close(a, o.file_id);
        if (peer) {
            smb2_close(peer, po.file_id);
        }
    }
} /* sec_o12 */

int
main(
    int   argc,
    char *argv[])
{
    struct smb2_env      env;
    struct smb2_env_opts opts = { .oplocks          = 1, .leases = 1,
                                  .directory_leases = 1 };
    struct smb2_conn    *a, *b, *g;

    smb2_env_start_opts(&env, &opts);
    a = smb2_conn_open(&env);
    b = smb2_conn_open(&env);
    /* A second connection of A's CLIENT: same ClientGuid, so chimera derives
     * the same lease client key for it (O12 needs a same-client peer that is
     * not merely a same-connection peer). */
    g           = smb2_conn_open(&env);
    g->guid_tag = a->guid_tag;
    smb2_handshake(a);
    smb2_handshake(b);
    smb2_handshake(g);
    printf("# holder A and opener B connected; dialect=0x%04x\n", a->dialect);

    sec_o1(&env, a);
    sec_o2(&env, a, b);
    sec_o3(&env, a, b);
    sec_o4(&env, a, b);
    sec_o5(&env, a, b);
    sec_o6(&env, a, b);
    sec_o7(&env, a, b);
    sec_o8(&env, a, b);
    sec_o9(&env, a, b);
    sec_o11(&env, a);
    sec_o12(&env, a, b, g);

    smb2_env_stop(&env);

    sec_o10();

    printf("\n# summary: %d recorded deviation(s) (see DEVIATIONS-SMB.md)\n",
           ndev);
    if (nfail) {
        fprintf(stderr, "%d oplock/lease MANDATE check(s) FAILED\n", nfail);
        return 1;
    }
    printf("all SMB2 oplock/lease mandate checks passed"
           " (%d documented deviation(s))\n", ndev);
    return 0;
} /* main */
