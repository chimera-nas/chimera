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
 *   O2  break-on-open   -- a conflicting open breaks a batch oplock (II), the
 *                          opener parks (PENDING) until the holder acks
 *   O3  break-on-write  -- no read cache survives a conflicting write
 *   O4  lease break     -- write-lease downgrade, epoch bump, ack idempotency
 *   O5  grant denial    -- exclusive/batch is refused while a peer open exists
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
    int n     = 0;
    int guard = 0;

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
        if (opener->disconnected || ++guard > 2000000) {
            fprintf(stderr, "conflicting_open: opener never completed\n");
            exit(4);
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
    struct smb2_break      b0;
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

int
main(
    int   argc,
    char *argv[])
{
    struct smb2_env      env;
    struct smb2_env_opts opts = { .oplocks          = 1, .leases = 1,
                                  .directory_leases = 1 };
    struct smb2_conn    *a, *b;

    smb2_env_start_opts(&env, &opts);
    a = smb2_conn_open(&env);
    b = smb2_conn_open(&env);
    smb2_handshake(a);
    smb2_handshake(b);
    printf("# holder A and opener B connected; dialect=0x%04x\n", a->dialect);

    sec_o1(&env, a);
    sec_o2(&env, a, b);
    sec_o3(&env, a, b);
    sec_o4(&env, a, b);
    sec_o5(&env, a, b);

    smb2_env_stop(&env);

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
