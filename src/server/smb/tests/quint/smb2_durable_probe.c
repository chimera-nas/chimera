/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * SMB2 durable / persistent / resilient handle ground-truth probe -- the
 * durable twin of smb2_oplock_probe.c (ROADMAP-SMB.md Increment 1, extended
 * to the durable-handle families).  It drives the real chimera SMB server
 * over the in-process transport through the whole disconnect-survival
 * lifecycle -- request, grant, park, reclaim, expire, yield -- and pins
 * chimera's behavior BEFORE the generative model bakes any expectation.
 *
 * Conformance discipline (DEVIATIONS-SMB.md "Conformance discipline"): every
 * EXPECT() asserts an MS-SMB2 / MS-FSA-required outcome and cites the clause.
 * Where the spec leaves the server a choice -- WHETHER to grant durability at
 * all, and what Timeout to grant (MS-SMB2 3.3.5.9.10: "the server MAY") --
 * the value is recorded with NOTE() and, where the model must predict the
 * exact reply, pinned as policy with an EXPECT that says so.  A chimera
 * divergence from a mandate is a DEVIATIONS-SMB.md entry, never a weakened
 * assertion.
 *
 * Sections:
 *   D1  grant matrix    -- WHICH caching states make a durable request
 *                          grantable (v1 DHnQ and v2 DH2Q), across no-oplock /
 *                          LEVEL_II / EXCLUSIVE / BATCH and R / RH / RWH leases
 *   D2  timeout         -- what Timeout a DH2Q grant reports for a requested
 *                          0 / in-range / over-maximum value
 *   D3  park + reclaim  -- a transport drop parks a leased durable open; a
 *                          DH2C on a fresh connection of the same client
 *                          reclaims the SAME open, with its data and its lease
 *   D4  reclaim denial  -- every way a reconnect can fail to name the handle:
 *                          wrong CreateGuid / no lease context / wrong lease
 *                          key / wrong name / unknown id / wrong ClientGuid /
 *                          illegal context combinations / persistent-on-durable
 *   D5  v1 reconnect    -- DHnQ grant under a BATCH oplock, DHnC reclaim
 *   D6  non-durable     -- an open with no durable request does NOT survive
 *                          its connection
 *   D7  parked yield    -- a conflicting open against a parked DURABLE-only
 *                          holder: who wins, and can the loser still reclaim
 *   D8  expiry          -- the disconnect-survival deadline is enforced: a
 *                          reclaim after the granted Timeout has provably
 *                          elapsed is refused
 *   D9  replay          -- SMB2_FLAGS_REPLAY_OPERATION + the same DH2Q
 *                          CreateGuid: the eligibility window, and the
 *                          duplicate-open collision
 *   D10 resiliency      -- FSCTL_LMR_REQUEST_RESILIENCY: the timeout policy,
 *                          the malformed-buffer rejection, and whether a
 *                          resilient (but not durable) open survives a drop
 *   D11 persistent      -- a continuously-available share (its own server
 *                          instance): SMB2_DHANDLE_FLAG_PERSISTENT, the
 *                          persistent reclaim, and what a parked PERSISTENT
 *                          holder does to a conflicting opener
 *
 * Each connection gets its own ClientGuid unless it is deliberately reopened
 * as the same client (smb2_conn_reopen inherits guid_tag) -- chimera derives
 * both the lease owner's client key and the durable reclaim's identity check
 * from the ClientGuid, so a shared guid would silently disable the
 * cross-client arbitration this probe is measuring.
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
 * pass/fail. */
#define NOTE(...)                                    \
        do { printf("note - "); printf(__VA_ARGS__); printf("\n"); } while (0)

/* A recorded, spec-cited DEVIATION: chimera contradicts a mandate.  Loud and
 * counted, but does NOT fail CI -- it pins a known, documented
 * non-conformance so the probe stays a regression anchor.  Every use has a
 * DEVIATIONS-SMB.md entry. */
#define DEVIATION(id, ...)                                       \
        do {                                                     \
            printf("DEVIATION %s - ", id); ndev++;               \
            printf(__VA_ARGS__);                                 \
            printf("\n");                                        \
        } while (0)

/* ---- small helpers ------------------------------------------------------ */

static void
fill_guid(
    uint8_t guid[16],
    int     tag)
{
    memset(guid, 0, 16);
    guid[0]  = (uint8_t) tag;
    guid[1]  = 0xD0;
    guid[2]  = 0x0D;
    guid[15] = (uint8_t) ~tag;
} /* fill_guid */

/* An RqLs lease request with a distinct key per (file, tag). */
static void
mk_lease(
    struct smb2_oplock_req *r,
    int                     key_tag,
    uint32_t                state)
{
    memset(r, 0, sizeof(*r));
    r->is_lease     = 1;
    r->lease_key[0] = (uint8_t) key_tag;
    r->lease_key[1] = 0x5E;
    r->lease_state  = state;
    r->lease_epoch  = 1;
} /* mk_lease */

static void
mk_oplock(
    struct smb2_oplock_req *r,
    uint8_t                 level)
{
    memset(r, 0, sizeof(*r));
    r->level = level;
} /* mk_oplock */

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

/* Block until the server has finished PARKING the durable handle `file_id`.
 *
 * This is a real barrier, not a sleep, and the suite needs one: a
 * client-initiated disconnect returns as soon as the DISCONNECTED notification
 * reaches the CLIENT bind, but the server's teardown -- which is what parks the
 * open (chimera_smb_durable_conn_disconnecting) -- runs on the dropped
 * connection's own server thread, and smb2_quiesce() cannot order against a
 * thread that has no connection left to echo on.  Any third party that acts on
 * the parked handle before that teardown lands is racing it: measured, this
 * turned D11 into a 1-in-N flake where the conflicting opener arrived first and
 * took a file the parked persistent holder should have kept.
 *
 * The barrier is a DH2C reconnect carrying a deliberately WRONG CreateGuid.
 * chimera_smb_durable_claim answers a not-yet-parked entry with *r_retry, and
 * chimera_smb_durable_reconnect then retries on a timer until the entry parks
 * -- so the reply cannot come back before the park has happened.  Once parked,
 * the CreateGuid mismatch answers OBJECT_NAME_NOT_FOUND and the entry is left
 * exactly as it was: a refused reconnect consumes nothing (D4j).
 */
static void
wait_parked(
    struct smb2_conn *c,
    const uint8_t     file_id[16])
{
    struct smb2_durable_req dur;
    struct smb2_create_out  r;
    uint32_t                st;

    memset(&dur, 0, sizeof(dur));
    dur.dh2c = 1;
    memcpy(dur.file_id, file_id, 16);
    memset(dur.create_guid, 0xFE, 16);   /* cannot match any real create */
    st = smb2_create_dur(c, "", FILE_OPEN, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                         NULL, &dur, &r);
    EXPECT(st == ST_OBJECT_NAME_NOT_FOUND,
           "park barrier: a wrong-CreateGuid DH2C is refused (0x%08x)", st);
} /* wait_parked */

/* ------------------------------------------------------------------------
 * D1 -- under WHICH caching state is a durable handle actually granted?
 *
 * MS-SMB2 3.3.5.9.6 (DHnQ, durable v1) and 3.3.5.9.10 (DH2Q, durable v2) both
 * gate the grant on the open ending up with a BATCH oplock or a lease carrying
 * SMB2_LEASE_HANDLE_CACHING: without one of those the server MUST NOT grant a
 * durable open (a disconnected handle with no handle-caching guarantee would
 * hold the file against openers that were never told).  Whether it grants at
 * all when the precondition IS met is the server's choice -- recorded as
 * policy, and asserted so the model can predict the reply exactly.
 *
 * Presence of the response context IS the grant signal: chimera emits DHnQ /
 * DH2Q only when it granted (build_dhnq_response / build_dh2q_response return
 * -1 otherwise), and the DHnQ response body is 8 zero bytes.
 * ------------------------------------------------------------------------ */
static void
sec_d1(
    struct smb2_env  *env,
    struct smb2_conn *a)
{
    struct {
        const char *label;
        int         is_lease;
        uint8_t     level;
        uint32_t    lease_state;
        int         v2;         /* 1 = DH2Q, 0 = DHnQ */
        int         exp_grant;
        const char *why;
    } rows[] = {
        { "no oplock          + DHnQ", 0, SMB2_OPLOCK_LEVEL_NONE,      0,                                         0,
          0,
          "no caching at all" },
        { "no oplock          + DH2Q", 0, SMB2_OPLOCK_LEVEL_NONE,      0,                                         1,
          0,
          "no caching at all" },
        { "LEVEL_II           + DHnQ", 0, SMB2_OPLOCK_LEVEL_II,        0,                                         0,
          0,
          "a read cache carries no handle guarantee" },
        { "LEVEL_II           + DH2Q", 0, SMB2_OPLOCK_LEVEL_II,        0,                                         1,
          0,
          "a read cache carries no handle guarantee" },
        { "EXCLUSIVE          + DHnQ", 0, SMB2_OPLOCK_LEVEL_EXCLUSIVE, 0,                                         0,
          0,
          "EXCLUSIVE caches data, not the handle" },
        { "EXCLUSIVE          + DH2Q", 0, SMB2_OPLOCK_LEVEL_EXCLUSIVE, 0,                                         1,
          0,
          "EXCLUSIVE caches data, not the handle" },
        { "BATCH              + DHnQ", 0, SMB2_OPLOCK_LEVEL_BATCH,     0,                                         0,
          1,
          "BATCH is the v1 precondition" },
        { "BATCH              + DH2Q", 0, SMB2_OPLOCK_LEVEL_BATCH,     0,                                         1,
          1,
          "BATCH is the v2 precondition" },
        { "lease R            + DH2Q", 1, 0,                           SMB2_LEASE_READ,                           1,
          0,
          "no HANDLE caching bit" },
        { "lease RH           + DH2Q", 1, 0,
          SMB2_LEASE_READ | SMB2_LEASE_HANDLE, 1, 1,
          "HANDLE caching is the lease precondition" },
        { "lease RWH          + DH2Q", 1, 0,                           SMB2_LEASE_RWH,                            1,
          1,
          "HANDLE caching is the lease precondition" },
        { "lease RWH          + DHnQ", 1, 0,                           SMB2_LEASE_RWH,                            0,
          1,
          "v1 accepts a HANDLE-caching lease too" },
        { "lease R            + DHnQ", 1, 0,                           SMB2_LEASE_READ,                           0,
          0,
          "no HANDLE caching bit" },
    };
    int n = (int) (sizeof(rows) / sizeof(rows[0]));

    printf("\n# D1 durable grant matrix (MS-SMB2 3.3.5.9.6 / 3.3.5.9.10)\n");

    for (int i = 0; i < n; i++) {
        char                    name[32];
        struct smb2_oplock_req  oreq;
        struct smb2_durable_req dur;
        struct smb2_create_out  o;
        uint32_t                st;
        int                     got;

        /* One file per row, and one lease key per file: a lease key binds to
         * exactly one file per client (MS-SMB2 3.3.5.9.8, C-10), so reusing a
         * key across rows would answer INVALID_PARAMETER instead of measuring
         * the durable grant. */
        snprintf(name, sizeof(name), "d1_%d", i);
        if (rows[i].is_lease) {
            mk_lease(&oreq, 0x10 + i, rows[i].lease_state);
        } else {
            mk_oplock(&oreq, rows[i].level);
        }

        memset(&dur, 0, sizeof(dur));
        if (rows[i].v2) {
            dur.dh2q = 1;
            fill_guid(dur.create_guid, 0x10 + i);
        } else {
            dur.dhnq = 1;
        }

        st = smb2_create_dur(a, name, FILE_OPEN_IF, FILE_ALL_ACCESS,
                             FILE_SHARE_RWD,
                             rows[i].level == SMB2_OPLOCK_LEVEL_NONE &&
                             !rows[i].is_lease ? NULL : &oreq, &dur, &o);
        EXPECT(st == ST_SUCCESS, "D1[%2d] %s: CREATE -> 0x%08x", i,
               rows[i].label, st);
        if (st != ST_SUCCESS) {
            continue;
        }

        got = rows[i].v2 ? o.has_dh2q : o.has_dhnq;
        NOTE("D1[%2d] %s: granted caching opl 0x%02x lease %s -> durable %s",
             i, rows[i].label, o.oplock,
             o.has_lease ? lease_str(o.lease_state) : "-",
             got ? "GRANTED" : "refused");

        if (rows[i].exp_grant) {
            /* Policy (MS-SMB2 3.3.5.9.10 "the server MAY"): chimera grants
             * whenever the precondition holds.  Pinned so the model can
             * predict the exact reply. */
            EXPECT(got, "D1[%2d] %s: durable GRANTED (policy: %s)", i,
                   rows[i].label, rows[i].why);
        } else {
            /* Mandate: no batch oplock and no HANDLE-caching lease means no
             * durable open (MS-SMB2 3.3.5.9.6 / 3.3.5.9.10). */
            EXPECT(!got, "D1[%2d] %s: durable REFUSED (%s)", i, rows[i].label,
                   rows[i].why);
        }
        smb2_close(a, o.file_id);
    }
    smb2_quiesce(env);
} /* sec_d1 */

/* ------------------------------------------------------------------------
 * D2 -- the granted Timeout.
 *
 * MS-SMB2 2.2.14.2.12: the DH2Q response carries the Timeout the server
 * granted, which need not be the one asked for.  chimera's policy
 * (smb_proc_create.c chimera_smb_create_grant_durable): 0 selects the default
 * (CHIMERA_SMB_DURABLE_TIMEOUT_DEFAULT_MS), an in-range value is honored, and
 * an over-maximum value is CLAMPED to CHIMERA_SMB_DURABLE_TIMEOUT_MAX_MS
 * rather than refused.  Discretion, pinned because the model predicts the
 * reply.
 * ------------------------------------------------------------------------ */
static void
sec_d2(
    struct smb2_env  *env,
    struct smb2_conn *a)
{
    /* *INDENT-OFF* */ /* uncrustify oscillates on aligned struct-init tables */
    struct {
        const char *label;
        uint32_t    ask;
        uint32_t    exp;
    } rows[] = {
        { "Timeout 0 -> server default",      0,      SMB2C_DURABLE_TIMEOUT_DEFAULT_MS },
        { "Timeout 5000 -> honored",          5000,   5000                             },
        { "Timeout 400000 -> clamped to max", 400000, SMB2C_DURABLE_TIMEOUT_MAX_MS     },
    };
    /* *INDENT-ON* */
    int n = (int) (sizeof(rows) / sizeof(rows[0]));

    printf("\n# D2 granted durable Timeout (MS-SMB2 2.2.14.2.12)\n");

    for (int i = 0; i < n; i++) {
        char                    name[32];
        struct smb2_oplock_req  oreq;
        struct smb2_durable_req dur;
        struct smb2_create_out  o;
        uint32_t                st;

        snprintf(name, sizeof(name), "d2_%d", i);
        mk_oplock(&oreq, SMB2_OPLOCK_LEVEL_BATCH);
        memset(&dur, 0, sizeof(dur));
        dur.dh2q       = 1;
        dur.timeout_ms = rows[i].ask;
        fill_guid(dur.create_guid, 0x30 + i);

        st = smb2_create_dur(a, name, FILE_OPEN_IF, FILE_ALL_ACCESS,
                             FILE_SHARE_RWD, &oreq, &dur, &o);
        EXPECT(st == ST_SUCCESS && o.has_dh2q,
               "D2[%d] %s: CREATE -> 0x%08x dh2q %d", i, rows[i].label, st,
               o.has_dh2q);
        if (st != ST_SUCCESS || !o.has_dh2q) {
            continue;
        }
        NOTE("D2[%d] asked %u ms -> granted %u ms, flags 0x%08x", i,
             rows[i].ask, o.dh2q_timeout, o.dh2q_flags);
        EXPECT(o.dh2q_timeout == rows[i].exp,
               "D2[%d] %s: granted %u (policy %u)", i, rows[i].label,
               o.dh2q_timeout, rows[i].exp);
        /* No CA share here, so nothing may come back persistent. */
        EXPECT((o.dh2q_flags & SMB2_DHANDLE_FLAG_PERSISTENT) == 0,
               "D2[%d] not persistent on a non-CA share (MS-SMB2 3.3.5.9.10)",
               i);
        smb2_close(a, o.file_id);
    }
    smb2_quiesce(env);
} /* sec_d2 */

/* ------------------------------------------------------------------------
 * D3 -- park on disconnect, reclaim on reconnect.
 *
 * MS-SMB2 3.3.7.1: when a connection drops, an open whose IsDurable is TRUE is
 * NOT closed -- it is preserved until its durable timeout expires.  3.3.5.9.12:
 * a CREATE carrying DH2C on a new connection reclaims it, and the reply is an
 * OPEN of the same file (CreateAction = OPENED) with the same FileId.  The
 * reclaim reply carries the lease context but no DH2Q (nothing new is being
 * granted).
 *
 * Proving it is the SAME open, not a fresh one that happens to name the same
 * path, needs three independent facts: the FileId is byte-identical, data
 * written before the drop reads back, and the lease survives at its epoch.
 * ------------------------------------------------------------------------ */
static struct smb2_conn *
sec_d3(
    struct smb2_env  *env,
    struct smb2_conn *a)
{
    struct smb2_oplock_req  lreq;
    struct smb2_durable_req dur;
    struct smb2_create_out  o, rec;
    struct smb2_conn       *b;
    uint8_t                 buf[32];
    uint32_t                st, n;

    printf("\n# D3 park on disconnect, DH2C reclaim (MS-SMB2 3.3.7.1,"
           " 3.3.5.9.12)\n");

    mk_lease(&lreq, 0x41, SMB2_LEASE_RWH);
    memset(&dur, 0, sizeof(dur));
    dur.dh2q = 1;
    fill_guid(dur.create_guid, 0x41);

    st = smb2_create_dur(a, "d3", FILE_OPEN_IF, FILE_ALL_ACCESS,
                         FILE_SHARE_RWD, &lreq, &dur, &o);
    EXPECT(st == ST_SUCCESS && o.has_dh2q && o.has_lease,
           "D3 durable leased open: st 0x%08x dh2q %d lease %s", st,
           o.has_dh2q, o.has_lease ? lease_str(o.lease_state) : "-");
    if (st != ST_SUCCESS || !o.has_dh2q) {
        return NULL;
    }

    st = smb2_write(a, o.file_id, 0, "DURABLE-PAYLOAD", 15, &n);
    EXPECT(st == ST_SUCCESS && n == 15, "D3 write through the durable handle");

    /* The transport drop.  smb2_conn_disconnect returns only once the
     * DISCONNECTED notification has actually been delivered, so "the
     * connection is gone" is an event, not a timeout. */
    smb2_conn_disconnect(a);
    EXPECT(a->disconnected && a->closed_by_client,
           "D3 the client-initiated drop was delivered");

    b = smb2_conn_reopen(env, a);
    EXPECT(b->guid_tag == a->guid_tag,
           "D3 the reconnect presents the SAME ClientGuid");
    smb2_quiesce(env);

    memset(&dur, 0, sizeof(dur));
    dur.dh2c = 1;
    memcpy(dur.file_id, o.file_id, 16);
    fill_guid(dur.create_guid, 0x41);
    /* An empty Name is the ordinary v2 reconnect form; the surviving open
     * holds a lease, so the same lease key must be presented (3.3.5.9.7). */
    st = smb2_create_dur(b, "", FILE_OPEN, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                         &lreq, &dur, &rec);
    EXPECT(st == ST_SUCCESS, "D3 DH2C reclaim -> 0x%08x", st);
    if (st != ST_SUCCESS) {
        return b;
    }
    EXPECT(rec.action == FILE_ACT_OPENED,
           "D3 the reclaim reports CreateAction = OPENED (%u)", rec.action);
    EXPECT(memcmp(rec.file_id, o.file_id, 16) == 0,
           "D3 the reclaimed handle carries the ORIGINAL FileId");
    EXPECT(rec.has_lease && rec.lease_state == o.lease_state,
           "D3 the lease survives the drop: %s (was %s)",
           rec.has_lease ? lease_str(rec.lease_state) : "-",
           lease_str(o.lease_state));
    EXPECT(rec.lease_epoch == o.lease_epoch,
           "D3 the lease epoch is unchanged across the reclaim (%u vs %u)",
           rec.lease_epoch, o.lease_epoch);
    /* MS-SMB2 3.3.5.9.12: a reconnect grants nothing new, so no DH2Q. */
    EXPECT(!rec.has_dh2q,
           "D3 the reclaim reply carries NO DH2Q response context");

    memset(buf, 0, sizeof(buf));
    st = smb2_read(b, rec.file_id, 0, 15, buf, &n);
    EXPECT(st == ST_SUCCESS && n == 15 &&
           memcmp(buf, "DURABLE-PAYLOAD", 15) == 0,
           "D3 data written before the drop reads back through the reclaim"
           " (st 0x%08x n %u '%.*s')", st, n, (int) n, buf);

    /* D3b -- a reclaimed handle is durable AGAIN.  MS-SMB2 3.3.5.9.12 re-homes
     * the surviving open rather than creating a new one, so its IsDurable and
     * its identity are unchanged and a SECOND drop must park it again.  The
     * model has to know this: a trace may disconnect the same handle twice, and
     * a model that forgot the durability on the first reclaim would predict the
     * handle dying on the second drop. */
    {
        struct smb2_conn      *c2;
        struct smb2_create_out rec2;

        smb2_conn_disconnect(b);
        c2 = smb2_conn_reopen(env, b);
        smb2_quiesce(env);
        memset(&dur, 0, sizeof(dur));
        dur.dh2c = 1;
        memcpy(dur.file_id, o.file_id, 16);
        fill_guid(dur.create_guid, 0x41);
        st = smb2_create_dur(c2, "", FILE_OPEN, FILE_ALL_ACCESS,
                             FILE_SHARE_RWD, &lreq, &dur, &rec2);
        EXPECT(st == ST_SUCCESS &&
               memcmp(rec2.file_id, o.file_id, 16) == 0,
               "D3b a reclaimed handle parks and reclaims a SECOND time"
               " (0x%08x)", st);
        b = c2;
    }

    return b;
} /* sec_d3 */

/* ------------------------------------------------------------------------
 * D4 -- every way a reconnect can fail.
 *
 * MS-SMB2 3.3.5.9.7 / 3.3.5.9.12 enumerate the checks a reclaim must pass.
 * Each row below parks a handle, presents ONE malformed reconnect, and
 * asserts the mandated status -- the handle must survive a refused reclaim,
 * which the final correct reclaim proves.
 * ------------------------------------------------------------------------ */
static void
sec_d4(
    struct smb2_env  *env,
    struct smb2_conn *seed)
{
    struct smb2_oplock_req  lreq, wrongkey;
    struct smb2_durable_req dur;
    struct smb2_create_out  o, r;
    struct smb2_conn       *b, *stranger;
    uint32_t                st;

    printf("\n# D4 reclaim denial matrix (MS-SMB2 3.3.5.9.7 / 3.3.5.9.12)\n");

    mk_lease(&lreq, 0x51, SMB2_LEASE_RWH);
    memset(&dur, 0, sizeof(dur));
    dur.dh2q = 1;
    fill_guid(dur.create_guid, 0x51);
    st = smb2_create_dur(seed, "d4", FILE_OPEN_IF, FILE_ALL_ACCESS,
                         FILE_SHARE_RWD, &lreq, &dur, &o);
    EXPECT(st == ST_SUCCESS && o.has_dh2q, "D4 durable leased open");
    if (st != ST_SUCCESS || !o.has_dh2q) {
        return;
    }
    smb2_conn_disconnect(seed);
    b = smb2_conn_reopen(env, seed);
    smb2_quiesce(env);

    /* (a) wrong CreateGuid: the v2 identity is (persistent id, CreateGuid). */
    memset(&dur, 0, sizeof(dur));
    dur.dh2c = 1;
    memcpy(dur.file_id, o.file_id, 16);
    fill_guid(dur.create_guid, 0x99);
    st = smb2_create_dur(b, "", FILE_OPEN, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                         &lreq, &dur, &r);
    EXPECT(st == ST_OBJECT_NAME_NOT_FOUND,
           "D4a wrong CreateGuid -> OBJECT_NAME_NOT_FOUND (0x%08x)", st);

    /* (b) the surviving open holds a lease and the reconnect omits RqLs. */
    memset(&dur, 0, sizeof(dur));
    dur.dh2c = 1;
    memcpy(dur.file_id, o.file_id, 16);
    fill_guid(dur.create_guid, 0x51);
    st = smb2_create_dur(b, "", FILE_OPEN, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                         NULL, &dur, &r);
    EXPECT(st == ST_OBJECT_NAME_NOT_FOUND,
           "D4b leased handle, reconnect without a lease context ->"
           " OBJECT_NAME_NOT_FOUND (0x%08x)", st);

    /* (c) lease context present but naming a different lease key. */
    mk_lease(&wrongkey, 0x52, SMB2_LEASE_RWH);
    st = smb2_create_dur(b, "", FILE_OPEN, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                         &wrongkey, &dur, &r);
    EXPECT(st == ST_OBJECT_NAME_NOT_FOUND,
           "D4c wrong lease key -> OBJECT_NAME_NOT_FOUND (0x%08x)", st);

    /* (d) a leased reconnect naming a DIFFERENT, non-empty file name. */
    st = smb2_create_dur(b, "d4_other", FILE_OPEN, FILE_ALL_ACCESS,
                         FILE_SHARE_RWD, &lreq, &dur, &r);
    EXPECT(st == ST_INVALID_PARAMETER,
           "D4d leased reconnect naming another file -> INVALID_PARAMETER"
           " (0x%08x)", st);

    /* (e) DH2C combined with DH2Q: 3.3.5.9.12 forbids the combination. */
    memset(&dur, 0, sizeof(dur));
    dur.dh2c = 1;
    dur.dh2q = 1;
    memcpy(dur.file_id, o.file_id, 16);
    fill_guid(dur.create_guid, 0x51);
    st = smb2_create_dur(b, "", FILE_OPEN, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                         &lreq, &dur, &r);
    EXPECT(st == ST_INVALID_PARAMETER,
           "D4e DH2C + DH2Q -> INVALID_PARAMETER (0x%08x)", st);

    /* (f) DH2C combined with DHnC: two reconnect contexts at once. */
    memset(&dur, 0, sizeof(dur));
    dur.dh2c = 1;
    dur.dhnc = 1;
    memcpy(dur.file_id, o.file_id, 16);
    fill_guid(dur.create_guid, 0x51);
    st = smb2_create_dur(b, "", FILE_OPEN, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                         &lreq, &dur, &r);
    EXPECT(st == ST_INVALID_PARAMETER,
           "D4f DH2C + DHnC -> INVALID_PARAMETER (0x%08x)", st);

    /* (g) reconnect asking for PERSISTENT against a non-persistent open. */
    memset(&dur, 0, sizeof(dur));
    dur.dh2c            = 1;
    dur.reconnect_flags = SMB2_DHANDLE_FLAG_PERSISTENT;
    memcpy(dur.file_id, o.file_id, 16);
    fill_guid(dur.create_guid, 0x51);
    st = smb2_create_dur(b, "", FILE_OPEN, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                         &lreq, &dur, &r);
    EXPECT(st == ST_INVALID_PARAMETER,
           "D4g DH2C with FLAG_PERSISTENT against a durable-only open ->"
           " INVALID_PARAMETER (0x%08x)", st);

    /* (h) an id that names nothing. */
    memset(&dur, 0, sizeof(dur));
    dur.dh2c = 1;
    memset(dur.file_id, 0xEE, 16);
    fill_guid(dur.create_guid, 0x51);
    st = smb2_create_dur(b, "", FILE_OPEN, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                         &lreq, &dur, &r);
    EXPECT(st == ST_OBJECT_NAME_NOT_FOUND,
           "D4h unknown persistent id -> OBJECT_NAME_NOT_FOUND (0x%08x)", st);

    /* (i) a DIFFERENT client (fresh ClientGuid) reclaiming a LEASED handle.
     * 3.3.5.9.7 binds the ClientGuid check to leased opens. */
    stranger = smb2_conn_reopen_raw(env, NULL);
    smb2_handshake(stranger);
    EXPECT(stranger->guid_tag != b->guid_tag,
           "D4i the stranger presents a different ClientGuid");
    memset(&dur, 0, sizeof(dur));
    dur.dh2c = 1;
    memcpy(dur.file_id, o.file_id, 16);
    fill_guid(dur.create_guid, 0x51);
    st = smb2_create_dur(stranger, "", FILE_OPEN, FILE_ALL_ACCESS,
                         FILE_SHARE_RWD, &lreq, &dur, &r);
    EXPECT(st == ST_OBJECT_NAME_NOT_FOUND,
           "D4i cross-client reclaim of a leased handle ->"
           " OBJECT_NAME_NOT_FOUND (0x%08x)", st);

    /* (j) after all of that the handle must still be reclaimable: a refused
     * reconnect must not consume it. */
    st = smb2_create_dur(b, "", FILE_OPEN, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                         &lreq, &dur, &r);
    EXPECT(st == ST_SUCCESS &&
           memcmp(r.file_id, o.file_id, 16) == 0,
           "D4j the handle survived nine refused reconnects and reclaims"
           " (0x%08x)", st);

    /* (k) a reconnect naming a handle that is now LIVE again.  A live handle
     * is never stolen -- but the refusal is not immediate: chimera cannot tell
     * this case apart from a reclaim that raced its own disconnect, so it
     * announces an ASYNC INTERIM and retries on a timer until its budget
     * lapses (chimera_smb_durable_reconnect_retry_cb, ~3 s).  Both halves are
     * asserted: the status AND the interim.  This is the one probe row that
     * deliberately costs seconds, and it is why the generated corpus excludes
     * the shape (smb2.qnt's unknownFids). */
    if (st == ST_SUCCESS) {
        int i0 = b->ninterim;

        st = smb2_create_dur(b, "", FILE_OPEN, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                             &lreq, &dur, &r);
        EXPECT(st == ST_OBJECT_NAME_NOT_FOUND,
               "D4k a reconnect naming a LIVE handle is refused, never stolen"
               " (0x%08x)", st);
        EXPECT(b->ninterim > i0,
               "D4k ... and the refusal is announced with an async interim"
               " first (%d)", b->ninterim - i0);
        smb2_close(b, r.file_id);
    }
    smb2_quiesce(env);
} /* sec_d4 */

/* ------------------------------------------------------------------------
 * D5 -- durable v1: DHnQ grant under a BATCH oplock, DHnC reclaim.
 *
 * MS-SMB2 3.3.5.9.7: a v1 reconnect is keyed on the persistent id alone -- it
 * carries no CreateGuid, and a non-leased handle ignores the name entirely.
 * ------------------------------------------------------------------------ */
static void
sec_d5(struct smb2_env *env)
{
    struct smb2_oplock_req  breq;
    struct smb2_durable_req dur;
    struct smb2_create_out  o, r;
    struct smb2_conn       *a, *b;
    uint8_t                 buf[32];
    uint32_t                st, n;

    printf("\n# D5 durable v1 DHnQ / DHnC (MS-SMB2 3.3.5.9.6, 3.3.5.9.7)\n");

    a = smb2_conn_open(env);
    smb2_handshake(a);

    mk_oplock(&breq, SMB2_OPLOCK_LEVEL_BATCH);
    memset(&dur, 0, sizeof(dur));
    dur.dhnq = 1;
    st       = smb2_create_dur(a, "d5", FILE_OPEN_IF, FILE_ALL_ACCESS,
                               FILE_SHARE_RWD, &breq, &dur, &o);
    EXPECT(st == ST_SUCCESS && o.has_dhnq,
           "D5 BATCH + DHnQ grants durable v1 (st 0x%08x dhnq %d opl 0x%02x)",
           st, o.has_dhnq, o.oplock);
    if (st != ST_SUCCESS || !o.has_dhnq) {
        return;
    }
    st = smb2_write(a, o.file_id, 0, "V1-PAYLOAD", 10, &n);
    EXPECT(st == ST_SUCCESS, "D5 write through the v1 durable handle");

    smb2_conn_disconnect(a);
    b = smb2_conn_reopen(env, a);
    smb2_quiesce(env);

    memset(&dur, 0, sizeof(dur));
    dur.dhnc = 1;
    memcpy(dur.file_id, o.file_id, 16);
    st = smb2_create_dur(b, "", FILE_OPEN, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                         NULL, &dur, &r);
    EXPECT(st == ST_SUCCESS, "D5 DHnC reclaim -> 0x%08x", st);
    if (st != ST_SUCCESS) {
        return;
    }
    EXPECT(memcmp(r.file_id, o.file_id, 16) == 0,
           "D5 the v1 reclaim carries the ORIGINAL FileId");
    EXPECT(r.action == FILE_ACT_OPENED, "D5 CreateAction = OPENED (%u)", r.action);
    memset(buf, 0, sizeof(buf));
    st = smb2_read(b, r.file_id, 0, 10, buf, &n);
    EXPECT(st == ST_SUCCESS && n == 10 && memcmp(buf, "V1-PAYLOAD", 10) == 0,
           "D5 pre-drop data reads back through the v1 reclaim");

    /* D5b -- does a v1-reclaimed handle park AGAIN?  D3b answered yes for a v2
     * (DH2C) reclaim; the model needs the v1 answer too, and a generated trace
     * disagreed with the model here, which is why this is measured rather than
     * assumed. */
    {
        struct smb2_conn      *c2;
        struct smb2_create_out r2;

        smb2_conn_disconnect(b);
        c2 = smb2_conn_reopen(env, b);
        smb2_quiesce(env);
        memset(&dur, 0, sizeof(dur));
        dur.dhnc = 1;
        memcpy(dur.file_id, o.file_id, 16);
        st = smb2_create_dur(c2, "", FILE_OPEN, FILE_ALL_ACCESS,
                             FILE_SHARE_RWD, NULL, &dur, &r2);
        NOTE("D5b second DHnC reclaim after a second drop -> 0x%08x", st);
        EXPECT(st == ST_SUCCESS &&
               memcmp(r2.file_id, o.file_id, 16) == 0,
               "D5b a v1-reclaimed handle parks and reclaims a SECOND time"
               " (0x%08x)", st);
        if (st == ST_SUCCESS) {
            b = c2;
            r = r2;
        } else {
            smb2_quiesce(env);
            return;
        }
    }

    /* D5c -- DHnC + DHnQ on one CREATE is explicitly legal, unlike DH2C +
     * anything (3.3.5.9.7 vs 3.3.5.9.12).  What the reply then carries is the
     * question the model has to answer: is the v1 REQUEST ignored outright, or
     * does the reconnect report a durable grant back? */
    {
        struct smb2_conn      *c3;
        struct smb2_create_out r3;

        smb2_conn_disconnect(b);
        c3 = smb2_conn_reopen(env, b);
        smb2_quiesce(env);
        memset(&dur, 0, sizeof(dur));
        dur.dhnc = 1;
        dur.dhnq = 1;
        memcpy(dur.file_id, o.file_id, 16);
        st = smb2_create_dur(c3, "", FILE_OPEN, FILE_ALL_ACCESS,
                             FILE_SHARE_RWD, NULL, &dur, &r3);
        NOTE("D5c DHnC + DHnQ reconnect -> 0x%08x dhnq_response %d", st,
             r3.has_dhnq);
        EXPECT(st == ST_SUCCESS,
               "D5c DHnC + DHnQ is a legal reconnect (0x%08x)", st);
        if (st == ST_SUCCESS) {
            smb2_close(c3, r3.file_id);
        }
    }
    smb2_quiesce(env);
} /* sec_d5 */

/* ------------------------------------------------------------------------
 * D6 -- an open with NO durable request does not survive its connection.
 *
 * MS-SMB2 3.3.7.1: on a transport drop the server closes every open of the
 * connection whose IsDurable/IsResilient/IsPersistent is FALSE.  Measured
 * positively -- a conflicting exclusive open of the same file must succeed
 * afterwards, which it cannot while the original handle lives.
 * ------------------------------------------------------------------------ */
static void
sec_d6(struct smb2_env *env)
{
    struct smb2_create_out o, r;
    struct smb2_conn      *a, *b;
    uint32_t               st;

    printf("\n# D6 a non-durable open dies with its connection"
           " (MS-SMB2 3.3.7.1)\n");

    a = smb2_conn_open(env);
    smb2_handshake(a);
    /* ShareAccess NONE: while this handle lives, nobody else may open it. */
    st = smb2_create(a, "d6", FILE_OPEN_IF, FILE_ALL_ACCESS, 0, NULL, &o);
    EXPECT(st == ST_SUCCESS, "D6 exclusive-share open (st 0x%08x)", st);

    b = smb2_conn_open(env);
    smb2_handshake(b);
    st = smb2_create(b, "d6", FILE_OPEN, FILE_ALL_ACCESS, 0, NULL, &r);
    EXPECT(st == ST_SHARING_VIOLATION,
           "D6 a second opener is refused while the handle lives (0x%08x)",
           st);

    smb2_conn_disconnect(a);
    smb2_quiesce(env);

    st = smb2_create(b, "d6", FILE_OPEN, FILE_ALL_ACCESS, 0, NULL, &r);
    EXPECT(st == ST_SUCCESS,
           "D6 the same open succeeds once the connection is gone (0x%08x)",
           st);
    if (st == ST_SUCCESS) {
        smb2_close(b, r.file_id);
    }
    smb2_quiesce(env);
} /* sec_d6 */

/* ------------------------------------------------------------------------
 * D7 -- a conflicting open against a PARKED durable-only holder.
 *
 * MS-SMB2 3.3.4.6 / 3.3.4.7: a durable open that is disconnected and whose
 * batch oplock / write-caching lease must break has nobody to break it -- the
 * server closes it and admits the opener.  chimera implements exactly that
 * (chimera_smb_durable_purge_parked, the YIELDED flag), and the loser's later
 * reclaim must then FAIL: the handle is gone, not merely busy.
 * ------------------------------------------------------------------------ */
static void
sec_d7(struct smb2_env *env)
{
    struct smb2_oplock_req  breq;
    struct smb2_durable_req dur;
    struct smb2_create_out  o, r;
    struct smb2_conn       *a, *b, *c;
    uint32_t                st;

    printf("\n# D7 conflicting open vs a parked durable-only holder"
           " (MS-SMB2 3.3.4.6)\n");

    a = smb2_conn_open(env);
    smb2_handshake(a);
    mk_oplock(&breq, SMB2_OPLOCK_LEVEL_BATCH);
    memset(&dur, 0, sizeof(dur));
    dur.dh2q = 1;
    fill_guid(dur.create_guid, 0x71);
    st = smb2_create_dur(a, "d7", FILE_OPEN_IF, FILE_ALL_ACCESS, 0, &breq,
                         &dur, &o);
    EXPECT(st == ST_SUCCESS && o.has_dh2q && o.oplock ==
           SMB2_OPLOCK_LEVEL_BATCH,
           "D7 durable BATCH holder with ShareAccess NONE (st 0x%08x opl"
           " 0x%02x dh2q %d)", st, o.oplock, o.has_dh2q);
    if (st != ST_SUCCESS || !o.has_dh2q) {
        return;
    }

    smb2_conn_disconnect(a);
    smb2_quiesce(env);

    /* Serialize behind the park before letting a third party touch the file. */
    c = smb2_conn_reopen(env, a);
    wait_parked(c, o.file_id);

    b = smb2_conn_open(env);
    smb2_handshake(b);
    {
        int i0 = b->ninterim;

        st = smb2_create(b, "d7", FILE_OPEN, FILE_ALL_ACCESS, 0, NULL, &r);
        /* Does dissolving a parked holder cost an async interim?  chimera
         * purges the parked holder and RETRIES the share check on a timer
         * (smb_proc_create.c gen_share_retry), and a retried create announces
         * itself first -- which the model has to predict, because `parked` is
         * asserted field-for-field by the replayer. */
        NOTE("D7 conflicting open against the parked durable holder -> 0x%08x"
             " (interims %d)", st, b->ninterim - i0);
    }
    EXPECT(st == ST_SUCCESS,
           "D7 a durable-only parked holder YIELDS to a conflicting open"
           " (MS-SMB2 3.3.4.6; 0x%08x)", st);

    /* The original client comes back: its handle was yielded, so the reclaim
     * must fail rather than resurrect a handle the server already gave away. */
    smb2_quiesce(env);
    memset(&dur, 0, sizeof(dur));
    dur.dh2c = 1;
    memcpy(dur.file_id, o.file_id, 16);
    fill_guid(dur.create_guid, 0x71);
    {
        struct smb2_create_out r2;
        uint32_t               st2 =
            smb2_create_dur(c, "", FILE_OPEN, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                            NULL, &dur, &r2);
        EXPECT(st2 == ST_OBJECT_NAME_NOT_FOUND,
               "D7 the yielded handle cannot be reclaimed (0x%08x)", st2);
    }
    if (st == ST_SUCCESS) {
        smb2_close(b, r.file_id);
    }
    smb2_quiesce(env);

    /* D7b -- the converse, and the one the MODEL turns on: an open that breaks
     * nobody must not purge a parked holder either.  An attribute-only,
     * non-truncating open takes no part in share arbitration and triggers no
     * oplock break (MS-FSA 2.1.5.1; smb_proc_create.c break_for_open's
     * break_trigger is exactly its inverse), so the parked handle must still be
     * reclaimable afterwards.  Without this measurement the model's purge rule
     * would be an inference rather than a fact. */
    {
        struct smb2_oplock_req  breq2;
        struct smb2_durable_req dur2;
        struct smb2_create_out  o2, r2;
        struct smb2_conn       *h, *peer, *back;
        uint32_t                st2;

        h = smb2_conn_open(env);
        smb2_handshake(h);
        mk_oplock(&breq2, SMB2_OPLOCK_LEVEL_BATCH);
        memset(&dur2, 0, sizeof(dur2));
        dur2.dh2q = 1;
        fill_guid(dur2.create_guid, 0x72);
        st2 = smb2_create_dur(h, "d7b", FILE_OPEN_IF, FILE_ALL_ACCESS, 0,
                              &breq2, &dur2, &o2);
        EXPECT(st2 == ST_SUCCESS && o2.has_dh2q,
               "D7b durable BATCH holder (st 0x%08x dh2q %d)", st2,
               o2.has_dh2q);
        smb2_conn_disconnect(h);
        smb2_quiesce(env);
        back = smb2_conn_reopen(env, h);
        wait_parked(back, o2.file_id);

        peer = smb2_conn_open(env);
        smb2_handshake(peer);
        /* FILE_READ_ATTRIBUTES only, FILE_OPEN: the attribute-only open. */
        st2 = smb2_create(peer, "d7b", FILE_OPEN, 0x00000080u, 0, NULL, &r2);
        NOTE("D7b attribute-only open against the parked holder -> 0x%08x",
             st2);
        EXPECT(st2 == ST_SUCCESS,
               "D7b an attribute-only open succeeds (0x%08x)", st2);
        if (st2 == ST_SUCCESS) {
            smb2_close(peer, r2.file_id);
        }
        smb2_quiesce(env);

        smb2_quiesce(env);
        memset(&dur2, 0, sizeof(dur2));
        dur2.dh2c = 1;
        memcpy(dur2.file_id, o2.file_id, 16);
        fill_guid(dur2.create_guid, 0x72);
        st2 = smb2_create_dur(back, "", FILE_OPEN, FILE_ALL_ACCESS,
                              FILE_SHARE_RWD, NULL, &dur2, &r2);
        NOTE("D7b reclaim after an attribute-only open -> 0x%08x", st2);
        EXPECT(st2 == ST_SUCCESS,
               "D7b an open that breaks nobody does NOT purge the parked"
               " handle (0x%08x)", st2);
        if (st2 == ST_SUCCESS) {
            smb2_close(back, r2.file_id);
        }
        smb2_quiesce(env);
    }

    /* D7c -- a COMPATIBLE open against a parked durable LEASE holder.
     *
     * D7 purged the parked holder with an open that share-CONFLICTED with it;
     * D7b left it alone with an open that broke nobody.  The case in between is
     * the one a generated trace tripped over: an open that shares everything
     * (so no sharing violation) but is not attribute-only (so it does trigger a
     * break).  Two things are measured, and the model needs both: what lease
     * state the NEW opener is granted -- i.e. whether the parked holder still
     * counts for the sole-opener rule of MS-FSA 2.1.5.18.1 -- and whether the
     * parked handle survives to be reclaimed. */
    {
        struct smb2_oplock_req  lreq3, lreq3b;
        struct smb2_durable_req dur3;
        struct smb2_create_out  o3, n3, r3;
        struct smb2_conn       *h3, *peer3, *back3;
        uint32_t                st3;

        h3 = smb2_conn_open(env);
        smb2_handshake(h3);
        mk_lease(&lreq3, 0x73, SMB2_LEASE_RWH);
        memset(&dur3, 0, sizeof(dur3));
        dur3.dh2q = 1;
        fill_guid(dur3.create_guid, 0x73);
        st3 = smb2_create_dur(h3, "d7c", FILE_OPEN_IF, FILE_ALL_ACCESS,
                              FILE_SHARE_RWD, &lreq3, &dur3, &o3);
        EXPECT(st3 == ST_SUCCESS && o3.has_dh2q,
               "D7c durable RWH-lease holder (st 0x%08x lease %s dh2q %d)",
               st3, o3.has_lease ? lease_str(o3.lease_state) : "-",
               o3.has_dh2q);
        smb2_conn_disconnect(h3);
        smb2_quiesce(env);
        back3 = smb2_conn_reopen(env, h3);
        wait_parked(back3, o3.file_id);

        peer3 = smb2_conn_open(env);
        smb2_handshake(peer3);
        mk_lease(&lreq3b, 0x74, SMB2_LEASE_RWH);
        st3 = smb2_create_dur(peer3, "d7c", FILE_OPEN, FILE_ALL_ACCESS,
                              FILE_SHARE_RWD, &lreq3b, NULL, &n3);
        NOTE("D7c compatible RWH-lease open behind the parked holder ->"
             " st 0x%08x lease %s", st3,
             n3.has_lease ? lease_str(n3.lease_state) : "-");
        EXPECT(st3 == ST_SUCCESS, "D7c the compatible open succeeds (0x%08x)",
               st3);

        memset(&dur3, 0, sizeof(dur3));
        dur3.dh2c = 1;
        memcpy(dur3.file_id, o3.file_id, 16);
        fill_guid(dur3.create_guid, 0x73);
        st3 = smb2_create_dur(back3, "", FILE_OPEN, FILE_ALL_ACCESS,
                              FILE_SHARE_RWD, &lreq3, &dur3, &r3);
        NOTE("D7c reclaim after a COMPATIBLE open -> 0x%08x", st3);
        if (st3 == ST_SUCCESS) {
            smb2_close(back3, r3.file_id);
        }
        if (n3.status == ST_SUCCESS) {
            smb2_close(peer3, n3.file_id);
        }
        smb2_quiesce(env);
    }

    /* D7d / D7e -- WHICH parked holders a compatible open dissolves.
     *
     * D7c's parked holder held a WRITE cache (an RWH lease), and the opener
     * both got the full RWH lease and left nothing behind to reclaim.  The
     * question the model cannot answer without measuring is whether that is
     * because the open was compatible, or because the parked holder held a
     * write cache that the caching-acquire path had to revoke -- a distinction
     * a generated trace ran straight into, granting RWH where the wire granted
     * RH.  These two rows separate the variables: same compatible opener, one
     * parked holder WITH a write cache (batch oplock) and one WITHOUT (an RH
     * lease). */
    {
        /* *INDENT-OFF* */ /* uncrustify oscillates on aligned init tables */
        struct {
            const char *label;
            const char *name;
            int         holder_is_lease;
            uint32_t    holder_lease;   /* when holder_is_lease */
            int         key;
        } rows[] = {
            { "parked RH lease (no write cache)",  "d7d",  1,
              SMB2_LEASE_READ | SMB2_LEASE_HANDLE, 0x75 },
            { "parked BATCH oplock (write cache)", "d7e",  0, 0, 0x76 },
        };
        /* *INDENT-ON* */

        for (int i = 0; i < 2; i++) {
            struct smb2_oplock_req  hreq, preq;
            struct smb2_durable_req d4;
            struct smb2_create_out  o4, n4, r4;
            struct smb2_conn       *h4, *peer4, *back4;
            uint32_t                st4;

            h4 = smb2_conn_open(env);
            smb2_handshake(h4);
            if (rows[i].holder_is_lease) {
                mk_lease(&hreq, rows[i].key, rows[i].holder_lease);
            } else {
                mk_oplock(&hreq, SMB2_OPLOCK_LEVEL_BATCH);
            }
            memset(&d4, 0, sizeof(d4));
            d4.dh2q = 1;
            fill_guid(d4.create_guid, rows[i].key);
            st4 = smb2_create_dur(h4, rows[i].name, FILE_OPEN_IF,
                                  FILE_ALL_ACCESS, FILE_SHARE_RWD, &hreq, &d4,
                                  &o4);
            EXPECT(st4 == ST_SUCCESS && o4.has_dh2q,
                   "D7%c durable holder, %s (st 0x%08x dh2q %d)",
                   'd' + i, rows[i].label, st4, o4.has_dh2q);
            if (st4 != ST_SUCCESS || !o4.has_dh2q) {
                continue;
            }
            smb2_conn_disconnect(h4);
            smb2_quiesce(env);
            back4 = smb2_conn_reopen(env, h4);
            wait_parked(back4, o4.file_id);

            /* A fully compatible open (shares everything) that is NOT
             * attribute-only, asking for the strongest lease. */
            peer4 = smb2_conn_open(env);
            smb2_handshake(peer4);
            mk_lease(&preq, rows[i].key + 0x40, SMB2_LEASE_RWH);
            {
                int i4 = peer4->ninterim;

                st4 = smb2_create_dur(peer4, rows[i].name, FILE_OPEN,
                                      FILE_ALL_ACCESS, FILE_SHARE_RWD, &preq,
                                      NULL, &n4);
                NOTE("D7%c compatible RWH-lease open behind %s -> st 0x%08x"
                     " granted %s (interims %d)", 'd' + i, rows[i].label, st4,
                     n4.has_lease ? lease_str(n4.lease_state) : "-",
                     peer4->ninterim - i4);
            }

            memset(&d4, 0, sizeof(d4));
            d4.dh2c = 1;
            memcpy(d4.file_id, o4.file_id, 16);
            fill_guid(d4.create_guid, rows[i].key);
            st4 = smb2_create_dur(back4, "", FILE_OPEN, FILE_ALL_ACCESS,
                                  FILE_SHARE_RWD,
                                  rows[i].holder_is_lease ? &hreq : NULL, &d4,
                                  &r4);
            NOTE("D7%c reclaim after that open -> 0x%08x (%s)", 'd' + i, st4,
                 st4 == ST_SUCCESS ? "SURVIVED" : "PURGED");
            if (st4 == ST_SUCCESS) {
                smb2_close(back4, r4.file_id);
            }
            if (n4.status == ST_SUCCESS) {
                smb2_close(peer4, n4.file_id);
            }
            smb2_quiesce(env);
        }
    }

    /* D7f -- a SHARE-CONFLICTING open against a parked holder that has no
     * write cache.  D7 purged such an opener's way through a write-cache
     * holder; D7d showed a non-write-cache holder survives a COMPATIBLE open
     * and caps it.  The remaining corner is the conflicting open against the
     * surviving kind: does the parked holder's share reservation refuse it, or
     * does the share path purge the holder the way it does a write-cache one? */
    {
        struct smb2_oplock_req  hreq;
        struct smb2_durable_req d5;
        struct smb2_create_out  o5, n5, r5;
        struct smb2_conn       *h5, *peer5, *back5;
        uint32_t                st5;

        h5 = smb2_conn_open(env);
        smb2_handshake(h5);
        mk_lease(&hreq, 0x77, SMB2_LEASE_READ | SMB2_LEASE_HANDLE);
        memset(&d5, 0, sizeof(d5));
        d5.dh2q = 1;
        fill_guid(d5.create_guid, 0x77);
        /* ShareAccess NONE: while this handle counts, nobody else may open. */
        st5 = smb2_create_dur(h5, "d7f", FILE_OPEN_IF, FILE_ALL_ACCESS, 0,
                              &hreq, &d5, &o5);
        EXPECT(st5 == ST_SUCCESS && o5.has_dh2q,
               "D7f durable RH-lease holder, ShareAccess NONE (st 0x%08x"
               " lease %s dh2q %d)", st5,
               o5.has_lease ? lease_str(o5.lease_state) : "-", o5.has_dh2q);
        if (st5 == ST_SUCCESS && o5.has_dh2q) {
            smb2_conn_disconnect(h5);
            smb2_quiesce(env);
            back5 = smb2_conn_reopen(env, h5);
            wait_parked(back5, o5.file_id);

            peer5 = smb2_conn_open(env);
            smb2_handshake(peer5);
            {
                int i5 = peer5->ninterim;

                st5 = smb2_create(peer5, "d7f", FILE_OPEN, FILE_ALL_ACCESS, 0,
                                  NULL, &n5);
                NOTE("D7f share-conflicting open behind a parked RH holder ->"
                     " 0x%08x (interims %d)", st5, peer5->ninterim - i5);
            }

            memset(&d5, 0, sizeof(d5));
            d5.dh2c = 1;
            memcpy(d5.file_id, o5.file_id, 16);
            fill_guid(d5.create_guid, 0x77);
            st5 = smb2_create_dur(back5, "", FILE_OPEN, FILE_ALL_ACCESS,
                                  FILE_SHARE_RWD, &hreq, &d5, &r5);
            NOTE("D7f reclaim after that open -> 0x%08x (%s)", st5,
                 st5 == ST_SUCCESS ? "SURVIVED" : "PURGED");
            if (st5 == ST_SUCCESS) {
                smb2_close(back5, r5.file_id);
            }
            if (n5.status == ST_SUCCESS) {
                smb2_close(peer5, n5.file_id);
            }
            smb2_quiesce(env);
        }
    }
} /* sec_d7 */

/* ------------------------------------------------------------------------
 * D8 -- the disconnect-survival deadline is enforced.
 *
 * MS-SMB2 3.3.7.1: the preserved open is discarded once Open.DurableTimeout
 * has elapsed since the disconnect.  This is the ONE assertion in the SMB
 * suite that involves elapsed time, and it is written so that no verdict can
 * turn on a guess: the handle is granted a 1 ms timeout, and the reclaim is
 * only attempted after CLOCK_MONOTONIC has PROVABLY advanced past the
 * deadline by a wide margin.  Waiting longer never changes the answer (the
 * entry is gone for good), so the test cannot flake in either direction --
 * unlike a fixed sleep chosen to be "probably enough".
 * ------------------------------------------------------------------------ */
static void
sec_d8(struct smb2_env *env)
{
    struct smb2_oplock_req  breq;
    struct smb2_durable_req dur;
    struct smb2_create_out  o, r;
    struct smb2_conn       *a, *b;
    uint32_t                st;
    uint64_t                t0;

    printf("\n# D8 durable timeout expiry (MS-SMB2 3.3.7.1)\n");

    a = smb2_conn_open(env);
    smb2_handshake(a);
    mk_oplock(&breq, SMB2_OPLOCK_LEVEL_BATCH);
    memset(&dur, 0, sizeof(dur));
    dur.dh2q       = 1;
    dur.timeout_ms = 1;                 /* the shortest grantable window */
    fill_guid(dur.create_guid, 0x81);
    st = smb2_create_dur(a, "d8", FILE_OPEN_IF, FILE_ALL_ACCESS,
                         FILE_SHARE_RWD, &breq, &dur, &o);
    EXPECT(st == ST_SUCCESS && o.has_dh2q,
           "D8 durable open with a 1 ms timeout (granted %u ms)",
           o.dh2q_timeout);
    if (st != ST_SUCCESS || !o.has_dh2q) {
        return;
    }
    EXPECT(o.dh2q_timeout == 1,
           "D8 a 1 ms request is honored verbatim (%u)", o.dh2q_timeout);

    smb2_conn_disconnect(a);
    t0 = smb2c_now_ms();
    b  = smb2_conn_reopen(env, a);
    /* Drive the server while the (monotonic) clock passes the deadline by
     * two orders of magnitude.  The wait is bounded from BELOW by a proven
     * elapsed interval, which is what makes the expectation sound. */
    while (smb2c_now_ms() - t0 < 200) {
        smb2_quiesce(env);
    }

    memset(&dur, 0, sizeof(dur));
    dur.dh2c = 1;
    memcpy(dur.file_id, o.file_id, 16);
    fill_guid(dur.create_guid, 0x81);
    st = smb2_create_dur(b, "", FILE_OPEN, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                         NULL, &dur, &r);
    EXPECT(st == ST_OBJECT_NAME_NOT_FOUND,
           "D8 a reclaim %llu ms after a 1 ms timeout is refused (0x%08x)",
           (unsigned long long) (smb2c_now_ms() - t0), st);
    smb2_quiesce(env);
} /* sec_d8 */

/* ------------------------------------------------------------------------
 * D9 -- replay (MS-SMB2 3.3.5.9.10, 3.2.4.1.5).
 *
 * A replay on the wire is a FRESH MessageId carrying
 * SMB2_FLAGS_REPLAY_OPERATION, correlated to the original by the DH2Q
 * CreateGuid -- re-sending the original MessageId is a sequence-window
 * violation that costs the connection (DEVIATIONS-SMB.md R-10), so it is not
 * a retry mechanism.  Two facts are pinned here: the eligibility window is
 * exactly one operation wide, and a replay that collides with a still-live
 * open of the same CreateGuid on another connection is
 * STATUS_DUPLICATE_OBJECTID.
 * ------------------------------------------------------------------------ */
static void
sec_d9(struct smb2_env *env)
{
    struct smb2_oplock_req  lreq;
    struct smb2_durable_req dur;
    struct smb2_create_out  o1, o2, o3;
    struct smb2_conn       *a, *b;
    uint32_t                st, n;

    printf("\n# D9 replay: REPLAY_OPERATION + DH2Q CreateGuid"
           " (MS-SMB2 3.3.5.9.10)\n");

    a = smb2_conn_open(env);
    smb2_handshake(a);

    /* (a) replay with nothing in between: the SAME open comes back, and the
     * ORIGINAL CreateAction is echoed. */
    mk_lease(&lreq, 0x91, SMB2_LEASE_RWH);
    memset(&dur, 0, sizeof(dur));
    dur.dh2q = 1;
    fill_guid(dur.create_guid, 0x91);
    st = smb2_create_dur(a, "d9a", FILE_CREATE, FILE_ALL_ACCESS,
                         FILE_SHARE_RWD, &lreq, &dur, &o1);
    EXPECT(st == ST_SUCCESS && o1.action == FILE_ACT_CREATED,
           "D9a original create (st 0x%08x action %u)", st, o1.action);

    smb2c_set_next_flags(a, SMB2_FLAGS_REPLAY_OPERATION);
    st = smb2_create_dur(a, "d9a", FILE_CREATE, FILE_ALL_ACCESS,
                         FILE_SHARE_RWD, &lreq, &dur, &o2);
    EXPECT(st == ST_SUCCESS && memcmp(o1.file_id, o2.file_id, 16) == 0,
           "D9a an immediate replay returns the SAME FileId (0x%08x)", st);
    EXPECT(o2.action == o1.action,
           "D9a the replay echoes the ORIGINAL CreateAction (%u vs %u)",
           o2.action, o1.action);

    /* (b) the eligibility window closes on the first non-replay operation. */
    st = smb2_write(a, o1.file_id, 0, "x", 1, &n);
    EXPECT(st == ST_SUCCESS, "D9b an ordinary WRITE on the handle");
    smb2c_set_next_flags(a, SMB2_FLAGS_REPLAY_OPERATION);
    st = smb2_create_dur(a, "d9a", FILE_OPEN_IF, FILE_ALL_ACCESS,
                         FILE_SHARE_RWD, &lreq, &dur, &o3);
    NOTE("D9b replay after an intervening op -> st 0x%08x same_fid %d", st,
         st == ST_SUCCESS && memcmp(o1.file_id, o3.file_id, 16) == 0);
    EXPECT(st != ST_SUCCESS || memcmp(o1.file_id, o3.file_id, 16) != 0,
           "D9b replay eligibility is ONE operation wide: the replay no"
           " longer resolves to the original handle");
    if (st == ST_SUCCESS) {
        smb2_close(a, o3.file_id);
    }

    /* (c) a replay from ANOTHER connection of the same client while the
     * original is still live: STATUS_DUPLICATE_OBJECTID. */
    b = smb2_conn_reopen_raw(env, a);   /* same ClientGuid, new connection */
    smb2_handshake(b);
    {
        struct smb2_oplock_req  l2;
        struct smb2_durable_req d2;
        struct smb2_create_out  c1, c2;

        mk_lease(&l2, 0x92, SMB2_LEASE_RWH);
        memset(&d2, 0, sizeof(d2));
        d2.dh2q = 1;
        fill_guid(d2.create_guid, 0x92);
        st = smb2_create_dur(a, "d9c", FILE_OPEN_IF, FILE_ALL_ACCESS,
                             FILE_SHARE_RWD, &l2, &d2, &c1);
        EXPECT(st == ST_SUCCESS && c1.has_dh2q,
               "D9c live durable open on connection A (st 0x%08x)", st);
        smb2c_set_next_flags(b, SMB2_FLAGS_REPLAY_OPERATION);
        st = smb2_create_dur(b, "d9c", FILE_OPEN_IF, FILE_ALL_ACCESS,
                             FILE_SHARE_RWD, &l2, &d2, &c2);
        NOTE("D9c cross-connection replay of a LIVE durable open -> 0x%08x",
             st);
        EXPECT(st == ST_DUPLICATE_OBJECTID,
               "D9c a replay colliding with a live open of the same"
               " CreateGuid -> DUPLICATE_OBJECTID (0x%08x)", st);
        smb2_close(a, c1.file_id);
    }
    smb2_close(a, o1.file_id);
    smb2_quiesce(env);
} /* sec_d9 */

/* ------------------------------------------------------------------------
 * D10 -- FSCTL_LMR_REQUEST_RESILIENCY (MS-SMB2 2.2.31.3, 3.3.5.15.9).
 *
 * Resiliency is the pre-SMB3 way to ask for disconnect survival: it is
 * requested AFTER the create, on an already-open handle, and -- unlike a
 * durable request -- has NO caching precondition.  chimera implements it over
 * the same durable registry, so a resilient open parks on a drop and is
 * reclaimed with a DHnC (there is no CreateGuid to key a v2 reconnect on).
 * ------------------------------------------------------------------------ */
static void
sec_d10(struct smb2_env *env)
{
    struct smb2_durable_req dur;
    struct smb2_create_out  o, r;
    struct smb2_conn       *a, *b;
    uint8_t                 buf[32];
    uint8_t                 shortbuf[4];
    uint32_t                st, n;

    printf("\n# D10 FSCTL_LMR_REQUEST_RESILIENCY (MS-SMB2 3.3.5.15.9)\n");

    a = smb2_conn_open(env);
    smb2_handshake(a);

    /* No oplock, no lease, no durable context: resiliency has no caching
     * precondition, which is exactly what distinguishes it from D1. */
    st = smb2_create(a, "d10", FILE_OPEN_IF, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                     NULL, &o);
    EXPECT(st == ST_SUCCESS, "D10 plain open (st 0x%08x)", st);
    if (st != ST_SUCCESS) {
        return;
    }

    /* A NETWORK_RESILIENCY_REQUEST shorter than 8 bytes is malformed. */
    memset(shortbuf, 0, sizeof(shortbuf));
    st = smb2_ioctl(a, SMB2_FSCTL_LMR_REQUEST_RESILIENCY, o.file_id, shortbuf,
                    sizeof(shortbuf));
    EXPECT(st == ST_INVALID_PARAMETER,
           "D10 a 4-byte NETWORK_RESILIENCY_REQUEST -> INVALID_PARAMETER"
           " (0x%08x)", st);

    /* A Timeout above the server maximum MUST be refused, not clamped
     * (MS-SMB2 3.3.5.15.9). */
    st = smb2_resiliency(a, o.file_id, SMB2C_RESILIENCY_MAX_MS + 1);
    EXPECT(st == ST_INVALID_PARAMETER,
           "D10 Timeout above the maximum -> INVALID_PARAMETER (0x%08x)", st);

    /* Timeout 0 selects the server default; the grant reports only SUCCESS. */
    st = smb2_resiliency(a, o.file_id, 0);
    EXPECT(st == ST_SUCCESS, "D10 resiliency granted, Timeout 0 (0x%08x)", st);

    st = smb2_write(a, o.file_id, 0, "RESILIENT!", 10, &n);
    EXPECT(st == ST_SUCCESS, "D10 write through the resilient handle");

    smb2_conn_disconnect(a);
    b = smb2_conn_reopen(env, a);
    smb2_quiesce(env);

    memset(&dur, 0, sizeof(dur));
    dur.dhnc = 1;
    memcpy(dur.file_id, o.file_id, 16);
    st = smb2_create_dur(b, "", FILE_OPEN, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                         NULL, &dur, &r);
    NOTE("D10 DHnC reclaim of a RESILIENT (not durable) handle -> 0x%08x", st);
    EXPECT(st == ST_SUCCESS,
           "D10 a resilient open survives its connection and is reclaimed"
           " (MS-SMB2 3.3.5.15.9; 0x%08x)", st);
    if (st != ST_SUCCESS) {
        smb2_quiesce(env);
        return;
    }
    EXPECT(memcmp(r.file_id, o.file_id, 16) == 0,
           "D10 the resilient reclaim carries the ORIGINAL FileId");
    memset(buf, 0, sizeof(buf));
    st = smb2_read(b, r.file_id, 0, 10, buf, &n);
    EXPECT(st == ST_SUCCESS && n == 10 && memcmp(buf, "RESILIENT!", 10) == 0,
           "D10 pre-drop data reads back through the resilient reclaim");
    smb2_close(b, r.file_id);
    smb2_quiesce(env);
} /* sec_d10 */

/* ------------------------------------------------------------------------
 * D11 -- persistent handles on a continuously-available share.
 *
 * MS-SMB2 3.3.5.9.10: SMB2_DHANDLE_FLAG_PERSISTENT is honored only when the
 * share has SHARE_CAP_CONTINUOUS_AVAILABILITY, and then the grant reports the
 * flag back.  3.3.4.6 / chimera_smb_durable_parked_hold: a PARKED persistent
 * holder does not yield to a conflicting opener the way a durable-only one
 * does (D7) -- it blocks it, because a CA handle is supposed to be recoverable
 * even across a client outage.
 *
 * Runs on its own server instance: continuous availability is a share
 * property fixed at chimera_server_create_share time.
 * ------------------------------------------------------------------------ */
static void
sec_d11(void)
{
    struct smb2_env         env;
    struct smb2_env_opts    opts = { .oplocks                 = 1,
                                     .leases                  = 1,
                                     .directory_leases        = 1,
                                     .persistent_handles      = 1,
                                     .continuous_availability = 1 };
    struct smb2_oplock_req  breq;
    struct smb2_durable_req dur;
    struct smb2_create_out  o, r;
    struct smb2_conn       *a, *b, *cc;
    uint32_t                st;

    printf("\n# D11 persistent handles on a CA share (MS-SMB2 3.3.5.9.10)\n");

    smb2_env_start_opts(&env, &opts);
    a = smb2_conn_open(&env);
    smb2_handshake(a);

    mk_oplock(&breq, SMB2_OPLOCK_LEVEL_BATCH);
    memset(&dur, 0, sizeof(dur));
    dur.dh2q  = 1;
    dur.flags = SMB2_DHANDLE_FLAG_PERSISTENT;
    fill_guid(dur.create_guid, 0xB1);
    st = smb2_create_dur(a, "d11", FILE_OPEN_IF, FILE_ALL_ACCESS,
                         FILE_SHARE_RWD, &breq, &dur, &o);
    EXPECT(st == ST_SUCCESS && o.has_dh2q,
           "D11 DH2Q with FLAG_PERSISTENT on a CA share (st 0x%08x dh2q %d)",
           st, o.has_dh2q);
    if (st != ST_SUCCESS || !o.has_dh2q) {
        smb2_env_stop(&env);
        return;
    }
    NOTE("D11 granted timeout %u ms flags 0x%08x", o.dh2q_timeout,
         o.dh2q_flags);
    EXPECT((o.dh2q_flags & SMB2_DHANDLE_FLAG_PERSISTENT) != 0,
           "D11 the grant reports FLAG_PERSISTENT back (0x%08x)",
           o.dh2q_flags);

    smb2_conn_disconnect(a);
    smb2_quiesce(&env);
    b = smb2_conn_reopen(&env, a);
    wait_parked(b, o.file_id);

    /* A conflicting opener meets a PARKED PERSISTENT holder.  Unlike the
     * durable-only holder of D7, it must not simply take the file. */
    cc = smb2_conn_open(&env);
    smb2_handshake(cc);
    st = smb2_create(cc, "d11", FILE_OPEN, FILE_ALL_ACCESS, 0, NULL, &r);
    NOTE("D11 conflicting open against a parked PERSISTENT holder -> 0x%08x",
         st);
    EXPECT(st != ST_SUCCESS,
           "D11 a parked persistent holder does NOT yield the file"
           " (0x%08x)", st);
    if (st == ST_SUCCESS) {
        smb2_close(cc, r.file_id);
    }

    /* The owning client reclaims with FLAG_PERSISTENT. */
    smb2_quiesce(&env);
    memset(&dur, 0, sizeof(dur));
    dur.dh2c            = 1;
    dur.reconnect_flags = SMB2_DHANDLE_FLAG_PERSISTENT;
    memcpy(dur.file_id, o.file_id, 16);
    fill_guid(dur.create_guid, 0xB1);
    st = smb2_create_dur(b, "", FILE_OPEN, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                         NULL, &dur, &r);
    EXPECT(st == ST_SUCCESS,
           "D11 persistent reclaim with FLAG_PERSISTENT -> 0x%08x", st);
    if (st == ST_SUCCESS) {
        EXPECT(memcmp(r.file_id, o.file_id, 16) == 0,
               "D11 the persistent reclaim carries the ORIGINAL FileId");
        smb2_close(b, r.file_id);
    }

    smb2_quiesce(&env);
    smb2_env_stop(&env);
} /* sec_d11 */

int
main(
    int   argc,
    char *argv[])
{
    struct smb2_env      env;
    struct smb2_env_opts opts = { .oplocks            = 1,
                                  .leases             = 1,
                                  .directory_leases   = 1,
                                  .persistent_handles = 1 };
    struct smb2_conn    *a, *seed, *b;

    (void) argc;
    (void) argv;

    smb2_env_start_opts(&env, &opts);
    a = smb2_conn_open(&env);
    smb2_handshake(a);
    printf("# durable probe up; dialect=0x%04x, persistent_handles on,"
           " continuous_availability off\n", a->dialect);

    sec_d1(&env, a);
    sec_d2(&env, a);

    /* D3 consumes its connection (it disconnects it), so it gets its own. */
    seed = smb2_conn_open(&env);
    smb2_handshake(seed);
    b = sec_d3(&env, seed);
    if (b) {
        smb2_quiesce(&env);
    }

    seed = smb2_conn_open(&env);
    smb2_handshake(seed);
    sec_d4(&env, seed);

    sec_d5(&env);
    sec_d6(&env);
    sec_d7(&env);
    sec_d8(&env);
    sec_d9(&env);
    sec_d10(&env);

    smb2_env_stop(&env);

    sec_d11();

    printf("\n# summary: %d recorded deviation(s) (see DEVIATIONS-SMB.md)\n",
           ndev);
    if (nfail) {
        fprintf(stderr, "%d durable-handle MANDATE check(s) FAILED\n", nfail);
        return 1;
    }
    printf("all SMB2 durable/persistent/resilient mandate checks passed"
           " (%d documented deviation(s))\n", ndev);
    return 0;
} /* main */
