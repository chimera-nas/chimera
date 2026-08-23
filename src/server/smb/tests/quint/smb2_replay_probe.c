/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * SMB2 replay / ChannelSequence ground-truth probe -- ROADMAP-SMB.md blocker
 * B4, the exactly-once layer.  This is the third of the ground-truth probes
 * (smb2_oplock_probe.c, smb2_durable_probe.c) and it pins the behavior the
 * single largest WPTS family exercises: ~573 cases across ReplayFileOperation,
 * ReplayCreateDurableHandleV1/V2/V2Persistent, ReplayCreateNormalHandle,
 * ReplayCreateWithoutHandle and the wire Replay tests.
 *
 * WHAT "REPLAY" MEANS ON THE WIRE (measured here, section R1, not assumed).
 * After a channel or transport failure a client RE-SENDS a request with
 * SMB2_FLAGS_REPLAY_OPERATION (0x20000000) set.  What it does NOT do is reuse
 * the MessageId: MS-SMB2 3.3.5.2.3 makes the server tear the connection down
 * when a MessageId falls outside the command-sequence window, and R1 shows
 * chimera doing exactly that.  So a wire replay is a FRESH MessageId carrying
 * the replay flag, and the request is correlated with its original by
 * something in the payload:
 *
 *   - a CREATE by its DH2Q CreateGuid  -- a true reply cache: the original
 *     open is RETURNED, its effect is not re-applied (R3);
 *   - a mutating op by its ChannelSequence -- NOT a reply cache: there is no
 *     cached reply, the rule is that a request from a SUPERSEDED channel is
 *     REJECTED so it cannot clobber newer data (R6, R7).
 *
 * Those are two different mechanisms with two different oracles, and the
 * distinction is the whole point of the section split below.  A model that
 * treats them alike will be wrong about one of them.
 *
 * ORACLE STRENGTH.  A replayed operation that was applied twice and one that
 * was applied once are indistinguishable if the only thing asserted is the
 * status code -- both say STATUS_SUCCESS.  Every exactly-once claim below is
 * therefore asserted on the EFFECT:
 *   - a replayed FILE_CREATE reports CreateAction = CREATED, which a second
 *     application could not (it would collide), and leaves exactly ONE open,
 *     proved by handing the file to a share-conflicting opener after a single
 *     CLOSE (R3);
 *   - a rejected stale WRITE leaves the file CONTENT unchanged (R6);
 *   - a rejected stale SET_INFO leaves the file SIZE unchanged (R7).
 *
 * Conformance discipline (DEVIATIONS-SMB.md): every EXPECT cites the clause it
 * asserts.  Server-discretion values are NOTE()d.  A divergence from a mandate
 * becomes a DEVIATIONS-SMB.md entry, never a weakened assertion.
 *
 * Sections:
 *   R1  wire mechanics  -- a re-sent MessageId is fatal to the connection, so
 *                          a replay must carry a fresh one
 *   R2  eligibility     -- what makes a CREATE replayable at all: the DH2Q
 *                          context, or the durable GRANT?
 *   R3  exactly once    -- the core: a replayed FILE_CREATE is answered from
 *                          the original open and is NOT re-applied
 *   R4  classifier      -- the four outcomes of chimera's create_guid replay
 *                          classification: RECLAIM / DUPLICATE / DENIED / NONE
 *   R5  no key          -- a replay flag with no CreateGuid has no idempotence
 *                          key and IS re-applied
 *   R6  CS window       -- the ChannelSequence accept/reject window and its
 *                          effect on file content
 *   R7  CS sites        -- WRITE / SET_INFO / IOCTL reject a stale sequence;
 *                          READ does not, and what each does to the tracked
 *                          high-water mark
 *   R8  CS scope        -- the tracked sequence is per-OPEN, not per-session
 *   R9  CS vs flag      -- the replay flag plays no part in the CS rule
 */

#include "smb2_mbt_common.h"

static int nfail = 0;
static int ndev  = 0;

/* A spec-mandated assertion.  Fails the probe (and CI) on a violation. */
#define EXPECT(ok, ...)                              \
        do {                                         \
            if (ok) { printf("ok   - "); }           \
            else { printf("FAIL - "); nfail++; }     \
            printf(__VA_ARGS__);                     \
            printf("\n");                            \
        } while (0)

/* A ground-truth observation (server discretion / recorded behavior). */
#define NOTE(...)                                    \
        do { printf("note - "); printf(__VA_ARGS__); printf("\n"); } while (0)

/* A recorded, spec-cited DEVIATION.  Loud and counted, but does not fail CI:
 * it pins a known non-conformance so the probe stays a regression anchor.
 * Every use has a DEVIATIONS-SMB.md entry. */
#define DEVIATION(id, ...)                                       \
        do {                                                     \
            printf("DEVIATION %s - ", id); ndev++;               \
            printf(__VA_ARGS__);                                 \
            printf("\n");                                        \
        } while (0)

/* ---- helpers ------------------------------------------------------------ */

static void
fill_guid(
    uint8_t guid[16],
    int     tag)
{
    memset(guid, 0, 16);
    guid[0]  = (uint8_t) tag;
    guid[1]  = 0x9E;
    guid[15] = (uint8_t) ~tag;
} /* fill_guid */

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

/* Pump until either a reply lands or the connection dies.  Returns 1 for a
 * reply, 0 for a drop, -1 for the wedge ceiling.  The packaged waits treat a
 * drop as fatal, which is exactly wrong for R1 -- there the drop IS the
 * expected outcome and has to be observable as a value. */
static int
pump_reply_or_drop(struct smb2_conn *c)
{
    uint64_t deadline = smb2c_now_ms() + SMB2C_HANG_MS;

    while (!c->reply_ready && !c->disconnected) {
        smb2_pump(c->env);
        if (smb2c_now_ms() >= deadline) {
            return -1;
        }
    }
    return c->reply_ready ? 1 : 0;
} /* pump_reply_or_drop */

/* Barrier for "the server has PARKED the handles of the connection we just
 * dropped".  A client-side close returns as soon as the drop is delivered,
 * which is strictly earlier than the server finishing its teardown, so a
 * reclaim issued immediately afterwards would be racing.
 *
 * A DH2C carrying a deliberately WRONG CreateGuid is the barrier: chimera
 * answers a not-yet-parked entry with a reconnect RETRY rather than a refusal,
 * so the OBJECT_NAME_NOT_FOUND can only be produced once the park has actually
 * happened -- and a guid mismatch consumes nothing. */
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

/* The file's current EndOfFile, read through a fresh share-everything open on
 * `c`.  Observing the EFFECT of a mutating op needs a channel independent of
 * the handle whose ChannelSequence is under test. */
static uint64_t
file_size(
    struct smb2_conn *c,
    const char       *name,
    uint32_t         *r_status)
{
    struct smb2_create_out o;
    uint32_t               st = smb2_create(c, name, FILE_OPEN, FILE_READ_ACCESS,
                                            FILE_SHARE_RWD, NULL, &o);

    if (r_status) {
        *r_status = st;
    }
    if (st != ST_SUCCESS) {
        return (uint64_t) -1;
    }
    smb2_close(c, o.file_id);
    return o.end_of_file;
} /* file_size */

/* Read the first `len` bytes of the open `file_id` into a NUL-terminated
 * buffer, so a content assertion can be printed as well as compared.
 *
 * The ChannelSequence is an EXPLICIT parameter and callers pass the Open's
 * current high-water mark.  That is not pedantry: a READ carries a
 * ChannelSequence like any other request, an unset one is 0, and 0 against a
 * mark in the upper half of the space is a FORWARD jump under the modular
 * rule -- so an "innocent" verification read would silently reset the very
 * mark the surrounding test is measuring.  (R7 pins that behavior on
 * purpose; here it must not contaminate.) */
static void
read_text(
    struct smb2_conn *c,
    const uint8_t     file_id[16],
    char             *out,
    uint32_t          len,
    uint16_t          cs)
{
    uint32_t got = 0;

    memset(out, 0, len + 1);
    smb2c_set_next_channel_sequence(c, cs);
    smb2_read(c, file_id, 0, len, (uint8_t *) out, &got);
    out[got] = 0;
} /* read_text */

/* ------------------------------------------------------------------------
 * R1  A RE-SENT MessageId is fatal; a replay must carry a fresh one.
 *
 * MS-SMB2 3.3.5.2.3: the server maintains a command-sequence window and MUST
 * disconnect when a request's MessageId falls outside it.  A MessageId already
 * consumed is outside it.  This is the fact that decides what "replay" can
 * mean on the wire, so it is measured rather than assumed -- and it is the
 * reason smb2c_pin_msg_id() is not the replay primitive.
 * ------------------------------------------------------------------------ */
static void
sec_r1(struct smb2_env *env)
{
    struct smb2_conn      *c;
    struct smb2_create_out o;
    uint32_t               st;
    uint64_t               reused;
    int                    r;

    printf("\n# R1 the command-sequence window (MS-SMB2 3.3.5.2.3)\n");

    c = smb2_conn_open(env);
    smb2_handshake(c);

    st = smb2_create(c, "r1", FILE_OPEN_IF, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                     NULL, &o);
    EXPECT(st == ST_SUCCESS, "R1 setup CREATE (0x%08x)", st);
    if (st != ST_SUCCESS) {
        return;
    }
    reused = c->last_msg_id;          /* this CREATE's MessageId, now consumed */
    smb2_close(c, o.file_id);

    /* A fresh MessageId carrying the replay flag is ACCEPTED -- establish that
     * first, so the drop below cannot be blamed on the flag. */
    smb2c_set_next_flags(c, SMB2_FLAGS_REPLAY_OPERATION);
    st = smb2_create(c, "r1", FILE_OPEN, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                     NULL, &o);
    EXPECT(st == ST_SUCCESS,
           "R1 a replay-flagged request with a FRESH MessageId is answered"
           " normally (0x%08x)", st);
    if (st == ST_SUCCESS) {
        smb2_close(c, o.file_id);
    }

    /* Now the same request with a MessageId that has already been used. */
    uint64_t before = c->msg_id;

    smb2c_pin_msg_id(c, reused);
    smb2c_set_next_flags(c, SMB2_FLAGS_REPLAY_OPERATION);
    smb2_create_post(c, "r1", FILE_OPEN, FILE_ALL_ACCESS, FILE_SHARE_RWD, NULL);

    r = pump_reply_or_drop(c);
    NOTE("R1 re-sent MessageId %" PRIu64 " -> %s", reused,
         r == 1 ? "a reply" : r == 0 ? "the connection was dropped"
         : "nothing (wedged)");
    EXPECT(r == 0,
           "R1 a request re-using a consumed MessageId gets NO reply and the"
           " connection is torn down (MS-SMB2 3.3.5.2.3)");
    EXPECT(c->last_msg_id == reused,
           "R1 the request really carried the re-used MessageId %" PRIu64
           " (stamped %" PRIu64 ")", reused, c->last_msg_id);
    EXPECT(c->msg_id == before,
           "R1 a pinned send does not advance the client's MessageId counter"
           " (%" PRIu64 " vs %" PRIu64 ")", c->msg_id, before);

    NOTE("R1 CONSEQUENCE: a wire replay is a FRESH MessageId + "
         "SMB2_FLAGS_REPLAY_OPERATION, correlated by DH2Q CreateGuid (creates)"
         " or ChannelSequence (mutating ops) -- never by re-sending an id");
} /* sec_r1 */

/* ------------------------------------------------------------------------
 * R2  What makes a CREATE replayable: the DH2Q context, not the durable GRANT.
 *
 * chimera_smb_create_grant_durable records the create_guid and sets
 * IsReplayEligible for ANY create carrying a DH2Q context, whether or not the
 * caching precondition for a durable grant was met.  That is a real behavioral
 * fork the model has to get right: a replayable open and a durable open are
 * not the same set.  (Both are gated on the persistent_handles config: with it
 * off, grant_durable returns immediately and nothing is replayable.)
 * ------------------------------------------------------------------------ */
static void
sec_r2(struct smb2_env *env)
{
    struct smb2_conn       *a;
    struct smb2_durable_req dur;
    struct smb2_create_out  o, r;
    uint32_t                st;

    printf("\n# R2 replay eligibility vs. the durable grant"
           " (MS-SMB2 3.3.5.9.10)\n");

    a = smb2_conn_open(env);
    smb2_handshake(a);

    /* No oplock, no lease: the durable request CANNOT be granted (pinned by
     * the durable probe's D1 grant matrix). */
    memset(&dur, 0, sizeof(dur));
    dur.dh2q = 1;
    fill_guid(dur.create_guid, 0x21);

    st = smb2_create_dur(a, "r2", FILE_CREATE, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                         NULL, &dur, &o);
    EXPECT(st == ST_SUCCESS, "R2 non-caching create with a DH2Q (0x%08x)", st);
    if (st != ST_SUCCESS) {
        return;
    }
    EXPECT(!o.has_dh2q,
           "R2 the durable request is REFUSED (no caching precondition)");
    EXPECT(o.action == FILE_ACT_CREATED, "R2 the original action is CREATED");

    /* ... and yet the create IS replayable. */
    smb2c_set_next_flags(a, SMB2_FLAGS_REPLAY_OPERATION);
    st = smb2_create_dur(a, "r2", FILE_CREATE, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                         NULL, &dur, &r);
    NOTE("R2 replay of a NON-durable DH2Q create -> 0x%08x, same fid %d,"
         " action %u", st, memcmp(r.file_id, o.file_id, 16) == 0, r.action);
    EXPECT(st == ST_SUCCESS && memcmp(r.file_id, o.file_id, 16) == 0,
           "R2 a DH2Q create is replay-eligible even when NO durable handle was"
           " granted (0x%08x)", st);
    EXPECT(r.action == FILE_ACT_CREATED,
           "R2 the replay echoes the ORIGINAL CreateAction (%u), which a"
           " re-applied FILE_CREATE could not produce", r.action);
    NOTE("R2 the replay reply: dh2q %d, %d context(s)", r.has_dh2q, r.nctx);
    EXPECT(!r.has_dh2q,
           "R2 ... and it advertises no durable grant, because none was made"
           " (%d)", r.has_dh2q);

    smb2_close(a, o.file_id);
    smb2_quiesce(env);
} /* sec_r2 */

/* ------------------------------------------------------------------------
 * R3  EXACTLY ONCE: a replayed FILE_CREATE is answered, not re-applied.
 *
 * This is the property the whole ReplayCreate* family exists to test, and it
 * is the one place where asserting the status code alone would be worthless:
 * "applied once" and "applied twice" both report STATUS_SUCCESS.  Three
 * independent effect-level oracles are used instead.
 *
 *   1. CreateAction.  The replay reports CREATED.  A genuine second
 *      application of FILE_CREATE against a file that now exists could not
 *      report CREATED -- it would report OBJECT_NAME_COLLISION.  R3a proves
 *      that premise on the same server before relying on it.
 *   2. Handle identity.  The reply carries the ORIGINAL FileId.
 *   3. Open count.  The file is opened with ShareAccess NONE.  ONE close
 *      releases it: a share-conflicting opener then succeeds.  Had the replay
 *      produced a second open, the reservation would have outlived that close.
 *
 * R3e is the non-vacuity control: the identical request WITHOUT the replay
 * flag must NOT be answered from the original.
 * ------------------------------------------------------------------------ */
static void
sec_r3(struct smb2_env *env)
{
    struct smb2_conn       *a, *b;
    struct smb2_oplock_req  oreq;
    struct smb2_durable_req dur;
    struct smb2_create_out  o, r, o2;
    uint32_t                st;

    printf("\n# R3 exactly-once CREATE replay (MS-SMB2 3.3.5.9.10)\n");

    a = smb2_conn_open(env);
    b = smb2_conn_open(env);
    smb2_handshake(a);
    smb2_handshake(b);

    /* (a) The premise: FILE_CREATE is NOT idempotent on this server. */
    st = smb2_create(a, "r3ctl", FILE_CREATE, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                     NULL, &o);
    EXPECT(st == ST_SUCCESS && o.action == FILE_ACT_CREATED,
           "R3a control: the first FILE_CREATE succeeds with action CREATED"
           " (0x%08x action %u)", st, o.action);
    smb2_close(a, o.file_id);
    st = smb2_create(a, "r3ctl", FILE_CREATE, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                     NULL, &o2);
    EXPECT(st == ST_OBJECT_NAME_COLLISION,
           "R3a control: a SECOND FILE_CREATE collides -- so 'action CREATED'"
           " below can only come from a cached reply (0x%08x)", st);

    /* (b) The original: durable, batch-oplocked, ShareAccess NONE. */
    mk_oplock(&oreq, SMB2_OPLOCK_LEVEL_BATCH);
    memset(&dur, 0, sizeof(dur));
    dur.dh2q = 1;
    fill_guid(dur.create_guid, 0x31);

    st = smb2_create_dur(a, "r3", FILE_CREATE, FILE_ALL_ACCESS,
                         0 /* ShareAccess NONE */, &oreq, &dur, &o);
    EXPECT(st == ST_SUCCESS && o.action == FILE_ACT_CREATED && o.has_dh2q,
           "R3b original durable FILE_CREATE (0x%08x action %u dh2q %d)",
           st, o.action, o.has_dh2q);
    if (st != ST_SUCCESS) {
        return;
    }

    /* (c) The replay: byte-identical request, fresh MessageId, replay flag. */
    smb2c_set_next_flags(a, SMB2_FLAGS_REPLAY_OPERATION);
    st = smb2_create_dur(a, "r3", FILE_CREATE, FILE_ALL_ACCESS, 0, &oreq,
                         &dur, &r);
    EXPECT(st == ST_SUCCESS,
           "R3c the replay is answered SUCCESS, not OBJECT_NAME_COLLISION"
           " (0x%08x)", st);
    EXPECT(memcmp(r.file_id, o.file_id, 16) == 0,
           "R3c the replay returns the ORIGINAL FileId");
    EXPECT(r.action == FILE_ACT_CREATED,
           "R3c the replay echoes CreateAction = CREATED (%u): the create was"
           " NOT re-evaluated", r.action);
    /* Whether the replay's reply re-advertises the durable grant is a wire
     * fact the model has to predict, and it is not obvious either way: the
     * request carried a DH2Q, but nothing new was granted. */
    NOTE("R3c the replay reply: dh2q %d (timeout %u flags 0x%08x), lease %d,"
         " %d context(s)", r.has_dh2q, r.dh2q_timeout, r.dh2q_flags,
         r.has_lease, r.nctx);
    EXPECT(r.has_dh2q == o.has_dh2q,
           "R3c the replay re-advertises the durable grant exactly as the"
           " original did (%d vs %d)", r.has_dh2q, o.has_dh2q);

    /* (d) The effect: exactly ONE open exists. */
    st = smb2_close(a, o.file_id);
    EXPECT(st == ST_SUCCESS, "R3d one CLOSE of the FileId (0x%08x)", st);
    st = smb2_close(a, o.file_id);
    EXPECT(st == ST_FILE_CLOSED,
           "R3d a SECOND close finds nothing -- the replay produced no second"
           " handle (0x%08x)", st);
    smb2_quiesce(env);
    st = smb2_create(b, "r3", FILE_OPEN, FILE_ALL_ACCESS,
                     0 /* ShareAccess NONE */, NULL, &o2);
    EXPECT(st == ST_SUCCESS,
           "R3d ... and one CLOSE released the ShareAccess-NONE reservation:"
           " the replay created NO second open (0x%08x)", st);
    if (st == ST_SUCCESS) {
        smb2_close(b, o2.file_id);
    }
    smb2_quiesce(env);

    /* (e) Non-vacuity: the SAME request without the flag is not a replay. */
    memset(&dur, 0, sizeof(dur));
    dur.dh2q = 1;
    fill_guid(dur.create_guid, 0x32);
    st = smb2_create_dur(a, "r3e", FILE_CREATE, FILE_ALL_ACCESS,
                         FILE_SHARE_RWD, &oreq, &dur, &o);
    EXPECT(st == ST_SUCCESS && o.has_dh2q, "R3e original (0x%08x)", st);

    st = smb2_create_dur(a, "r3e", FILE_CREATE, FILE_ALL_ACCESS,
                         FILE_SHARE_RWD, &oreq, &dur, &r);
    NOTE("R3e the identical create WITHOUT the replay flag -> 0x%08x", st);
    EXPECT(st == ST_DUPLICATE_OBJECTID,
           "R3e a NON-replay create colliding on CreateGuid is"
           " DUPLICATE_OBJECTID -- the flag is what buys exactly-once"
           " (0x%08x)", st);
    if (st == ST_SUCCESS) {
        smb2_close(a, r.file_id);
    }
    smb2_close(a, o.file_id);
    smb2_quiesce(env);
} /* sec_r3 */

/* ------------------------------------------------------------------------
 * R4  The four outcomes of chimera's create_guid classification
 * (chimera_smb_durable_claim_by_guid): RECLAIM / DUPLICATE / DENIED / NONE.
 *
 * Order matters and is deliberate: DUPLICATE and DENIED must not consume the
 * parked entry, so they are exercised BEFORE the RECLAIM that does.
 * ------------------------------------------------------------------------ */
static void
sec_r4(struct smb2_env *env)
{
    struct smb2_conn       *a, *b;
    struct smb2_oplock_req  lreq, wrongkey;
    struct smb2_durable_req dur;
    struct smb2_create_out  o, r;
    uint32_t                st;

    printf("\n# R4 create_guid replay classification"
           " (MS-SMB2 3.3.5.9.10)\n");

    a = smb2_conn_open(env);
    smb2_handshake(a);

    mk_lease(&lreq, 0x44, SMB2_LEASE_RWH);
    memset(&dur, 0, sizeof(dur));
    dur.dh2q = 1;
    fill_guid(dur.create_guid, 0x44);

    st = smb2_create_dur(a, "r4", FILE_OPEN_IF, FILE_ALL_ACCESS,
                         FILE_SHARE_RWD, &lreq, &dur, &o);
    EXPECT(st == ST_SUCCESS && o.has_dh2q && o.has_lease,
           "R4 durable leased open (0x%08x dh2q %d lease %d)", st, o.has_dh2q,
           o.has_lease);
    if (st != ST_SUCCESS || !o.has_dh2q) {
        return;
    }

    smb2_conn_disconnect(a);
    b = smb2_conn_reopen(env, a);
    wait_parked(b, o.file_id);

    /* (1) DUPLICATE: a NON-replay create whose guid matches a parked open. */
    st = smb2_create_dur(b, "r4", FILE_OPEN_IF, FILE_ALL_ACCESS,
                         FILE_SHARE_RWD, &lreq, &dur, &r);
    EXPECT(st == ST_DUPLICATE_OBJECTID,
           "R4/DUPLICATE a non-replay create colliding with a PARKED open"
           " (0x%08x)", st);

    /* (2) DENIED: a replay whose handle TYPE differs from the parked open's --
     * the parked open is leased, this replay carries no lease context. */
    smb2c_set_next_flags(b, SMB2_FLAGS_REPLAY_OPERATION);
    st = smb2_create_dur(b, "r4", FILE_OPEN_IF, FILE_ALL_ACCESS,
                         FILE_SHARE_RWD, NULL, &dur, &r);
    EXPECT(st == ST_ACCESS_DENIED,
           "R4/DENIED a replay of a LEASED parked open that carries no lease"
           " context -> ACCESS_DENIED (0x%08x)", st);

    /* (2b) DENIED: right type, wrong lease key. */
    mk_lease(&wrongkey, 0x45, SMB2_LEASE_RWH);
    smb2c_set_next_flags(b, SMB2_FLAGS_REPLAY_OPERATION);
    st = smb2_create_dur(b, "r4", FILE_OPEN_IF, FILE_ALL_ACCESS,
                         FILE_SHARE_RWD, &wrongkey, &dur, &r);
    EXPECT(st == ST_ACCESS_DENIED,
           "R4/DENIED a replay with the WRONG LeaseKey -> ACCESS_DENIED"
           " (0x%08x)", st);

    NOTE("R4 contrast: the DH2C RECONNECT path answers both of those with"
         " OBJECT_NAME_NOT_FOUND (durable probe D4b/D4c); the REPLAY path"
         " answers ACCESS_DENIED -- two different mandates, MS-SMB2 3.3.5.9.12"
         " vs 3.3.5.9.10");

    /* (3) NONE: a replay whose guid matches nothing falls through to a fresh
     * open of the file, which already exists -> OPENED, a NEW FileId. */
    {
        struct smb2_durable_req other = dur;

        fill_guid(other.create_guid, 0x46);
        smb2c_set_next_flags(b, SMB2_FLAGS_REPLAY_OPERATION);
        st = smb2_create_dur(b, "r4b", FILE_OPEN_IF, FILE_ALL_ACCESS,
                             FILE_SHARE_RWD, NULL, &other, &r);
        EXPECT(st == ST_SUCCESS && r.action == FILE_ACT_CREATED,
               "R4/NONE a replay whose CreateGuid matches nothing is an"
               " ORDINARY create (0x%08x action %u)", st, r.action);
        if (st == ST_SUCCESS) {
            smb2_close(b, r.file_id);
        }
    }

    /* (4) RECLAIM: the replay that matches, with the right handle type. */
    smb2c_set_next_flags(b, SMB2_FLAGS_REPLAY_OPERATION);
    st = smb2_create_dur(b, "r4", FILE_OPEN_IF, FILE_ALL_ACCESS,
                         FILE_SHARE_RWD, &lreq, &dur, &r);
    EXPECT(st == ST_SUCCESS,
           "R4/RECLAIM a matching replay reclaims the parked open (0x%08x)",
           st);
    EXPECT(memcmp(r.file_id, o.file_id, 16) == 0,
           "R4/RECLAIM ... and returns the ORIGINAL FileId");
    NOTE("R4/RECLAIM reply: dh2q %d, lease %d, %d context(s)", r.has_dh2q,
         r.has_lease, r.nctx);
    /* MEASURED, and NOT what the DH2C reconnect path does.  A DH2C reclaim
     * carries no DH2Q response context (durable probe D3) because the request
     * carried none; a REPLAY-driven reclaim of the same parked handle carries
     * one, because the replayed request is a full DH2Q create and chimera's
     * response emitters key on the REQUEST's context mask.  Same reclaim, two
     * different replies, decided by how it was asked for. */
    EXPECT(r.has_dh2q,
           "R4/RECLAIM a REPLAY-driven reclaim DOES carry a DH2Q response"
           " context -- unlike a DH2C reclaim of the same handle (%d)",
           r.has_dh2q);
    NOTE("R4/RECLAIM reclaim action %u (the original open's), lease %d",
         r.action, r.has_lease);

    if (st == ST_SUCCESS) {
        smb2_close(b, r.file_id);
    }
    smb2_quiesce(env);
} /* sec_r4 */

/* ------------------------------------------------------------------------
 * R5  A replay with NO idempotence key is re-applied.
 *
 * The negative half that gives the CreateGuid mechanism its meaning, and the
 * shape WPTS ReplayCreateWithoutHandle / ReplayCreateNormalHandle drive: the
 * replay FLAG on its own carries no correlation information, so the server has
 * nothing to answer from and must execute the request.
 * ------------------------------------------------------------------------ */
static void
sec_r5(struct smb2_env *env)
{
    struct smb2_conn      *a;
    struct smb2_create_out o, r;
    uint32_t               st;

    printf("\n# R5 a replay with no CreateGuid has no idempotence key\n");

    a = smb2_conn_open(env);
    smb2_handshake(a);

    st = smb2_create(a, "r5", FILE_CREATE, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                     NULL, &o);
    EXPECT(st == ST_SUCCESS && o.action == FILE_ACT_CREATED,
           "R5 original FILE_CREATE, no create context (0x%08x)", st);

    smb2c_set_next_flags(a, SMB2_FLAGS_REPLAY_OPERATION);
    st = smb2_create(a, "r5", FILE_CREATE, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                     NULL, &r);
    NOTE("R5 replay-flagged FILE_CREATE without a DH2Q -> 0x%08x", st);
    EXPECT(st == ST_OBJECT_NAME_COLLISION,
           "R5 the replay is EXECUTED, not answered: with no CreateGuid there"
           " is no key to answer from (0x%08x)", st);

    if (st == ST_SUCCESS) {
        smb2_close(a, r.file_id);
    }
    smb2_close(a, o.file_id);
    smb2_quiesce(env);
} /* sec_r5 */

/* ------------------------------------------------------------------------
 * R6  The ChannelSequence window, asserted on FILE CONTENT.
 *
 * MS-SMB2 3.3.5.2.10.  The Open tracks a high-water ChannelSequence, seeded
 * from its CREATE.  For a mutating op, delta = req_cs - open_cs computed mod
 * 2^16: delta >= 0x8000 means the request comes from a SUPERSEDED channel and
 * is rejected with STATUS_FILE_NOT_AVAILABLE; anything else is accepted and
 * becomes the new high-water mark.
 *
 * This is NOT a reply cache.  Nothing is remembered about the reply; a request
 * at the SAME sequence is simply executed again (R6/4 shows that directly).
 * The exactly-once guarantee for a write is entirely negative: the write that
 * would have clobbered newer data is refused.  Every row here therefore reads
 * the file back -- the status code alone cannot tell the two apart.
 * ------------------------------------------------------------------------ */
static void
sec_r6(struct smb2_env *env)
{
    struct smb2_conn      *a;
    struct smb2_create_out o;
    uint32_t               st, n;
    char                   txt[16];

    printf("\n# R6 the ChannelSequence window (MS-SMB2 3.3.5.2.10)\n");

    a = smb2_conn_open(env);
    smb2_handshake(a);

    /* The CREATE carries ChannelSequence 0, which seeds the Open. */
    st = smb2_create(a, "r6", FILE_OPEN_IF, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                     NULL, &o);
    EXPECT(st == ST_SUCCESS, "R6 open, seeding ChannelSequence 0 (0x%08x)", st);
    if (st != ST_SUCCESS) {
        return;
    }

    /* (1) a forward jump is accepted and becomes the high-water mark */
    smb2c_set_next_channel_sequence(a, 5);
    st = smb2_write(a, o.file_id, 0, "AAAA", 4, &n);
    EXPECT(st == ST_SUCCESS && n == 4,
           "R6/1 WRITE at CS 5 (forward from 0) is accepted (0x%08x)", st);
    read_text(a, o.file_id, txt, 4, 5);
    EXPECT(strcmp(txt, "AAAA") == 0, "R6/1 content is 'AAAA' (got '%s')", txt);

    /* (2) a stale sequence is rejected AND CHANGES NOTHING */
    smb2c_set_next_channel_sequence(a, 0);
    st = smb2_write(a, o.file_id, 0, "BBBB", 4, &n);
    EXPECT(st == ST_FILE_NOT_AVAILABLE,
           "R6/2 WRITE at the superseded CS 0 -> FILE_NOT_AVAILABLE (0x%08x)",
           st);
    read_text(a, o.file_id, txt, 4, 5);
    EXPECT(strcmp(txt, "AAAA") == 0,
           "R6/2 THE EFFECT: the rejected write did not touch the file"
           " (content '%s')", txt);

    /* (3) the rejection did not disturb the high-water mark */
    smb2c_set_next_channel_sequence(a, 5);
    st = smb2_write(a, o.file_id, 0, "CCCC", 4, &n);
    EXPECT(st == ST_SUCCESS,
           "R6/3 WRITE at CS 5 still works: a rejected request does not move"
           " the mark (0x%08x)", st);

    /* (4) the SAME sequence is re-executed -- there is no reply cache */
    smb2c_set_next_channel_sequence(a, 5);
    smb2c_set_next_flags(a, SMB2_FLAGS_REPLAY_OPERATION);
    st = smb2_write(a, o.file_id, 0, "DDDD", 4, &n);
    EXPECT(st == ST_SUCCESS,
           "R6/4 a replay-flagged WRITE at the SAME CS is accepted (0x%08x)",
           st);
    read_text(a, o.file_id, txt, 4, 5);
    EXPECT(strcmp(txt, "DDDD") == 0,
           "R6/4 THE EFFECT: it was RE-EXECUTED, not answered from a cache"
           " (content '%s') -- writes have no reply cache; the exactly-once"
           " guarantee is the stale-rejection of R6/2 alone", txt);

    /* (5) the accept boundary: delta 0x7FFF is still forward */
    smb2c_set_next_channel_sequence(a, (uint16_t) (5 + 0x7FFF));
    st = smb2_write(a, o.file_id, 0, "EEEE", 4, &n);
    EXPECT(st == ST_SUCCESS,
           "R6/5 delta 0x7FFF is forward and accepted (CS 0x%04x, 0x%08x)",
           5 + 0x7FFF, st);

    /* (6) the reject boundary: delta 0x8000 exactly is stale */
    smb2c_set_next_channel_sequence(a, (uint16_t) (5 + 0x7FFF + 0x8000));
    st = smb2_write(a, o.file_id, 0, "FFFF", 4, &n);
    EXPECT(st == ST_FILE_NOT_AVAILABLE,
           "R6/6 delta 0x8000 exactly is stale (CS 0x%04x, 0x%08x)",
           (uint16_t) (5 + 0x7FFF + 0x8000), st);
    read_text(a, o.file_id, txt, 4, (uint16_t) (5 + 0x7FFF));
    EXPECT(strcmp(txt, "EEEE") == 0,
           "R6/6 THE EFFECT: unchanged at the boundary (content '%s')", txt);

    /* (7) the comparison is MODULAR: from 0xFFFF, CS 0x0001 is FORWARD by 2.
     * A naive unsigned `<` would have called it stale and refused it. */
    smb2c_set_next_channel_sequence(a, 0xFFFF);
    st = smb2_write(a, o.file_id, 0, "GGGG", 4, &n);
    EXPECT(st == ST_SUCCESS, "R6/7 advance to CS 0xFFFF (0x%08x)", st);

    smb2c_set_next_channel_sequence(a, 0x0001);
    st = smb2_write(a, o.file_id, 0, "HHHH", 4, &n);
    EXPECT(st == ST_SUCCESS,
           "R6/7 CS 0x0001 after 0xFFFF WRAPS FORWARD and is accepted"
           " (0x%08x)", st);
    read_text(a, o.file_id, txt, 4, 0x0001);
    EXPECT(strcmp(txt, "HHHH") == 0, "R6/7 content is 'HHHH' (got '%s')", txt);

    smb2_close(a, o.file_id);
    smb2_quiesce(env);
} /* sec_r6 */

/* ------------------------------------------------------------------------
 * R7  Which operations consult the ChannelSequence, and what each does to the
 * tracked mark.  MS-SMB2 3.3.5.2.10 names the mutating set (WRITE, SET_INFO,
 * IOCTL); READ is explicitly NOT rejected.  chimera's implementation adds a
 * detail the spec text does not spell out and the model must reproduce: a READ
 * that jumps FORWARD still ADVANCES the mark, so a subsequent write at the old
 * sequence becomes stale.  That is measured, not assumed.
 * ------------------------------------------------------------------------ */
static void
sec_r7(struct smb2_env *env)
{
    struct smb2_conn      *a, *w;
    struct smb2_create_out o;
    uint32_t               st, n, sz_st;
    uint64_t               sz;
    uint8_t                sparse = 1;
    char                   txt[16];

    printf("\n# R7 the channel-sequence-checked operation set\n");

    a = smb2_conn_open(env);
    w = smb2_conn_open(env);
    smb2_handshake(a);
    smb2_handshake(w);

    st = smb2_create(a, "r7", FILE_OPEN_IF, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                     NULL, &o);
    EXPECT(st == ST_SUCCESS, "R7 open (0x%08x)", st);
    if (st != ST_SUCCESS) {
        return;
    }

    /* Advance the mark to 10. */
    smb2c_set_next_channel_sequence(a, 10);
    st = smb2_write(a, o.file_id, 0, "0123456789", 10, &n);
    EXPECT(st == ST_SUCCESS, "R7 seed: WRITE at CS 10 (0x%08x)", st);

    /* SET_INFO: rejected when stale, and the SIZE is the effect. */
    smb2c_set_next_channel_sequence(a, 10);
    st = smb2_set_eof(a, o.file_id, 4096);
    EXPECT(st == ST_SUCCESS, "R7 SET_INFO EndOfFile 4096 at CS 10 (0x%08x)",
           st);
    sz = file_size(w, "r7", &sz_st);
    EXPECT(sz_st == ST_SUCCESS && sz == 4096,
           "R7 ... the size really is 4096 (%" PRIu64 ")", sz);

    smb2c_set_next_channel_sequence(a, 1);
    st = smb2_set_eof(a, o.file_id, 0);
    EXPECT(st == ST_FILE_NOT_AVAILABLE,
           "R7 SET_INFO at the superseded CS 1 -> FILE_NOT_AVAILABLE"
           " (0x%08x)", st);
    sz = file_size(w, "r7", &sz_st);
    EXPECT(sz_st == ST_SUCCESS && sz == 4096,
           "R7 THE EFFECT: the rejected SET_INFO did not truncate the file"
           " (%" PRIu64 ")", sz);

    /* IOCTL: in the checked set. */
    smb2c_set_next_channel_sequence(a, 10);
    st = smb2_ioctl(a, SMB2_FSCTL_SET_SPARSE, o.file_id, &sparse, 1);
    NOTE("R7 IOCTL FSCTL_SET_SPARSE at the current CS 10 -> 0x%08x", st);

    smb2c_set_next_channel_sequence(a, 1);
    st = smb2_ioctl(a, SMB2_FSCTL_SET_SPARSE, o.file_id, &sparse, 1);
    EXPECT(st == ST_FILE_NOT_AVAILABLE,
           "R7 IOCTL at the superseded CS 1 -> FILE_NOT_AVAILABLE (0x%08x)",
           st);

    /* READ at a stale sequence is NOT rejected, and does NOT move the mark. */
    smb2c_set_next_channel_sequence(a, 1);
    memset(txt, 0, sizeof(txt));
    st = smb2_read(a, o.file_id, 0, 4, (uint8_t *) txt, &n);
    EXPECT(st == ST_SUCCESS,
           "R7 READ at the superseded CS 1 is NOT rejected (0x%08x)", st);

    smb2c_set_next_channel_sequence(a, 10);
    st = smb2_write(a, o.file_id, 0, "ZZZZ", 4, &n);
    EXPECT(st == ST_SUCCESS,
           "R7 ... and it did not move the mark backwards: CS 10 still works"
           " (0x%08x)", st);

    /* A READ that jumps FORWARD, however, DOES move the mark. */
    smb2c_set_next_channel_sequence(a, 200);
    st = smb2_read(a, o.file_id, 0, 4, (uint8_t *) txt, &n);
    EXPECT(st == ST_SUCCESS, "R7 READ at CS 200 (forward) (0x%08x)", st);

    smb2c_set_next_channel_sequence(a, 10);
    st = smb2_write(a, o.file_id, 0, "YYYY", 4, &n);
    NOTE("R7 WRITE back at CS 10 after a forward READ at CS 200 -> 0x%08x",
         st);
    EXPECT(st == ST_FILE_NOT_AVAILABLE,
           "R7 a FORWARD READ advances the tracked high-water sequence, so the"
           " earlier sequence is now stale (0x%08x)", st);
    read_text(a, o.file_id, txt, 4, 200);
    EXPECT(strcmp(txt, "ZZZZ") == 0,
           "R7 THE EFFECT: that write did not land (content '%s')", txt);

    smb2_close(a, o.file_id);
    smb2_quiesce(env);
} /* sec_r7 */

/* ------------------------------------------------------------------------
 * R8  The tracked sequence is per-OPEN.
 *
 * MS-SMB2 3.3.5.2.10 attaches the high-water mark to the Open, not to the
 * session or the connection.  If the model attached it anywhere else, a
 * two-handle trace would diverge immediately.
 * ------------------------------------------------------------------------ */
static void
sec_r8(struct smb2_env *env)
{
    struct smb2_conn      *a;
    struct smb2_create_out o1, o2;
    uint32_t               st, n;

    printf("\n# R8 the ChannelSequence high-water mark is per-Open\n");

    a = smb2_conn_open(env);
    smb2_handshake(a);

    st = smb2_create(a, "r8", FILE_OPEN_IF, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                     NULL, &o1);
    EXPECT(st == ST_SUCCESS, "R8 first open (0x%08x)", st);
    st = smb2_create(a, "r8", FILE_OPEN, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                     NULL, &o2);
    EXPECT(st == ST_SUCCESS, "R8 second open of the same file (0x%08x)", st);
    if (st != ST_SUCCESS) {
        return;
    }
    EXPECT(memcmp(o1.file_id, o2.file_id, 16) != 0,
           "R8 the two opens are distinct handles");

    /* Drive handle 1 far forward. */
    smb2c_set_next_channel_sequence(a, 1000);
    st = smb2_write(a, o1.file_id, 0, "AAAA", 4, &n);
    EXPECT(st == ST_SUCCESS, "R8 handle 1 advanced to CS 1000 (0x%08x)", st);

    /* Handle 2 was seeded at 0 by its own CREATE and is unaffected. */
    smb2c_set_next_channel_sequence(a, 1);
    st = smb2_write(a, o2.file_id, 0, "BBBB", 4, &n);
    EXPECT(st == ST_SUCCESS,
           "R8 handle 2 accepts CS 1: the mark did not leak across opens"
           " (0x%08x)", st);

    /* ... and handle 1 still rejects it. */
    smb2c_set_next_channel_sequence(a, 1);
    st = smb2_write(a, o1.file_id, 0, "CCCC", 4, &n);
    EXPECT(st == ST_FILE_NOT_AVAILABLE,
           "R8 handle 1 still rejects CS 1 (0x%08x)", st);

    smb2_close(a, o1.file_id);
    smb2_close(a, o2.file_id);
    smb2_quiesce(env);
} /* sec_r8 */

/* ------------------------------------------------------------------------
 * R9  SMB2_FLAGS_REPLAY_OPERATION plays NO part in the ChannelSequence rule.
 *
 * Worth its own section because it is a live question in the tree: chimera's
 * comment on chimera_smb_channel_sequence_stale records that the Windows
 * reference behavior treats a replayed and a fresh forward jump identically,
 * contradicting the premise of issue #1290.  If the model gated the rule on
 * the flag it would be wrong in both directions, so both are measured.
 * ------------------------------------------------------------------------ */
static void
sec_r9(struct smb2_env *env)
{
    struct smb2_conn      *a;
    struct smb2_create_out o;
    uint32_t               st, n;
    char                   txt[16];

    printf("\n# R9 the replay flag does not change the ChannelSequence rule\n");

    a = smb2_conn_open(env);
    smb2_handshake(a);

    st = smb2_create(a, "r9", FILE_OPEN_IF, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                     NULL, &o);
    EXPECT(st == ST_SUCCESS, "R9 open (0x%08x)", st);
    if (st != ST_SUCCESS) {
        return;
    }

    smb2c_set_next_channel_sequence(a, 50);
    st = smb2_write(a, o.file_id, 0, "AAAA", 4, &n);
    EXPECT(st == ST_SUCCESS, "R9 advance to CS 50 (0x%08x)", st);

    /* stale + replay flag is still stale */
    smb2c_set_next_channel_sequence(a, 1);
    smb2c_set_next_flags(a, SMB2_FLAGS_REPLAY_OPERATION);
    st = smb2_write(a, o.file_id, 0, "BBBB", 4, &n);
    EXPECT(st == ST_FILE_NOT_AVAILABLE,
           "R9 a REPLAY-flagged stale WRITE is still rejected (0x%08x)", st);
    read_text(a, o.file_id, txt, 4, 50);
    EXPECT(strcmp(txt, "AAAA") == 0,
           "R9 THE EFFECT: the flag bought it nothing (content '%s')", txt);

    /* forward + replay flag still advances */
    smb2c_set_next_channel_sequence(a, 60);
    smb2c_set_next_flags(a, SMB2_FLAGS_REPLAY_OPERATION);
    st = smb2_write(a, o.file_id, 0, "CCCC", 4, &n);
    EXPECT(st == ST_SUCCESS,
           "R9 a REPLAY-flagged forward WRITE is accepted (0x%08x)", st);

    smb2c_set_next_channel_sequence(a, 50);
    st = smb2_write(a, o.file_id, 0, "DDDD", 4, &n);
    EXPECT(st == ST_FILE_NOT_AVAILABLE,
           "R9 ... and it ADVANCED the mark exactly as a fresh request would"
           " (0x%08x)", st);

    smb2_close(a, o.file_id);
    smb2_quiesce(env);
} /* sec_r9 */

/* ------------------------------------------------------------------------
 * R10  Two consequences of the comparison being MODULAR that a model built
 * from the prose alone would get wrong.
 *
 *  (a) A forward jump is bounded.  delta is computed mod 2^16 and "stale" is
 *      delta >= 0x8000, so a single step of MORE than +0x7FFF is indis-
 *      tinguishable from going backwards and is REFUSED.  The sequence space
 *      is a circle, not a line.
 *
 *  (b) ChannelSequence 0 is not a neutral value.  Every request carries the
 *      field, and a client that never sets it sends 0.  Against a mark in the
 *      upper half of the space, 0 is a FORWARD jump -- so an unadorned request
 *      is accepted and RESETS the mark to 0, after which the high sequences
 *      that were valid a moment ago are stale.  (This probe tripped over it:
 *      an innocent verification read at an unset sequence silently moved the
 *      mark out from under the test.)
 * ------------------------------------------------------------------------ */
static void
sec_r10(struct smb2_env *env)
{
    struct smb2_conn      *a;
    struct smb2_create_out o;
    uint32_t               st, n;

    printf("\n# R10 consequences of the modular comparison\n");

    a = smb2_conn_open(env);
    smb2_handshake(a);

    st = smb2_create(a, "r10", FILE_OPEN_IF, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                     NULL, &o);
    EXPECT(st == ST_SUCCESS, "R10 open, mark seeded at 0 (0x%08x)", st);
    if (st != ST_SUCCESS) {
        return;
    }

    /* (a) a jump of more than +0x7FFF in one step is refused */
    smb2c_set_next_channel_sequence(a, 0x9000);
    st = smb2_write(a, o.file_id, 0, "AAAA", 4, &n);
    EXPECT(st == ST_FILE_NOT_AVAILABLE,
           "R10a a single jump of +0x9000 from mark 0 is STALE, not forward:"
           " the space is modular (0x%08x)", st);

    /* ... but the same distance in two legal steps is fine */
    smb2c_set_next_channel_sequence(a, 0x7000);
    st = smb2_write(a, o.file_id, 0, "BBBB", 4, &n);
    EXPECT(st == ST_SUCCESS, "R10a step 1 to 0x7000 (0x%08x)", st);
    smb2c_set_next_channel_sequence(a, 0xE000);
    st = smb2_write(a, o.file_id, 0, "CCCC", 4, &n);
    EXPECT(st == ST_SUCCESS, "R10a step 2 to 0xE000 (0x%08x)", st);

    /* (b) an unset ChannelSequence (0) is forward from 0xE000 */
    st = smb2_write(a, o.file_id, 0, "DDDD", 4, &n);
    EXPECT(st == ST_SUCCESS,
           "R10b a request with an UNSET ChannelSequence (0) is accepted from"
           " a mark of 0xE000 -- 0 is forward by 0x2000 (0x%08x)", st);

    smb2c_set_next_channel_sequence(a, 0xE000);
    st = smb2_write(a, o.file_id, 0, "EEEE", 4, &n);
    EXPECT(st == ST_FILE_NOT_AVAILABLE,
           "R10b ... and it RESET the mark to 0: 0xE000 is now stale"
           " (0x%08x)", st);

    smb2_close(a, o.file_id);
    smb2_quiesce(env);
} /* sec_r10 */

/* ------------------------------------------------------------------------
 * R11  Where the Open's high-water sequence COMES FROM.
 *
 * chimera seeds it from the CREATE's own ChannelSequence (smb_proc_create.c
 * sets channel_sequence / channel_sequence_valid at grant time) rather than
 * starting every Open at 0 and letting the first operation seed it.  The two
 * are indistinguishable in every test above, because every CREATE there
 * carried sequence 0 -- so this row is the one that separates them, and the
 * model needs the answer to predict the first operation on a handle opened by
 * a client that has already failed over once.
 * ------------------------------------------------------------------------ */
static void
sec_r11(struct smb2_env *env)
{
    struct smb2_conn      *a;
    struct smb2_create_out o;
    uint32_t               st, n;

    printf("\n# R11 the Open's sequence is seeded by its CREATE\n");

    a = smb2_conn_open(env);
    smb2_handshake(a);

    /* The CREATE itself carries ChannelSequence 0x0100. */
    smb2c_set_next_channel_sequence(a, 0x0100);
    st = smb2_create(a, "r11", FILE_OPEN_IF, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                     NULL, &o);
    EXPECT(st == ST_SUCCESS, "R11 CREATE at CS 0x0100 (0x%08x)", st);
    if (st != ST_SUCCESS) {
        return;
    }

    /* A WRITE at 0 is delta 0xFF00 from 0x0100 -- stale IF the CREATE seeded
     * the Open, and an ordinary forward-from-0 if it did not. */
    smb2c_set_next_channel_sequence(a, 0);
    st = smb2_write(a, o.file_id, 0, "AAAA", 4, &n);
    NOTE("R11 WRITE at CS 0 against an Open created at CS 0x0100 -> 0x%08x",
         st);
    EXPECT(st == ST_FILE_NOT_AVAILABLE,
           "R11 the CREATE SEEDED the Open at 0x0100, so CS 0 is already"
           " stale: an Open does not start at 0 (0x%08x)", st);

    smb2c_set_next_channel_sequence(a, 0x0100);
    st = smb2_write(a, o.file_id, 0, "BBBB", 4, &n);
    EXPECT(st == ST_SUCCESS,
           "R11 ... and the seeded value itself is accepted (0x%08x)", st);

    smb2_close(a, o.file_id);
    smb2_quiesce(env);
} /* sec_r11 */

/* ------------------------------------------------------------------------
 * R12  WHERE the ChannelSequence check sits among the other rejections.
 *
 * A model has to place the gate, not merely own it: if the gate runs before
 * the access check the model answers FILE_NOT_AVAILABLE where the server
 * answers ACCESS_DENIED, and every trace that lands on that pair diverges.
 * chimera's WRITE handler resolves the handle, rejects on access, and only
 * then consults the sequence -- measured here rather than read off the source.
 * ------------------------------------------------------------------------ */
static void
sec_r12(struct smb2_env *env)
{
    struct smb2_conn      *a;
    struct smb2_create_out ro, rw;
    uint32_t               st, n;

    printf("\n# R12 ordering of the ChannelSequence gate\n");

    a = smb2_conn_open(env);
    smb2_handshake(a);

    /* Two handles on one file: one read-only, one writable.  Both are seeded
     * at ChannelSequence 0 by their CREATEs, so a request at 0x9000 is a jump
     * of +0x9000 -- STALE under the modular rule (R10a). */
    st = smb2_create(a, "r12", FILE_OPEN_IF, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                     NULL, &rw);
    EXPECT(st == ST_SUCCESS, "R12 writable open (0x%08x)", st);
    st = smb2_create(a, "r12", FILE_OPEN, FILE_READ_ACCESS, FILE_SHARE_RWD,
                     NULL, &ro);
    EXPECT(st == ST_SUCCESS, "R12 read-only open of the same file (0x%08x)",
           st);
    if (st != ST_SUCCESS) {
        return;
    }

    /* The control: on the WRITABLE handle that request really is stale. */
    smb2c_set_next_channel_sequence(a, 0x9000);
    st = smb2_write(a, rw.file_id, 0, "AAAA", 4, &n);
    EXPECT(st == ST_FILE_NOT_AVAILABLE,
           "R12 control: CS 0x9000 from a mark of 0 IS stale (0x%08x)", st);

    /* The same request on the READ-ONLY handle: both faults apply at once. */
    smb2c_set_next_channel_sequence(a, 0x9000);
    st = smb2_write(a, ro.file_id, 0, "AAAA", 4, &n);
    NOTE("R12 a WRITE that is both unauthorized and stale -> 0x%08x", st);
    EXPECT(st == ST_ACCESS_DENIED,
           "R12 ACCESS_DENIED outranks the stale sequence: the gate sits AFTER"
           " the access check (0x%08x)", st);

    /* SET_INFO is the OTHER way round, and the asymmetry is real: chimera's
     * SET_INFO consults the sequence immediately after resolving the handle,
     * before any per-info-class check -- so on SET_INFO a stale sequence
     * outranks the access fault that outranked it on WRITE. */
    smb2c_set_next_channel_sequence(a, 0x9000);
    st = smb2_set_eof(a, ro.file_id, 0);
    NOTE("R12 SET_INFO EndOfFile on a read-only handle at a stale CS ->"
         " 0x%08x", st);
    EXPECT(st == ST_FILE_NOT_AVAILABLE,
           "R12 on SET_INFO the sequence gate runs BEFORE the class check, so"
           " it outranks what WRITE lets outrank it (0x%08x)", st);

    /* And a closed handle outranks both -- the gate needs a resolved Open. */
    smb2_close(a, ro.file_id);
    smb2c_set_next_channel_sequence(a, 0x9000);
    st = smb2_write(a, ro.file_id, 0, "AAAA", 4, &n);
    EXPECT(st == ST_FILE_CLOSED,
           "R12 FILE_CLOSED outranks it too: the gate needs a resolved Open"
           " (0x%08x)", st);

    smb2_close(a, rw.file_id);
    smb2_quiesce(env);
} /* sec_r12 */

/* ------------------------------------------------------------------------
 * R13  Which requests close the replay-eligibility window.
 *
 * chimera clears IsReplayEligible inside chimera_smb_open_file_resolve, so
 * every operation that resolves a handle by FileId closes the window -- and
 * an OPLOCK-form break acknowledgment resolves by FileId while a LEASE-form
 * one resolves by lease key through a different function.  That predicts a
 * split, which is exactly the kind of prediction that must be measured before
 * a model encodes it.
 * ------------------------------------------------------------------------ */
static void
sec_r13(struct smb2_env *env)
{
    struct smb2_conn       *a, *b;
    struct smb2_oplock_req  oreq;
    struct smb2_durable_req dur;
    struct smb2_create_out  o, r, ob;
    struct smb2_break       brk;
    uint32_t                st;
    int                     got;

    printf("\n# R13 what closes the replay-eligibility window\n");

    a = smb2_conn_open(env);
    b = smb2_conn_open(env);
    smb2_handshake(a);
    smb2_handshake(b);

    /* (1) a QUERY -- a NON-mutating, fid-targeted op -- closes it. */
    memset(&dur, 0, sizeof(dur));
    dur.dh2q = 1;
    fill_guid(dur.create_guid, 0x61);
    st = smb2_create_dur(a, "r13a", FILE_CREATE, FILE_ALL_ACCESS,
                         FILE_SHARE_RWD, NULL, &dur, &o);
    EXPECT(st == ST_SUCCESS, "R13/1 replay-eligible create (0x%08x)", st);
    {
        char     txt[8];
        uint32_t n = 0;

        smb2_read(a, o.file_id, 0, 4, (uint8_t *) txt, &n);
    }
    smb2c_set_next_flags(a, SMB2_FLAGS_REPLAY_OPERATION);
    st = smb2_create_dur(a, "r13a", FILE_CREATE, FILE_ALL_ACCESS,
                         FILE_SHARE_RWD, NULL, &dur, &r);
    NOTE("R13/1 replay after a READ -> 0x%08x", st);
    EXPECT(st != ST_SUCCESS || memcmp(r.file_id, o.file_id, 16) != 0,
           "R13/1 a plain READ closes the window: the replay no longer"
           " resolves to the original handle (0x%08x)", st);
    if (st == ST_SUCCESS) {
        smb2_close(a, r.file_id);
    }
    smb2_close(a, o.file_id);
    smb2_quiesce(env);

    /* (2) an OPLOCK-form break acknowledgment resolves by FileId. */
    mk_oplock(&oreq, SMB2_OPLOCK_LEVEL_BATCH);
    memset(&dur, 0, sizeof(dur));
    dur.dh2q = 1;
    fill_guid(dur.create_guid, 0x62);
    st = smb2_create_dur(a, "r13b", FILE_CREATE, FILE_ALL_ACCESS,
                         FILE_SHARE_RWD, &oreq, &dur, &o);
    EXPECT(st == ST_SUCCESS && o.oplock == SMB2_OPLOCK_LEVEL_BATCH,
           "R13/2 batch-oplocked replay-eligible create (0x%08x opl 0x%02x)",
           st, o.oplock);
    if (st != ST_SUCCESS) {
        return;
    }

    /* B's conflicting open breaks A's batch oplock. */
    smb2_create_post(b, "r13b", FILE_OPEN, FILE_READ_ACCESS, FILE_SHARE_RWD,
                     NULL);
    smb2_quiesce(env);
    got = smb2_conn_pop_break(a, &brk);
    EXPECT(got && !brk.is_lease,
           "R13/2 A received an oplock break notification (got %d lease %d)",
           got, got ? brk.is_lease : -1);
    if (got) {
        st = smb2_oplock_break_ack(a, brk.file_id, SMB2_OPLOCK_LEVEL_II);
        NOTE("R13/2 oplock break ack -> 0x%08x", st);
    }
    smb2_quiesce(env);
    smb2c_wait(b);
    smb2c_parse_create(b, &ob);
    if (ob.status == ST_SUCCESS) {
        smb2_close(b, ob.file_id);
    }
    smb2_quiesce(env);

    smb2c_set_next_flags(a, SMB2_FLAGS_REPLAY_OPERATION);
    st = smb2_create_dur(a, "r13b", FILE_CREATE, FILE_ALL_ACCESS,
                         FILE_SHARE_RWD, &oreq, &dur, &r);
    NOTE("R13/2 replay after an OPLOCK-form break ack -> 0x%08x, same fid %d",
         st, st == ST_SUCCESS ? memcmp(r.file_id, o.file_id, 16) == 0 : -1);
    EXPECT(st != ST_SUCCESS || memcmp(r.file_id, o.file_id, 16) != 0,
           "R13/2 an OPLOCK-form break ack resolves by FileId and therefore"
           " CLOSES the window (0x%08x)", st);
    if (st == ST_SUCCESS) {
        smb2_close(a, r.file_id);
    }
    smb2_close(a, o.file_id);
    smb2_quiesce(env);
} /* sec_r13 */

/* ------------------------------------------------------------------------
 * R14  WHEN the mark advances: at the gate, or when the operation succeeds?
 *
 * The distinction is invisible until an operation clears the sequence gate and
 * is then refused for some LATER reason.  chimera advances inside the gate
 * itself (chimera_smb_channel_sequence_stale advances on its accept path), and
 * on WRITE that gate runs before byte-range-lock enforcement -- so a write
 * refused for a lock conflict has still moved the mark.  A model that advanced
 * only on success would answer the NEXT request wrongly, which is why this is
 * measured rather than reasoned about.
 * ------------------------------------------------------------------------ */
static void
sec_r14(struct smb2_env *env)
{
    struct smb2_conn      *a, *b;
    struct smb2_create_out oa, ob;
    uint32_t               st, n;

    printf("\n# R14 the mark advances at the gate, not at success\n");

    a = smb2_conn_open(env);
    b = smb2_conn_open(env);
    smb2_handshake(a);
    smb2_handshake(b);

    st = smb2_create(a, "r14", FILE_OPEN_IF, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                     NULL, &oa);
    EXPECT(st == ST_SUCCESS, "R14 opener A (0x%08x)", st);
    st = smb2_create(b, "r14", FILE_OPEN, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                     NULL, &ob);
    EXPECT(st == ST_SUCCESS, "R14 opener B (0x%08x)", st);
    if (st != ST_SUCCESS) {
        return;
    }

    /* A takes an exclusive byte-range lock over [0, 16). */
    st = smb2_lock(a, oa.file_id, 0, 16,
                   SMB2_LOCKFLAG_EXCLUSIVE | SMB2_LOCKFLAG_FAIL_IMMEDIATELY);
    EXPECT(st == ST_SUCCESS, "R14 A holds an exclusive lock on [0,16) (0x%08x)",
           st);
    if (st != ST_SUCCESS) {
        return;
    }

    /* B writes into the locked range at ChannelSequence 100.  The sequence is
     * forward, so the gate PASSES; the lock then refuses the write. */
    smb2c_set_next_channel_sequence(b, 100);
    st = smb2_write(b, ob.file_id, 0, "AAAA", 4, &n);
    NOTE("R14 B's write into the locked range at CS 100 -> 0x%08x", st);
    EXPECT(st == ST_FILE_LOCK_CONFLICT || st == ST_LOCK_NOT_GRANTED,
           "R14 the write cleared the sequence gate and was refused by the"
           " LOCK (0x%08x)", st);

    /* Now the question: did that refused write move B's mark to 100?  A write
     * OUTSIDE the locked range at the earlier sequence answers it. */
    smb2c_set_next_channel_sequence(b, 1);
    st = smb2_write(b, ob.file_id, 32, "BBBB", 4, &n);
    NOTE("R14 B's unconflicted write at CS 1 afterwards -> 0x%08x", st);
    EXPECT(st == ST_FILE_NOT_AVAILABLE,
           "R14 the lock-refused write ALREADY advanced the mark: the gate"
           " advances, success does not (0x%08x)", st);

    /* ... and at the advanced sequence it goes through. */
    smb2c_set_next_channel_sequence(b, 100);
    st = smb2_write(b, ob.file_id, 32, "BBBB", 4, &n);
    EXPECT(st == ST_SUCCESS,
           "R14 ... and CS 100 is now the mark (0x%08x)", st);

    smb2_close(b, ob.file_id);
    smb2_close(a, oa.file_id);
    smb2_quiesce(env);
} /* sec_r14 */

/* ------------------------------------------------------------------------
 * R15  Is a SET_INFO access-checked at all?
 *
 * R12 measured only the STALE arm of SET_INFO -- a read-only handle at a
 * superseded ChannelSequence answers FILE_NOT_AVAILABLE -- and concluded that
 * the sequence gate runs BEFORE the per-class check.  That is true, and it
 * left the more basic question unasked: with a FRESH sequence, does the class
 * check reject the unauthorized set at all?
 *
 * MS-FSA 2.1.5.14 answers it.  Setting file information is gated on the access
 * the class requires -- FileEndOfFileInformation and FileAllocationInformation
 * on FILE_WRITE_DATA -- and the operation MUST be failed with
 * STATUS_ACCESS_DENIED when Open.GrantedAccess does not contain it.  MS-SMB2
 * 3.3.5.21 routes SMB2 SET_INFO straight into that algorithm.
 *
 * Found by the generated exactly-once corpus (smb2Replay/stepReplay), which
 * drew a SET_EOF on a read-only handle at a non-stale sequence and disagreed
 * with the model.  Measured here so the finding rests on a run of its own, and
 * asserted on the EFFECT (the file's size afterwards) as well as the status:
 * a status alone cannot tell "refused" from "accepted and did nothing".
 * ------------------------------------------------------------------------ */
static void
sec_r15(struct smb2_env *env)
{
    struct smb2_conn      *a;
    struct smb2_create_out rw, ro, at, chk;
    uint32_t               st;

    printf("\n# R15 the SET_INFO access check\n");

    a = smb2_conn_open(env);
    smb2_handshake(a);

    /* The control first: on a WRITABLE handle a SET_EOF really does resize. */
    st = smb2_create(a, "r15", FILE_OPEN_IF, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                     NULL, &rw);
    EXPECT(st == ST_SUCCESS, "R15 writable open (0x%08x)", st);
    st = smb2_set_eof(a, rw.file_id, 4096);
    EXPECT(st == ST_SUCCESS, "R15 control: SET_EOF on a writable handle "
           "(0x%08x)", st);
    st = smb2_create(a, "r15", FILE_OPEN, FILE_READ_ACCESS, FILE_SHARE_RWD,
                     NULL, &chk);
    EXPECT(st == ST_SUCCESS && chk.end_of_file == 4096,
           "R15 control: the file is %llu bytes, expected 4096",
           (unsigned long long) chk.end_of_file);
    smb2_close(a, chk.file_id);

    /* A handle with FILE_READ_DATA and no FILE_WRITE_DATA. */
    st = smb2_create(a, "r15", FILE_OPEN, FILE_READ_ACCESS, FILE_SHARE_RWD,
                     NULL, &ro);
    EXPECT(st == ST_SUCCESS, "R15 read-only open (0x%08x)", st);
    st = smb2_set_eof(a, ro.file_id, 8192);
    NOTE("R15 SET_EOF through a read-only handle -> 0x%08x", st);
    if (st == ST_ACCESS_DENIED) {
        EXPECT(1, "R15 MS-FSA 2.1.5.14: SET_EOF without FILE_WRITE_DATA is "
               "ACCESS_DENIED");
    } else {
        DEVIATION("S-4", "SET_EOF through a handle with no FILE_WRITE_DATA "
                  "answered 0x%08x; MS-FSA 2.1.5.14 mandates "
                  "STATUS_ACCESS_DENIED", st);
    }
    st = smb2_create(a, "r15", FILE_OPEN, FILE_READ_ACCESS, FILE_SHARE_RWD,
                     NULL, &chk);
    if (st == ST_SUCCESS) {
        if (chk.end_of_file == 4096) {
            EXPECT(1, "R15 the unauthorized set changed nothing (4096 bytes)");
        } else {
            DEVIATION("S-4", "the unauthorized set TOOK EFFECT: the file is "
                      "%llu bytes, was 4096",
                      (unsigned long long) chk.end_of_file);
        }
        smb2_close(a, chk.file_id);
    }
    smb2_close(a, ro.file_id);

    /* And an ATTRIBUTE-ONLY handle: FILE_READ_ATTRIBUTES alone, which is the
     * shape the generated corpus drew (an open whose DesiredAccess carries no
     * R/W/D at all).  It has strictly less access than the read-only handle
     * above, so a server that refuses one must refuse this. */
    st = smb2_create(a, "r15", FILE_OPEN, FILE_READ_ATTRIBUTES,
                     FILE_SHARE_RWD, NULL, &at);
    EXPECT(st == ST_SUCCESS, "R15 attribute-only open (0x%08x)", st);
    if (st == ST_SUCCESS) {
        st = smb2_set_eof(a, at.file_id, 16384);
        NOTE("R15 SET_EOF through an attribute-only handle -> 0x%08x", st);
        if (st != ST_ACCESS_DENIED) {
            DEVIATION("S-4", "SET_EOF through an attribute-only handle "
                      "answered 0x%08x; MS-FSA 2.1.5.14 mandates "
                      "STATUS_ACCESS_DENIED", st);
        }
        smb2_close(a, at.file_id);
    }

    smb2_close(a, rw.file_id);
    smb2_quiesce(env);
} /* sec_r15 */

/* ------------------------------------------------------------------------
 * R16  WHAT a replayed CREATE reports when the replay asks for something
 *      DIFFERENT from what the original was granted.
 *
 * R3c pinned the identical-request case: replay the same DH2Q create and the
 * durable grant is re-advertised.  A generated exactly-once trace drew the
 * case R3c cannot see -- a replay carrying the same CreateGuid but a LESSER
 * oplock request -- and the model, which reports the OPEN's current grant,
 * disagreed with the wire twice over.
 *
 * chimera says why in its own source, citing the WPTS case: on an oplock (non
 * lease) create_guid replay the reply echoes "the requested oplock level", and
 * build_dh2q_response refuses to emit a durable response at all unless the
 * REQUESTED level is BATCH ("MS-SMB2 replay-dhv2-oplock2 expects
 * durable_open_v2=false, timeout=0, no blobs").  A LEASE handle is the other
 * way round: the reply reports the grant's CURRENT granted mode, because a
 * coalesced lease may have been upgraded by a sibling open since.
 *
 * Measured here so the model's rule rests on a run.  DEVIATIONS-SMB.md M-15.
 * ------------------------------------------------------------------------ */
static void
sec_r16(struct smb2_env *env)
{
    struct smb2_conn       *a;
    struct smb2_oplock_req  oreq;
    struct smb2_durable_req dur;
    struct smb2_create_out  o, r;
    uint32_t                st;

    printf("\n# R16 a replay that asks for a different oplock than it holds\n");

    a = smb2_conn_open(env);
    smb2_handshake(a);

    /* The original: BATCH + DH2Q, so the open really does hold a batch oplock
     * and a durable v2 grant. */
    mk_oplock(&oreq, SMB2_OPLOCK_LEVEL_BATCH);
    memset(&dur, 0, sizeof(dur));
    dur.dh2q = 1;
    fill_guid(dur.create_guid, 0x61);
    st = smb2_create_dur(a, "r16", FILE_OPEN_IF, FILE_ALL_ACCESS,
                         FILE_SHARE_RWD, &oreq, &dur, &o);
    EXPECT(st == ST_SUCCESS && o.oplock == SMB2_OPLOCK_LEVEL_BATCH &&
           o.has_dh2q,
           "R16 original: BATCH + durable v2 (0x%08x oplock 0x%02x dh2q %d)",
           st, o.oplock, o.has_dh2q);
    if (st != ST_SUCCESS) {
        return;
    }

    /* The replay, same CreateGuid, asking for NO oplock at all. */
    smb2c_set_next_flags(a, SMB2_FLAGS_REPLAY_OPERATION);
    st = smb2_create_dur(a, "r16", FILE_OPEN_IF, FILE_ALL_ACCESS,
                         FILE_SHARE_RWD, NULL, &dur, &r);
    EXPECT(st == ST_SUCCESS && memcmp(r.file_id, o.file_id, 16) == 0,
           "R16 the replay returns the ORIGINAL handle (0x%08x)", st);
    EXPECT(r.oplock == SMB2_OPLOCK_LEVEL_NONE,
           "R16 the reply echoes the REQUESTED oplock level, not the one the "
           "open holds: 0x%02x (open holds BATCH)", r.oplock);
    EXPECT(!r.has_dh2q,
           "R16 and emits NO durable response, because the requested level "
           "could not itself have earned one (has_dh2q %d)", r.has_dh2q);

    /* The same replay asking for BATCH: both come back. */
    mk_oplock(&oreq, SMB2_OPLOCK_LEVEL_BATCH);
    smb2c_set_next_flags(a, SMB2_FLAGS_REPLAY_OPERATION);
    st = smb2_create_dur(a, "r16", FILE_OPEN_IF, FILE_ALL_ACCESS,
                         FILE_SHARE_RWD, &oreq, &dur, &r);
    EXPECT(st == ST_SUCCESS && memcmp(r.file_id, o.file_id, 16) == 0 &&
           r.oplock == SMB2_OPLOCK_LEVEL_BATCH && r.has_dh2q,
           "R16 asking for BATCH again reports BATCH and the durable grant "
           "(0x%08x oplock 0x%02x dh2q %d)", st, r.oplock, r.has_dh2q);

    /* A LEVEL_II request against the same batch-oplocked open: echoed, and
     * still no durable response -- LEVEL_II could not have earned one. */
    mk_oplock(&oreq, SMB2_OPLOCK_LEVEL_II);
    smb2c_set_next_flags(a, SMB2_FLAGS_REPLAY_OPERATION);
    st = smb2_create_dur(a, "r16", FILE_OPEN_IF, FILE_ALL_ACCESS,
                         FILE_SHARE_RWD, &oreq, &dur, &r);
    EXPECT(st == ST_SUCCESS && r.oplock == SMB2_OPLOCK_LEVEL_II && !r.has_dh2q,
           "R16 a LEVEL_II replay echoes LEVEL_II with no durable response "
           "(0x%08x oplock 0x%02x dh2q %d)", st, r.oplock, r.has_dh2q);
    smb2_close(a, o.file_id);

    /* The LEASE half: a lease handle reports the GRANT's current state and
     * keeps its durable response, whatever epoch the replay presents. */
    memset(&dur, 0, sizeof(dur));
    dur.dh2q = 1;
    fill_guid(dur.create_guid, 0x62);
    mk_lease(&oreq, 0x62, SMB2_LEASE_READ | SMB2_LEASE_HANDLE |
             SMB2_LEASE_WRITE);
    st = smb2_create_dur(a, "r16b", FILE_OPEN_IF, FILE_ALL_ACCESS,
                         FILE_SHARE_RWD, &oreq, &dur, &o);
    EXPECT(st == ST_SUCCESS && o.oplock == SMB2_OPLOCK_LEVEL_LEASE &&
           o.has_dh2q,
           "R16 lease original: RWH + durable v2 (0x%08x lease 0x%02x dh2q %d)",
           st, o.lease_state, o.has_dh2q);
    mk_lease(&oreq, 0x62, SMB2_LEASE_READ);
    smb2c_set_next_flags(a, SMB2_FLAGS_REPLAY_OPERATION);
    st = smb2_create_dur(a, "r16b", FILE_OPEN_IF, FILE_ALL_ACCESS,
                         FILE_SHARE_RWD, &oreq, &dur, &r);
    EXPECT(st == ST_SUCCESS && memcmp(r.file_id, o.file_id, 16) == 0,
           "R16 the lease replay returns the ORIGINAL handle (0x%08x)", st);
    EXPECT(r.lease_state == o.lease_state,
           "R16 a lease replay reports the GRANT's current state, not the "
           "requested one: 0x%02x (asked for R alone)", r.lease_state);
    EXPECT(r.has_dh2q,
           "R16 and keeps its durable response (has_dh2q %d)", r.has_dh2q);
    smb2_close(a, o.file_id);

    smb2_quiesce(env);
} /* sec_r16 */

/* ------------------------------------------------------------------------
 * R17  A reclaim RESEEDS the Open's ChannelSequence high-water mark.
 *
 * The mark tracks a channel (MS-SMB2 3.3.5.2.10) and a reclaim arrives on a
 * new one, so the question is whether the surviving Open keeps the mark it had
 * before the drop or takes the reclaiming CREATE's.  chimera reseeds
 * (chimera_smb_durable_rehome: "Reseed the channel-sequence baseline from the
 * reconnecting CREATE"), which a generated exactly-once trace found the hard
 * way: the model kept a pre-drop mark of 3 and refused a WRITE at 0 that the
 * wire executed.
 *
 * Measured with a mark deliberately far from the reclaiming sequence, so
 * "kept" and "reseeded" give opposite verdicts for both following writes.
 * DEVIATIONS-SMB.md M-16.
 * ------------------------------------------------------------------------ */
static void
sec_r17(struct smb2_env *env)
{
    struct smb2_conn       *a, *b;
    struct smb2_oplock_req  oreq;
    struct smb2_durable_req dur, rec;
    struct smb2_create_out  o, r;
    uint32_t                st, n;

    printf("\n# R17 a reclaim reseeds the ChannelSequence mark\n");

    a = smb2_conn_open(env);
    smb2_handshake(a);

    mk_oplock(&oreq, SMB2_OPLOCK_LEVEL_BATCH);
    memset(&dur, 0, sizeof(dur));
    dur.dh2q = 1;
    fill_guid(dur.create_guid, 0x71);
    st = smb2_create_dur(a, "r17", FILE_OPEN_IF, FILE_ALL_ACCESS,
                         FILE_SHARE_RWD, &oreq, &dur, &o);
    EXPECT(st == ST_SUCCESS && o.has_dh2q,
           "R17 durable batch open, seeded at ChannelSequence 0 (0x%08x)", st);
    if (st != ST_SUCCESS || !o.has_dh2q) {
        return;
    }

    /* Drive the mark up to 100. */
    smb2c_set_next_channel_sequence(a, 100);
    st = smb2_write(a, o.file_id, 0, "AAAA", 4, &n);
    EXPECT(st == ST_SUCCESS, "R17 a write at CS 100 advances the mark (0x%08x)",
           st);
    smb2c_set_next_channel_sequence(a, 99);
    st = smb2_write(a, o.file_id, 0, "BBBB", 4, &n);
    EXPECT(st == ST_FILE_NOT_AVAILABLE,
           "R17 control: CS 99 is now stale (0x%08x)", st);

    /* Drop, reclaim with a CREATE carrying ChannelSequence 5. */
    smb2_conn_disconnect(a);
    b = smb2_conn_reopen(env, a);
    wait_parked(b, o.file_id);

    memset(&rec, 0, sizeof(rec));
    rec.dh2c = 1;
    memcpy(rec.file_id, o.file_id, 16);
    fill_guid(rec.create_guid, 0x71);
    smb2c_set_next_channel_sequence(b, 5);
    st = smb2_create_dur(b, "", FILE_OPEN, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                         NULL, &rec, &r);
    EXPECT(st == ST_SUCCESS && memcmp(r.file_id, o.file_id, 16) == 0,
           "R17 the DH2C reclaim returns the original handle (0x%08x)", st);
    if (st != ST_SUCCESS) {
        return;
    }

    /* If the mark had been KEPT at 100, a write at 5 would be stale; if it was
     * RESEEDED to 5, a write at 5 is accepted and one at 4 is stale. */
    smb2c_set_next_channel_sequence(b, 5);
    st = smb2_write(b, r.file_id, 0, "CCCC", 4, &n);
    EXPECT(st == ST_SUCCESS,
           "R17 a write at the reclaiming CREATE's own CS 5 is accepted, so "
           "the mark was RESEEDED, not kept at 100 (0x%08x)", st);
    smb2c_set_next_channel_sequence(b, 4);
    st = smb2_write(b, r.file_id, 0, "DDDD", 4, &n);
    EXPECT(st == ST_FILE_NOT_AVAILABLE,
           "R17 and CS 4 is stale against the reseeded mark (0x%08x)", st);

    smb2_close(b, r.file_id);
    smb2_quiesce(env);
} /* sec_r17 */

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

    (void) argc;
    (void) argv;

    smb2_env_start_opts(&env, &opts);
    printf("# replay/ChannelSequence probe up; persistent_handles on\n");

    sec_r1(&env);
    sec_r2(&env);
    sec_r3(&env);
    sec_r4(&env);
    sec_r5(&env);
    sec_r6(&env);
    sec_r7(&env);
    sec_r8(&env);
    sec_r9(&env);
    sec_r10(&env);
    sec_r11(&env);
    sec_r12(&env);
    sec_r13(&env);
    sec_r14(&env);
    sec_r15(&env);
    sec_r16(&env);
    sec_r17(&env);

    smb2_env_stop(&env);

    printf("\n# summary: %d recorded deviation(s) (see DEVIATIONS-SMB.md)\n",
           ndev);
    if (nfail) {
        printf("%d SMB2 replay/ChannelSequence check(s) FAILED\n", nfail);
        return 1;
    }
    printf("all SMB2 replay/ChannelSequence mandate checks passed"
           " (%d documented deviation(s))\n", ndev);
    return 0;
} /* main */
