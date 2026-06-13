// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Unit tests for the NFS4.1 SEQUENCE replay slot state machine.
 * Drives nfs4_replay_slot_acquire / nfs4_replay_slot_finalize directly
 * against a real session built via the production factory functions.
 */

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "common/logging.h"
#include "nfs_common.h"
#include "nfs4_session.h"

/* Active in both debug (NDEBUG unset) and release (NDEBUG set) -- CHECK(3)
 * compiles to a no-op under NDEBUG, which causes -Wunused-but-set-variable
 * for inputs only consumed by asserts.  CHECK() always evaluates and dies
 * on failure. */
#define CHECK(cond) do { \
            if (!(cond)) { \
                fprintf(stderr, "%s:%d: CHECK failed: %s\n", \
                        __FILE__, __LINE__, #cond); \
                abort(); \
            } \
} while (0)

/* Minimum number of slots we'll request from the server cap.  Several
 * tests use slot 0 + slot 1 + an out-of-range slot, so we need at
 * least 2 valid slots. */
#define TEST_SLOTS   4
#define TEST_MAXRESP 4096

/* nfs4_callback.c references this from nfs4_cb_null_complete, which this
 * unit test never exercises -- stub it to satisfy the link. */
void
chimera_nfs4_open_resume_after_probe(struct nfs_request *req)
{
    (void) req;
} /* chimera_nfs4_open_resume_after_probe */

/*
 * Build a session attached to a fresh client table.  Caller frees with
 * destroy_session_table().  No NFS conn / encoding is created here --
 * acquire/finalize are exercised directly with a stub nfs_request that
 * carries NULL encoding (the capture arm is a no-op when encoding is
 * NULL).
 */
static struct nfs4_session *
make_session(
    struct nfs4_client_table *table,
    uint32_t                  replay_max_slots,
    uint32_t                  replay_maxresp_cached)
{
    static const uint8_t owner[] = "owner-x";
    uint64_t             clientid;
    struct nfs4_session *session;

    nfs4_client_table_init(table, 1);

    clientid = nfs4_client_register(table, owner, (int) sizeof(owner) - 1,
                                    0xdeadbeefULL, 40, NULL, NULL);
    CHECK(clientid != 0);

    session = nfs4_create_session(table, clientid, 0,
                                  replay_max_slots, replay_maxresp_cached,
                                  NULL, NULL, NULL);
    CHECK(session != NULL);

    return session;
} /* make_session */

static void
destroy_session_table(
    struct nfs4_client_table *table,
    struct nfs4_session      *session)
{
    /* Drop the +1 ref returned by nfs4_create_session.  The table still
     * holds its ref, which nfs4_client_table_free releases. */
    nfs4_session_put(session);

    /* nfs4_client_register also allocated a unified state hierarchy on the
     * client; tear that down before freeing the table.  No state objects
     * were created in this test, so NULL state_table / vfs_thread are safe
     * arguments. */
    nfs4_client_table_destroy_unified(table, NULL, NULL);
    nfs4_client_table_free(table);
} /* destroy_session_table */

static void
reset_request(struct nfs_request *req)
{
    memset(req, 0, sizeof(*req));
} /* reset_request */

/* Case 1: first SEQUENCE on an UNUSED slot with seqid=1 succeeds. */
static void
test_unused_slot_first_seq_ok(void)
{
    struct nfs4_client_table table;
    struct nfs4_session     *session = make_session(&table, TEST_SLOTS, TEST_MAXRESP);
    struct nfs_request       req;
    bool                     is_replay = true;
    nfsstat4                 status;

    reset_request(&req);
    req.session = session;

    status = nfs4_replay_slot_acquire(session, 0, 1, false, &req, &is_replay);

    CHECK(status == NFS4_OK);
    CHECK(!is_replay);
    CHECK(req.replay_slot == &session->replay_slots[0]);
    CHECK(req.replay_slot_id == 0);
    CHECK(req.replay_action == NFS4_REPLAY_ACTION_NEW);
    CHECK(nfs4_slot_state(&session->replay_slots[0]) == NFS4_SLOT_IN_PROGRESS);
    CHECK(nfs4_slot_seqid(&session->replay_slots[0]) == 1);

    /* Finalize transitions to COMPLETED (no cachethis, no cached_buf). */
    nfs4_replay_slot_finalize(&req);
    CHECK(nfs4_slot_state(&session->replay_slots[0]) == NFS4_SLOT_COMPLETED);
    CHECK(nfs4_slot_seqid(&session->replay_slots[0]) == 1);
    CHECK(session->replay_slots[0].cached_buf == NULL);

    destroy_session_table(&table, session);
} /* test_unused_slot_first_seq_ok */

/* Case 2: first SEQUENCE on an UNUSED slot with seqid != 1 -> MISORDERED. */
static void
test_unused_slot_wrong_seq_misordered(void)
{
    struct nfs4_client_table table;
    struct nfs4_session     *session = make_session(&table, TEST_SLOTS, TEST_MAXRESP);
    struct nfs_request       req;
    bool                     is_replay = true;
    nfsstat4                 status;

    reset_request(&req);
    req.session = session;

    status = nfs4_replay_slot_acquire(session, 0, 2, false, &req, &is_replay);

    CHECK(status == NFS4ERR_SEQ_MISORDERED);
    CHECK(!is_replay);
    CHECK(nfs4_slot_state(&session->replay_slots[0]) == NFS4_SLOT_UNUSED);

    destroy_session_table(&table, session);
} /* test_unused_slot_wrong_seq_misordered */

/* Case 3: out-of-range slot -> BADSLOT. */
static void
test_out_of_range_slot(void)
{
    struct nfs4_client_table table;
    struct nfs4_session     *session = make_session(&table, TEST_SLOTS, TEST_MAXRESP);
    struct nfs_request       req;
    bool                     is_replay = true;
    nfsstat4                 status;

    reset_request(&req);
    req.session = session;

    status = nfs4_replay_slot_acquire(session, TEST_SLOTS, 1, false, &req,
                                      &is_replay);
    CHECK(status == NFS4ERR_BADSLOT);
    CHECK(!is_replay);

    destroy_session_table(&table, session);
} /* test_out_of_range_slot */

/* Case 4: in-progress retry (same seqid) -> RETRY_UNCACHED_REP. */
static void
test_in_progress_retry(void)
{
    struct nfs4_client_table table;
    struct nfs4_session     *session = make_session(&table, TEST_SLOTS, TEST_MAXRESP);
    struct nfs_request       req1, req2;
    bool                     is_replay;
    nfsstat4                 status;

    reset_request(&req1);
    reset_request(&req2);
    req1.session = session;
    req2.session = session;

    status = nfs4_replay_slot_acquire(session, 0, 1, true, &req1, &is_replay);
    CHECK(status == NFS4_OK);
    CHECK(nfs4_slot_state(&session->replay_slots[0]) == NFS4_SLOT_IN_PROGRESS);

    /* Without calling finalize, a second SEQUENCE on the same slot with
     * the same seqid arrives -- libevpl has no DRC, so this hits us. */
    status = nfs4_replay_slot_acquire(session, 0, 1, true, &req2, &is_replay);
    CHECK(status == NFS4ERR_RETRY_UNCACHED_REP);
    CHECK(!is_replay);

    /* And a jump-ahead seqid -> MISORDERED. */
    status = nfs4_replay_slot_acquire(session, 0, 5, true, &req2, &is_replay);
    CHECK(status == NFS4ERR_SEQ_MISORDERED);

    /* Clean up the first request. */
    nfs4_replay_slot_finalize(&req1);
    destroy_session_table(&table, session);
} /* test_in_progress_retry */

/* Case 5: COMPLETED retry (cachethis was false) -> RETRY_UNCACHED_REP. */
static void
test_completed_retry_uncached(void)
{
    struct nfs4_client_table table;
    struct nfs4_session     *session = make_session(&table, TEST_SLOTS, TEST_MAXRESP);
    struct nfs_request       req;
    bool                     is_replay;
    nfsstat4                 status;

    reset_request(&req);
    req.session = session;

    /* Run a request with cachethis=false. */
    status = nfs4_replay_slot_acquire(session, 0, 1, false, &req, &is_replay);
    CHECK(status == NFS4_OK);
    nfs4_replay_slot_finalize(&req);
    CHECK(nfs4_slot_state(&session->replay_slots[0]) == NFS4_SLOT_COMPLETED);

    /* Retry (same seqid) -> no cached bytes -> RETRY_UNCACHED_REP. */
    reset_request(&req);
    req.session = session;
    status      = nfs4_replay_slot_acquire(session, 0, 1, false, &req, &is_replay);
    CHECK(status == NFS4ERR_RETRY_UNCACHED_REP);
    CHECK(!is_replay);

    destroy_session_table(&table, session);
} /* test_completed_retry_uncached */

/* Case 6: CACHED retry -> REPLAY hit.  Simulate capture by manually
 * installing a cached_buf on the slot post-finalize. */
static void
test_cached_retry_replay(void)
{
    struct nfs4_client_table table;
    struct nfs4_session     *session = make_session(&table, TEST_SLOTS, TEST_MAXRESP);
    struct nfs_request       req;
    bool                     is_replay;
    nfsstat4                 status;
    char                    *fake_reply;

    reset_request(&req);
    req.session = session;

    /* Run with cachethis=true.  Encoding is NULL so arm_capture is a
     * no-op; we simulate the capture callback effect manually. */
    status = nfs4_replay_slot_acquire(session, 0, 1, true, &req, &is_replay);
    CHECK(status == NFS4_OK);

    /* Pretend the capture callback fired and stored bytes on the slot. */
    fake_reply = malloc(64);
    memset(fake_reply, 'x', 64);
    session->replay_slots[0].cached_buf = fake_reply;
    session->replay_slots[0].cached_len = 64;
    session->replay_bytes_in_use       += 64;

    nfs4_replay_slot_finalize(&req);
    CHECK(nfs4_slot_state(&session->replay_slots[0]) == NFS4_SLOT_CACHED);
    CHECK(session->replay_slots[0].cached_buf == fake_reply);

    /* Retry same seqid -> REPLAY. */
    reset_request(&req);
    req.session = session;
    status      = nfs4_replay_slot_acquire(session, 0, 1, true, &req, &is_replay);
    CHECK(status == NFS4_OK);
    CHECK(is_replay);
    CHECK(req.replay_action == NFS4_REPLAY_ACTION_FROM_CACHE);
    CHECK(req.replay_slot == &session->replay_slots[0]);

    destroy_session_table(&table, session);
    /* fake_reply was freed by session_put -> slot teardown. */
} /* test_cached_retry_replay */

/* Case 7: advancing seqid frees the prior cached reply. */
static void
test_advance_frees_cache(void)
{
    struct nfs4_client_table table;
    struct nfs4_session     *session = make_session(&table, TEST_SLOTS, TEST_MAXRESP);
    struct nfs_request       req;
    bool                     is_replay;
    nfsstat4                 status;
    char                    *fake_reply;

    reset_request(&req);
    req.session = session;

    /* Get into CACHED with simulated bytes. */
    status = nfs4_replay_slot_acquire(session, 0, 1, true, &req, &is_replay);
    CHECK(status == NFS4_OK);
    fake_reply                          = malloc(128);
    session->replay_slots[0].cached_buf = fake_reply;
    session->replay_slots[0].cached_len = 128;
    session->replay_bytes_in_use       += 128;
    nfs4_replay_slot_finalize(&req);
    CHECK(nfs4_slot_state(&session->replay_slots[0]) == NFS4_SLOT_CACHED);
    CHECK(session->replay_bytes_in_use == 128);

    /* Advance: seqid + 1.  Old buf must be freed and bytes accounted. */
    reset_request(&req);
    req.session = session;
    status      = nfs4_replay_slot_acquire(session, 0, 2, false, &req, &is_replay);
    CHECK(status == NFS4_OK);
    CHECK(!is_replay);
    CHECK(req.replay_action == NFS4_REPLAY_ACTION_NEW);
    CHECK(nfs4_slot_state(&session->replay_slots[0]) == NFS4_SLOT_IN_PROGRESS);
    CHECK(session->replay_slots[0].cached_buf == NULL);
    CHECK(session->replay_bytes_in_use == 0);

    nfs4_replay_slot_finalize(&req);
    CHECK(nfs4_slot_state(&session->replay_slots[0]) == NFS4_SLOT_COMPLETED);

    destroy_session_table(&table, session);
} /* test_advance_frees_cache */

/* Case 8: misordered seqid on a completed slot. */
static void
test_misordered_after_completed(void)
{
    struct nfs4_client_table table;
    struct nfs4_session     *session = make_session(&table, TEST_SLOTS, TEST_MAXRESP);
    struct nfs_request       req;
    bool                     is_replay;
    nfsstat4                 status;

    reset_request(&req);
    req.session = session;

    status = nfs4_replay_slot_acquire(session, 0, 1, false, &req, &is_replay);
    CHECK(status == NFS4_OK);
    nfs4_replay_slot_finalize(&req);

    /* seqid = last+2 -> MISORDERED. */
    reset_request(&req);
    req.session = session;
    status      = nfs4_replay_slot_acquire(session, 0, 3, false, &req, &is_replay);
    CHECK(status == NFS4ERR_SEQ_MISORDERED);

    destroy_session_table(&table, session);
} /* test_misordered_after_completed */

/* Case 9: independent slots advance independently. */
static void
test_slots_independent(void)
{
    struct nfs4_client_table table;
    struct nfs4_session     *session = make_session(&table, TEST_SLOTS, TEST_MAXRESP);
    struct nfs_request       req;
    bool                     is_replay;
    nfsstat4                 status;

    reset_request(&req);
    req.session = session;

    status = nfs4_replay_slot_acquire(session, 0, 1, false, &req, &is_replay);
    CHECK(status == NFS4_OK);
    nfs4_replay_slot_finalize(&req);

    /* Slot 1 is still UNUSED.  Its first seqid must be 1, not slot 0's
     * last+1 value. */
    reset_request(&req);
    req.session = session;
    status      = nfs4_replay_slot_acquire(session, 1, 2, false, &req, &is_replay);
    CHECK(status == NFS4ERR_SEQ_MISORDERED);

    reset_request(&req);
    req.session = session;
    status      = nfs4_replay_slot_acquire(session, 1, 1, false, &req, &is_replay);
    CHECK(status == NFS4_OK);
    nfs4_replay_slot_finalize(&req);
    CHECK(nfs4_slot_state(&session->replay_slots[1]) == NFS4_SLOT_COMPLETED);
    CHECK(nfs4_slot_state(&session->replay_slots[0]) == NFS4_SLOT_COMPLETED);

    destroy_session_table(&table, session);
} /* test_slots_independent */

/* Case 10: implicit (NFS4.0) sessions have no slot table -> every
 * SEQUENCE attempt is BADSLOT. */
static void
test_implicit_session_no_slots(void)
{
    static const uint8_t     owner[] = "v40-client";
    struct nfs4_client_table table;
    uint64_t                 clientid;
    struct nfs4_session     *session;
    struct nfs_request       req;
    bool                     is_replay;
    nfsstat4                 status;

    nfs4_client_table_init(&table, 1);
    clientid = nfs4_client_register(&table, owner, (int) sizeof(owner) - 1,
                                    0xfeedfaceULL, 40, NULL, NULL);

    /* implicit=1 -> replay_slots stays NULL, replay_max_slots = 0. */
    session = nfs4_create_session(&table, clientid, 1, 0, 0, NULL, NULL, NULL);
    CHECK(session != NULL);
    CHECK(session->replay_slots == NULL);
    CHECK(session->replay_max_slots == 0);

    reset_request(&req);
    req.session = session;
    status      = nfs4_replay_slot_acquire(session, 0, 1, false, &req, &is_replay);
    CHECK(status == NFS4ERR_BADSLOT);

    destroy_session_table(&table, session);
} /* test_implicit_session_no_slots */

int
main(
    int   argc,
    char *argv[])
{
    (void) argc;
    (void) argv;

    /* chimera_nfs_info() and friends call into the logging library,
     * which requires init or it crashes inside its formatter. */
    chimera_log_init();

    test_unused_slot_first_seq_ok();
    test_unused_slot_wrong_seq_misordered();
    test_out_of_range_slot();
    test_in_progress_retry();
    test_completed_retry_uncached();
    test_cached_retry_replay();
    test_advance_frees_cache();
    test_misordered_after_completed();
    test_slots_independent();
    test_implicit_session_no_slots();

    printf("nfs4_replay_slot: all tests passed\n");
    return 0;
} /* main */
