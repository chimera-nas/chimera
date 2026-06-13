// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Unit test for the unified NFSv4 state model:
 *   - slot allocate / install / lookup / free
 *   - stateid encode/decode roundtrip
 *   - generation advance invalidates stale stateids
 *   - client / open_owner / open_state lifecycle
 *
 * Exercises the public API in nfs4_state.h without any VFS, RPC, or
 * compound dispatch in the picture.  Handles are passed as NULL since
 * destroy skips release when both handle and vfs_thread are NULL.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "nfs4_state.h"
#include "nfs4_stateid.h"

/* assert(3) compiles to a no-op under NDEBUG (Release builds), which
 * triggers -Werror=unused-but-set-variable for inputs only consumed by
 * asserts.  CHECK() always evaluates and dies on failure. */
#define CHECK(cond)                                                    \
        do {                                                               \
            if (!(cond)) {                                                 \
                fprintf(stderr, "%s:%d: CHECK failed: %s\n",               \
                        __FILE__, __LINE__, #cond);                        \
                abort();                                                   \
            }                                                              \
        } while (0)

static void
test_stateid_codec(void)
{
    struct stateid4          sid;
    struct nfs4_stateid_view view;

    nfs4_stateid_encode(&sid,
                        /*seqid*/ 1,
                        /*type*/ NFS4_STATEID_TYPE_OPEN,
                        /*shard*/ 7,
                        /*slot_idx*/ 0xABCDEF,
                        /*generation*/ 0x123456,
                        /*epoch*/ 0xDEADBEEF);

    nfs4_stateid_decode(&view, &sid);

    CHECK(sid.seqid == 1);
    CHECK(view.epoch == 0xDEADBEEF);
    CHECK(view.version == NFS4_STATEID_VERSION);
    CHECK(view.type == NFS4_STATEID_TYPE_OPEN);
    CHECK(view.shard == 7);
    CHECK(view.slot_idx == 0xABCDEF);
    CHECK(view.generation == 0x123456);

    /* Special-stateid detection */
    struct stateid4 zero = { 0 };
    CHECK(nfs4_stateid_is_special(&zero));

    struct stateid4 ones;
    ones.seqid = 0xffffffff;
    memset(ones.other, 0xff, sizeof(ones.other));
    CHECK(nfs4_stateid_is_special(&ones));

    CHECK(!nfs4_stateid_is_special(&sid));
    printf("ok: stateid_codec\n");
} /* test_stateid_codec */

static void
test_slot_lifecycle(void)
{
    struct nfs_state_table table;
    uint8_t                first_shard;
    uint32_t               first_slot, first_gen;
    int                    rc;

    nfs_state_table_init(&table, 1);

    /* Allocate a slot.  Generation must start at 1 (every (re)use bumps). */
    rc = nfs_state_table_alloc(&table, NFS4_SLOT_TYPE_OPEN,
                               &first_shard, &first_slot, &first_gen);
    CHECK(rc == 0);
    CHECK(first_gen == 1);

    /* Validate an encoded stateid resolves: build one and confirm
     * nfs_state_table_validate accepts it (we haven't installed a state
     * pointer yet, so acquire would fault; validate just checks the slot). */
    struct stateid4 sid;
    nfs4_stateid_encode(&sid, 1, NFS4_STATEID_TYPE_OPEN,
                        first_shard, first_slot, first_gen, table.epoch);
    nfsstat4        v = nfs_state_table_validate(&table, &sid);
    CHECK(v == NFS4_OK);

    /* Free the slot.  free advances the generation again so the prior
     * stateid is now stale. */
    nfs_state_table_free_slot(&table, first_shard, first_slot);

    /* The previously-encoded stateid now references a freed slot.  Validate
    * must reject -- either as BAD_STATEID (slot is TYPE_FREE) or
    * STALE_STATEID (generation advanced).  Either way it MUST NOT be OK. */
    v = nfs_state_table_validate(&table, &sid);
    CHECK(v != NFS4_OK);

    /* A stateid with a wrong generation also rejects as stale. */
    struct stateid4 stale;
    nfs4_stateid_encode(&stale, 1, NFS4_STATEID_TYPE_OPEN,
                        first_shard, first_slot, /*gen*/ 999, table.epoch);
    v = nfs_state_table_validate(&table, &stale);
    CHECK(v != NFS4_OK);

    /* A stateid carrying a different server-instance epoch (i.e. minted
     * before a reboot) must be reported as stale, not merely bad. */
    rc = nfs_state_table_alloc(&table, NFS4_SLOT_TYPE_OPEN,
                               &first_shard, &first_slot, &first_gen);
    CHECK(rc == 0);
    struct stateid4 wrong_epoch;
    nfs4_stateid_encode(&wrong_epoch, 1, NFS4_STATEID_TYPE_OPEN,
                        first_shard, first_slot, first_gen, table.epoch ^ 0x1u);
    v = nfs_state_table_validate(&table, &wrong_epoch);
    CHECK(v == NFS4ERR_STALE_STATEID);

    nfs_state_table_free(&table, NULL);
    printf("ok: slot_lifecycle\n");
} /* test_slot_lifecycle */

static void
test_owner_state_lifecycle(void)
{
    struct nfs_state_table table;
    struct nfs_client     *client;
    struct nfs_open_owner *owner;
    struct nfs_open_state *state;
    struct stateid4        sid;
    nfsstat4               status;
    void                  *acquired;
    uint8_t                acquired_type;
    bool                   created;
    uint8_t                fh[4]          = { 0xCA, 0xFE, 0xBA, 0xBE };
    uint8_t                owner_bytes[8] = "owner-A";

    nfs_state_table_init(&table, 1);
    client = nfs_client_alloc(42, "client-A", 8, 0xABCD, /*minor*/ 0);

    owner = nfs_open_owner_find_or_create(client, owner_bytes,
                                          sizeof(owner_bytes), &created);
    CHECK(created);
    CHECK(!owner->confirmed);
    CHECK(owner->seqid == 0);

    /* find_or_create is idempotent. */
    bool                   created_again;
    struct nfs_open_owner *owner_again = nfs_open_owner_find_or_create(
        client, owner_bytes, sizeof(owner_bytes), &created_again);
    CHECK(!created_again);
    CHECK(owner_again == owner);

    /* Create an open_state with NULL handle (no VFS). */
    state = nfs_open_state_create(owner, 0, NULL, 0, fh, sizeof(fh),
                                  OPEN4_SHARE_ACCESS_READ,
                                  OPEN4_SHARE_DENY_NONE,
                                  /*handle_dup*/ NULL,
                                  &table,
                                  &sid);
    CHECK(state != NULL);
    CHECK(state->seqid == 1);
    CHECK(sid.seqid == 1);

    /* Lookup by stateid: should bump refcount and return the state. */
    status = nfs_state_table_acquire(&table, &sid, NFS4_SLOT_TYPE_OPEN,
                                     &acquired, &acquired_type);
    CHECK(status == NFS4_OK);
    CHECK(acquired == state);
    CHECK(acquired_type == NFS4_SLOT_TYPE_OPEN);

    nfs_state_table_release(&table, acquired, NFS4_SLOT_TYPE_OPEN, NULL);

    /* Coalesce: same owner + same fh; share bits widen, seqid bumps. */
    struct stateid4 sid2;
    nfs_open_state_coalesce(state,
                            OPEN4_SHARE_ACCESS_WRITE,
                            OPEN4_SHARE_DENY_NONE,
                            &table, &sid2);
    CHECK((state->share_access &
           (OPEN4_SHARE_ACCESS_READ | OPEN4_SHARE_ACCESS_WRITE)) ==
          (OPEN4_SHARE_ACCESS_READ | OPEN4_SHARE_ACCESS_WRITE));
    CHECK(state->seqid == 2);
    CHECK(sid2.seqid == 2);
    /* Stateid 'other' identifies the same slot; only seqid changes. */
    CHECK(memcmp(sid.other, sid2.other, sizeof(sid.other)) == 0);

    /* Destroy + re-acquire should fail. */
    nfs_open_state_destroy(state, &table, NULL);
    status = nfs_state_table_acquire(&table, &sid2, NFS4_SLOT_TYPE_OPEN,
                                     &acquired, &acquired_type);
    CHECK(status != NFS4_OK);

    nfs_client_destroy(client, &table, NULL, true);
    nfs_state_table_free(&table, NULL);
    printf("ok: owner_state_lifecycle\n");
} /* test_owner_state_lifecycle */

static void
test_replay_helpers(void)
{
    struct nfs4_replay_cache replay = { 0 };
    int                      cls;
    uint32_t                 owner_seqid = 0;

    /* First op: incoming = owner_seqid + 1 = 1, treat as NEW. */
    cls = nfs4_owner_seqid_classify(owner_seqid, &replay, 1);
    CHECK(cls == NFS4_SEQID_NEW);

    /* RFC 7530 §9.1.7: the very first op against a fresh owner may carry
     * any seqid the client chose -- the Linux kernel client picks 0.
     * Replay cache is still empty here, so this must classify NEW. */
    cls = nfs4_owner_seqid_classify(owner_seqid, &replay, 0);
    CHECK(cls == NFS4_SEQID_NEW);
    cls = nfs4_owner_seqid_classify(owner_seqid, &replay, 0xDEADBEEF);
    CHECK(cls == NFS4_SEQID_NEW);

    /* Pretend we executed and recorded the reply. */
    struct stateid4 sid;
    nfs4_stateid_encode(&sid, 1, NFS4_STATEID_TYPE_OPEN,
                        0, 0, 1, 0xDEADBEEF);
    nfs4_replay_record(&replay, 1, OP_OPEN, NFS4_OK, &sid);
    owner_seqid = 1;

    /* Retransmit of seqid=1 is REPLAY. */
    cls = nfs4_owner_seqid_classify(owner_seqid, &replay, 1);
    CHECK(cls == NFS4_SEQID_REPLAY);

    /* Next valid seqid is 2 (NEW). */
    cls = nfs4_owner_seqid_classify(owner_seqid, &replay, 2);
    CHECK(cls == NFS4_SEQID_NEW);

    /* Out-of-window seqids -> BAD. */
    cls = nfs4_owner_seqid_classify(owner_seqid, &replay, 5);
    CHECK(cls == NFS4_SEQID_BAD);

    cls = nfs4_owner_seqid_classify(owner_seqid, &replay, 0);
    CHECK(cls == NFS4_SEQID_BAD);

    printf("ok: replay_helpers\n");
} /* test_replay_helpers */

/* RFC 7530 §9.10: cross-owner SHARE_DENY conflicts. */
static void
test_share_mode_conflict(void)
{
    struct nfs_state_table table;
    struct nfs_client     *client;
    struct nfs_open_owner *owner_a, *owner_b;
    struct nfs_open_state *state_a;
    struct stateid4        sid_a;
    bool                   created;
    uint8_t                fh[4] = { 0xDE, 0xAD, 0xBE, 0xEF };
    nfsstat4               status;

    nfs_state_table_init(&table, 1);
    client = nfs_client_alloc(99, "share-cli", 9, 0xFFFF, 1);

    owner_a = nfs_open_owner_find_or_create(client, "owner-A", 7, &created);
    owner_b = nfs_open_owner_find_or_create(client, "owner-B", 7, &created);
    CHECK(owner_a != owner_b);

    /* Owner A opens with SHARE_DENY_WRITE. */
    state_a = nfs_open_state_create(owner_a, 0, NULL, 0, fh, sizeof(fh),
                                    OPEN4_SHARE_ACCESS_READ,
                                    OPEN4_SHARE_DENY_WRITE,
                                    NULL, &table, &sid_a);
    CHECK(state_a != NULL);

    /* Owner B asking for WRITE should be denied. */
    status = nfs_client_check_share_conflict(client, owner_b,
                                             fh, sizeof(fh),
                                             OPEN4_SHARE_ACCESS_WRITE,
                                             OPEN4_SHARE_DENY_NONE);
    CHECK(status == NFS4ERR_SHARE_DENIED);

    /* Owner B asking only for READ should be fine (no access overlap with
     * owner A's deny-write). */
    status = nfs_client_check_share_conflict(client, owner_b,
                                             fh, sizeof(fh),
                                             OPEN4_SHARE_ACCESS_READ,
                                             OPEN4_SHARE_DENY_NONE);
    CHECK(status == NFS4_OK);

    /* Owner B denying READ would clash with A's existing READ access. */
    status = nfs_client_check_share_conflict(client, owner_b,
                                             fh, sizeof(fh),
                                             OPEN4_SHARE_ACCESS_READ,
                                             OPEN4_SHARE_DENY_READ);
    CHECK(status == NFS4ERR_SHARE_DENIED);

    /* Same-owner check skips the requesting owner -- coalesce path. */
    status = nfs_client_check_share_conflict(client, owner_a,
                                             fh, sizeof(fh),
                                             OPEN4_SHARE_ACCESS_WRITE,
                                             OPEN4_SHARE_DENY_NONE);
    CHECK(status == NFS4_OK);

    /* Different FH on same client -- no conflict regardless of bits. */
    uint8_t fh2[4] = { 0x11, 0x22, 0x33, 0x44 };
    status = nfs_client_check_share_conflict(client, owner_b,
                                             fh2, sizeof(fh2),
                                             OPEN4_SHARE_ACCESS_WRITE,
                                             OPEN4_SHARE_DENY_BOTH);
    CHECK(status == NFS4_OK);

    nfs_open_state_destroy(state_a, &table, NULL);
    nfs_client_destroy(client, &table, NULL, true);
    nfs_state_table_free(&table, NULL);
    printf("ok: share_mode_conflict\n");
} /* test_share_mode_conflict */

int
main(
    int   argc,
    char *argv[])
{
    (void) argc;
    (void) argv;
    test_stateid_codec();
    test_slot_lifecycle();
    test_owner_state_lifecycle();
    test_replay_helpers();
    test_share_mode_conflict();
    printf("PASS: all state table tests\n");
    return 0;
} /* main */
