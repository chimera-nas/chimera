/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * SMB lease records are per (ClientGuid, LeaseKey); Windows object-store
 * caching uses LeaseKey as ClientLeaseId (MS-SMB2 3.3.1.4, product behavior
 * 211). Same-key opens must coexist without breaking, while distinct clients
 * retain their own lease version, epoch and break acknowledgment state.
 */

#include "smb2_mbt_common.h"

static int failures;

#define CHECK(cond, ...) do {                      \
            if (!(cond)) {                             \
                fprintf(stderr, "FAIL: " __VA_ARGS__); \
                fprintf(stderr, "\n");                \
                failures++;                            \
            }                                          \
} while (0)

static void
wait_reply(struct smb2_conn *c)
{
    uint64_t deadline = smb2c_now_ms() + SMB2C_HANG_MS;

    while (!c->reply_ready) {
        smb2_pump(c->env);
        if (c->disconnected || smb2c_now_ms() >= deadline) {
            smb2c_hang(c, "lease identity CREATE reply");
        }
    }
} /* wait_reply */

static void
run_case(
    int encrypted,
    int first_v1,
    int second_v1,
    int same_client)
{
    struct smb2_env          env;
    struct smb2_env_opts     opts = { .oplocks = 1, .leases = 1 };
    struct smb2_wire_profile wire = {
        .name        = "lease identity",        .max_dialect = 0x0311,
        .ntlmv2      = 1,                       .encrypt     = encrypted,
        .cipher      = SMB2W_CIPHER_AES128_GCM,
        .signing_alg = SMB2W_SIGN_AES_GMAC,
    };
    struct smb2_conn        *a, *b, *peer;
    struct smb2_create_out   oa, ob, op;
    struct smb2_oplock_req   req = {
        .is_lease    = 1, .lease_state = SMB2_LEASE_RWH,
        .lease_epoch = 1, .force_v1    = first_v1,
    };
    struct smb2_break        ba = { 0 }, bb = { 0 };
    uint64_t                 deadline;
    uint32_t                 count;
    int                      got_a = 0, got_b = 0;

    printf("lease identity: encrypted=%d first_v1=%d second_v1=%d same_client=%d\n",
           encrypted, first_v1, second_v1, same_client);
    smb2_env_open_wire(&env, &opts, &wire);
    smb2_env_fs_setup(&env, "fs0");
    a    = smb2_conn_open(&env);
    b    = smb2_conn_open(&env);
    peer = smb2_conn_open(&env);
    if (same_client) {
        b->guid_tag = a->guid_tag;
    }
    smb2_handshake(a);
    smb2_handshake(b);
    smb2_handshake(peer);
    CHECK(a->session_id != b->session_id, "connections have distinct sessions");

    req.lease_key[0] = 0xc7;
    smb2_create(a, "samekey", FILE_OPEN_IF, FILE_ALL_ACCESS, FILE_SHARE_RWD,
                &req, &oa);
    CHECK(oa.status == ST_SUCCESS && oa.lease_state == SMB2_LEASE_RWH,
          "first open gets RWH");
    req.force_v1 = second_v1;
    smb2_create_post(b, "samekey", FILE_OPEN_IF, FILE_ALL_ACCESS, FILE_SHARE_RWD, &req);
    deadline = smb2c_now_ms() + SMB2C_HANG_MS;
    while (!b->reply_ready) {
        smb2_pump(&env);
        if (smb2_conn_pop_break(a, &ba)) {
            CHECK(0, "same ClientLeaseId caused a break before second CREATE");
            /* Drain a regression so its assertion is visible instead of a timeout. */
            smb2_lease_break_ack(a, ba.lease_key, ba.new_state);
        }
        if (smb2c_now_ms() >= deadline) {
            smb2c_hang(b, "same-key CREATE without a break");
        }
    }
    smb2c_parse_create(b, &ob);
    CHECK(ob.status == ST_SUCCESS && ob.lease_state == SMB2_LEASE_RWH,
          "same-key second open keeps RWH, status=%08x lease=%u",
          ob.status, ob.lease_state);
    CHECK(oa.lease_epoch == (first_v1 ? 0 : req.lease_epoch + 1), "first lease version/epoch retained");
    CHECK(ob.lease_epoch == (second_v1 ? 0 : req.lease_epoch + 1), "second lease has its own version/epoch");

    /* An early failure would make a blocking write wait for another break. */
    if (oa.lease_state != SMB2_LEASE_RWH || ob.lease_state != SMB2_LEASE_RWH) {
        goto close;
    }
    CHECK(smb2_write(a, oa.file_id, 0, "a", 1, &count) == ST_SUCCESS,
          "first client writes without breaking same-key peer");
    CHECK(smb2_write(b, ob.file_id, 0, "b", 1, &count) == ST_SUCCESS,
          "second client writes without breaking same-key peer");
    CHECK(!smb2_conn_pop_break(a, &ba) && !smb2_conn_pop_break(b, &bb),
          "same-key writes emitted no break");

    if (same_client) {
        goto close;
    }

    CHECK(smb2_lock(a, oa.file_id, 0, 1,
                    SMB2_LOCKFLAG_EXCLUSIVE | SMB2_LOCKFLAG_FAIL_IMMEDIATELY) == ST_SUCCESS,
          "first client's byte-range lock succeeds without a caching break");
    CHECK(smb2_write(b, ob.file_id, 0, "c", 1, &count) == ST_FILE_LOCK_CONFLICT,
          "same caching key does not bypass another client's byte-range lock");
    CHECK(smb2_lock(a, oa.file_id, 0, 1, SMB2_LOCKFLAG_UNLOCK) == ST_SUCCESS,
          "first client's byte-range lock released");

    /* A DIFFERENT key must break both protocol lease records. One ACK must
     * not settle the other client's record just because their key bytes match. */
    req.lease_key[0] = 0xc8;
    req.lease_state  = SMB2_LEASE_RH;
    req.force_v1     = 0;
    smb2_create_post(peer, "samekey", FILE_OPEN, FILE_READ_ACCESS, FILE_SHARE_RWD, &req);
    deadline = smb2c_now_ms() + SMB2C_HANG_MS;
    while (!got_a || !got_b) {
        smb2_pump(&env);
        if (!got_a) {
            got_a = smb2_conn_pop_break(a, &ba);
        }
        if (!got_b) {
            got_b = smb2_conn_pop_break(b, &bb);
        }
        if (smb2c_now_ms() >= deadline) {
            smb2c_hang(peer, "distinct-client lease break notifications");
        }
    }
    CHECK(ba.is_lease && bb.is_lease && ba.new_state == SMB2_LEASE_RH &&
          bb.new_state == SMB2_LEASE_RH, "both holders break RWH to RH");
    CHECK(ba.new_epoch == (first_v1 ? 0 : oa.lease_epoch + 1),
          "first holder break uses its own version and epoch");
    CHECK(bb.new_epoch == (second_v1 ? 0 : ob.lease_epoch + 1),
          "second holder break uses its own version and epoch");
    CHECK(!peer->reply_ready, "conflicting CREATE waits for both ACKs");
    CHECK(smb2_lease_break_ack(a, ba.lease_key, ba.new_state) == ST_SUCCESS,
          "first holder ACK accepted");
    smb2_echo_barrier(a);
    CHECK(!peer->reply_ready, "one client's ACK does not settle the other lease");
    CHECK(smb2_lease_break_ack(b, bb.lease_key, bb.new_state) == ST_SUCCESS,
          "second holder ACK accepted");
    wait_reply(peer);
    smb2c_parse_create(peer, &op);
    CHECK(op.status == ST_SUCCESS && op.lease_state == SMB2_LEASE_RH,
          "third client completes with shared read/handle caching");
    smb2_close(peer, op.file_id);

 close:
    smb2_close(a, oa.file_id);
    smb2_close(b, ob.file_id);
    smb2_env_stop(&env);
} /* run_case */

int
main(void)
{
    for (int encrypted = 0; encrypted <= 1; encrypted++) {
        for (int first_v1 = 0; first_v1 <= 1; first_v1++) {
            run_case(encrypted, first_v1, first_v1, 1);
            for (int second_v1 = 0; second_v1 <= 1; second_v1++) {
                run_case(encrypted, first_v1, second_v1, 0);
            }
        }
    }
    printf("lease identity: %d failures\n", failures);
    return failures ? 1 : 0;
} /* main */
