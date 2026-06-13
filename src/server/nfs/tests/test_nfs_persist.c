// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Cross-reboot persistence unit tests.
 *
 * These validate the on-stable-storage record formats and the cold-start
 * reconstruction path WITHOUT a live VFS / event loop, by serializing records
 * exactly as the persist path does, then "rebooting" (a fresh client table)
 * and feeding the bytes back through the same reconstruction code the cold
 * load uses.  The end-to-end test proves a session restored with its original
 * sessionid + a repopulated slot replays a retransmit as a cache hit -- the
 * essence of NFSv4.1 cross-reboot exactly-once semantics.
 */

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "common/logging.h"
#include "nfs_common.h"
#include "nfs4_session.h"
#include "nfs4_recovery.h"
#include "nfs4_drc.h"
#include "nfs4_state.h"
#include "nfs_kv_keys.h"

#define CHECK(cond) do { \
            if (!(cond)) { \
                fprintf(stderr, "%s:%d: CHECK failed: %s\n", \
                        __FILE__, __LINE__, #cond); \
                abort(); \
            } \
} while (0)

/* Referenced from nfs4_callback.c (linked in); unused here. */
void
chimera_nfs4_open_resume_after_probe(struct nfs_request*req)
{
    (void) req;
} /* chimera_nfs4_open_resume_after_probe */

/* ------------------------------------------------------------------ *
*  Key encoding                                                      *
* ------------------------------------------------------------------ */

static void
test_key_encoding(void)
{
    uint8_t  sid[NFS4_SESSIONID_SIZE];
    uint8_t  rkey[CHIMERA_KV_REPLY_KEY_LEN];
    uint8_t  skey[CHIMERA_KV_SESSION_KEY_LEN];
    uint8_t  ekey[CHIMERA_KV_HDR_LEN + 1];
    uint8_t  ckey[CHIMERA_KV_NFS_KEY_MAX];
    uint32_t klen;

    memset(sid, 0xAB, sizeof(sid));

    /* Every key starts with the chimera magic + version, disjoint from SMB's
     * ASCII "smbdh" band (byte 0 = 0xC4). */
    klen = nfs_kv_reply_key(rkey, sid, 7, 9);
    CHECK(klen == CHIMERA_KV_REPLY_KEY_LEN);
    CHECK(rkey[0] == CHIMERA_KV_MAGIC && rkey[1] == CHIMERA_KV_VERSION);
    CHECK(rkey[2] == CHIMERA_KV_TYPE_NFS4_REPLY);
    CHECK(memcmp(rkey + CHIMERA_KV_HDR_LEN, sid, NFS4_SESSIONID_SIZE) == 0);
    /* slotid + seqid encoded little-endian after the sessionid. */
    CHECK(nfs_kv_le32(rkey + CHIMERA_KV_HDR_LEN + NFS4_SESSIONID_SIZE) == 7);
    CHECK(nfs_kv_le32(rkey + CHIMERA_KV_HDR_LEN + NFS4_SESSIONID_SIZE + 4) == 9);

    klen = nfs_kv_session_key(skey, sid);
    CHECK(klen == CHIMERA_KV_SESSION_KEY_LEN);
    CHECK(skey[2] == CHIMERA_KV_TYPE_NFS4_SESSION);

    klen = nfs_kv_epoch_key(ekey);
    CHECK(klen == CHIMERA_KV_HDR_LEN + 1);
    CHECK(ekey[2] == CHIMERA_KV_TYPE_NFS4_EPOCH);

    klen = nfs_kv_recovery_key(ckey, (const uint8_t *) "owner", 5);
    CHECK(klen == CHIMERA_KV_HDR_LEN + 5);
    CHECK(ckey[2] == CHIMERA_KV_TYPE_NFS4_RECOVERY);
    CHECK(memcmp(ckey + CHIMERA_KV_HDR_LEN, "owner", 5) == 0);

    /* Type bytes are ordered recovery < epoch < session < reply, so a
    * [type, type+1) search range isolates exactly one record kind. */
    CHECK(CHIMERA_KV_TYPE_NFS4_RECOVERY < CHIMERA_KV_TYPE_NFS4_EPOCH);
    CHECK(CHIMERA_KV_TYPE_NFS4_EPOCH < CHIMERA_KV_TYPE_NFS4_SESSION);
    CHECK(CHIMERA_KV_TYPE_NFS4_SESSION < CHIMERA_KV_TYPE_NFS4_REPLY);

    printf("ok: key_encoding\n");
} /* test_key_encoding */

/* ------------------------------------------------------------------ *
*  Record round-trips                                                *
* ------------------------------------------------------------------ */

static void
test_recovery_record_roundtrip(void)
{
    struct nfs_client          c;
    struct nfs_recovery_record out;
    uint8_t                    buf[2048];
    uint32_t                   len;

    memset(&c, 0, sizeof(c));
    c.client_id = 0x1122334455667788ULL;
    c.verifier  = 0xCAFEF00DDEADBEEFULL;
    c.boot_id   = 0x0102030405060708ULL;
    c.owner_len = 11;
    memcpy(c.owner_string, "client/abc1", 11);

    len = nfs_recovery_serialize(buf, sizeof(buf), &c);
    CHECK(len > 0);

    CHECK(nfs_recovery_deserialize(buf, len, &out) == 0);
    CHECK(out.client_id_hint == c.client_id);
    CHECK(out.verifier == c.verifier);
    CHECK(out.boot_id == c.boot_id);
    CHECK(out.owner_len == c.owner_len);
    CHECK(memcmp(out.owner_string, c.owner_string, c.owner_len) == 0);
    CHECK(!out.reclaimed);

    /* A truncated / corrupt buffer is rejected, not misparsed. */
    CHECK(nfs_recovery_deserialize(buf, 4, &out) != 0);
    buf[0] ^= 0xff;  /* break the magic */
    CHECK(nfs_recovery_deserialize(buf, len, &out) != 0);

    printf("ok: recovery_record_roundtrip\n");
} /* test_recovery_record_roundtrip */

static void
test_epoch_record_roundtrip(void)
{
    uint8_t  buf[64];
    uint32_t len;
    uint64_t boot = 0;

    len = nfs_recovery_epoch_serialize(buf, 0x0011223344556677ULL);
    CHECK(len > 0);
    CHECK(nfs_recovery_epoch_deserialize(buf, len, &boot) == 0);
    CHECK(boot == 0x0011223344556677ULL);
    CHECK(nfs_recovery_epoch_deserialize(buf, 4, &boot) != 0);

    printf("ok: epoch_record_roundtrip\n");
} /* test_epoch_record_roundtrip */

static void
fill_session_record(struct nfs4_drc_session_record*r)
{
    memset(r, 0, sizeof(*r));
    r->clientid                       = 0xAABBCCDD11223344ULL;
    r->verifier                       = 0x5566778899AABBCCULL;
    r->princ_flavor                   = 1;
    r->princ_uid                      = 1000;
    r->princ_gid                      = 1001;
    r->replay_max_slots               = 8;
    r->replay_maxresp_cached          = 4096;
    r->cb_program                     = 0x40000000;
    r->flags                          = 3;
    r->fore.ca_maxrequestsize         = 1048576;
    r->fore.ca_maxresponsesize        = 1048576;
    r->fore.ca_maxresponsesize_cached = 4096;
    r->fore.ca_maxoperations          = 8;
    r->fore.ca_maxrequests            = 8;
    r->back.ca_maxrequestsize         = 4096;
    r->back.ca_maxresponsesize        = 4096;
    r->back.ca_maxrequests            = 1;
    r->mach_len                       = 5;
    memcpy(r->mach, "node1", 5);
    r->owner_len = 9;
    memcpy(r->owner, "co_owner1", 9);
} /* fill_session_record */

static void
test_session_record_roundtrip(void)
{
    struct nfs4_drc_session_record in, out;
    uint8_t                        buf[4096];
    uint32_t                       len;

    fill_session_record(&in);

    len = nfs4_drc_session_serialize(buf, sizeof(buf), &in);
    CHECK(len > 0);
    CHECK(nfs4_drc_session_deserialize(buf, len, &out) == 0);

    CHECK(out.clientid == in.clientid);
    CHECK(out.verifier == in.verifier);
    CHECK(out.princ_flavor == in.princ_flavor);
    CHECK(out.princ_uid == in.princ_uid);
    CHECK(out.princ_gid == in.princ_gid);
    CHECK(out.replay_max_slots == in.replay_max_slots);
    CHECK(out.replay_maxresp_cached == in.replay_maxresp_cached);
    CHECK(out.cb_program == in.cb_program);
    CHECK(out.flags == in.flags);
    CHECK(out.fore.ca_maxrequests == in.fore.ca_maxrequests);
    CHECK(out.fore.ca_maxresponsesize_cached == in.fore.ca_maxresponsesize_cached);
    CHECK(out.back.ca_maxrequests == in.back.ca_maxrequests);
    CHECK(out.mach_len == in.mach_len);
    CHECK(memcmp(out.mach, in.mach, in.mach_len) == 0);
    CHECK(out.owner_len == in.owner_len);
    CHECK(memcmp(out.owner, in.owner, in.owner_len) == 0);

    /* Overflow + corruption are rejected. */
    CHECK(nfs4_drc_session_serialize(buf, 8, &in) == 0);
    buf[0] ^= 0xff;
    CHECK(nfs4_drc_session_deserialize(buf, len, &out) != 0);

    printf("ok: session_record_roundtrip\n");
} /* test_session_record_roundtrip */

static void
test_reply_record_roundtrip(void)
{
    uint8_t       buf[256];
    const uint8_t payload[] = { 0xde, 0xad, 0xbe, 0xef, 0x00, 0x42, 0x99 };
    uint32_t      len, seqid = 0;
    const uint8_t*data     = NULL;
    uint32_t      data_len = 0;

    len = nfs4_drc_reply_serialize(buf, sizeof(buf), 12345, payload,
                                   sizeof(payload));
    CHECK(len == 12 + sizeof(payload));
    CHECK(nfs4_drc_reply_parse(buf, len, &seqid, &data, &data_len) == 0);
    CHECK(seqid == 12345);
    CHECK(data_len == sizeof(payload));
    CHECK(memcmp(data, payload, sizeof(payload)) == 0);

    CHECK(nfs4_drc_reply_serialize(buf, 4, 1, payload, sizeof(payload)) == 0);
    CHECK(nfs4_drc_reply_parse(buf, 3, &seqid, &data, &data_len) != 0);

    printf("ok: reply_record_roundtrip\n");
} /* test_reply_record_roundtrip */

/* ------------------------------------------------------------------ *
*  End-to-end reconstruction (the cross-reboot proof)                *
* ------------------------------------------------------------------ */

static void
test_cross_reboot_replay(void)
{
    struct nfs4_client_table       table_a, table_b;
    struct nfs4_session           *session;
    struct nfs4_drc_session_record srec;
    uint8_t                        sessionid[NFS4_SESSIONID_SIZE];
    uint8_t                        sbuf[4096];
    uint8_t                        rbuf[256];
    const uint8_t                  reply_bytes[] = "CACHED-MKDIR-OK-REPLY";
    const uint32_t                 SLOT = 2,SEQ = 5;
    uint32_t                       slen,rlen,pseqid,pdata_len;
    const uint8_t                 *pdata;
    struct nfs_request             req;
    bool                           is_replay = false;
    nfsstat4                       status;
    static const uint8_t           owner[] = "co_owner_reboot";
    uint64_t                       clientid;

    /* --- instance A: a live persistent session with one cached reply --- */
    nfs4_client_table_init(&table_a);
    clientid = nfs4_client_register(&table_a,owner,(int) sizeof(owner) - 1,
                                    0x1234ULL,40,NULL,NULL);
    session = nfs4_create_session(&table_a,clientid,0,8,4096,
                                  NULL,NULL,NULL);
    CHECK(session != NULL);
    session->nfs4_session_persist = true;
    memcpy(sessionid,session->nfs4_session_id,NFS4_SESSIONID_SIZE);

    /* Persist its metadata + the cached reply exactly as the write path does. */
    memset(&srec,0,sizeof(srec));
    srec.clientid              = session->client_unified->client_id;
    srec.verifier              = session->client_unified->verifier;
    srec.princ_flavor          = 1;
    srec.replay_max_slots      = session->replay_max_slots;
    srec.replay_maxresp_cached = session->replay_maxresp_cached;
    srec.fore                  = session->nfs4_session_fore_attrs;
    srec.back                  = session->nfs4_session_back_attrs;
    srec.owner_len             = sizeof(owner) - 1;
    memcpy(srec.owner,owner,srec.owner_len);

    slen = nfs4_drc_session_serialize(sbuf,sizeof(sbuf),&srec);
    CHECK(slen > 0);
    rlen = nfs4_drc_reply_serialize(rbuf,sizeof(rbuf),SEQ,reply_bytes,
                                    sizeof(reply_bytes));
    CHECK(rlen > 0);

    /* --- reboot: instance A's in-memory state is gone --- */
    nfs4_session_put(session);
    nfs4_client_table_destroy_unified(&table_a,NULL,NULL);
    nfs4_client_table_free(&table_a);

    /* --- instance B: cold-start reconstruction from the persisted bytes --- */
    nfs4_client_table_init(&table_b);

    CHECK(nfs4_drc_session_deserialize(sbuf,slen,&srec) == 0);
    nfs4_drc_reconstruct_session(&table_b,sessionid,&srec,0x9999ULL);

    /* The session is back, under its ORIGINAL sessionid. */
    session = nfs4_session_lookup(&table_b,sessionid);
    CHECK(session != NULL);
    CHECK(memcmp(session->nfs4_session_id,sessionid,NFS4_SESSIONID_SIZE) == 0);
    CHECK(session->nfs4_session_clientid == srec.clientid);
    CHECK(session->nfs4_session_persist);

    /* Repopulate the slot from the persisted reply. */
    CHECK(nfs4_drc_reply_parse(rbuf,rlen,&pseqid,&pdata,&pdata_len) == 0);
    nfs4_drc_repopulate_slot(session,SLOT,pseqid,pdata,pdata_len);
    CHECK(nfs4_slot_state(&session->replay_slots[SLOT]) == NFS4_SLOT_CACHED);

    /* The decisive check: a retransmit on {SLOT, SEQ} is served from cache,
     * i.e. cross-reboot exactly-once holds. */
    memset(&req,0,sizeof(req));
    req.session = session;
    status      = nfs4_replay_slot_acquire(session,SLOT,SEQ,true,&req,
                                           &is_replay);
    CHECK(status == NFS4_OK);
    CHECK(is_replay);
    CHECK(req.replay_action == NFS4_REPLAY_ACTION_FROM_CACHE);
    CHECK(session->replay_slots[SLOT].cached_len == sizeof(reply_bytes));
    CHECK(memcmp(session->replay_slots[SLOT].cached_buf,reply_bytes,
                 sizeof(reply_bytes)) == 0);

    nfs4_session_put(session);
    nfs4_client_table_destroy_unified(&table_b,NULL,NULL);
    nfs4_client_table_free(&table_b);

    printf("ok: cross_reboot_replay\n");
} /* test_cross_reboot_replay */

int
main(void)
{
    /* chimera_nfs_info() and friends call into the logging library, which
     * requires init or it crashes inside its formatter. */
    chimera_log_init();

    test_key_encoding();
    test_recovery_record_roundtrip();
    test_epoch_record_roundtrip();
    test_session_record_roundtrip();
    test_reply_record_roundtrip();
    test_cross_reboot_replay();

    printf("PASS: all nfs persistence tests\n");
    return 0;
} /* main */
