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
#include "nfs3_drc.h"
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
    const uint16_t node = 0x0102; /* distinct high/low bytes to check BE order */
    uint8_t        sid[NFS4_SESSIONID_SIZE];
    uint8_t        rkey[CHIMERA_KV_REPLY_KEY_LEN];
    uint8_t        skey[CHIMERA_KV_SESSION_KEY_LEN];
    uint8_t        ekey[CHIMERA_KV_PREFIX_LEN];
    uint8_t        ckey[CHIMERA_KV_NFS_KEY_MAX];
    uint8_t        pfx[CHIMERA_KV_PREFIX_LEN];
    uint32_t       klen;

    memset(sid, 0xAB, sizeof(sid));

    /* Every key starts with magic + version + type + node_id (big-endian),
     * disjoint from SMB's ASCII "smbdh" band (byte 0 = 0xC4). */
    klen = nfs_kv_reply_key(rkey, node, sid, 7, 9);
    CHECK(klen == CHIMERA_KV_REPLY_KEY_LEN);
    CHECK(rkey[0] == CHIMERA_KV_MAGIC && rkey[1] == CHIMERA_KV_VERSION);
    CHECK(rkey[2] == CHIMERA_KV_TYPE_NFS4_REPLY);
    CHECK(rkey[3] == 0x01 && rkey[4] == 0x02); /* node_id 0x0102, big-endian */
    CHECK(memcmp(rkey + CHIMERA_KV_PREFIX_LEN, sid, NFS4_SESSIONID_SIZE) == 0);
    /* slotid + seqid encoded little-endian after the sessionid. */
    CHECK(nfs_kv_le32(rkey + CHIMERA_KV_PREFIX_LEN + NFS4_SESSIONID_SIZE) == 7);
    CHECK(nfs_kv_le32(rkey + CHIMERA_KV_PREFIX_LEN + NFS4_SESSIONID_SIZE + 4) == 9);

    klen = nfs_kv_session_key(skey, node, sid);
    CHECK(klen == CHIMERA_KV_SESSION_KEY_LEN);
    CHECK(skey[2] == CHIMERA_KV_TYPE_NFS4_SESSION);

    klen = nfs_kv_epoch_key(ekey, node);
    CHECK(klen == CHIMERA_KV_PREFIX_LEN);
    CHECK(ekey[2] == CHIMERA_KV_TYPE_NFS4_EPOCH);

    klen = nfs_kv_recovery_key(ckey, node, (const uint8_t *) "owner", 5);
    CHECK(klen == CHIMERA_KV_PREFIX_LEN + 5);
    CHECK(ckey[2] == CHIMERA_KV_TYPE_NFS4_RECOVERY);
    CHECK(memcmp(ckey + CHIMERA_KV_PREFIX_LEN, "owner", 5) == 0);

    /* The 5-byte node prefix is exactly the head of a key of that type/node, so
     * a prefix scan selects one record type owned by one instance. */
    nfs_kv_node_prefix(pfx, CHIMERA_KV_TYPE_NFS4_RECOVERY, node);
    CHECK(memcmp(pfx, ckey, CHIMERA_KV_PREFIX_LEN) == 0);
    /* A different node yields a different prefix (and so a disjoint band). */
    nfs_kv_node_prefix(pfx, CHIMERA_KV_TYPE_NFS4_RECOVERY, node + 1);
    CHECK(memcmp(pfx, ckey, CHIMERA_KV_PREFIX_LEN) != 0);

    /* Type bytes are ordered recovery < epoch < session < reply. */
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
    nfs4_client_table_init(&table_a,1);
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
    nfs4_client_table_init(&table_b,1);

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

/* ------------------------------------------------------------------ *
*  NFSv3 DRC                                                          *
* ------------------------------------------------------------------ */

/* Build the keybuf the dispatch path would for a given client/xid/proc/body. */
static struct nfs3_drc_keybuf
nfs3_make_key(
    const char*addr,
    uint32_t   proc,
    uint32_t   xid,
    const void*body,
    uint32_t   body_len)
{
    struct nfs3_drc_keybuf k;

    memset(&k,0,sizeof(k));
    k.addr_len = (uint8_t) strlen(addr);
    memcpy(k.addr,addr,k.addr_len);
    k.proc  = proc;
    k.xid   = xid;
    k.cksum = nfs3_drc_checksum(body,body_len);
    return k;
} /* nfs3_make_key */

static void
test_nfs3_key_encoding(void)
{
    struct nfs3_drc_keybuf k = nfs3_make_key("10.1.2.3",9,0x11223344,
                                             "abc",3);
    uint8_t                buf[CHIMERA_KV_NFS3_REPLY_KEY_MAX];
    uint32_t               klen,p;

    klen = nfs_kv_nfs3_reply_key(buf,0x0102,k.addr,k.addr_len,k.proc,k.xid,
                                 k.cksum);

    /* header + node_id (big-endian) */
    CHECK(buf[0] == CHIMERA_KV_MAGIC);
    CHECK(buf[1] == CHIMERA_KV_VERSION);
    CHECK(buf[2] == CHIMERA_KV_TYPE_NFS3_REPLY);
    CHECK(buf[3] == 0x01 && buf[4] == 0x02);
    /* addr_len + addr */
    p = CHIMERA_KV_PREFIX_LEN;
    CHECK(buf[p] == k.addr_len);
    p++;
    CHECK(memcmp(buf + p,"10.1.2.3",8) == 0);
    p += k.addr_len;
    /* proc, xid (LE32), cksum (LE64) */
    CHECK(nfs_kv_le32(buf + p) == 9); p          += 4;
    CHECK(nfs_kv_le32(buf + p) == 0x11223344); p += 4;
    CHECK(nfs_kv_le64(buf + p) == k.cksum); p    += 8;
    CHECK(klen == p);
    CHECK(klen <= CHIMERA_KV_NFS3_REPLY_KEY_MAX);

    printf("ok: nfs3_key_encoding\n");
} /* test_nfs3_key_encoding */

static void
test_nfs3_checksum_and_cacheable(void)
{
    /* Deterministic + sensitive to content. */
    CHECK(nfs3_drc_checksum("hello",5) == nfs3_drc_checksum("hello",5));
    CHECK(nfs3_drc_checksum("hello",5) != nfs3_drc_checksum("hellp",5));
    CHECK(nfs3_drc_checksum("hello",5) != nfs3_drc_checksum("hell",4));

    /* Non-idempotent ops are cached; idempotent ones bypass. */
    CHECK(nfs3_drc_proc_cacheable(9));   /* MKDIR   */
    CHECK(nfs3_drc_proc_cacheable(8));   /* CREATE  */
    CHECK(nfs3_drc_proc_cacheable(12));  /* REMOVE  */
    CHECK(nfs3_drc_proc_cacheable(14));  /* RENAME  */
    CHECK(nfs3_drc_proc_cacheable(2));   /* SETATTR */
    CHECK(!nfs3_drc_proc_cacheable(0));  /* NULL    */
    CHECK(!nfs3_drc_proc_cacheable(1));  /* GETATTR */
    CHECK(!nfs3_drc_proc_cacheable(6));  /* READ    */
    CHECK(!nfs3_drc_proc_cacheable(7));  /* WRITE   */
    CHECK(!nfs3_drc_proc_cacheable(21)); /* COMMIT  */

    printf("ok: nfs3_checksum_and_cacheable\n");
} /* test_nfs3_checksum_and_cacheable */

static void
test_nfs3_value_roundtrip(void)
{
    const uint8_t body[] = { 0xDE,0xAD,0xBE,0xEF,0x01,0x02 };
    uint8_t       buf[NFS3_DRC_VALUE_HDR_LEN + sizeof(body)];
    uint64_t      ts_in = 0x0123456789ABCDEFull,ts_out;
    const uint8_t*out_body;
    uint32_t      n,out_len;

    n = nfs3_drc_value_serialize(buf,sizeof(buf),ts_in,body,sizeof(body));
    CHECK(n == sizeof(buf));

    CHECK(nfs3_drc_value_parse(buf,n,&ts_out,&out_body,&out_len) == 0);
    CHECK(ts_out == ts_in);
    CHECK(out_len == sizeof(body));
    CHECK(memcmp(out_body,body,sizeof(body)) == 0);

    /* A truncated / wrong-magic buffer is rejected, not trusted. */
    CHECK(nfs3_drc_value_parse(buf,NFS3_DRC_VALUE_HDR_LEN - 1,&ts_out,
                               &out_body,&out_len) != 0);
    buf[0] ^= 0xFF;
    CHECK(nfs3_drc_value_parse(buf,n,&ts_out,&out_body,&out_len) != 0);

    printf("ok: nfs3_value_roundtrip\n");
} /* test_nfs3_value_roundtrip */

static void
test_nfs3_cache_lookup(void)
{
    struct nfs3_drc        drc;
    struct nfs3_drc_keybuf k1 = nfs3_make_key("192.168.0.9",9,7001,
                                              "mkdir-args-A",12);
    struct nfs3_drc_keybuf k2 = nfs3_make_key("192.168.0.9",9,7002,
                                              "mkdir-args-B",12);
    const uint8_t          reply[] = "REPLY-BYTES-FOR-K1";
    uint8_t               *out;
    uint32_t               out_len;

    nfs3_drc_init(&drc);

    /* Miss on an empty cache. */
    CHECK(nfs3_drc_cache_lookup(&drc,&k1,&out,&out_len) == 0);

    nfs3_drc_cache_insert(&drc,&k1,reply,sizeof(reply),1);

    /* Hit returns the exact bytes. */
    CHECK(nfs3_drc_cache_lookup(&drc,&k1,&out,&out_len) == 1);
    CHECK(out_len == sizeof(reply));
    CHECK(memcmp(out,reply,sizeof(reply)) == 0);
    free(out);

    /* A different xid (same client/proc) is a distinct identity -> miss. */
    CHECK(nfs3_drc_cache_lookup(&drc,&k2,&out,&out_len) == 0);

    /* Re-insert under the same key replaces last-writer-wins. */
    nfs3_drc_cache_insert(&drc,&k1,"NEW",3,2);
    CHECK(nfs3_drc_cache_lookup(&drc,&k1,&out,&out_len) == 1);
    CHECK(out_len == 3 && memcmp(out,"NEW",3) == 0);
    free(out);

    nfs3_drc_destroy(&drc);
    printf("ok: nfs3_cache_lookup\n");
} /* test_nfs3_cache_lookup */

/*
 * The decisive NFSv3 check: persist a reply exactly as the capture path does
 * (KV key + value bytes), "reboot" into a fresh cache, parse those bytes back
 * the way the cold-start scan does, and confirm the retransmit's identity hits
 * and replays the original reply.
 */
static void
test_nfs3_cross_reboot(void)
{
    const char            *addr = "172.16.5.20";
    const uint32_t         proc = 9 /* MKDIR */,xid = 0xCAFEBABE;
    const uint8_t          args[]   = "dirfh+name+attrs";
    const uint8_t          reply[]  = "cached MKDIR3res success body";
    struct nfs3_drc_keybuf k_before = nfs3_make_key(addr,proc,xid,
                                                    args,sizeof(args));

    /* --- before reboot: serialize the KV key + value --- */
    uint8_t                kvkey[CHIMERA_KV_NFS3_REPLY_KEY_MAX];
    uint32_t               kvkey_len = nfs_kv_nfs3_reply_key(kvkey,0x0102,
                                                             k_before.addr,
                                                             k_before.addr_len,k_before.proc,
                                                             k_before.xid,k_before.cksum);
    uint8_t                kvval[NFS3_DRC_VALUE_HDR_LEN + sizeof(reply)];
    uint32_t               kvval_len = nfs3_drc_value_serialize(kvval,sizeof(kvval),
                                                                0xABCDull,reply,
                                                                sizeof(reply));

    CHECK(kvval_len == sizeof(kvval));

    /* --- reboot: a fresh, empty cache --- */
    struct nfs3_drc        drc;

    nfs3_drc_init(&drc);

    /* --- reload: parse the persisted key back into a keybuf (mirrors
     * nfs3_drc_reload_scan_cb), parse the value, and insert. --- */
    struct nfs3_drc_keybuf k_after;
    const uint8_t         *body;
    uint64_t               ts;
    uint32_t               body_len,p;
    uint8_t                addr_len;

    CHECK(memcmp(kvkey,(uint8_t[]) { CHIMERA_KV_MAGIC,CHIMERA_KV_VERSION,
                                     CHIMERA_KV_TYPE_NFS3_REPLY,0x01,0x02 },
                 CHIMERA_KV_PREFIX_LEN) == 0);
    addr_len = kvkey[CHIMERA_KV_PREFIX_LEN];
    p        = CHIMERA_KV_PREFIX_LEN + 1;
    CHECK(addr_len <= NFS3_DRC_ADDR_MAX);
    CHECK(p + addr_len + 16 <= kvkey_len);

    memset(&k_after,0,sizeof(k_after));
    memcpy(k_after.addr,kvkey + p,addr_len);
    k_after.addr_len = addr_len;
    p               += addr_len;
    k_after.proc     = nfs_kv_le32(kvkey + p); p += 4;
    k_after.xid      = nfs_kv_le32(kvkey + p); p += 4;
    k_after.cksum    = nfs_kv_le64(kvkey + p);

    CHECK(nfs3_drc_value_parse(kvval,kvval_len,&ts,&body,&body_len) == 0);
    nfs3_drc_cache_insert(&drc,&k_after,body,body_len,ts);

    /* --- retransmit after reboot: the client re-presents an identical call.
     * The independently-recomputed key must hit and return the cached reply. */
    struct nfs3_drc_keybuf k_retransmit = nfs3_make_key(addr,proc,xid,
                                                        args,sizeof(args));
    uint8_t               *out;
    uint32_t               out_len;

    CHECK(nfs3_drc_cache_lookup(&drc,&k_retransmit,&out,&out_len) == 1);
    CHECK(out_len == sizeof(reply));
    CHECK(memcmp(out,reply,sizeof(reply)) == 0);
    free(out);

    /* A retransmit whose body differs (xid reuse for a new call) must NOT hit
     * the stale entry -- the checksum disambiguates it. */
    struct nfs3_drc_keybuf k_other = nfs3_make_key(addr,proc,xid,
                                                   "different-args",14);
    CHECK(nfs3_drc_cache_lookup(&drc,&k_other,&out,&out_len) == 0);

    nfs3_drc_destroy(&drc);
    printf("ok: nfs3_cross_reboot\n");
} /* test_nfs3_cross_reboot */

/* ------------------------------------------------------------------ *
*  Multi-node namespacing (N instances over one shared backing store) *
* ------------------------------------------------------------------ */

/* clientid carries the minting node in its high 16 bits and a per-instance
 * counter in the low 48, and the stateid epoch carries the node in its high 16
 * bits -- so two instances over one store never hand out colliding values. */
static void
test_node_scoped_identifiers(void)
{
    struct nfs_state_table ta,tb;

    /* clientid pack/unpack: node recoverable, counter preserved, no aliasing. */
    CHECK((nfs4_make_clientid(7,42) >> 48) == 7);
    CHECK((nfs4_make_clientid(7,42) & NFS4_CLIENTID_COUNTER_MASK) == 42);
    /* Same counter on two different nodes -> two different clientids. */
    CHECK(nfs4_make_clientid(7,42) != nfs4_make_clientid(8,42));
    /* A node's whole 48-bit counter space stays within its own band. */
    CHECK((nfs4_make_clientid(7,NFS4_CLIENTID_COUNTER_MASK) >> 48) == 7);

    /* stateid epoch: node in the high 16 bits, low bit set, and two nodes that
     * init in the same second still get distinct epochs. */
    nfs_state_table_init(&ta,7);
    nfs_state_table_init(&tb,8);
    CHECK((ta.epoch >> 16) == 7);
    CHECK((tb.epoch >> 16) == 8);
    CHECK((ta.epoch & 1u) == 1u);
    CHECK(ta.epoch != tb.epoch);
    nfs_state_table_free(&ta,NULL);
    nfs_state_table_free(&tb,NULL);

    printf("ok: node_scoped_identifiers\n");
} /* test_node_scoped_identifiers */

/* The decisive sharing property: with the same owner/sessionid persisted by two
 * nodes, the records land under disjoint keys and each node's 5-byte scan prefix
 * matches only its own -- so a cold-start reload never loads a live peer's
 * records out of the shared store. */
static void
test_two_node_key_isolation(void)
{
    const uint16_t a = 11,b = 22;
    uint8_t        sid[NFS4_SESSIONID_SIZE];
    uint8_t        ka[CHIMERA_KV_NFS_KEY_MAX],kb[CHIMERA_KV_NFS_KEY_MAX];
    uint8_t        sa[CHIMERA_KV_SESSION_KEY_LEN],sb[CHIMERA_KV_SESSION_KEY_LEN];
    uint8_t        pa[CHIMERA_KV_PREFIX_LEN];
    uint32_t       la,lb;

    memset(sid,0x5A,sizeof(sid));

    /* Identical owner, two nodes -> distinct recovery keys (both coexist). */
    la = nfs_kv_recovery_key(ka,a,(const uint8_t*) "same-owner",10);
    lb = nfs_kv_recovery_key(kb,b,(const uint8_t*) "same-owner",10);
    CHECK(la == lb);
    CHECK(memcmp(ka,kb,la) != 0);

    /* Node A's recovery scan prefix matches A's key, not B's. */
    nfs_kv_node_prefix(pa,CHIMERA_KV_TYPE_NFS4_RECOVERY,a);
    CHECK(memcmp(pa,ka,CHIMERA_KV_PREFIX_LEN) == 0);
    CHECK(memcmp(pa,kb,CHIMERA_KV_PREFIX_LEN) != 0);

    /* Same for an identical sessionid persisted by both nodes. */
    nfs_kv_session_key(sa,a,sid);
    nfs_kv_session_key(sb,b,sid);
    CHECK(memcmp(sa,sb,CHIMERA_KV_SESSION_KEY_LEN) != 0);
    nfs_kv_node_prefix(pa,CHIMERA_KV_TYPE_NFS4_SESSION,a);
    CHECK(memcmp(pa,sa,CHIMERA_KV_PREFIX_LEN) == 0);
    CHECK(memcmp(pa,sb,CHIMERA_KV_PREFIX_LEN) != 0);

    printf("ok: two_node_key_isolation\n");
} /* test_two_node_key_isolation */

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

    test_nfs3_key_encoding();
    test_nfs3_checksum_and_cacheable();
    test_nfs3_value_roundtrip();
    test_nfs3_cache_lookup();
    test_nfs3_cross_reboot();

    test_node_scoped_identifiers();
    test_two_node_key_isolation();

    printf("PASS: all nfs persistence tests\n");
    return 0;
} /* main */
