/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * SMB Direct (MS-SMBD) probe.  Drives the in-process chimera SMB server over
 * its SMB Direct listener, which on the in-process transport is a datagram
 * endpoint standing in for RDMA, so the SMB Direct framing -- the transport
 * negotiate, the data-transfer header, and the server's fragmentation of a
 * reply larger than the client's MaxReceiveSize -- runs without RDMA
 * hardware.  The harness reassembles each reply and records how it was
 * fragmented, so every READ here is checked twice: the bytes against what was
 * written, and the fragment count and sizes against what MaxReceiveSize
 * dictates.  The harness accepts no message larger than the PreferredSendSize
 * the server answered the negotiate with, as Windows does, and each
 * connection checks that PreferredSendSize is the smaller of the server's own
 * send size and the MaxReceiveSize offered.  Keepalives -- empty messages
 * asking for a response -- are checked to be answered on a full-size and a
 * minimum-size connection.
 *
 * Given a wire profile name (signed311), every session is signed and every
 * signed reply's signature is checked, which is what shows the server signed
 * each reply from the right place behind the SMB Direct header.
 */

#include "smb2_mbt_common.h"

static int failures = 0;

#define CHECK(cond, ...)                             \
        do {                                         \
            if (cond) {                              \
                printf("ok   - " __VA_ARGS__);       \
                printf("\n");                        \
            } else {                                 \
                printf("FAIL - " __VA_ARGS__);       \
                printf("\n");                        \
                failures++;                          \
            }                                        \
        } while (0)

/* The MaxReceiveSize this client offers, and so the largest message the server
 * may send it; each fragment carries that less the 24-byte header. */
#define DIRECT_MAX_RECV        8192
#define DIRECT_MAX_SEND        (DIRECT_MAX_RECV - SMB2C_DIRECT_HDR) /* 8168 */
#define DIRECT_FILE_SIZE       65536

/* The most the server sends in one message, however much a client offers. */
#define DIRECT_SERVER_MAX_SEND 8192

static uint8_t pattern[DIRECT_FILE_SIZE];
static uint8_t rdbuf[DIRECT_FILE_SIZE];

/* An SMB Direct connection that also checks the signature on every signed
 * reply, which only matters on a signed run.  Once negotiated, the server's
 * PreferredSendSize must be its MaxSendSize, the smaller of its own and the
 * MaxReceiveSize offered (MS-SMBD 3.1.5.6 and 3.1.5.3): a client takes it as
 * the most the server will send and sizes its receive buffers to it. */
static struct smb2_conn *
open_direct(
    struct smb2_env *env,
    uint32_t         max_receive_size)
{
    struct smb2_conn *c        = smb2_conn_open_direct(env, max_receive_size);
    uint32_t          max_send = max_receive_size < DIRECT_SERVER_MAX_SEND ?
        max_receive_size : DIRECT_SERVER_MAX_SEND;

    c->check_reply_sigs = 1;
    if (c->direct_negotiated) {
        CHECK(c->direct_peer_pref_send == max_send,
              "MaxReceiveSize %u: server PreferredSendSize %u (%u)",
              max_receive_size, max_send, c->direct_peer_pref_send);
    }
    return c;
} /* open_direct */

/* READ len bytes at offset 0, then check the data and the fragmentation: the
 * reply arrived in nfrag fragments, each but the final one carrying max_send
 * bytes and the final one carrying last bytes. */
static void
check_read(
    struct smb2_conn *c,
    const uint8_t     fid[16],
    uint32_t          len,
    uint32_t          max_send,
    int               nfrag,
    uint32_t          last)
{
    uint32_t rlen = 0;
    uint32_t st   = smb2_read(c, fid, 0, len, rdbuf, &rlen);

    CHECK(st == ST_SUCCESS && rlen == len, "READ %u -> 0x%08x len=%u", len, st, rlen);
    CHECK(memcmp(rdbuf, pattern, len) == 0, "READ %u data intact", len);
    if (c->signing_on) {
        /* The harness already rejected a bad signature; this makes sure there
         * was one to check. */
        CHECK(g32(c->rbuf + 4, 16) & SMB2_FLAGS_SIGNED, "READ %u reply is signed", len);
    }
    CHECK(c->direct_nfrag == nfrag, "READ %u in %d fragment(s) (%d)", len, nfrag, c->direct_nfrag);
    CHECK(c->direct_nfrag > 0, "READ %u left a fragment record", len);
    if (c->direct_nfrag == 0) {
        return;
    }
    for (int i = 0; i + 1 < c->direct_nfrag; i++) {
        CHECK(c->direct_frag_len[i] == max_send, "fragment %d is full (%u)", i, c->direct_frag_len[i]);
    }
    CHECK(c->direct_frag_len[c->direct_nfrag - 1] == last, "last fragment %u (%u)", last,
          c->direct_frag_len[c->direct_nfrag - 1]);
} /* check_read */

/* COUNT unrelated READs of 16 bytes (the i-th at offset 16 * i) in ONE
 * compound.  The reply is 96 * COUNT bytes -- far below one fragment -- but
 * carries 2 * COUNT + 1 iovecs, more than one send may hold. */
static void
check_compound_read(
    struct smb2_conn *c,
    const uint8_t     fid[16],
    int               count)
{
    int      off, total, nrsp, i;
    uint8_t *h, *body;

    /* Each request is a 64-byte header and a 49-byte body, padded to 120 so
     * the next header is 8-byte aligned; the last one is not padded. */
    total = 120 * (count - 1) + SMB2_HDR_SIZE + 49;
    memset(c->sbuf, 0, 4 + (size_t) total);

    for (i = 0; i < count; i++) {
        h    = c->sbuf + 4 + 120 * i;
        body = h + SMB2_HDR_SIZE;

        h[0] = 0xFE;
        h[1] = 'S';
        h[2] = 'M';
        h[3] = 'B';
        p16(h, 4, SMB2_HDR_SIZE);
        p16(h, 6, 1);                     /* CreditCharge */
        p16(h, 12, SMB2_READ);
        p16(h, 14, 256);                  /* CreditRequest */
        p32(h, 20, i + 1 < count ? 120 : 0); /* NextCommand */
        p64(h, 24, c->msg_id + (uint64_t) i);
        p32(h, 36, c->tree_id);
        p64(h, 40, c->session_id);

        p16(body, 0, 49);                 /* StructureSize */
        body[2] = SMB2_HDR_SIZE + 16;     /* Padding: desired data offset */
        p32(body, 4, 16);                 /* Length */
        p64(body, 8, (uint64_t) (16 * i)); /* Offset */
        memcpy(body + 16, fid, 16);       /* FileId */
    }

    if (c->signing_on) {
        uint8_t sig[16];

        /* Each request in a compound is signed on its own, up to its
         * NextCommand (MS-SMB2 3.2.4.1.1), but smb2c_send signs its buffer as
         * one message: sign here and keep smb2c_send from signing again. */
        for (i = 0; i < count; i++) {
            h = c->sbuf + 4 + 120 * i;
            p32(h, 16, g32(h, 16) | SMB2_FLAGS_SIGNED);
            smb2w_sign(c->dialect, c->signing_alg, c->signing_key, h,
                       i + 1 < count ? 120 : SMB2_HDR_SIZE + 49, sig);
            memcpy(h + 48, sig, 16);
        }
        c->signing_on = 0;
        smb2c_send(c, total - SMB2_HDR_SIZE);
        c->signing_on = 1;
    } else {
        smb2c_send(c, total - SMB2_HDR_SIZE);
    }
    c->msg_id += (uint64_t) (count - 1);
    smb2c_wait(c);

    /* Walk the responses by NextCommand. */
    nrsp = 0;
    off  = 4;
    while (off + SMB2_HDR_SIZE + 16 <= c->rlen) {
        uint32_t st, dlen, next;
        uint8_t  doff;

        h    = c->rbuf + off;
        body = h + SMB2_HDR_SIZE;
        st   = g32(h, 8);
        next = g32(h, 20);
        doff = body[2];
        dlen = g32(body, 4);

        CHECK(nrsp < count && st == ST_SUCCESS && dlen == 16 &&
              off + doff + 16 <= c->rlen &&
              memcmp(h + doff, pattern + 16 * nrsp, 16) == 0,
              "compound READ %d intact (0x%08x, %u bytes)", nrsp, st, dlen);
        if (c->signing_on) {
            CHECK(g32(h, 16) & SMB2_FLAGS_SIGNED, "compound READ %d reply is signed", nrsp);
        }
        nrsp++;
        if (next == 0) {
            break;
        }
        off += (int) next;
    }

    CHECK(nrsp == count, "compound READ: %d responses (%d)", count, nrsp);
    CHECK(c->rlen - 4 == count * 96, "compound reply is %d bytes (%d)", count * 96, c->rlen - 4);
    /* The payload's bytes sit in 2 * COUNT of those iovecs (the one after the
     * last READ's data is empty), and a fragment carries at most 7 of them,
     * SMB_DIRECT_FRAGMENT_MAX_IOV less its header.  The fragment count pins
     * that limit, which the in-process transport would otherwise never notice:
     * unlike an RDMA device, it has no send-SGE budget to overrun. */
    CHECK(c->direct_nfrag == (2 * count + 6) / 7,
          "a reply with too many iovecs for one send is split %d ways (%d)",
          (2 * count + 6) / 7, c->direct_nfrag);
} /* check_compound_read */

/* Keepalives (MS-SMBD 3.1.5.8): a data-transfer message with
 * SMB_DIRECT_RESPONSE_REQUESTED must be answered promptly, or the peer's
 * idle timer tears the connection down.  The answer is checked after an ECHO
 * round trip: the transport keeps order and the server answers as it
 * receives, so an answer would have landed before the ECHO reply. */
static void
check_keepalives(struct smb2_conn *c)
{
    int n0;

    /* K1: an empty message asking for a response, as an idle peer's
     * keepalive timer sends it, gets exactly one empty message back: no
     * payload (DataOffset 0, 20 bytes), not asking for one in turn, and
     * granting credits. */
    n0 = c->direct_nempty;
    smb2_direct_send_empty(c, SMB2C_DIRECT_RESPONSE_REQUESTED);
    smb2_echo_barrier(c);
    CHECK(c->direct_nempty == n0 + 1, "K1: keepalive answered with one empty message (%d)",
          c->direct_nempty - n0);
    if (c->direct_nempty == n0 + 1) {
        CHECK(c->direct_empty_len == 20 && c->direct_empty_data_offset == 0,
              "K1: the answer is a bare header (%d bytes, DataOffset %u)",
              c->direct_empty_len, c->direct_empty_data_offset);
        CHECK(!(c->direct_empty_flags & SMB2C_DIRECT_RESPONSE_REQUESTED),
              "K1: the answer does not ask for a response (Flags 0x%04x)", c->direct_empty_flags);
        CHECK(c->direct_empty_credits_granted > 0, "K1: the answer grants credits (%u)",
              c->direct_empty_credits_granted);
    }

    /* K2: an empty message that does not ask for a response is a bare
     * credit grant, and nothing comes back. */
    n0 = c->direct_nempty;
    smb2_direct_send_empty(c, 0);
    smb2_echo_barrier(c);
    CHECK(c->direct_nempty == n0, "K2: an empty message without the flag is not answered (%d)",
          c->direct_nempty - n0);

    /* K3: the flag on a message that carries an SMB2 request: answered at
     * once, whether or not the request itself completes soon. */
    n0                   = c->direct_nempty;
    c->direct_next_flags = SMB2C_DIRECT_RESPONSE_REQUESTED;
    smb2_echo_barrier(c);
    CHECK(c->direct_nempty == n0 + 1, "K3: a request asking for a response is answered at once (%d)",
          c->direct_nempty - n0);

    /* K4: repeated keepalives on an idle connection are each answered. */
    n0 = c->direct_nempty;
    for (int i = 0; i < 3; i++) {
        smb2_direct_send_empty(c, SMB2C_DIRECT_RESPONSE_REQUESTED);
    }
    smb2_echo_barrier(c);
    CHECK(c->direct_nempty == n0 + 3, "K4: three keepalives, three answers (%d)", c->direct_nempty - n0);
} /* check_keepalives */

int
main(
    int   argc,
    char *argv[])
{
    struct smb2_env                 env;
    struct smb2_env_opts            opts = { 0 };
    const struct smb2_wire_profile *wire = argc > 1 ? smb2_wire_profile_find(argv[1]) : NULL;
    struct smb2_conn               *c, *c2, *c3, *c4, *bad;
    struct smb2_create_out          co, co2, co3, co4;
    uint32_t                        st, count, off;

    for (uint32_t i = 0; i < DIRECT_FILE_SIZE; i++) {
        pattern[i] = (uint8_t) (i ^ (i >> 8) ^ (i >> 16));
    }

    opts.smb_direct       = 1;
    opts.memfs_block_size = 4096;
    smb2_env_open_wire(&env, &opts, wire);
    smb2_env_fs_setup(&env, "fs0");

    c = open_direct(&env, DIRECT_MAX_RECV);
    CHECK(c->direct_negotiated, "SMB Direct negotiate accepted");
    if (!c->direct_negotiated) {
        /* Nothing below can run on a connection the server refused. */
        smb2_env_stop(&env);
        fprintf(stderr, "SMB Direct negotiate failed\n");
        return 1;
    }
    CHECK(c->direct_peer_max_recv == 8192, "server MaxReceiveSize 8192 (%u)", c->direct_peer_max_recv);
    smb2_handshake(c);
    check_keepalives(c);

    st = smb2_create(c, "direct.bin", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &co);
    CHECK(st == ST_SUCCESS, "CREATE direct.bin -> 0x%08x", st);

    /* 16 WRITEs of 4096: a request must fit one SMB Direct message, and the
     * harness does not fragment requests. */
    for (off = 0; off < DIRECT_FILE_SIZE; off += 4096) {
        count = 0;
        st    = smb2_write(c, co.file_id, off, pattern + off, 4096, &count);
        CHECK(st == ST_SUCCESS && count == 4096, "WRITE 4096 at %u -> 0x%08x count=%u",
              off, st, count);
    }

    /* R1: a reply well under MaxReceiveSize is one fragment, the whole
     * 88-byte READ response (80 bytes of header and body, 8 of data). */
    check_read(c, co.file_id, 8, DIRECT_MAX_SEND, 1, 88);

    /* Replies past MaxReceiveSize: every fragment but the last carries
     * DIRECT_MAX_SEND bytes, and memfs hands the data back as one iovec per
     * 4096-byte block, so most fragments span several reply iovecs. */
    check_read(c, co.file_id, 8088, DIRECT_MAX_SEND, 1, 8168);    /* R2: exactly one fragment */
    check_read(c, co.file_id, 8089, DIRECT_MAX_SEND, 2, 1);       /* R3: 1-byte tail (inline send on RDMA) */
    check_read(c, co.file_id, 13088, DIRECT_MAX_SEND, 2, 5000);   /* R4: tail between 4096 and max_send */
    check_read(c, co.file_id, 32768, DIRECT_MAX_SEND, 5, 176);    /* R5: fragments span 3 block iovecs */
    check_read(c, co.file_id, 65536, DIRECT_MAX_SEND, 9, 272);    /* R6 */

    /* A reply short enough for one fragment but with too many iovecs for one
     * send. */
    check_compound_read(c, co.file_id, 12);

    st = smb2_close(c, co.file_id);
    CHECK(st == ST_SUCCESS, "CLOSE -> 0x%08x", st);

    /* A second connection offering a small MaxReceiveSize, so every reply
     * larger than 1000 bytes fragments, the handshake's included. */
    c2 = open_direct(&env, 1024);
    CHECK(c2->direct_negotiated, "SMB Direct negotiate accepted at MaxReceiveSize 1024");
    smb2_handshake(c2);
    st = smb2_create(c2, "direct.bin", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &co2);
    CHECK(st == ST_SUCCESS, "re-open on the small-MTU connection -> 0x%08x", st);
    check_read(c2, co2.file_id, 4096, 1000, 5, 176);              /* R7: 4176 = 4 * 1000 + 176 */
    st = smb2_close(c2, co2.file_id);
    CHECK(st == ST_SUCCESS, "CLOSE on the small-MTU connection -> 0x%08x", st);

    /* Below the MS-SMBD minimum: refused, and the server keeps serving. */
    bad = open_direct(&env, 64);
    CHECK(!bad->direct_negotiated && bad->disconnected, "MaxReceiveSize 64 is refused");
    bad = open_direct(&env, 127);
    CHECK(!bad->direct_negotiated && bad->disconnected, "MaxReceiveSize 127 is refused");
    bad = open_direct(&env, 16);
    CHECK(!bad->direct_negotiated && bad->disconnected, "MaxReceiveSize 16 is refused");

    /* The minimum itself works: 104-byte fragments. */
    c3 = open_direct(&env, 128);
    CHECK(c3->direct_negotiated, "MaxReceiveSize 128 is accepted");
    smb2_handshake(c3);
    check_keepalives(c3);
    st = smb2_create(c3, "direct.bin", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &co3);
    CHECK(st == ST_SUCCESS, "open on the minimum-size connection -> 0x%08x", st);
    check_read(c3, co3.file_id, 1024, 104, 11, 64);               /* 1104 = 10 * 104 + 64 */
    st = smb2_close(c3, co3.file_id);
    CHECK(st == ST_SUCCESS, "CLOSE on the minimum-size connection -> 0x%08x", st);

    /* A peer that would take 64 KiB at once still gets the server's own size. */
    c4 = open_direct(&env, 65536);
    CHECK(c4->direct_negotiated, "MaxReceiveSize 65536 is accepted");
    smb2_handshake(c4);
    st = smb2_create(c4, "direct.bin", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &co4);
    CHECK(st == ST_SUCCESS, "open on the large-MTU connection -> 0x%08x", st);
    check_read(c4, co4.file_id, 32768, DIRECT_MAX_SEND, 5, 176);
    st = smb2_close(c4, co4.file_id);
    CHECK(st == ST_SUCCESS, "CLOSE on the large-MTU connection -> 0x%08x", st);

    smb2_env_stop(&env);

    if (failures) {
        fprintf(stderr, "%d SMB Direct check(s) FAILED\n", failures);
        return 1;
    }
    printf("all SMB Direct checks passed\n");
    return 0;
} /* main */
