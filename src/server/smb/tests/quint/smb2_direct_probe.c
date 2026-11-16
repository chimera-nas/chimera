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
 * dictates.
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
#define DIRECT_MAX_RECV  8192
#define DIRECT_MAX_SEND  (DIRECT_MAX_RECV - SMB2C_DIRECT_HDR)   /* 8168 */
#define DIRECT_FILE_SIZE 65536

static uint8_t pattern[DIRECT_FILE_SIZE];
static uint8_t rdbuf[DIRECT_FILE_SIZE];

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
    CHECK(c->direct_nfrag == nfrag, "READ %u in %d fragment(s) (%d)", len, nfrag, c->direct_nfrag);
    for (int i = 0; i + 1 < c->direct_nfrag; i++) {
        CHECK(c->direct_frag_len[i] == max_send, "fragment %d is full (%u)", i, c->direct_frag_len[i]);
    }
    CHECK(c->direct_frag_len[c->direct_nfrag - 1] == last, "last fragment %u (%u)", last,
          c->direct_frag_len[c->direct_nfrag - 1]);
} /* check_read */

int
main(
    int   argc,
    char *argv[])
{
    struct smb2_env        env;
    struct smb2_env_opts   opts = { 0 };
    struct smb2_conn      *c;
    struct smb2_create_out co;
    uint32_t               st, count, off;

    for (uint32_t i = 0; i < DIRECT_FILE_SIZE; i++) {
        pattern[i] = (uint8_t) (i ^ (i >> 8) ^ (i >> 16));
    }

    opts.smb_direct       = 1;
    opts.memfs_block_size = 4096;
    smb2_env_start_opts(&env, &opts);

    c = smb2_conn_open_direct(&env, DIRECT_MAX_RECV);
    CHECK(c->direct_negotiated, "SMB Direct negotiate accepted");
    if (!c->direct_negotiated) {
        /* Nothing below can run on a connection the server refused. */
        smb2_env_stop(&env);
        fprintf(stderr, "SMB Direct negotiate failed\n");
        return 1;
    }
    CHECK(c->direct_peer_max_recv == 8192, "server MaxReceiveSize 8192 (%u)", c->direct_peer_max_recv);
    smb2_handshake(c);

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

    st = smb2_close(c, co.file_id);
    CHECK(st == ST_SUCCESS, "CLOSE -> 0x%08x", st);

    smb2_env_stop(&env);

    if (failures) {
        fprintf(stderr, "%d SMB Direct check(s) FAILED\n", failures);
        return 1;
    }
    printf("all SMB Direct checks passed\n");
    return 0;
} /* main */
