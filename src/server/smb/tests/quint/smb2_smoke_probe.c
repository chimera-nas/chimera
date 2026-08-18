/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * SMB2 wire-shim smoke probe (ROADMAP-SMB.md Increment 0).  Drives the
 * in-process chimera SMB server over the hand-rolled SMB2 client in
 * smb2_mbt_common.h: NEGOTIATE, anonymous SESSION_SETUP, TREE_CONNECT,
 * then CREATE a file, WRITE bytes, READ them back and assert they match,
 * CLOSE, and re-open to confirm the write persisted and the change
 * attribute advanced.  It proves the wire codec talks to the real server
 * end to end before any model-driven replay is built on top of it.
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

int
main(
    int   argc,
    char *argv[])
{
    struct smb2_env        env;
    struct smb2_conn      *c;
    struct smb2_create_out c1, c2;
    const char             payload[] = "SMBSMOKE";   /* 8 bytes, no NUL */
    uint8_t                rdbuf[64];
    uint32_t               st, count = 0, rlen = 0;

    smb2_env_start(&env);
    c = smb2_conn_open(&env);
    smb2_handshake(c);

    printf("# handshake: dialect=0x%04x session_id=0x%" PRIx64 " tree_id=%u\n",
           c->dialect, c->session_id, c->tree_id);

    /* CREATE (create-if-absent) a fresh file, no oplock/lease. */
    st = smb2_create(c, "smoke.txt", FILE_OPEN_IF, FILE_ALL_ACCESS,
                     FILE_SHARE_RWD, NULL, &c1);
    CHECK(st == ST_SUCCESS, "CREATE smoke.txt -> 0x%08x", st);
    CHECK(c1.action == FILE_ACT_CREATED, "CreateAction=CREATED (%u)", c1.action);
    CHECK(c1.end_of_file == 0, "new file EndOfFile=0 (%" PRIu64 ")",
          c1.end_of_file);

    /* WRITE the payload at offset 0. */
    st = smb2_write(c, c1.file_id, 0, payload, 8, &count);
    CHECK(st == ST_SUCCESS, "WRITE 8 bytes -> 0x%08x", st);
    CHECK(count == 8, "WRITE count=8 (%u)", count);

    /* READ it back and compare. */
    st = smb2_read(c, c1.file_id, 0, 8, rdbuf, &rlen);
    CHECK(st == ST_SUCCESS, "READ 8 bytes -> 0x%08x", st);
    CHECK(rlen == 8 && memcmp(rdbuf, payload, 8) == 0,
          "READ data matches WRITE (len=%u)", rlen);

    st = smb2_close(c, c1.file_id);
    CHECK(st == ST_SUCCESS, "CLOSE -> 0x%08x", st);

    /* Re-open and confirm the write persisted and the change attribute moved. */
    st = smb2_create(c, "smoke.txt", FILE_OPEN, FILE_ALL_ACCESS,
                     FILE_SHARE_RWD, NULL, &c2);
    CHECK(st == ST_SUCCESS, "re-CREATE (OPEN) -> 0x%08x", st);
    CHECK(c2.action == FILE_ACT_OPENED, "CreateAction=OPENED (%u)", c2.action);
    CHECK(c2.end_of_file == 8, "re-open EndOfFile=8 (%" PRIu64 ")",
          c2.end_of_file);
    CHECK(c2.change_time != c1.change_time,
          "ChangeTime advanced after write (0x%" PRIx64 " -> 0x%" PRIx64 ")",
          c1.change_time, c2.change_time);

    smb2_close(c, c2.file_id);

    smb2_env_stop(&env);

    if (failures) {
        fprintf(stderr, "%d smoke check(s) FAILED\n", failures);
        return 1;
    }
    printf("all SMB2 smoke checks passed\n");
    return 0;
} /* main */
