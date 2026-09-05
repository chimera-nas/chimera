/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * SMB2 alternate-data-stream (ADS / named stream) ground-truth probe.
 *
 * The MBT corpus never enables named streams, so the whole ADS lifecycle --
 * creating a named stream with the "file:stream" CREATE syntax, writing and
 * reading its independent content, enumerating streams with
 * FILE_STREAM_INFORMATION, and deleting a stream -- is dark to the model.  It
 * is also the SMB view of the exact VFS named-stream storage that NFSv4 named
 * attributes project onto (open_stream/list_streams/remove_stream,
 * CAP_NAMED_STREAMS, the memfs per-inode stream list), so this probe is the SMB
 * half of the cross-protocol stream coverage.
 *
 * Like the other ground-truth probes the classes constrain each other: a
 * stream's content read back must equal what was written; the size reported by
 * FILE_STREAM_INFORMATION must equal the bytes written; a write to one stream
 * must not perturb the base file's data or the sibling streams; and a deleted
 * stream must disappear from the enumeration while its siblings survive.
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

/* Enumerate the named streams of the file open on `fid` via
 * FILE_STREAM_INFORMATION.  Returns the number of entries; if `want` is
 * non-NULL, sets *want_size to the size of the stream whose (case-sensitive)
 * name contains `want` and *want_found to 1 when it was seen. */
static int
enum_streams(
    struct smb2_conn *c,
    const uint8_t     fid[16],
    const char       *want,
    uint64_t         *want_size,
    int              *want_found)
{
    uint8_t  out[2048];
    uint32_t st, len = 0, off = 0;
    int      entries = 0;

    if (want_found) {
        *want_found = 0;
    }
    if (want_size) {
        *want_size = 0;
    }

    st = smb2_query_info(c, SMB2_INFO_FILE_T, SMB2_FILE_STREAM_INFO_T,
                         fid, 0, out, sizeof(out), &len);
    if (st != ST_SUCCESS || len == 0) {
        return -1;
    }

    /* Each record: NextEntryOffset(4) StreamNameLength(4) StreamSize(8)
    * StreamAllocationSize(8) StreamName(UTF-16LE).  (MS-FSCC 2.4.43) */
    while (off + 24 <= len) {
        uint32_t next = g32(out, (int) off);
        uint32_t nlen = g32(out, (int) off + 4);
        uint64_t size = g64(out, (int) off + 8);
        char     nm[256];
        uint32_t i;

        entries++;
        if (nlen / 2 < sizeof(nm) - 1 && off + 24 + nlen <= len) {
            for (i = 0; i < nlen / 2; i++) {
                nm[i] = (char) out[off + 24 + i * 2];
            }
            nm[nlen / 2] = '\0';
            if (want && strstr(nm, want)) {
                if (want_found) {
                    *want_found = 1;
                }
                if (want_size) {
                    *want_size = size;
                }
            }
        }
        if (next == 0) {
            break;
        }
        off += next;
    }
    return entries;
} /* enum_streams */

/* The full ADS lifecycle against one base file. */
static void
probe_stream_lifecycle(struct smb2_conn *c)
{
    struct smb2_create_out base, alt, beta, reopen;
    uint8_t                rd[64];
    uint32_t               st, cnt = 0, rlen = 0;
    uint64_t               sz = 0;
    int                    entries, found = 0;

    printf("# --- ADS lifecycle ---\n");

    /* Base file with its own data stream. */
    st = smb2_create(c, "ads.bin", FILE_OVERWRITE_IF, FILE_ALL_ACCESS,
                     FILE_SHARE_RWD, NULL, &base);
    CHECK(st == ST_SUCCESS, "CREATE base ads.bin -> 0x%08x", st);
    if (st != ST_SUCCESS) {
        return;
    }
    st = smb2_write(c, base.file_id, 0, "BASEDATA", 8, &cnt);
    CHECK(st == ST_SUCCESS && cnt == 8, "WRITE 8 bytes to the base data stream");

    /* Only the unnamed default stream exists so far. */
    entries = enum_streams(c, base.file_id, NULL, NULL, NULL);
    CHECK(entries == 1, "enumeration reports only the default stream (%d)",
          entries);

    /* Create an alternate named stream via the file:stream CREATE syntax and
     * give it content distinct from the base. */
    st = smb2_create(c, "ads.bin:alt", FILE_OVERWRITE_IF, FILE_ALL_ACCESS,
                     FILE_SHARE_RWD, NULL, &alt);
    CHECK(st == ST_SUCCESS, "CREATE ads.bin:alt -> 0x%08x", st);
    if (st == ST_SUCCESS) {
        st = smb2_write(c, alt.file_id, 0, "ALTERNATE", 9, &cnt);
        CHECK(st == ST_SUCCESS && cnt == 9, "WRITE 9 bytes to :alt");

        /* Read the stream back through its own handle: byte-identical. */
        st = smb2_read(c, alt.file_id, 0, sizeof(rd), rd, &rlen);
        CHECK(st == ST_SUCCESS && rlen == 9 && memcmp(rd, "ALTERNATE", 9) == 0,
              "READ :alt returns its 9 bytes byte-identical (0x%08x, %u)", st,
              rlen);
        smb2_close(c, alt.file_id);
    }

    /* Enumeration now lists the default stream plus :alt at 9 bytes. */
    entries = enum_streams(c, base.file_id, "alt", &sz, &found);
    CHECK(entries == 2, "enumeration lists both streams (%d)", entries);
    CHECK(found && sz == 9, ":alt is named and carries its 9 bytes (sz=%llu)",
          (unsigned long long) sz);

    /* A second named stream is independent again. */
    st = smb2_create(c, "ads.bin:beta", FILE_OVERWRITE_IF, FILE_ALL_ACCESS,
                     FILE_SHARE_RWD, NULL, &beta);
    CHECK(st == ST_SUCCESS, "CREATE ads.bin:beta -> 0x%08x", st);
    if (st == ST_SUCCESS) {
        smb2_write(c, beta.file_id, 0, "BB", 2, &cnt);
        smb2_close(c, beta.file_id);
    }
    entries = enum_streams(c, base.file_id, "beta", &sz, &found);
    CHECK(entries == 3, "enumeration lists the default + two streams (%d)",
          entries);
    CHECK(found && sz == 2, ":beta carries its 2 bytes (sz=%llu)",
          (unsigned long long) sz);

    /* Base/stream independence: the base data stream still reads back its own
     * 8 bytes, untouched by the stream writes. */
    st = smb2_read(c, base.file_id, 0, sizeof(rd), rd, &rlen);
    CHECK(st == ST_SUCCESS && rlen == 8 && memcmp(rd, "BASEDATA", 8) == 0,
          "the base data stream is intact after the stream writes (%u)", rlen);

    /* Delete :alt via delete-on-close disposition on its own handle
     * (FILE_ALL_ACCESS already includes the DELETE right). */
    st = smb2_create(c, "ads.bin:alt", FILE_OPEN, FILE_ALL_ACCESS,
                     FILE_SHARE_RWD, NULL, &reopen);
    CHECK(st == ST_SUCCESS, "re-open :alt for delete -> 0x%08x", st);
    if (st == ST_SUCCESS) {
        st = smb2_set_disposition(c, reopen.file_id, 1);
        CHECK(st == ST_SUCCESS, "set delete-on-close on :alt -> 0x%08x", st);
        smb2_close(c, reopen.file_id);
    }

    /* :alt is gone; the default stream and :beta survive. */
    entries = enum_streams(c, base.file_id, "alt", &sz, &found);
    CHECK(entries == 2, "after delete the enumeration lists two streams (%d)",
          entries);
    CHECK(!found, ":alt no longer appears in the enumeration");

    /* Opening the deleted stream by name is refused. */
    st = smb2_create(c, "ads.bin:alt", FILE_OPEN, FILE_ALL_ACCESS,
                     FILE_SHARE_RWD, NULL, &reopen);
    CHECK(st != ST_SUCCESS, "opening the deleted stream :alt is refused (0x%08x)",
          st);
    if (st == ST_SUCCESS) {
        smb2_close(c, reopen.file_id);
    }

    smb2_close(c, base.file_id);
} /* probe_stream_lifecycle */

int
main(
    int   argc,
    char *argv[])
{
    struct smb2_env      env;
    /* Named streams are off in the server by default; the ADS surface needs
     * them advertised (CAP_NAMED_STREAMS on the backend + the config knob). */
    struct smb2_env_opts opts = { .named_streams = 1 };
    struct smb2_conn    *c;

    (void) argc;
    (void) argv;

    setvbuf(stdout, NULL, _IONBF, 0);

    smb2_env_start_opts(&env, &opts);
    c = smb2_conn_open(&env);
    smb2_handshake(c);

    printf("# dialect=0x%04x\n", c->dialect);

    probe_stream_lifecycle(c);

    smb2_env_stop(&env);

    if (failures) {
        fprintf(stderr, "%d ADS stream check(s) FAILED\n", failures);
        return 1;
    }
    printf("all SMB2 named-stream checks passed\n");
    return 0;
} /* main */
