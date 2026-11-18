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
 *
 * The probe embeds memfs and observes its dispatch, so it can also hold the
 * server to the backend's own books: every stream handle memfs opened must
 * have been closed by the time the server has shut down.  memfs reclaims an
 * abandoned stream open itself when the filesystem is destroyed, so a leaked
 * handle is otherwise invisible here; on a backend that holds a resource per
 * open it stays pinned until the process exits.
 */

#include <stdatomic.h>
#include "smb2_mbt_common.h"
#include "vfs/vfs.h"

extern struct chimera_vfs_module               vfs_memfs;

static void                                    (*memfs_dispatch)(
    struct chimera_vfs_request *,
    void *);
static _Atomic chimera_vfs_complete_callback_t open_stream_complete;
static _Atomic chimera_vfs_complete_callback_t open_fh_complete;
static atomic_int                              backend_stream_opens;
static atomic_int                              backend_stream_closes;

static int                                     failures = 0;

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

/* Remember the completion an opcode's request arrived with.  One slot per
 * opcode is only sound while the VFS issues every request of that opcode with
 * the same completion function; a second one (a completion gate, say) would
 * send requests through the wrong function, so stop the probe there. */
static void
save_complete(
    _Atomic chimera_vfs_complete_callback_t *slot,
    chimera_vfs_complete_callback_t          complete,
    const char                              *op)
{
    chimera_vfs_complete_callback_t prev = atomic_exchange(slot, complete);

    if (prev && prev != complete) {
        fprintf(stderr, "smb2_stream_probe: %s arrived with two different "
                "completion functions; the counting hook cannot wrap it\n",
                op);
        abort();
    }
} /* save_complete */

/* A backend stream open is an OPEN_STREAM, or an OPEN_FH of a stream fh, that
 * memfs completed successfully. */
static void
count_open_stream_complete(struct chimera_vfs_request *request)
{
    if (request->status == CHIMERA_VFS_OK) {
        atomic_fetch_add(&backend_stream_opens, 1);
    }
    atomic_load(&open_stream_complete)(request);
} /* count_open_stream_complete */

static void
count_open_fh_complete(struct chimera_vfs_request *request)
{
    if (request->status == CHIMERA_VFS_OK && request->open_fh.r_stream) {
        atomic_fetch_add(&backend_stream_opens, 1);
    }
    atomic_load(&open_fh_complete)(request);
} /* count_open_fh_complete */

/* Pass every request through to memfs, counting stream opens on completion
 * and stream closes on dispatch.  memfs tags a stream open's vfs_private with
 * bit 0 (struct memfs_stream_open), which is what its CLOSE carries back. */
static void
count_stream_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    switch (request->opcode) {
        case CHIMERA_VFS_OP_OPEN_STREAM:
            save_complete(&open_stream_complete, request->complete,
                          "OPEN_STREAM");
            request->complete = count_open_stream_complete;
            break;
        case CHIMERA_VFS_OP_OPEN_FH:
            save_complete(&open_fh_complete, request->complete, "OPEN_FH");
            request->complete = count_open_fh_complete;
            break;
        case CHIMERA_VFS_OP_CLOSE:
            if (request->close.vfs_private & 1) {
                atomic_fetch_add(&backend_stream_closes, 1);
            }
            break;
        default:
            break;
    } /* switch */

    memfs_dispatch(request, private_data);
} /* count_stream_dispatch */

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
    st = smb2_create(c, "ads.bin", MBT_FILE_OVERWRITE_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &base);
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
    st = smb2_create(c, "ads.bin:alt", MBT_FILE_OVERWRITE_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &alt);
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
    st = smb2_create(c, "ads.bin:beta", MBT_FILE_OVERWRITE_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &beta);
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
     * (MBT_FILE_ALL_ACCESS already includes the DELETE right). */
    st = smb2_create(c, "ads.bin:alt", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &reopen);
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
    st = smb2_create(c, "ads.bin:alt", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &reopen);
    CHECK(st != ST_SUCCESS, "opening the deleted stream :alt is refused (0x%08x)",
          st);
    if (st == ST_SUCCESS) {
        smb2_close(c, reopen.file_id);
    }

    smb2_close(c, base.file_id);
} /* probe_stream_lifecycle */

/* Renaming a stream (MS-FSA 2.1.5.15.12.1 "Algorithm for Performing Stream
 * Rename"): the target ":name[:$DATA]" names another stream of the same file,
 * so the stream is renamed in place -- the base file keeps its name -- and a
 * stream of the target name is replaced only with ReplaceIfExists and only
 * while empty. */
static void
probe_stream_rename(struct smb2_conn *c)
{
    struct smb2_create_out base, s1, s2, reopen;
    uint8_t                rd[16];
    uint32_t               st, cnt = 0, rlen = 0;

    printf("# --- stream rename ---\n");

    st = smb2_create(c, "sren.bin", MBT_FILE_OVERWRITE_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &base);
    CHECK(st == ST_SUCCESS, "CREATE sren.bin -> 0x%08x", st);
    if (st != ST_SUCCESS) {
        return;
    }
    smb2_close(c, base.file_id);

    st = smb2_create(c, "sren.bin:one", MBT_FILE_OVERWRITE_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &s1);
    CHECK(st == ST_SUCCESS, "CREATE sren.bin:one -> 0x%08x", st);
    if (st != ST_SUCCESS) {
        return;
    }
    smb2_write(c, s1.file_id, 0, "ONE", 3, &cnt);

    st = smb2_rename(c, s1.file_id, ":two:$DATA", 0);
    CHECK(st == ST_SUCCESS, "rename :one -> :two:$DATA -> 0x%08x", st);
    st = smb2_read(c, s1.file_id, 0, sizeof(rd), rd, &rlen);
    CHECK(st == ST_SUCCESS && rlen == 3, "  ... the open handle still reads it "
          "(0x%08x, %u)", st, rlen);
    smb2_close(c, s1.file_id);

    st = smb2_create(c, "sren.bin:two", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &reopen);
    CHECK(st == ST_SUCCESS, "  ... sren.bin:two opens -> 0x%08x", st);
    if (st == ST_SUCCESS) {
        smb2_close(c, reopen.file_id);
    }
    st = smb2_create(c, "sren.bin:one", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &reopen);
    CHECK(st == ST_OBJECT_NAME_NOT_FOUND, "  ... and :one is gone (0x%08x)", st);
    if (st == ST_SUCCESS) {
        smb2_close(c, reopen.file_id);
    }
    st = smb2_create(c, "sren.bin", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &reopen);
    CHECK(st == ST_SUCCESS, "  ... the base file kept its name (0x%08x)", st);
    if (st == ST_SUCCESS) {
        smb2_close(c, reopen.file_id);
    }

    /* An empty stream named as the target. */
    st = smb2_create(c, "sren.bin:three", MBT_FILE_OVERWRITE_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &s2);
    CHECK(st == ST_SUCCESS, "CREATE sren.bin:three -> 0x%08x", st);
    if (st == ST_SUCCESS) {
        smb2_close(c, s2.file_id);
    }
    st = smb2_create(c, "sren.bin:two", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &s1);
    if (st != ST_SUCCESS) {
        return;
    }
    st = smb2_rename(c, s1.file_id, ":three", 0);
    CHECK(st == ST_OBJECT_NAME_COLLISION, "rename onto an existing stream "
          "without ReplaceIfExists -> OBJECT_NAME_COLLISION (0x%08x)", st);
    st = smb2_rename(c, s1.file_id, ":three", 1);
    CHECK(st == ST_SUCCESS, "  ... with it, onto an empty one -> 0x%08x", st);
    st = smb2_rename(c, s1.file_id, ":x:$INDEX_ALLOCATION", 0);
    CHECK(st != ST_SUCCESS, "a data stream cannot take another stream type "
          "(0x%08x)", st);
    smb2_close(c, s1.file_id);

    /* Renaming the file's unnamed data stream moves its data to the named
     * stream and leaves the file an empty unnamed one; renaming a named
     * stream onto the (empty) unnamed one moves it back. */
    st = smb2_create(c, "sren.bin", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &base);
    if (st != ST_SUCCESS) {
        return;
    }
    smb2_write(c, base.file_id, 0, "MAIN", 4, &cnt);
    st = smb2_rename(c, base.file_id, ":moved", 0);
    CHECK(st == ST_SUCCESS, "rename ::$DATA -> :moved -> 0x%08x", st);
    st = smb2_read(c, base.file_id, 0, sizeof(rd), rd, &rlen);
    CHECK(rlen == 0, "  ... the file's unnamed stream is now empty (0x%08x, %u)",
          st, rlen);
    smb2_close(c, base.file_id);
    st = smb2_create(c, "sren.bin:moved", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &s1);
    CHECK(st == ST_SUCCESS, "  ... and :moved holds its data (0x%08x)", st);
    if (st != ST_SUCCESS) {
        return;
    }
    st = smb2_read(c, s1.file_id, 0, sizeof(rd), rd, &rlen);
    CHECK(st == ST_SUCCESS && rlen == 4 && memcmp(rd, "MAIN", 4) == 0,
          "  ... (0x%08x, %u bytes)", st, rlen);
    st = smb2_rename(c, s1.file_id, "::$DATA", 0);
    CHECK(st == ST_SUCCESS, "rename :moved -> ::$DATA -> 0x%08x", st);
    smb2_close(c, s1.file_id);
    st = smb2_create(c, "sren.bin", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &base);
    if (st == ST_SUCCESS) {
        st = smb2_read(c, base.file_id, 0, sizeof(rd), rd, &rlen);
        CHECK(st == ST_SUCCESS && rlen == 4 && memcmp(rd, "MAIN", 4) == 0,
              "  ... the file's data is back (0x%08x, %u bytes)", st, rlen);
        smb2_close(c, base.file_id);
    }
} /* probe_stream_rename */

/* A named stream of a directory is a data stream: it opens, reads and writes
 * like a file's, and reports itself as one (no DIRECTORY attribute, its own
 * size) rather than as the directory carrying it. */
static void
probe_dir_stream(struct smb2_conn *c)
{
    struct smb2_create_out d, s1;
    uint8_t                rd[16];
    uint8_t                out[64] = { 0 };
    uint32_t               st, cnt = 0, rlen = 0, len = 0;

    printf("# --- directory stream ---\n");

    st = smb2_create_opts(c, "dstream", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                          MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &d);
    CHECK(st == ST_SUCCESS, "CREATE directory dstream -> 0x%08x", st);
    if (st != ST_SUCCESS) {
        return;
    }
    smb2_close(c, d.file_id);

    st = smb2_create_opts(c, "dstream:data", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                          MBT_FILE_SHARE_RWD, 0, NULL, &s1);
    CHECK(st == ST_SUCCESS, "CREATE dstream:data -> 0x%08x", st);
    if (st != ST_SUCCESS) {
        return;
    }
    st = smb2_write(c, s1.file_id, 0, "DIRDATA", 7, &cnt);
    CHECK(st == ST_SUCCESS && cnt == 7, "WRITE 7 bytes to dstream:data -> 0x%08x", st);
    st = smb2_read(c, s1.file_id, 0, sizeof(rd), rd, &rlen);
    CHECK(st == ST_SUCCESS && rlen == 7 && memcmp(rd, "DIRDATA", 7) == 0,
          "  ... and READ it back (0x%08x, %u)", st, rlen);
    st = smb2_query_info(c, SMB2_INFO_FILE_T, SMB2_FILE_STANDARD_INFO_T, s1.file_id,
                         0, out, sizeof(out), &len);
    CHECK(st == ST_SUCCESS && len >= 22 && g64(out, 8) == 7 && out[21] == 0,
          "  ... FileStandardInformation: EOF 7, not a directory (0x%08x, eof "
          "%llu, dir %u)", st, (unsigned long long) g64(out, 8), out[21]);
    smb2_close(c, s1.file_id);
} /* probe_dir_stream */

/* The base file `name` still opens and reads back the 8 bytes every case
 * below writes to it. */
static void
check_base_survives(
    struct smb2_conn *c,
    const char       *name)
{
    struct smb2_create_out base;
    uint8_t                rd[16];
    uint32_t               st, rlen = 0;

    st = smb2_create(c, name, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &base);
    CHECK(st == ST_SUCCESS, "  ... the base file %s survives (0x%08x)", name,
          st);
    if (st != ST_SUCCESS) {
        return;
    }
    st = smb2_read(c, base.file_id, 0, sizeof(rd), rd, &rlen);
    CHECK(st == ST_SUCCESS && rlen == 8 && memcmp(rd, "BASEDATA", 8) == 0,
          "  ... with its data (0x%08x, %u bytes)", st, rlen);
    smb2_close(c, base.file_id);
} /* check_base_survives */

/* Besides FileDispositionInformation (probe_stream_lifecycle), a stream is
 * deleted by opening it with FILE_DELETE_ON_CLOSE and closing it (smbtorture's
 * smb2_util_unlink of "file:stream").  Only the stream goes; the base keeps
 * its data.  The stream open's backend handle is accounted for at exit (see
 * main). */
static void
probe_stream_create_doc(struct smb2_conn *c)
{
    struct smb2_create_out base, s1, reopen;
    uint8_t                rd[16];
    uint32_t               st, cnt = 0, rlen = 0;
    int                    opens, entries, found = 0;

    printf("# --- stream delete-on-close at CREATE ---\n");

    st = smb2_create(c, "cdoc.bin", MBT_FILE_OVERWRITE_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &base);
    CHECK(st == ST_SUCCESS, "CREATE cdoc.bin -> 0x%08x", st);
    if (st != ST_SUCCESS) {
        return;
    }
    smb2_write(c, base.file_id, 0, "BASEDATA", 8, &cnt);

    opens = atomic_load(&backend_stream_opens);
    st    = smb2_create_opts(c, "cdoc.bin:gone", MBT_FILE_OVERWRITE_IF,
                             MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD,
                             MBT_FILE_DELETE_ON_CLOSE, NULL, &s1);
    CHECK(st == ST_SUCCESS, "CREATE cdoc.bin:gone with FILE_DELETE_ON_CLOSE "
          "-> 0x%08x", st);
    if (st != ST_SUCCESS) {
        smb2_close(c, base.file_id);
        return;
    }
    opens = atomic_load(&backend_stream_opens) - opens;
    CHECK(opens == 1, "  ... the backend opened one stream handle for it (%d)",
          opens);
    st = smb2_write(c, s1.file_id, 0, "GONE", 4, &cnt);
    CHECK(st == ST_SUCCESS && cnt == 4, "  ... WRITE 4 bytes to it -> 0x%08x",
          st);
    st = smb2_close(c, s1.file_id);
    CHECK(st == ST_SUCCESS, "  ... CLOSE -> 0x%08x", st);

    entries = enum_streams(c, base.file_id, "gone", NULL, &found);
    CHECK(entries == 1 && !found, "  ... the stream is gone from the "
          "enumeration (%d entries)", entries);
    st = smb2_create(c, "cdoc.bin:gone", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &reopen);
    CHECK(st == ST_OBJECT_NAME_NOT_FOUND, "  ... and no longer opens (0x%08x)",
          st);
    if (st == ST_SUCCESS) {
        smb2_close(c, reopen.file_id);
    }
    st = smb2_read(c, base.file_id, 0, sizeof(rd), rd, &rlen);
    CHECK(st == ST_SUCCESS && rlen == 8 && memcmp(rd, "BASEDATA", 8) == 0,
          "  ... the base file keeps its data (0x%08x, %u bytes)", st, rlen);
    smb2_close(c, base.file_id);
} /* probe_stream_create_doc */

/* A stream opened with FILE_DELETE_ON_CLOSE whose open is never closed by the
 * client -- its tree is disconnected under it -- must not take the base file
 * with it when the server discards the open.  The stream itself should go
 * too and today does not (chimera-nas/chimera#1866), so that is not checked
 * here yet. */
static void
probe_stream_create_doc_teardown(
    struct smb2_env  *env,
    struct smb2_conn *c)
{
    struct smb2_conn      *t;
    struct smb2_create_out base, s1;
    uint32_t               st, cnt = 0;

    printf("# --- stream delete-on-close at CREATE, then TREE_DISCONNECT ---\n");

    st = smb2_create(c, "tdoc.bin", MBT_FILE_OVERWRITE_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &base);
    CHECK(st == ST_SUCCESS, "CREATE tdoc.bin -> 0x%08x", st);
    if (st != ST_SUCCESS) {
        return;
    }
    smb2_write(c, base.file_id, 0, "BASEDATA", 8, &cnt);
    smb2_close(c, base.file_id);

    t = smb2_conn_open(env);
    smb2_handshake(t);
    st = smb2_create_opts(t, "tdoc.bin:s", MBT_FILE_OVERWRITE_IF,
                          MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD,
                          MBT_FILE_DELETE_ON_CLOSE, NULL, &s1);
    CHECK(st == ST_SUCCESS, "CREATE tdoc.bin:s with FILE_DELETE_ON_CLOSE "
          "-> 0x%08x", st);
    if (st != ST_SUCCESS) {
        return;
    }
    st = smb2_tree_disconnect(t);
    CHECK(st == ST_SUCCESS, "  ... TREE_DISCONNECT with the stream open "
          "-> 0x%08x", st);
    smb2_quiesce(env);

    check_base_survives(c, "tdoc.bin");
} /* probe_stream_create_doc_teardown */

/* A stream opened with FILE_DELETE_ON_CLOSE whose delete is then turned off
 * through FileDispositionInformation (DeletePending = 0) and closed: the CLOSE
 * removes no stream, and must not remove the base file either. */
static void
probe_stream_create_doc_cleared(struct smb2_conn *c)
{
    struct smb2_create_out base, s1;
    uint32_t               st, cnt = 0;

    printf("# --- stream delete-on-close at CREATE, cleared by SET_INFO, then "
           "CLOSE ---\n");

    st = smb2_create(c, "xdoc.bin", MBT_FILE_OVERWRITE_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &base);
    CHECK(st == ST_SUCCESS, "CREATE xdoc.bin -> 0x%08x", st);
    if (st != ST_SUCCESS) {
        return;
    }
    smb2_write(c, base.file_id, 0, "BASEDATA", 8, &cnt);
    smb2_close(c, base.file_id);

    st = smb2_create_opts(c, "xdoc.bin:s", MBT_FILE_OVERWRITE_IF,
                          MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD,
                          MBT_FILE_DELETE_ON_CLOSE, NULL, &s1);
    CHECK(st == ST_SUCCESS, "CREATE xdoc.bin:s with FILE_DELETE_ON_CLOSE "
          "-> 0x%08x", st);
    if (st != ST_SUCCESS) {
        return;
    }
    st = smb2_set_disposition(c, s1.file_id, 0);
    CHECK(st == ST_SUCCESS, "  ... clear its delete-on-close -> 0x%08x", st);
    st = smb2_close(c, s1.file_id);
    CHECK(st == ST_SUCCESS, "  ... CLOSE -> 0x%08x", st);

    check_base_survives(c, "xdoc.bin");
} /* probe_stream_create_doc_cleared */

/* A file rename (a FileRenameInformation target with no leading ':') issued on
 * a stream handle renames the base file.  When that handle was opened with
 * FILE_DELETE_ON_CLOSE, the renamed base file must not be deleted with it,
 * whether the open is then closed (`discard` 0) or discarded with its tree
 * (`discard` 1). */
static void
probe_stream_create_doc_rename(
    struct smb2_env  *env,
    struct smb2_conn *c,
    int               discard)
{
    const char            *from   = discard ? "rtdoc.bin" : "rcdoc.bin";
    const char            *stream = discard ? "rtdoc.bin:s" : "rcdoc.bin:s";
    const char            *to     = discard ? "rtdoc2.bin" : "rcdoc2.bin";
    struct smb2_conn      *o      = c;
    struct smb2_create_out base, s1;
    uint32_t               st, cnt = 0;
    int                    entries, found = 0;

    printf("# --- stream delete-on-close at CREATE, base renamed through the "
           "stream handle, then %s ---\n", discard ? "TREE_DISCONNECT" : "CLOSE");

    st = smb2_create(c, from, MBT_FILE_OVERWRITE_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &base);
    CHECK(st == ST_SUCCESS, "CREATE %s -> 0x%08x", from, st);
    if (st != ST_SUCCESS) {
        return;
    }
    smb2_write(c, base.file_id, 0, "BASEDATA", 8, &cnt);
    smb2_close(c, base.file_id);

    if (discard) {
        o = smb2_conn_open(env);
        smb2_handshake(o);
    }
    st = smb2_create_opts(o, stream, MBT_FILE_OVERWRITE_IF,
                          MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD,
                          MBT_FILE_DELETE_ON_CLOSE, NULL, &s1);
    CHECK(st == ST_SUCCESS, "CREATE %s with FILE_DELETE_ON_CLOSE -> 0x%08x",
          stream, st);
    if (st != ST_SUCCESS) {
        return;
    }
    st = smb2_rename(o, s1.file_id, to, 0);
    CHECK(st == ST_SUCCESS, "  ... rename the base to %s through it -> 0x%08x",
          to, st);
    if (discard) {
        st = smb2_tree_disconnect(o);
        CHECK(st == ST_SUCCESS, "  ... TREE_DISCONNECT with the stream open "
              "-> 0x%08x", st);
    } else {
        st = smb2_close(o, s1.file_id);
        CHECK(st == ST_SUCCESS, "  ... CLOSE -> 0x%08x", st);
    }
    smb2_quiesce(env);

    check_base_survives(c, to);
    if (!discard) {
        st = smb2_create(c, to, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                         MBT_FILE_SHARE_RWD, NULL, &base);
        if (st == ST_SUCCESS) {
            entries = enum_streams(c, base.file_id, "s", NULL, &found);
            CHECK(entries == 1 && !found, "  ... and the stream is gone from "
                  "its enumeration (%d entries)", entries);
            smb2_close(c, base.file_id);
        }
    }
} /* probe_stream_create_doc_rename */

/* SET_REPARSE_POINT of a symlink replaces the object under the open's name
 * with a new inode and rebinds the open to it.  Issued on a stream handle
 * opened with FILE_DELETE_ON_CLOSE, it must not leave that name to be deleted
 * when the open is discarded.
 *
 * This case relies on today's answer to SET_REPARSE_POINT on a stream handle
 * (STATUS_SUCCESS, the base file replaced by the link) only to reach that
 * rebind.  That answer is not the specification (chimera-nas/chimera#1867);
 * when it changes, this case changes or goes with it. */
static void
probe_stream_create_doc_reparse(
    struct smb2_env  *env,
    struct smb2_conn *c)
{
    struct smb2_conn      *t;
    struct smb2_create_out base, s1;
    uint8_t                in[64];
    uint32_t               st;
    int                    in_len;

    printf("# --- stream delete-on-close at CREATE, SET_REPARSE_POINT through "
           "the stream handle, then TREE_DISCONNECT ---\n");

    st = smb2_create(c, "pdoc.bin", MBT_FILE_OVERWRITE_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &base);
    CHECK(st == ST_SUCCESS, "CREATE pdoc.bin -> 0x%08x", st);
    if (st != ST_SUCCESS) {
        return;
    }
    smb2_close(c, base.file_id);

    t = smb2_conn_open(env);
    smb2_handshake(t);
    st = smb2_create_opts(t, "pdoc.bin:s", MBT_FILE_OVERWRITE_IF,
                          MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD,
                          MBT_FILE_DELETE_ON_CLOSE, NULL, &s1);
    CHECK(st == ST_SUCCESS, "CREATE pdoc.bin:s with FILE_DELETE_ON_CLOSE "
          "-> 0x%08x", st);
    if (st != ST_SUCCESS) {
        return;
    }

    in_len = smb2_symlink_reparse_buffer(in, "target");
    st     = smb2_ioctl(t, SMB2_FSCTL_SET_REPARSE_POINT, s1.file_id, in,
                        (uint32_t) in_len);
    CHECK(st == ST_SUCCESS, "  ... SET_REPARSE_POINT(symlink) through it "
          "-> 0x%08x", st);
    st = smb2_tree_disconnect(t);
    CHECK(st == ST_SUCCESS, "  ... TREE_DISCONNECT with the stream open "
          "-> 0x%08x", st);
    smb2_quiesce(env);

    /* The name now holds a link to a target that does not exist. */
    st = smb2_create(c, "pdoc.bin", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &base);
    CHECK(st == ST_STOPPED_ON_SYMLINK, "  ... the link is still there: "
          "opening pdoc.bin -> STATUS_STOPPED_ON_SYMLINK (0x%08x)", st);
    if (st == ST_SUCCESS) {
        smb2_close(c, base.file_id);
    }
} /* probe_stream_create_doc_reparse */

/* The two re-arms the stream cases above must not get still have to happen for
 * a plain file.  A file opened with FILE_DELETE_ON_CLOSE and then renamed is
 * deleted under its new name when it is closed. */
static void
probe_file_create_doc_rename(struct smb2_conn *c)
{
    struct smb2_create_out f, reopen;
    uint32_t               st;

    printf("# --- file delete-on-close at CREATE, renamed, then CLOSE ---\n");

    st = smb2_create_opts(c, "fdoc.bin", MBT_FILE_OVERWRITE_IF,
                          MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD,
                          MBT_FILE_DELETE_ON_CLOSE, NULL, &f);
    CHECK(st == ST_SUCCESS, "CREATE fdoc.bin with FILE_DELETE_ON_CLOSE "
          "-> 0x%08x", st);
    if (st != ST_SUCCESS) {
        return;
    }
    st = smb2_rename(c, f.file_id, "fdoc2.bin", 0);
    CHECK(st == ST_SUCCESS, "  ... rename it to fdoc2.bin -> 0x%08x", st);
    st = smb2_close(c, f.file_id);
    CHECK(st == ST_SUCCESS, "  ... CLOSE -> 0x%08x", st);

    st = smb2_create(c, "fdoc2.bin", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &reopen);
    CHECK(st == ST_OBJECT_NAME_NOT_FOUND, "  ... fdoc2.bin is gone (0x%08x)",
          st);
    if (st == ST_SUCCESS) {
        smb2_close(c, reopen.file_id);
    }
    st = smb2_create(c, "fdoc.bin", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &reopen);
    CHECK(st == ST_OBJECT_NAME_NOT_FOUND, "  ... and so is fdoc.bin (0x%08x)",
          st);
    if (st == ST_SUCCESS) {
        smb2_close(c, reopen.file_id);
    }
} /* probe_file_create_doc_rename */

/* Likewise for the rebind after SET_REPARSE_POINT: an (empty) file opened with
 * FILE_DELETE_ON_CLOSE and then turned into a symlink is deleted, as the link
 * it has become, when it is closed. */
static void
probe_file_create_doc_reparse(struct smb2_conn *c)
{
    struct smb2_create_out f, reopen;
    uint8_t                in[64];
    uint32_t               st;
    int                    in_len;

    printf("# --- file delete-on-close at CREATE, SET_REPARSE_POINT, then "
           "CLOSE ---\n");

    st = smb2_create_opts(c, "ldoc.bin", MBT_FILE_OVERWRITE_IF,
                          MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD,
                          MBT_FILE_DELETE_ON_CLOSE, NULL, &f);
    CHECK(st == ST_SUCCESS, "CREATE ldoc.bin with FILE_DELETE_ON_CLOSE "
          "-> 0x%08x", st);
    if (st != ST_SUCCESS) {
        return;
    }
    in_len = smb2_symlink_reparse_buffer(in, "target");
    st     = smb2_ioctl(c, SMB2_FSCTL_SET_REPARSE_POINT, f.file_id, in,
                        (uint32_t) in_len);
    CHECK(st == ST_SUCCESS, "  ... SET_REPARSE_POINT(symlink) -> 0x%08x", st);
    st = smb2_close(c, f.file_id);
    CHECK(st == ST_SUCCESS, "  ... CLOSE -> 0x%08x", st);

    st = smb2_create(c, "ldoc.bin", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &reopen);
    CHECK(st == ST_OBJECT_NAME_NOT_FOUND, "  ... the link is gone (0x%08x)",
          st);
    if (st == ST_SUCCESS) {
        smb2_close(c, reopen.file_id);
    }
} /* probe_file_create_doc_reparse */

/* "file::$DATA" names the default data stream, which is the file itself:
 * FILE_DELETE_ON_CLOSE on that spelling still deletes the file. */
static void
probe_default_stream_create_doc(struct smb2_conn *c)
{
    struct smb2_create_out base;
    uint32_t               st;

    printf("# --- delete-on-close at CREATE of the default stream ---\n");

    st = smb2_create(c, "ddoc.bin", MBT_FILE_OVERWRITE_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &base);
    CHECK(st == ST_SUCCESS, "CREATE ddoc.bin -> 0x%08x", st);
    if (st != ST_SUCCESS) {
        return;
    }
    smb2_close(c, base.file_id);

    st = smb2_create_opts(c, "ddoc.bin::$DATA", MBT_FILE_OPEN,
                          MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD,
                          MBT_FILE_DELETE_ON_CLOSE, NULL, &base);
    CHECK(st == ST_SUCCESS, "CREATE ddoc.bin::$DATA with FILE_DELETE_ON_CLOSE "
          "-> 0x%08x", st);
    if (st != ST_SUCCESS) {
        return;
    }
    st = smb2_close(c, base.file_id);
    CHECK(st == ST_SUCCESS, "  ... CLOSE -> 0x%08x", st);

    st = smb2_create(c, "ddoc.bin", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &base);
    CHECK(st == ST_OBJECT_NAME_NOT_FOUND, "  ... the file is gone (0x%08x)",
          st);
    if (st == ST_SUCCESS) {
        smb2_close(c, base.file_id);
    }
} /* probe_default_stream_create_doc */

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
    int                  opens, closes;

    (void) argc;
    (void) argv;

    setvbuf(stdout, NULL, _IONBF, 0);

    memfs_dispatch     = vfs_memfs.dispatch;
    vfs_memfs.dispatch = count_stream_dispatch;

    smb2_env_start_opts(&env, &opts);
    c = smb2_conn_open(&env);
    smb2_handshake(c);

    printf("# dialect=0x%04x\n", c->dialect);

    probe_stream_lifecycle(c);
    probe_stream_rename(c);
    probe_dir_stream(c);
    probe_stream_create_doc(c);
    probe_stream_create_doc_teardown(&env, c);
    probe_stream_create_doc_cleared(c);
    probe_stream_create_doc_rename(&env, c, 0);
    probe_stream_create_doc_rename(&env, c, 1);
    probe_stream_create_doc_reparse(&env, c);
    probe_file_create_doc_rename(c);
    probe_file_create_doc_reparse(c);
    probe_default_stream_create_doc(c);

    /* smb2_env_stop() drains the open cache, closing every handle the server
     * still holds, so once it returns the backend's stream opens and closes
     * must balance. */
    smb2_env_stop(&env);
    vfs_memfs.dispatch = memfs_dispatch;

    printf("# --- backend stream handles at exit ---\n");
    opens  = atomic_load(&backend_stream_opens);
    closes = atomic_load(&backend_stream_closes);
    CHECK(opens > 0 && opens == closes,
          "every backend stream open was closed (opens %d, closes %d)",
          opens, closes);

    if (failures) {
        fprintf(stderr, "%d ADS stream check(s) FAILED\n", failures);
        return 1;
    }
    printf("all SMB2 named-stream checks passed\n");
    return 0;
} /* main */
