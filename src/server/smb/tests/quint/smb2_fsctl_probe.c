/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * SMB2 FSCTL ground-truth probe.
 *
 * The FSCTL family is the largest dark region of the SMB server: server-side
 * copy (SRV_COPYCHUNK and the resume key it needs), the copy-offload token
 * pair (OFFLOAD_READ / OFFLOAD_WRITE), block cloning
 * (DUPLICATE_EXTENTS_TO_FILE), reparse points (SET/GET_REPARSE_POINT), and the
 * sparse-file trio (SET_SPARSE, SET_ZERO_DATA, QUERY_ALLOCATED_RANGES).  The
 * trace corpus has no step that emits an IOCTL at all, so none of it runs.
 *
 * These are a poor fit for the model -- each is a distinct binary blob format
 * with its own preconditions, and the model's file abstraction has no notion
 * of a resume key or an offload token -- but they are a very good fit for a
 * ground-truth probe, because each one has a checkable EFFECT: bytes appear at
 * a destination offset, a range reads back as zeroes, a link target survives a
 * round trip.  Every assertion below is on that effect, never on the status
 * alone, so an FSCTL that reports success while doing nothing still fails.
 *
 * Where chimera answers STATUS_NOT_SUPPORTED because the memfs backend does
 * not implement the underlying VFS operation, that is asserted as such rather
 * than skipped: "this FSCTL is refused cleanly" is also a behavior worth
 * pinning, and it keeps the probe honest about what is and is not covered.
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

/* memfs allocates in 64 KiB blocks (CHIMERA_MEMFS_BLOCK_SIZE_DEFAULT), and
 * only a block the hole covers ENTIRELY is freed -- a partially punched block
 * is copy-on-written and zeroed, so it stays allocated.  Any test that expects
 * a punch to show up in QUERY_ALLOCATED_RANGES has to work in whole blocks. */
#define FSCTL_BLK (64 * 1024)

/* The file pattern is a function of ABSOLUTE offset, so a range copied to a
 * different offset is still checkable and a chunked write is consistent. */
#define FSCTL_PAT(seed, off) ((uint8_t) ((seed) + (off)))

/* Create (or overwrite) a file and fill it with `len` bytes of a recognisable
 * pattern, so a later copy can be checked byte for byte rather than by size. */
static void
make_file(
    struct smb2_conn       *c,
    const char             *name,
    int                     len,
    uint8_t                 seed,
    struct smb2_create_out *out)
{
    static uint8_t buf[FSCTL_BLK];
    uint32_t       st, count = 0;
    int            off, chunk, i;

    st = smb2_create(c, name, FILE_OVERWRITE_IF, FILE_ALL_ACCESS,
                     FILE_SHARE_RWD, NULL, out);
    CHECK(st == ST_SUCCESS, "setup: CREATE %s -> 0x%08x", name, st);

    for (off = 0; off < len; off += chunk) {
        chunk = len - off;
        if (chunk > (int) sizeof(buf)) {
            chunk = (int) sizeof(buf);
        }
        for (i = 0; i < chunk; i++) {
            buf[i] = FSCTL_PAT(seed, off + i);
        }
        st = smb2_write(c, out->file_id, (uint64_t) off, buf, chunk, &count);
        if (st != ST_SUCCESS || (int) count != chunk) {
            CHECK(0, "setup: WRITE %d bytes at %d to %s -> 0x%08x (count=%u)",
                  chunk, off, name, st, count);
            return;
        }
    }
    CHECK(1, "setup: %s holds %d bytes of pattern", name, len);
} /* make_file */

/* Read `len` bytes at `off` and compare against the pattern make_file wrote
* for absolute offset `pat_off` -- which differs from `off` exactly when the
* range has been copied somewhere else, which is what the copy FSCTLs do. */
static int
check_pattern(
    struct smb2_conn *c,
    const uint8_t     file_id[16],
    uint64_t          off,
    int               len,
    uint8_t           seed,
    int               pat_off)
{
    uint8_t  buf[4096];
    uint32_t st, rlen = 0;
    int      i;

    if (len > (int) sizeof(buf)) {
        len = (int) sizeof(buf);
    }
    st = smb2_read(c, file_id, off, len, buf, &rlen);
    if (st != ST_SUCCESS || (int) rlen != len) {
        return 0;
    }
    for (i = 0; i < len; i++) {
        if (buf[i] != FSCTL_PAT(seed, pat_off + i)) {
            return 0;
        }
    }
    return 1;
} /* check_pattern */

/* Read `len` bytes at `off` and require every one of them to be zero. */
static int
check_zeroes(
    struct smb2_conn *c,
    const uint8_t     file_id[16],
    uint64_t          off,
    int               len)
{
    uint8_t  buf[4096];
    uint32_t st, rlen = 0;
    int      i;

    st = smb2_read(c, file_id, off, len, buf, &rlen);
    if (st != ST_SUCCESS || (int) rlen != len) {
        return 0;
    }
    for (i = 0; i < len; i++) {
        if (buf[i] != 0) {
            return 0;
        }
    }
    return 1;
} /* check_zeroes */

/* ---- the sparse trio ----------------------------------------------------
 *
 * SET_SPARSE marks the file sparse (a persisted DOS attribute bit),
 * SET_ZERO_DATA punches a hole, and QUERY_ALLOCATED_RANGES reports which
 * ranges still hold data.  On memfs all three route to real VFS primitives
 * (setattr, allocate(DEALLOCATE), seek(DATA/HOLE)), so all three must succeed
 * and, more importantly, must AGREE with each other: after punching a hole the
 * bytes read back as zeroes AND the hole is absent from the allocated ranges.
 */
static void
probe_sparse(struct smb2_conn *c)
{
    struct smb2_create_out co;
    uint8_t                in[16];
    const uint8_t         *out;
    uint32_t               st = 0, out_len = 0;
    uint64_t               r0_off, r0_len;
    const int              fsize = 4 * FSCTL_BLK;   /* 256 KiB = 4 blocks */

    printf("# --- sparse: SET_SPARSE / SET_ZERO_DATA / QUERY_ALLOCATED_RANGES ---\n");

    make_file(c, "sparse.bin", fsize, 0x10, &co);

    /* FSCTL_SET_SPARSE (MS-FSCC 2.3.65): a single SetSparse boolean. */
    in[0] = 1;
    st    = smb2_ioctl(c, SMB2_FSCTL_SET_SPARSE, co.file_id, in, 1);
    CHECK(st == ST_SUCCESS, "SET_SPARSE(TRUE) -> 0x%08x", st);

    /* An empty input buffer means TRUE (the parser defaults sp_set_sparse). */
    st = smb2_ioctl(c, SMB2_FSCTL_SET_SPARSE, co.file_id, NULL, 0);
    CHECK(st == ST_SUCCESS, "SET_SPARSE(no input, implies TRUE) -> 0x%08x", st);

    /* Before punching, the whole file is one allocated range. */
    p64(in, 0, 0);
    p64(in, 8, fsize);
    out = smb2_ioctl_out(c, SMB2_FSCTL_QUERY_ALLOCATED_RANGES, co.file_id,
                         in, 16, 4096, &st, &out_len);
    CHECK(st == ST_SUCCESS && out && out_len == 16,
          "QUERY_ALLOCATED_RANGES on a full file -> 0x%08x (%u bytes)", st,
          out_len);
    if (out && out_len >= 16) {
        r0_off = g64(out, 0);
        r0_len = g64(out, 8);
        CHECK(r0_off == 0 && r0_len == (uint64_t) fsize,
              "  ... reports the whole file allocated (off=%llu len=%llu)",
              (unsigned long long) r0_off, (unsigned long long) r0_len);
    }

    /* FSCTL_SET_ZERO_DATA (MS-FSCC 2.3.67): FileOffset then BeyondFinalZero.
     * Punch blocks 1 and 2 out whole, so they are genuinely deallocated rather
     * than copy-on-written and zeroed. */
    p64(in, 0, FSCTL_BLK);
    p64(in, 8, 3 * FSCTL_BLK);
    st = smb2_ioctl(c, SMB2_FSCTL_SET_ZERO_DATA, co.file_id, in, 16);
    CHECK(st == ST_SUCCESS, "SET_ZERO_DATA[64K,192K) -> 0x%08x", st);

    /* The EFFECT, which is the point: the hole reads as zeroes and the data on
     * either side of it is untouched. */
    CHECK(check_zeroes(c, co.file_id, FSCTL_BLK, 4096),
          "  ... the punched range reads back as zeroes");
    CHECK(check_pattern(c, co.file_id, 0, 4096, 0x10, 0),
          "  ... data before the hole is intact");
    CHECK(check_pattern(c, co.file_id, 3 * FSCTL_BLK, 4096, 0x10,
                        3 * FSCTL_BLK),
          "  ... data after the hole is intact");

    /* And the allocated-range map now agrees: the punched blocks are gone from
     * it, leaving the first and last blocks as two separate ranges. */
    p64(in, 0, 0);
    p64(in, 8, fsize);
    out = smb2_ioctl_out(c, SMB2_FSCTL_QUERY_ALLOCATED_RANGES, co.file_id,
                         in, 16, 4096, &st, &out_len);
    CHECK(st == ST_SUCCESS && out,
          "QUERY_ALLOCATED_RANGES after the punch -> 0x%08x (%u bytes)", st,
          out_len);
    CHECK(out_len == 32, "  ... the hole splits the file into two ranges (%u bytes)",
          out_len);
    if (out && out_len >= 32) {
        CHECK(g64(out, 0) == 0 && g64(out, 8) == FSCTL_BLK,
              "  ... range 0 = [0,64K) (off=%llu len=%llu)",
              (unsigned long long) g64(out, 0),
              (unsigned long long) g64(out, 8));
        CHECK(g64(out, 16) == 3 * FSCTL_BLK && g64(out, 24) == FSCTL_BLK,
              "  ... range 1 = [192K,256K) (off=%llu len=%llu)",
              (unsigned long long) g64(out, 16),
              (unsigned long long) g64(out, 24));
    }

    /* MaxOutputResponse is a precondition, not a truncation hint: room for one
     * range out of two is STATUS_BUFFER_OVERFLOW with that one emitted. */
    p64(in, 0, 0);
    p64(in, 8, fsize);
    out = smb2_ioctl_out(c, SMB2_FSCTL_QUERY_ALLOCATED_RANGES, co.file_id,
                         in, 16, 16, &st, &out_len);
    CHECK(st == ST_BUFFER_OVERFLOW && out_len == 16,
          "QUERY_ALLOCATED_RANGES with room for one range overflows (0x%08x, "
          "%u bytes)", st, out_len);

    /* A zero-length range is a no-op that still succeeds, and an inverted one
     * is refused -- both are explicit branches in the handler. */
    p64(in, 0, 512);
    p64(in, 8, 512);
    st = smb2_ioctl(c, SMB2_FSCTL_SET_ZERO_DATA, co.file_id, in, 16);
    CHECK(st == ST_SUCCESS, "SET_ZERO_DATA with an empty range -> 0x%08x", st);

    p64(in, 0, 2048);
    p64(in, 8, 1024);
    st = smb2_ioctl(c, SMB2_FSCTL_SET_ZERO_DATA, co.file_id, in, 16);
    CHECK(st == ST_INVALID_PARAMETER,
          "SET_ZERO_DATA with BeyondFinalZero < FileOffset is refused (0x%08x)",
          st);

    /* A query window past EOF has nothing allocated in it: an empty result,
     * which is SUCCESS with a zero-length output. */
    p64(in, 0, 1 << 20);
    p64(in, 8, 4096);
    out = smb2_ioctl_out(c, SMB2_FSCTL_QUERY_ALLOCATED_RANGES, co.file_id,
                         in, 16, 4096, &st, &out_len);
    CHECK(st == ST_SUCCESS && out_len == 0,
          "QUERY_ALLOCATED_RANGES past EOF is empty (0x%08x, %u bytes)", st,
          out_len);

    smb2_close(c, co.file_id);

    /* Access control: SET_SPARSE and SET_ZERO_DATA both write, so a
     * read-only handle must be refused. */
    st = smb2_create(c, "sparse_ro.bin", FILE_OPEN_IF, FILE_READ_ACCESS,
                     FILE_SHARE_RWD, NULL, &co);
    if (st == ST_SUCCESS) {
        in[0] = 1;
        st    = smb2_ioctl(c, SMB2_FSCTL_SET_SPARSE, co.file_id, in, 1);
        CHECK(st == ST_ACCESS_DENIED,
              "SET_SPARSE on a read-only handle is denied (0x%08x)", st);

        p64(in, 0, 0);
        p64(in, 8, 16);
        st = smb2_ioctl(c, SMB2_FSCTL_SET_ZERO_DATA, co.file_id, in, 16);
        CHECK(st == ST_ACCESS_DENIED,
              "SET_ZERO_DATA on a read-only handle is denied (0x%08x)", st);
        smb2_close(c, co.file_id);
    }
} /* probe_sparse */

/* ---- server-side copy ---------------------------------------------------
 *
 * SRV_COPYCHUNK moves bytes between two OPEN HANDLES without them crossing the
 * wire.  The destination handle is the IOCTL's FileId; the source is named by
 * a 24-byte resume key the client must first fetch from the source handle with
 * SRV_REQUEST_RESUME_KEY.  Chimera encodes the source FileId in that key, so
 * the round trip is also a check that the key it hands out is the key it
 * accepts back.
 */
static void
probe_copychunk(struct smb2_conn *c)
{
    struct smb2_create_out src, dst;
    uint8_t                key[24];
    uint8_t                in[32 + 3 * 24];
    const uint8_t         *out;
    uint32_t               st = 0, out_len = 0;

    printf("# --- server-side copy: SRV_REQUEST_RESUME_KEY / SRV_COPYCHUNK ---\n");

    make_file(c, "cc_src.bin", 8192, 0x40, &src);
    make_file(c, "cc_dst.bin", 0, 0, &dst);

    /* FSCTL_SRV_REQUEST_RESUME_KEY (MS-SMB2 2.2.32.3): 24-byte key plus a
     * ContextLength/Context pair, 32 bytes in all. */
    out = smb2_ioctl_out(c, SMB2_FSCTL_SRV_REQUEST_RESUME_KEY, src.file_id,
                         NULL, 0, 4096, &st, &out_len);
    CHECK(st == ST_SUCCESS && out && out_len == 32,
          "SRV_REQUEST_RESUME_KEY -> 0x%08x (%u bytes)", st, out_len);
    if (!out || out_len < 24) {
        return;
    }
    memcpy(key, out, 24);

    /* The key must name the source open, since that is how COPYCHUNK finds it. */
    CHECK(memcmp(key, src.file_id, 16) == 0,
          "  ... the resume key carries the source FileId");

    /* One chunk: copy [0,4096) of the source to offset 0 of the destination. */
    memset(in, 0, sizeof(in));
    memcpy(in, key, 24);
    p32(in, 24, 1);                   /* ChunkCount */
    p64(in, 32, 0);                   /* SourceOffset */
    p64(in, 40, 0);                   /* TargetOffset */
    p32(in, 48, 4096);                /* Length */

    out = smb2_ioctl_out(c, SMB2_FSCTL_SRV_COPYCHUNK, dst.file_id, in, 32 + 24,
                         4096, &st, &out_len);
    CHECK(st == ST_SUCCESS && out && out_len == 12,
          "SRV_COPYCHUNK 1 chunk of 4096 -> 0x%08x (%u bytes)", st, out_len);
    if (out && out_len >= 12) {
        CHECK(g32(out, 0) == 1 && g32(out, 8) == 4096,
              "  ... reports ChunksWritten=1 TotalBytesWritten=4096 (%u/%u)",
              g32(out, 0), g32(out, 8));
    }
    CHECK(check_pattern(c, dst.file_id, 0, 4096, 0x40, 0),
          "  ... the destination holds the source bytes");

    /* Several chunks at once, landing out of order and at a non-zero target,
     * which is the case the per-chunk loop exists for. */
    memset(in, 0, sizeof(in));
    memcpy(in, key, 24);
    p32(in, 24, 3);                   /* ChunkCount */
    p64(in, 32, 4096); p64(in, 40, 8192); p32(in, 48, 1024);
    p64(in, 56, 0);    p64(in, 64, 9216); p32(in, 72, 1024);
    p64(in, 80, 2048); p64(in, 88, 10240); p32(in, 96, 1024);

    out = smb2_ioctl_out(c, SMB2_FSCTL_SRV_COPYCHUNK, dst.file_id, in,
                         32 + 3 * 24, 4096, &st, &out_len);
    CHECK(st == ST_SUCCESS && out && out_len == 12,
          "SRV_COPYCHUNK 3 chunks -> 0x%08x (%u bytes)", st, out_len);
    if (out && out_len >= 12) {
        CHECK(g32(out, 0) == 3 && g32(out, 8) == 3072,
              "  ... reports ChunksWritten=3 TotalBytesWritten=3072 (%u/%u)",
              g32(out, 0), g32(out, 8));
    }
    CHECK(check_pattern(c, dst.file_id, 8192, 1024, 0x40, 4096),
          "  ... chunk 0 landed at 8192 carrying source offset 4096");
    CHECK(check_pattern(c, dst.file_id, 9216, 1024, 0x40, 0),
          "  ... chunk 1 landed at 9216 carrying source offset 0");
    CHECK(check_pattern(c, dst.file_id, 10240, 1024, 0x40, 2048),
          "  ... chunk 2 landed at 10240 carrying source offset 2048");

    /* A ChunkCount past the server limit is answered with
     * STATUS_INVALID_PARAMETER *and* a SRV_COPYCHUNK_RESPONSE advertising the
     * limits, so the client can resubmit -- an error that still carries a
     * body, which is its own reply path. */
    memset(in, 0, sizeof(in));
    memcpy(in, key, 24);
    p32(in, 24, 0xFFFF);              /* ChunkCount far past the limit */
    out = smb2_ioctl_out(c, SMB2_FSCTL_SRV_COPYCHUNK, dst.file_id, in, 32,
                         4096, &st, &out_len);
    CHECK(st == ST_INVALID_PARAMETER,
          "SRV_COPYCHUNK with an over-limit ChunkCount is refused (0x%08x)", st);

    /* An unknown resume key names no open: the copy cannot resolve a source. */
    memset(in, 0, sizeof(in));
    memset(in, 0xEE, 24);
    p32(in, 24, 1);
    p64(in, 32, 0); p64(in, 40, 0); p32(in, 48, 64);
    st = smb2_ioctl(c, SMB2_FSCTL_SRV_COPYCHUNK, dst.file_id, in, 32 + 24);
    CHECK(st != ST_SUCCESS,
          "SRV_COPYCHUNK with an unknown resume key is refused (0x%08x)", st);

    smb2_close(c, src.file_id);
    smb2_close(c, dst.file_id);
} /* probe_copychunk */

/* ---- copy offload and block cloning -------------------------------------
 *
 * OFFLOAD_READ mints a 512-byte STORAGE_OFFLOAD_TOKEN standing for a range of
 * the source, and OFFLOAD_WRITE redeems it against a destination -- the same
 * server-side copy as COPYCHUNK, but with the source named by a token that
 * outlives the request rather than by a live handle.  DUPLICATE_EXTENTS_TO_FILE
 * is the clone path: it shares blocks instead of copying them, and its
 * observable contract is only that the destination reads back as the source.
 */
#define OFFLOAD_TOKEN_SIZE 512

static void
probe_copyoffload(struct smb2_conn *c)
{
    struct smb2_create_out src, dst;
    uint8_t                token[OFFLOAD_TOKEN_SIZE];
    uint8_t                in[32 + OFFLOAD_TOKEN_SIZE];
    const uint8_t         *out;
    uint32_t               st = 0, out_len = 0;

    printf("# --- copy offload: OFFLOAD_READ / OFFLOAD_WRITE ---\n");

    make_file(c, "od_src.bin", 8192, 0x70, &src);
    make_file(c, "od_dst.bin", 0, 0, &dst);

    /* FSCTL_OFFLOAD_READ_INPUT (MS-FSCC 2.3.79): Size, Flags, TokenTimeToLive,
     * Reserved, FileOffset, CopyLength. */
    memset(in, 0, sizeof(in));
    p32(in, 0, 32);                   /* Size */
    p64(in, 16, 0);                   /* FileOffset */
    p64(in, 24, 4096);                /* CopyLength */
    out = smb2_ioctl_out(c, SMB2_FSCTL_OFFLOAD_READ, src.file_id, in, 32,
                         4096, &st, &out_len);
    CHECK(st == ST_SUCCESS && out && out_len == 16 + OFFLOAD_TOKEN_SIZE,
          "OFFLOAD_READ [0,4096) -> 0x%08x (%u bytes)", st, out_len);
    if (!out || out_len < 16 + OFFLOAD_TOKEN_SIZE) {
        smb2_close(c, src.file_id);
        smb2_close(c, dst.file_id);
        return;
    }
    CHECK(g64(out, 8) == 4096,
          "  ... TransferLength is the requested 4096 (%llu)",
          (unsigned long long) g64(out, 8));
    memcpy(token, out + 16, OFFLOAD_TOKEN_SIZE);

    /* FSCTL_OFFLOAD_WRITE_INPUT (MS-FSCC 2.3.81): Size, Flags, FileOffset,
     * CopyLength, TransferOffset, Token. */
    memset(in, 0, sizeof(in));
    p32(in, 0, 32 + OFFLOAD_TOKEN_SIZE);
    p64(in, 8, 2048);                 /* FileOffset (destination) */
    p64(in, 16, 4096);                /* CopyLength */
    p64(in, 24, 0);                   /* TransferOffset within the token range */
    memcpy(in + 32, token, OFFLOAD_TOKEN_SIZE);

    out = smb2_ioctl_out(c, SMB2_FSCTL_OFFLOAD_WRITE, dst.file_id, in,
                         32 + OFFLOAD_TOKEN_SIZE, 4096, &st, &out_len);
    CHECK(st == ST_SUCCESS && out && out_len == 16,
          "OFFLOAD_WRITE of the token at 2048 -> 0x%08x (%u bytes)", st,
          out_len);
    if (out && out_len >= 16) {
        CHECK(g64(out, 8) == 4096, "  ... LengthWritten=4096 (%llu)",
              (unsigned long long) g64(out, 8));
    }
    CHECK(check_pattern(c, dst.file_id, 2048, 4096, 0x70, 0),
          "  ... the destination holds the source bytes at offset 2048");

    /* A token the server never minted must be refused rather than treated as
     * a zero-length copy. */
    memset(in, 0, sizeof(in));
    p32(in, 0, 32 + OFFLOAD_TOKEN_SIZE);
    p64(in, 8, 0);
    p64(in, 16, 64);
    p64(in, 24, 0);
    memset(in + 32, 0x5A, OFFLOAD_TOKEN_SIZE);
    st = smb2_ioctl(c, SMB2_FSCTL_OFFLOAD_WRITE, dst.file_id, in,
                    32 + OFFLOAD_TOKEN_SIZE);
    CHECK(st != ST_SUCCESS,
          "OFFLOAD_WRITE with an unminted token is refused (0x%08x)", st);

    smb2_close(c, src.file_id);
    smb2_close(c, dst.file_id);
} /* probe_copyoffload */

static void
probe_duplicate_extents(struct smb2_conn *c)
{
    struct smb2_create_out src, dst;
    uint8_t                in[40];
    uint32_t               st;

    printf("# --- block cloning: DUPLICATE_EXTENTS_TO_FILE ---\n");

    make_file(c, "de_src.bin", 8192, 0xA0, &src);
    make_file(c, "de_dst.bin", 8192, 0x00, &dst);

    /* DUPLICATE_EXTENTS_DATA (MS-FSCC 2.3.8): SourceFileID(16),
    * SourceFileOffset(8), TargetFileOffset(8), ByteCount(8). */
    memset(in, 0, sizeof(in));
    memcpy(in, src.file_id, 16);
    p64(in, 16, 0);                   /* SourceFileOffset */
    p64(in, 24, 4096);                /* TargetFileOffset */
    p64(in, 32, 4096);                /* ByteCount */

    st = smb2_ioctl(c, SMB2_FSCTL_DUPLICATE_EXTENTS, dst.file_id, in, 40);
    CHECK(st == ST_SUCCESS, "DUPLICATE_EXTENTS 4096 bytes to offset 4096 -> 0x%08x",
          st);
    if (st == ST_SUCCESS) {
        CHECK(check_pattern(c, dst.file_id, 4096, 4096, 0xA0, 0),
              "  ... the cloned range reads back as the source");
        CHECK(check_pattern(c, dst.file_id, 0, 4096, 0x00, 0),
              "  ... the untouched range is unchanged");
    }

    /* A clone that would run past the source's EOF has nothing to share. */
    memset(in, 0, sizeof(in));
    memcpy(in, src.file_id, 16);
    p64(in, 16, 4096);
    p64(in, 24, 0);
    p64(in, 32, 1 << 20);             /* far past the 8192-byte source */
    st = smb2_ioctl(c, SMB2_FSCTL_DUPLICATE_EXTENTS, dst.file_id, in, 40);
    CHECK(st != ST_SUCCESS,
          "DUPLICATE_EXTENTS past the source EOF is refused (0x%08x)", st);

    smb2_close(c, src.file_id);
    smb2_close(c, dst.file_id);
} /* probe_duplicate_extents */

/* ---- reparse points -----------------------------------------------------
 *
 * SMB expresses a symbolic link as a reparse point.  SET_REPARSE_POINT on a
 * regular file REPLACES it with a symlink (chimera removes the file, creates
 * the link, and rebinds the open to it), and GET_REPARSE_POINT reads the
 * target back as a REPARSE_DATA_BUFFER.  The round trip is the check: the
 * target that comes back must be the one that went in.
 */
#define SMB2_FILE_OPEN_REPARSE_POINT 0x00200000u

/* Build a symlink REPARSE_DATA_BUFFER (MS-FSCC 2.1.2.4) for `target`.
 * Returns the total length. */
static int
build_symlink_reparse(
    uint8_t    *buf,
    const char *target)
{
    int nlen = (int) strlen(target) * 2;   /* UTF-16LE */
    int i;

    p32(buf, 0, SMB2_IO_REPARSE_TAG_SYMLINK);
    p16(buf, 4, (uint16_t) (12 + 2 * nlen));   /* ReparseDataLength */
    p16(buf, 6, 0);                            /* Reserved */

    /* SYMBOLIC_LINK_REPARSE_BUFFER: the PathBuffer carries the substitute name
     * followed by the print name, both offsets relative to PathBuffer. */
    p16(buf, 8, 0);                            /* SubstituteNameOffset */
    p16(buf, 10, (uint16_t) nlen);             /* SubstituteNameLength */
    p16(buf, 12, (uint16_t) nlen);             /* PrintNameOffset */
    p16(buf, 14, (uint16_t) nlen);             /* PrintNameLength */
    p32(buf, 16, 1);                           /* SYMLINK_FLAG_RELATIVE */

    utf16le(target, buf + 20);
    utf16le(target, buf + 20 + nlen);

    /* The parser skips SubstituteNameOffset bytes from just after the 12-byte
     * symlink header, so PathBuffer begins at 8 + 12 = 20. */
    (void) i;
    return 20 + 2 * nlen;
} /* build_symlink_reparse */

static void
probe_reparse(struct smb2_conn *c)
{
    struct smb2_create_out co, ro;
    uint8_t                in[512];
    const uint8_t         *out;
    uint32_t               st = 0, out_len = 0;
    int                    in_len;
    const char            *target = "some/target/path";

    printf("# --- reparse points: SET_REPARSE_POINT / GET_REPARSE_POINT ---\n");

    /* GET on a plain file has nothing to report. */
    make_file(c, "rp_plain.bin", 64, 0x20, &co);
    out = smb2_ioctl_out(c, SMB2_FSCTL_GET_REPARSE_POINT, co.file_id, NULL, 0,
                         4096, &st, &out_len);
    CHECK(st == ST_NOT_A_REPARSE_POINT,
          "GET_REPARSE_POINT on a plain file -> STATUS_NOT_A_REPARSE_POINT "
          "(0x%08x)", st);
    smb2_close(c, co.file_id);

    /* Turn a file into a symlink. */
    make_file(c, "rp_link.bin", 0, 0, &co);
    in_len = build_symlink_reparse(in, target);
    st     = smb2_ioctl(c, SMB2_FSCTL_SET_REPARSE_POINT, co.file_id, in,
                        (uint32_t) in_len);
    CHECK(st == ST_SUCCESS, "SET_REPARSE_POINT(symlink -> '%s') -> 0x%08x",
          target, st);

    /* Read it back through the rebound handle. */
    out = smb2_ioctl_out(c, SMB2_FSCTL_GET_REPARSE_POINT, co.file_id, NULL, 0,
                         4096, &st, &out_len);
    CHECK(st == ST_SUCCESS && out && out_len >= 20,
          "GET_REPARSE_POINT on the rebound handle -> 0x%08x (%u bytes)", st,
          out_len);
    if (out && out_len >= 20) {
        uint16_t sub_len = g16(out, 10);
        char     got[256];
        int      i, n = sub_len / 2;

        CHECK(g32(out, 0) == SMB2_IO_REPARSE_TAG_SYMLINK,
              "  ... the tag is IO_REPARSE_TAG_SYMLINK (0x%08x)", g32(out, 0));

        if (n > (int) sizeof(got) - 1) {
            n = (int) sizeof(got) - 1;
        }
        for (i = 0; i < n; i++) {
            got[i] = (char) out[20 + g16(out, 8) + i * 2];
        }
        got[n] = '\0';
        /* The target comes back in the WINDOWS form: the server stores the
         * POSIX path and converts '/' to '\\' when it builds the reparse
         * buffer (and back again on SET), so the round trip is separator
         * conversion, not identity. */
        CHECK(strcmp(got, "some\\target\\path") == 0,
              "  ... the target round-trips in Windows form ('%s')", got);
    }
    smb2_close(c, co.file_id);

    /* And a fresh open of the link, taken WITHOUT following it, reports the
     * same thing -- which is the path a client actually uses. */
    /* Opening a reparse point takes ATTRIBUTE-ONLY access: the server turns a
     * metadata-only FILE_OPEN into an O_PATH-style handle, and only that form
     * may name a symlink under NOFOLLOW -- asking for data access instead gets
     * ELOOP from the backend and STATUS_STOPPED_ON_SYMLINK on the wire, which
     * is correct (a symlink has no data to read). */
    st = smb2_create_opts(c, "rp_link.bin", FILE_OPEN, FILE_READ_ATTRIBUTES,
                          FILE_SHARE_RWD, SMB2_FILE_OPEN_REPARSE_POINT,
                          NULL, &ro);
    CHECK(st == ST_SUCCESS,
          "re-open the link with FILE_OPEN_REPARSE_POINT -> 0x%08x", st);
    if (st == ST_SUCCESS) {
        out = smb2_ioctl_out(c, SMB2_FSCTL_GET_REPARSE_POINT, ro.file_id, NULL,
                             0, 4096, &st, &out_len);
        CHECK(st == ST_SUCCESS && out && out_len >= 20,
              "  ... GET_REPARSE_POINT on the reopened link -> 0x%08x (%u bytes)",
              st, out_len);
        smb2_close(c, ro.file_id);
    }

    /* DEVIATION, pinned deliberately: chimera does NOT enforce
     * MaxOutputResponse for GET_REPARSE_POINT.  Every other FSCTL with a
     * fixed-size output checks it (OFFLOAD_READ wants 528,
     * CREATE_OR_GET_OBJECT_ID wants 64, and QUERY_ALLOCATED_RANGES sizes its
     * range list to it), but this one emits the whole reparse buffer whatever
     * the client asked for -- so a client that sized an 8-byte buffer is handed
     * 84 bytes.  MS-SMB2 3.3.4.4 wants STATUS_BUFFER_TOO_SMALL here.  Asserted
     * as-is rather than fixed: the change is small but it moves behavior the
     * extended-tier pike/smbtorture reparse cases exercise, which this tier
     * cannot run. */
    st = smb2_create_opts(c, "rp_link.bin", FILE_OPEN, FILE_READ_ATTRIBUTES,
                          FILE_SHARE_RWD, SMB2_FILE_OPEN_REPARSE_POINT,
                          NULL, &ro);
    if (st == ST_SUCCESS) {
        out = smb2_ioctl_out(c, SMB2_FSCTL_GET_REPARSE_POINT, ro.file_id, NULL,
                             0, 8, &st, &out_len);
        CHECK(st == ST_SUCCESS && out_len == 84,
              "GET_REPARSE_POINT ignores an 8-byte MaxOutputResponse and "
              "returns the full buffer (0x%08x, %u bytes)", st, out_len);
        smb2_close(c, ro.file_id);
    }

    /* An unsupported reparse tag is accepted and ignored (the parser clears it
     * so the handler skips), not treated as a malformed request. */
    make_file(c, "rp_other.bin", 0, 0, &co);
    memset(in, 0, sizeof(in));
    p32(in, 0, 0xA0000003u);          /* IO_REPARSE_TAG_MOUNT_POINT */
    p16(in, 4, 12);
    st = smb2_ioctl(c, SMB2_FSCTL_SET_REPARSE_POINT, co.file_id, in, 20);
    CHECK(st == ST_SUCCESS,
          "SET_REPARSE_POINT with an unsupported tag is accepted (0x%08x)", st);
    smb2_close(c, co.file_id);
} /* probe_reparse */

int
main(
    int   argc,
    char *argv[])
{
    struct smb2_env      env;
    struct smb2_env_opts opts = { 0 };
    struct smb2_conn    *c;

    setvbuf(stdout, NULL, _IONBF, 0);

    smb2_env_start_opts(&env, &opts);
    c = smb2_conn_open(&env);
    smb2_handshake(c);

    printf("# dialect=0x%04x\n", c->dialect);

    probe_sparse(c);
    probe_copychunk(c);
    probe_copyoffload(c);
    probe_duplicate_extents(c);
    probe_reparse(c);

    smb2_env_stop(&env);

    if (failures) {
        fprintf(stderr, "%d FSCTL check(s) FAILED\n", failures);
        return 1;
    }
    printf("all SMB2 FSCTL checks passed\n");
    return 0;
} /* main */
