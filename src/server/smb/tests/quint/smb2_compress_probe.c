/* SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * SMB3 transport-compression ground-truth probe.
 *
 * smb_compress.c is the largest file in the tree that the quick tier never
 * reaches: the MS-XCA codecs and the COMPRESSION_TRANSFORM framing only run
 * once a connection has negotiated SMB2_COMPRESSION, and no trace in the
 * corpus does -- nor could one usefully, since the model has no notion of how
 * a message is framed on the wire.  Compression is a property of the
 * CONNECTION, like signing and encryption, so it belongs to the harness rather
 * than to the model.
 *
 * The corpus cannot reach it for a second reason worth stating, because it
 * decides the shape of this probe: the server compresses only a READ reply
 * whose data segment actually shrinks, and the corpus reads 1 to 4 bytes at a
 * time.  A wire profile alone would negotiate compression and then never
 * compress a single message.  A probe can choose its payload, so it drives
 * reads big enough and compressible enough to force real compressed traffic,
 * and asserts that traffic HAPPENED (conn->compressed_replies) rather than
 * merely that it was negotiated.
 *
 * TWO KINDS OF CHECK, because a loopback alone cannot do the job:
 *
 *   1. REFERENCE VECTORS (MS-XCA, produced by the Microsoft Xpress DLL via
 *      WPTS).  These are the asymmetric ground truth.  Everything else here
 *      runs chimera's compressor into chimera's decompressor, and a bug that
 *      is symmetric between the two -- a codec that encodes and decodes the
 *      same wrong thing -- round-trips perfectly and proves nothing.  Only
 *      bytes that came from Windows can catch that, which is why these vectors
 *      are the part of this probe that must never be dropped.
 *
 *   2. END-TO-END through the harness, over a real connection: the negotiate
 *      context, algorithm selection, the transform framing, the per-connection
 *      compression context, and the codecs, all on the path of an actual READ.
 *      The client's FRAMING is an independent implementation (smb2w_decompress
 *      in smb2_mbt_wire.h) so a wrong field offset disagrees instead of
 *      cancelling out; the codecs are necessarily shared, which is what (1) is
 *      for.
 *
 * This began as src/server/smb/tests/smb_compress_test.c, an extended-tier
 * unit test that ran once every six hours and never crossed an SMB connection.
 * The codec sections below are that test, carried over verbatim.
 */

#include "smb2_mbt_common.h"

static int failures = 0;

/* The codec sections below were written against these two macros; keeping them
 * lets that code carry over unchanged, and routes it into this probe's tally. */
#define TEST_PASS(name) do { printf("ok   - %s\n", name); } while (0)
#define TEST_FAIL(name) do { printf("FAIL - %s\n", name); failures++; } while (0)

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

/* ======================================================================== */
/* 1. Codec ground truth (no connection)                                    */
/* ======================================================================== */

/* A small deterministic PRNG so the "random" payload is reproducible. */
static uint32_t
xorshift(uint32_t *s)
{
    uint32_t x = *s;

    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    *s = x;
    return x;
} /* xorshift */

static void
roundtrip(
    const char    *name,
    const uint8_t *data,
    int            len)
{
    /* Worst-case LZ77 expansion is modest; give generous headroom. */
    int      cap   = len + len / 8 + 64;
    uint8_t *comp  = malloc(cap);
    uint8_t *plain = malloc(len ? len : 1);
    int      clen, dlen;

    clen = chimera_smb_lz77_compress(data, len, comp, cap);
    if (clen < 0) {
        TEST_FAIL(name);
        fprintf(stderr, "    compress failed (len=%d)\n", len);
        goto out;
    }

    dlen = chimera_smb_lz77_decompress(comp, clen, plain, len);
    if (dlen != len || memcmp(plain, data, len) != 0) {
        TEST_FAIL(name);
        fprintf(stderr, "    round-trip mismatch (len=%d clen=%d dlen=%d)\n",
                len, clen, dlen);
        goto out;
    }

    TEST_PASS(name);
    fprintf(stderr, "    %d -> %d bytes (%.1f%%)\n", len, clen,
            len ? 100.0 * clen / len : 0.0);

 out:
    free(comp);
    free(plain);
} /* roundtrip */

static void
test_roundtrip_random(void)
{
    uint8_t  buf[4096];
    uint32_t s = 0x1234567u;
    int      i;

    for (i = 0; i < (int) sizeof(buf); i++) {
        buf[i] = (uint8_t) xorshift(&s);
    }
    roundtrip("roundtrip: incompressible random 4096", buf, sizeof(buf));
} /* test_roundtrip_random */

static void
test_roundtrip_constant(void)
{
    uint8_t buf[8192];

    memset(buf, 'A', sizeof(buf));
    roundtrip("roundtrip: constant 8192 (long matches)", buf, sizeof(buf));
} /* test_roundtrip_constant */

static void
test_roundtrip_huge_constant(void)
{
    /* 200000 identical bytes exercises matches longer than 65538, i.e. the
     * 32-bit extended-length escape (16-bit 0 -> 32-bit length). */
    int      len = 200000;
    uint8_t *buf = malloc(len);

    memset(buf, 0x5a, len);
    roundtrip("roundtrip: constant 200000 (32-bit length escape)", buf, len);
    free(buf);
} /* test_roundtrip_huge_constant */

static void
test_roundtrip_text(void)
{
    /* Repeating phrase: lots of medium-length near-repeats. */
    static const char *phrase = "the quick brown fox jumps over the lazy dog. ";
    uint8_t            buf[5000];
    int                i, n = (int) strlen(phrase);

    for (i = 0; i < (int) sizeof(buf); i++) {
        buf[i] = (uint8_t) phrase[i % n];
    }
    roundtrip("roundtrip: repeating text 5000", buf, sizeof(buf));
} /* test_roundtrip_text */

static void
test_roundtrip_mixed_large(void)
{
    int      len = 200000;
    uint8_t *buf = malloc(len);
    uint32_t s   = 0xdeadbeefu;
    int      i;

    /* Alternating runs of random and constant data so both the literal and the
     * match paths get heavy exercise across multiple flag groups. */
    for (i = 0; i < len; i++) {
        buf[i] = (i / 137) & 1 ? (uint8_t) xorshift(&s) : (uint8_t) (i & 0xff);
    }
    roundtrip("roundtrip: mixed 200000", buf, len);
    free(buf);
} /* test_roundtrip_mixed_large */

static void
test_roundtrip_small_sizes(void)
{
    uint8_t buf[40];
    int     i, n;

    for (i = 0; i < (int) sizeof(buf); i++) {
        buf[i] = (uint8_t) (i * 7 + 1);
    }
    for (n = 1; n <= 16; n++) {
        char name[64];

        snprintf(name, sizeof(name), "roundtrip: tiny len=%d", n);
        roundtrip(name, buf, n);
    }
} /* test_roundtrip_small_sizes */

/* Hand-built literal-only stream: one flag dword of all-zero (32 literals are
 * all literals) followed by the literal bytes.  Independently validates the
 * decoder's flag handling and literal path. */
static void
test_decode_literals(void)
{
    uint8_t in[4 + 10];
    uint8_t out[10];
    int     i, dlen;

    in[0] = 0; in[1] = 0; in[2] = 0; in[3] = 0;   /* flags: all literals */
    for (i = 0; i < 10; i++) {
        in[4 + i] = (uint8_t) ('a' + i);
    }

    dlen = chimera_smb_lz77_decompress(in, sizeof(in), out, 10);
    if (dlen == 10 && memcmp(out, "abcdefghij", 10) == 0) {
        TEST_PASS("decode: literal-only stream");
    } else {
        TEST_FAIL("decode: literal-only stream");
    }
} /* test_decode_literals */

/* Hand-built short-match stream: literal 'A', then a match (offset 1, length 5)
 * that extends the run of 'A' to 6 bytes total.  Token layout: flags with bit31
 * = 0 (literal) and bit30 = 1 (match); literal 'A'; 16-bit match token with
 * low-3-bits = (length-3)=2 and high-13-bits = (offset-1)=0. */
static void
test_decode_short_match(void)
{
    uint8_t in[4 + 1 + 2];
    uint8_t out[6];
    int     dlen;

    /* flags: bit31=0 (literal), bit30=1 (match) => 0x40000000, little-endian. */
    in[0] = 0x00; in[1] = 0x00; in[2] = 0x00; in[3] = 0x40;
    in[4] = 'A';
    /* match token: (offset-1)<<3 | (length-3) = 0<<3 | 2 = 0x0002. */
    in[5] = 0x02; in[6] = 0x00;

    dlen = chimera_smb_lz77_decompress(in, sizeof(in), out, 6);
    if (dlen == 6 && memcmp(out, "AAAAAA", 6) == 0) {
        TEST_PASS("decode: short overlapping match");
    } else {
        TEST_FAIL("decode: short overlapping match");
    }
} /* test_decode_short_match */

/* Regression: a match whose 32-bit length escape is near INT_MAX must be
 * rejected, not wrapped past the bound check.  Wire layout: one literal (so
 * outpos > 0 and offset 1 is valid), then a match token with length code 7
 * (escape) and offset 1, a half-byte nibble of 15, an extended byte of 255, a
 * 16-bit 0 (escape to 32-bit), and a 32-bit length of 0x7FFFFFFC (decoded
 * length 0x7FFFFFFF).  With the old signed `outpos + length` test this wrapped
 * negative and drove a ~2 GB overlapping copy off the 64-byte output; the codec
 * must now return -1. */
static void
test_decode_length_overflow(void)
{
    uint8_t in[15];
    uint8_t out[64];
    int     dlen;

    in[0]  = 0x00; in[1] = 0x00; in[2] = 0x00; in[3] = 0x40; /* literal, then match */
    in[4]  = 'A';                                            /* literal */
    in[5]  = 0x07; in[6] = 0x00;                             /* match: len-code 7, offset 1 */
    in[7]  = 0x0F;                                           /* half-byte nibble = 15 */
    in[8]  = 0xFF;                                           /* extended byte = 255 */
    in[9]  = 0x00; in[10] = 0x00;                            /* 16-bit 0 => 32-bit escape */
    in[11] = 0xFC; in[12] = 0xFF; in[13] = 0xFF; in[14] = 0x7F; /* 32-bit len 0x7FFFFFFC */

    dlen = chimera_smb_lz77_decompress(in, sizeof(in), out, sizeof(out));
    if (dlen == -1) {
        TEST_PASS("decode: 32-bit length escape overflow rejected");
    } else {
        TEST_FAIL("decode: 32-bit length escape overflow rejected");
        fprintf(stderr, "    expected -1, got %d\n", dlen);
    }
} /* test_decode_length_overflow */

/* LZNT1 compress -> decompress round-trip over the same payload shapes. */
static void
roundtrip_lznt1(
    const char    *name,
    const uint8_t *data,
    int            len)
{
    int      cap   = len + len / 8 + 4096;
    uint8_t *comp  = malloc(cap);
    uint8_t *plain = malloc(len ? len : 1);
    int      clen, dlen;

    clen = chimera_smb_lznt1_compress(data, len, comp, cap);
    if (clen < 0) {
        TEST_FAIL(name);
        fprintf(stderr, "    lznt1 compress failed (len=%d)\n", len);
        goto out;
    }
    dlen = chimera_smb_lznt1_decompress(comp, clen, plain, len);
    if (dlen != len || memcmp(plain, data, len) != 0) {
        TEST_FAIL(name);
        fprintf(stderr, "    lznt1 round-trip mismatch (len=%d clen=%d dlen=%d)\n",
                len, clen, dlen);
        goto out;
    }
    TEST_PASS(name);
    fprintf(stderr, "    %d -> %d bytes (%.1f%%)\n", len, clen,
            len ? 100.0 * clen / len : 0.0);
 out:
    free(comp);
    free(plain);
} /* roundtrip_lznt1 */

static void
test_lznt1_roundtrips(void)
{
    uint8_t  buf[8192];
    uint32_t s = 0x9e3779b9u;
    int      i;

    memset(buf, 'A', 256);
    roundtrip_lznt1("lznt1 roundtrip: constant 256", buf, 256);
    memset(buf, 0x5a, sizeof(buf));
    roundtrip_lznt1("lznt1 roundtrip: constant 8192 (multi-chunk)", buf, sizeof(buf));
    for (i = 0; i < (int) sizeof(buf); i++) {
        buf[i] = (uint8_t) xorshift(&s);
    }
    roundtrip_lznt1("lznt1 roundtrip: incompressible 8192", buf, sizeof(buf));
    for (i = 0; i < (int) sizeof(buf); i++) {
        buf[i] = (uint8_t) ("the quick brown fox jumps over the lazy dog. "[i % 45]);
    }
    roundtrip_lznt1("lznt1 roundtrip: repeating text 8192", buf, sizeof(buf));
    for (i = 1; i <= 20; i++) {
        char name[48];
        int  j;
        for (j = 0; j < i; j++) {
            buf[j] = (uint8_t) (j * 7 + 1);
        }
        snprintf(name, sizeof(name), "lznt1 roundtrip: tiny len=%d", i);
        roundtrip_lznt1(name, buf, i);
    }
} /* test_lznt1_roundtrips */

/* Hex-string -> bytes, returns length. */
static int
unhex(
    const char *hex,
    uint8_t    *out)
{
    int i, n = (int) strlen(hex) / 2;

    for (i = 0; i < n; i++) {
        unsigned v;
        sscanf(hex + i * 2, "%2x", &v);
        out[i] = (uint8_t) v;
    }
    return n;
} /* unhex */

/* Decompress reference vectors produced by the Microsoft MS-XCA LZNT1 codec
 * (via the WPTS Xpress DLL) and check they reconstruct the original — proves
 * interop with the real implementation, independent of our own compressor. */
static void
test_lznt1_ms_vectors(void)
{
    static const struct { const char *name, *in, *out; } vecs[] = {
        { "abc",         "616263",
          "0230616263" },
        { "hello",       "68656c6c6f2068656c6c6f2068656c6c6f2068656c6c6f20776f726c6420776f726c64",
          "10b04068656c6c6f200f5077106f726c640328" },
        { "constant256",
          "4141414141414141414141414141414141414141414141414141414141414141"
          "4141414141414141414141414141414141414141414141414141414141414141"
          "4141414141414141414141414141414141414141414141414141414141414141"
          "4141414141414141414141414141414141414141414141414141414141414141"
          "4141414141414141414141414141414141414141414141414141414141414141"
          "4141414141414141414141414141414141414141414141414141414141414141"
          "4141414141414141414141414141414141414141414141414141414141414141"
          "4141414141414141414141414141414141414141414141414141414141414141",
          "03b00241fc00" },
    };
    int k;

    for (k = 0; k < (int) (sizeof(vecs) / sizeof(vecs[0])); k++) {
        uint8_t in[512], expect[512], out[512];
        int     ilen = unhex(vecs[k].in, in);
        int     elen = unhex(vecs[k].out, expect);
        char    name[64];

        /* in[] is the plaintext, expect[] (the OUT hex) is the MS-compressed form:
         * decompress the MS output and compare to the plaintext. */
        int     dlen = chimera_smb_lznt1_decompress(expect, elen, out, sizeof(out));
        snprintf(name, sizeof(name), "lznt1 MS vector: %s", vecs[k].name);
        if (dlen == ilen && memcmp(out, in, ilen) == 0) {
            TEST_PASS(name);
        } else {
            TEST_FAIL(name);
            fprintf(stderr, "    expected %d bytes, got %d\n", ilen, dlen);
        }
    }
} /* test_lznt1_ms_vectors */

/* MS-XCA LZ77+Huffman reference vectors (generated by the WPTS Xpress DLL).  The
 * OUT strings are the MS-compressed form; decompressing them must reconstruct
 * the original — proving interop with the real implementation. */
#define HUFF_HELLO_IN "68656c6c6f2068656c6c6f2068656c6c6f2068656c6c6f20776f726c6420776f726c64"
#define HUFF_HELLO_OUT \
        "0000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000000440004000230000400400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000040000000000000000000000000000000040000000000030000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005bad50efd840000c00000"
#define HUFF_C256_OUT \
        "0000000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000020000000000001000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000980000ed"

static void
roundtrip_huffman(
    const char    *name,
    const uint8_t *data,
    int            len)
{
    int      cap   = len + len / 2 + 512;
    uint8_t *comp  = malloc(cap);
    uint8_t *plain = malloc(len ? len : 1);
    int      clen, dlen;

    clen = chimera_smb_lz77huffman_compress(data, len, comp, cap);
    if (clen < 0) {
        TEST_FAIL(name);
        fprintf(stderr, "    huffman compress failed (len=%d)\n", len);
        goto out;
    }
    dlen = chimera_smb_lz77huffman_decompress(comp, clen, plain, len);
    if (dlen != len || memcmp(plain, data, len) != 0) {
        TEST_FAIL(name);
        fprintf(stderr, "    huffman round-trip mismatch (len=%d clen=%d dlen=%d)\n",
                len, clen, dlen);
        goto out;
    }
    TEST_PASS(name);
    fprintf(stderr, "    %d -> %d bytes (%.1f%%)\n", len, clen,
            len ? 100.0 * clen / len : 0.0);
 out:
    free(comp);
    free(plain);
} /* roundtrip_huffman */

static void
test_lz77huffman_roundtrips(void)
{
    uint8_t *buf = malloc(200000);
    uint32_t s   = 0x12345u;
    int      i;

    memset(buf, 'A', 256);
    roundtrip_huffman("huffman roundtrip: constant 256", buf, 256);
    memset(buf, 0x33, 5000);
    roundtrip_huffman("huffman roundtrip: constant 5000", buf, 5000);
    for (i = 0; i < 8192; i++) {
        buf[i] = (uint8_t) ("the quick brown fox jumps over the lazy dog. "[i % 45]);
    }
    roundtrip_huffman("huffman roundtrip: repeating text 8192", buf, 8192);
    for (i = 0; i < 8192; i++) {
        buf[i] = (uint8_t) xorshift(&s);
    }
    roundtrip_huffman("huffman roundtrip: incompressible 8192", buf, 8192);
    /* Spans multiple 64 KB blocks. */
    for (i = 0; i < 200000; i++) {
        buf[i] = (uint8_t) ((i / 100) & 1 ? xorshift(&s) : (i & 0xff));
    }
    roundtrip_huffman("huffman roundtrip: mixed 200000 (multi-block)", buf, 200000);
    for (i = 1; i <= 20; i++) {
        char name[48];
        int  j;
        for (j = 0; j < i; j++) {
            buf[j] = (uint8_t) (j * 7 + 1);
        }
        snprintf(name, sizeof(name), "huffman roundtrip: tiny len=%d", i);
        roundtrip_huffman(name, buf, i);
    }
    free(buf);
} /* test_lz77huffman_roundtrips */

static void
test_lz77huffman_ms_vectors(void)
{
    uint8_t in[1024], out[512];
    int     ilen, dlen, i, ok;

    ilen = unhex(HUFF_HELLO_OUT, in);
    dlen = chimera_smb_lz77huffman_decompress(in, ilen, out, 35);
    {
        uint8_t expect[64];
        int     elen = unhex(HUFF_HELLO_IN, expect);
        if (dlen == elen && memcmp(out, expect, elen) == 0) {
            TEST_PASS("huffman MS vector: hello");
        } else {
            TEST_FAIL("huffman MS vector: hello");
            fprintf(stderr, "    expected %d bytes, got %d\n", elen, dlen);
        }
    }

    ilen = unhex(HUFF_C256_OUT, in);
    dlen = chimera_smb_lz77huffman_decompress(in, ilen, out, 256);
    ok   = (dlen == 256);
    for (i = 0; ok && i < 256; i++) {
        if (out[i] != 0x41) {
            ok = 0;
        }
    }
    if (ok) {
        TEST_PASS("huffman MS vector: constant256");
    } else {
        TEST_FAIL("huffman MS vector: constant256");
        fprintf(stderr, "    got %d bytes\n", dlen);
    }
} /* test_lz77huffman_ms_vectors */

/* ======================================================================== */
/* 2. End-to-end over a real connection                                     */
/* ======================================================================== */

/* A payload the codecs can actually shrink, and that a wrong decompression
 * cannot accidentally reproduce: long runs (so Pattern_V1 and the LZ77 match
 * tokens both have something to find) separated by varying literal text, with
 * a deterministic per-offset byte so a mis-assembled buffer differs from the
 * original at the first wrong byte rather than looking plausible. */
static void
fill_compressible(
    uint8_t *buf,
    int      len)
{
    int i = 0;

    while (i < len) {
        int run = 64 + (i % 193);
        int j;

        for (j = 0; j < run && i < len; j++, i++) {
            buf[i] = (uint8_t) ('A' + ((i / 691) % 23));
        }
        for (j = 0; j < 17 && i < len; j++, i++) {
            buf[i] = (uint8_t) (0x20 + ((i * 7 + j) % 90));
        }
    }
} /* fill_compressible */

/* Drive one compression profile end to end: negotiate it, write a large
 * compressible file, read it back, and require both that the bytes survive and
 * that the reply actually arrived compressed. */
static void
probe_profile(
    const char *label,
    uint16_t    alg,
    int         chained)
{
    struct smb2_env          env;
    struct smb2_env_opts     opts = { 0 };
    struct smb2_wire_profile w    = {
        .name             = label,
        .max_dialect      = 0x0311,
        .ntlmv2           = 1,
        .compress         = 1,
        .compress_alg     = alg,
        .compress_chained = chained,
        .signing_alg      = SMB2W_SIGN_AES_GMAC,
    };
    struct smb2_conn        *c;
    struct smb2_create_out   co;
    static uint8_t           payload[64 * 1024];
    static uint8_t           readback[64 * 1024];
    const int                len = (int) sizeof(payload);
    uint32_t                 st, count = 0, rlen = 0, before;
    int                      off;

    printf("# --- %s ---\n", label);

    fill_compressible(payload, len);

    smb2_env_open_wire(&env, &opts, &w);
    smb2_env_fs_setup(&env, "fs0");

    c = smb2_conn_open(&env);

    st = smb2_negotiate(c);
    CHECK(st == ST_SUCCESS, "%s: NEGOTIATE -> 0x%08x", label, st);
    CHECK(c->dialect == 0x0311, "%s: dialect 0x%04x is 3.1.1", label, c->dialect);
    /* The server's echo, not our offer: a server that ignored the context
     * would leave this off and the reads below would never compress. */
    CHECK(c->compress_on && c->compress_alg == alg,
          "%s: server selected compression alg 0x%04x (on=%d)",
          label, c->compress_alg, c->compress_on);

    st = smb2_session_setup(c);
    CHECK(st == ST_SUCCESS, "%s: SESSION_SETUP -> 0x%08x", label, st);

    st = smb2_tree_connect(c, "\\\\127.0.0.1\\share");
    CHECK(st == ST_SUCCESS, "%s: TREE_CONNECT -> 0x%08x", label, st);

    st = smb2_create(c, "comp.dat", FILE_OPEN_IF, FILE_ALL_ACCESS,
                     FILE_SHARE_RWD, NULL, &co);
    CHECK(st == ST_SUCCESS, "%s: CREATE -> 0x%08x", label, st);

    /* Write in chunks the server will accept, then read back in chunks large
     * enough that the reply's data segment can shrink below the 16-byte
     * transform header -- which is what makes the server choose to compress. */
    for (off = 0; off < len; off += 16384) {
        int n = len - off < 16384 ? len - off : 16384;

        st = smb2_write(c, co.file_id, (uint64_t) off, payload + off,
                        (uint32_t) n, &count);
        if (st != ST_SUCCESS || count != (uint32_t) n) {
            CHECK(0, "%s: WRITE at %d -> 0x%08x (count=%u)", label, off, st, count);
            break;
        }
    }
    CHECK(off >= len, "%s: wrote %d bytes", label, len);

    before = c->compressed_replies;

    for (off = 0; off < len; off += 16384) {
        int n = len - off < 16384 ? len - off : 16384;

        st = smb2_read(c, co.file_id, (uint64_t) off, (uint32_t) n,
                       readback + off, &rlen);
        if (st != ST_SUCCESS || rlen != (uint32_t) n) {
            CHECK(0, "%s: READ at %d -> 0x%08x (len=%u)", label, off, st, rlen);
            break;
        }
    }

    /* The whole point: the reply came back compressed, and the bytes are the
     * bytes.  Either half alone is worthless -- a server that never compresses
     * passes the content check, and a decompressor that returns garbage of the
     * right length passes a "was it compressed" check. */
    CHECK(c->compressed_replies > before,
          "%s: %u repl(ies) arrived COMPRESSION_TRANSFORM-framed",
          label, c->compressed_replies - before);
    CHECK(off >= len && memcmp(readback, payload, (size_t) len) == 0,
          "%s: %d bytes survive the compressed round trip", label, len);

    st = smb2_close(c, co.file_id);
    CHECK(st == ST_SUCCESS, "%s: CLOSE -> 0x%08x", label, st);

    smb2_env_fs_teardown(&env, "fs0");
    smb2_env_stop(&env);
} /* probe_profile */

/* The other direction: the CLIENT compresses its requests, so the server's
 * inbound decompression runs.  That path parses bytes the server did not
 * produce, which makes it the half of the feature most worth testing, and a
 * client that only decompresses never reaches it.
 *
 * The assertion is on the EFFECT: a compressed WRITE must land the same bytes
 * a plaintext one would, verified by reading them back over a plain
 * (uncompressed) read.  A server that quietly mis-decompressed would store
 * different bytes and fail here, where a status-only check would pass. */
static void
probe_compressed_requests(
    const char *label,
    uint16_t    alg,
    int         mode)          /* 1 = unchained, 2 = chained */
{
    struct smb2_env          env;
    struct smb2_env_opts     opts = { 0 };
    struct smb2_wire_profile w    = {
        .name         = label,
        .max_dialect  = 0x0311,
        .ntlmv2       = 1,
        .compress     = 1,
        .compress_alg = alg,
        .signing_alg  = SMB2W_SIGN_AES_GMAC,
    };
    struct smb2_conn        *c;
    struct smb2_create_out   co;
    static uint8_t           payload[32 * 1024];
    static uint8_t           readback[32 * 1024];
    const int                len = (int) sizeof(payload);
    uint32_t                 st, count = 0, rlen = 0;

    printf("# --- %s: compressed REQUESTS ---\n", label);

    fill_compressible(payload, len);

    smb2_env_open_wire(&env, &opts, &w);
    smb2_env_fs_setup(&env, "fs0");

    c = smb2_conn_open(&env);

    st = smb2_negotiate(c);
    CHECK(st == ST_SUCCESS, "%s: NEGOTIATE -> 0x%08x", label, st);
    st = smb2_session_setup(c);
    CHECK(st == ST_SUCCESS, "%s: SESSION_SETUP -> 0x%08x", label, st);
    st = smb2_tree_connect(c, "\\\\127.0.0.1\\share");
    CHECK(st == ST_SUCCESS, "%s: TREE_CONNECT -> 0x%08x", label, st);

    st = smb2_create(c, "creq.dat", FILE_OPEN_IF, FILE_ALL_ACCESS,
                     FILE_SHARE_RWD, NULL, &co);
    CHECK(st == ST_SUCCESS, "%s: CREATE -> 0x%08x", label, st);

    /* Only now: the handshake itself must go out in the clear, since the
     * algorithm is not agreed until NEGOTIATE completes. */
    c->compress_requests = mode;

    st = smb2_write(c, co.file_id, 0, payload, (uint32_t) len, &count);
    CHECK(st == ST_SUCCESS && count == (uint32_t) len,
          "%s: compressed WRITE of %d bytes -> 0x%08x (count=%u)",
          label, len, st, count);
    CHECK(c->compressed_sent > 0,
          "%s: %u request(s) went out COMPRESSION_TRANSFORM-framed",
          label, c->compressed_sent);

    c->compress_requests = 0;

    st = smb2_read(c, co.file_id, 0, (uint32_t) len, readback, &rlen);
    CHECK(st == ST_SUCCESS && rlen == (uint32_t) len &&
          memcmp(readback, payload, (size_t) len) == 0,
          "%s: the server stored exactly what the compressed WRITE carried",
          label);

    st = smb2_close(c, co.file_id);
    CHECK(st == ST_SUCCESS, "%s: CLOSE -> 0x%08x", label, st);

    smb2_env_fs_teardown(&env, "fs0");
    smb2_env_stop(&env);
} /* probe_compressed_requests */

/* A connection that does NOT negotiate compression must not receive a
 * compressed reply.  Without this, every positive result above is equally
 * consistent with a server that compresses unconditionally -- which would
 * break every client that never asked. */
static void
probe_not_negotiated(void)
{
    struct smb2_env          env;
    struct smb2_env_opts     opts = { 0 };
    struct smb2_wire_profile w    = {
        .name        = "no-compression",
        .max_dialect = 0x0311,
        .ntlmv2      = 1,
        .compress    = 1,   /* server-side enabled ... */
        .signing_alg = SMB2W_SIGN_AES_GMAC,
    };
    struct smb2_conn        *c;
    struct smb2_create_out   co;
    static uint8_t           payload[32 * 1024];
    static uint8_t           readback[32 * 1024];
    const int                len = (int) sizeof(payload);
    uint32_t                 st, count = 0, rlen = 0;

    printf("# --- compression enabled server-side, not offered by the client ---\n");

    /* ... but the client offers no algorithm, so nothing may compress. */
    w.compress_alg = 0;
    fill_compressible(payload, len);

    smb2_env_open_wire(&env, &opts, &w);
    smb2_env_fs_setup(&env, "fs0");

    c = smb2_conn_open(&env);
    /* Suppress the context entirely for this connection. */
    st = smb2_negotiate(c);
    CHECK(st == ST_SUCCESS, "no-compression: NEGOTIATE -> 0x%08x", st);

    st = smb2_session_setup(c);
    CHECK(st == ST_SUCCESS, "no-compression: SESSION_SETUP -> 0x%08x", st);
    st = smb2_tree_connect(c, "\\\\127.0.0.1\\share");
    CHECK(st == ST_SUCCESS, "no-compression: TREE_CONNECT -> 0x%08x", st);

    st = smb2_create(c, "plain.dat", FILE_OPEN_IF, FILE_ALL_ACCESS,
                     FILE_SHARE_RWD, NULL, &co);
    CHECK(st == ST_SUCCESS, "no-compression: CREATE -> 0x%08x", st);

    st = smb2_write(c, co.file_id, 0, payload, (uint32_t) len, &count);
    CHECK(st == ST_SUCCESS && count == (uint32_t) len,
          "no-compression: WRITE %d bytes -> 0x%08x", len, st);

    st = smb2_read(c, co.file_id, 0, (uint32_t) len, readback, &rlen);
    CHECK(st == ST_SUCCESS && rlen == (uint32_t) len &&
          memcmp(readback, payload, (size_t) len) == 0,
          "no-compression: READ returns the bytes written (len=%u)", rlen);
    CHECK(c->compressed_replies == 0,
          "no-compression: no reply was compressed (%u seen)",
          c->compressed_replies);

    st = smb2_close(c, co.file_id);
    CHECK(st == ST_SUCCESS, "no-compression: CLOSE -> 0x%08x", st);

    smb2_env_fs_teardown(&env, "fs0");
    smb2_env_stop(&env);
} /* probe_not_negotiated */

int
main(
    int    argc,
    char **argv)
{
    (void) argc;
    (void) argv;

    setvbuf(stdout, NULL, _IONBF, 0);

    printf("# === codec ground truth: Plain LZ77 round-trip ===\n");
    test_roundtrip_random();
    test_roundtrip_constant();
    test_roundtrip_huge_constant();
    test_roundtrip_text();
    test_roundtrip_mixed_large();
    test_roundtrip_small_sizes();

    printf("# === codec ground truth: hand-built LZ77 streams ===\n");
    test_decode_literals();
    test_decode_short_match();
    test_decode_length_overflow();

    printf("# === codec ground truth: LZNT1 ===\n");
    test_lznt1_roundtrips();
    test_lznt1_ms_vectors();

    printf("# === codec ground truth: LZ77+Huffman ===\n");
    test_lz77huffman_roundtrips();
    test_lz77huffman_ms_vectors();

    printf("# === end to end over an SMB3 connection ===\n");
    probe_profile("LZ77", SMB2_COMPRESSION_LZ77, 0);
    probe_profile("LZ77+Huffman", SMB2_COMPRESSION_LZ77_HUFFMAN, 0);
    probe_profile("LZNT1", SMB2_COMPRESSION_LZNT1, 0);
    probe_profile("LZ77 chained (Pattern_V1)", SMB2_COMPRESSION_LZ77, 1);
    probe_compressed_requests("LZ77", SMB2_COMPRESSION_LZ77, 1);
    probe_compressed_requests("LZNT1", SMB2_COMPRESSION_LZNT1, 1);
    probe_compressed_requests("LZ77 chained", SMB2_COMPRESSION_LZ77, 2);
    probe_not_negotiated();

    if (failures) {
        fprintf(stderr, "%d SMB3 compression check(s) FAILED\n", failures);
        return 1;
    }
    printf("all SMB3 compression checks passed\n");
    return 0;
} /* main */
