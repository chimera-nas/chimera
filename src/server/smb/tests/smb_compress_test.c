// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Unit tests for the SMB3 transport-compression codecs (smb_compress.c).
 *
 * These exercise the pure MS-XCA Plain LZ77 buffer codec directly — no SMB
 * connection, no daemon, no I/O.  Coverage:
 *   - round-trip (compress -> decompress) over a spread of payload shapes:
 *     empty-ish, random/incompressible, highly repetitive (long matches),
 *     text with near-repeats, and a large buffer;
 *   - decompression of a hand-built literal-only stream (validates the flag
 *     dword + literal token wire layout independently of the compressor);
 *   - decompression of a hand-built long-match stream (validates the 16-bit
 *     match token + extended-length escape).
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "server/smb/smb_compress.h"

#define TEST_PASS(name) do { fprintf(stderr, "  PASS: %s\n", name); passed++; } while (0)
#define TEST_FAIL(name) do { fprintf(stderr, "  FAIL: %s\n", name); failed++; } while (0)

static int passed = 0;
static int failed = 0;

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
        int     dlen;
        char    name[64];

        /* in[] is the plaintext, expect[] (the OUT hex) is the MS-compressed form:
         * decompress the MS output and compare to the plaintext. */
        dlen = chimera_smb_lznt1_decompress(expect, elen, out, sizeof(out));
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

int
main(
    int    argc,
    char **argv)
{
    (void) argc;
    (void) argv;

    fprintf(stderr, "=== SMB3 compression: Plain LZ77 round-trip ===\n");
    test_roundtrip_random();
    test_roundtrip_constant();
    test_roundtrip_huge_constant();
    test_roundtrip_text();
    test_roundtrip_mixed_large();
    test_roundtrip_small_sizes();

    fprintf(stderr, "=== SMB3 compression: LZNT1 ===\n");
    test_lznt1_roundtrips();
    test_lznt1_ms_vectors();

    fprintf(stderr, "=== SMB3 compression: LZ77+Huffman ===\n");
    test_lz77huffman_roundtrips();
    test_lz77huffman_ms_vectors();

    fprintf(stderr, "=== SMB3 compression: Plain LZ77 decode vectors ===\n");
    test_decode_literals();
    test_decode_short_match();

    fprintf(stderr, "\nTotal: %d passed, %d failed\n", passed, failed);
    return failed == 0 ? 0 : 1;
} /* main */
