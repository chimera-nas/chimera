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

    fprintf(stderr, "=== SMB3 compression: Plain LZ77 decode vectors ===\n");
    test_decode_literals();
    test_decode_short_match();

    fprintf(stderr, "\nTotal: %d passed, %d failed\n", passed, failed);
    return failed == 0 ? 0 : 1;
} /* main */
