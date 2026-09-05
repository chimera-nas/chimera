// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * chimera_sid: the native Windows SID value type carried on ACL principals
 * and owner/group attrs.  Covers the binary <-> string codec, the struct
 * helpers, and the malformed-input rejections the SMB security-descriptor
 * parser relies on.
 */

#include <stdio.h>
#include <string.h>
#undef NDEBUG
#include <assert.h>

#include "vfs/sdk/vfs_sid.h"

#define TEST_PASS(name) fprintf(stderr, "  PASS: %s\n", name)

/* A domain SID round-trips string -> binary -> string. */
static void
test_str_roundtrip(void)
{
    struct chimera_sid s;
    char               buf[CHIMERA_SID_STR_MAX];

    memset(&s, 0xa5, sizeof(s)); /* from_str must fully define the struct */

    assert(chimera_sid_from_str(&s, "S-1-5-21-1-2-3-1001") == 0);
    assert(chimera_sid_present(&s));
    assert(s.len == 8 + 5 * 4);
    assert(s.data[0] == 1);  /* revision */
    assert(s.data[1] == 5);  /* sub-authority count */
    assert(s.data[7] == 5);  /* NT authority, big-endian low byte */
    assert(chimera_sid_to_str(&s, buf, sizeof(buf)) > 0);
    assert(strcmp(buf, "S-1-5-21-1-2-3-1001") == 0);

    /* The struct is fully defined: every byte past the SID is zero, so a
     * copy of it (e.g. inside an ACE) compares byte-for-byte with one that
     * was deserialized into zeroed storage. */
    for (unsigned i = s.len; i < CHIMERA_SID_MAX_LEN; i++) {
        assert(s.data[i] == 0);
    }
    /* ... and a failed parse leaves nothing of the previous contents. */
    memset(&s, 0xa5, sizeof(s));
    assert(chimera_sid_from_str(&s, "S-1-5-abc") == -1);
    assert(!chimera_sid_present(&s));
    for (unsigned i = 0; i < CHIMERA_SID_MAX_LEN; i++) {
        assert(s.data[i] == 0);
    }

    /* A well-known SID with a single sub-authority. */
    assert(chimera_sid_from_str(&s, "S-1-1-0") == 0);
    assert(s.len == 12);
    assert(chimera_sid_to_str(&s, buf, sizeof(buf)) == 7);
    assert(strcmp(buf, "S-1-1-0") == 0);

    TEST_PASS("SID string round-trips through binary form");
} /* test_str_roundtrip */

/* The struct copies out of (and back into) a raw wire buffer. */
static void
test_bin_roundtrip(void)
{
    struct chimera_sid a, b;
    uint8_t            wire[CHIMERA_SID_MAX_LEN + 8];
    char               str[CHIMERA_SID_STR_MAX];
    int                n;

    assert(chimera_sid_from_str(&a, "S-1-5-21-100-200-300-513") == 0);

    /* Consume exactly the SID from a longer buffer. */
    memset(wire, 0xee, sizeof(wire));
    memcpy(wire, a.data, a.len);
    n = chimera_sid_from_bin(&b, wire, sizeof(wire));
    assert(n == (int) a.len);
    assert(chimera_sid_equal(&a, &b));

    /* Raw-buffer string codec agrees with the struct form. */
    n = chimera_sid_bin_to_str(wire, sizeof(wire), str, sizeof(str));
    assert(n == (int) a.len);
    assert(strcmp(str, "S-1-5-21-100-200-300-513") == 0);
    memset(wire, 0, sizeof(wire));
    n = chimera_sid_str_to_bin(str, wire, sizeof(wire));
    assert(n == (int) a.len);
    assert(memcmp(wire, a.data, a.len) == 0);

    /* Truncated: the buffer ends before the declared sub-authorities. */
    assert(chimera_sid_from_bin(&b, a.data, a.len - 1) == -1);

    /* Over-long sub-authority count is rejected, not overrun. */
    memset(wire, 0, sizeof(wire));
    wire[0] = 1;
    wire[1] = CHIMERA_SID_MAX_SUB_AUTHS + 1;
    assert(chimera_sid_from_bin(&b, wire, sizeof(wire)) == -1);
    assert(chimera_sid_bin_to_str(wire, sizeof(wire), str, sizeof(str)) == -1);

    /* Too short to even hold a header. */
    assert(chimera_sid_from_bin(&b, wire, 7) == -1);

    TEST_PASS("SID binary round-trips and rejects malformed input");
} /* test_bin_roundtrip */

/* The MS-DTYP maximum of 15 sub-authorities fits; 16 does not. */
static void
test_max_sub_auths(void)
{
    struct chimera_sid s;
    char               buf[CHIMERA_SID_STR_MAX];
    char               big[CHIMERA_SID_STR_MAX];
    int                pos;

    pos = snprintf(big, sizeof(big), "S-1-5");
    for (int i = 0; i < CHIMERA_SID_MAX_SUB_AUTHS; i++) {
        pos += snprintf(big + pos, sizeof(big) - pos, "-%u", 4000000000u + i);
    }
    assert(chimera_sid_from_str(&s, big) == 0);
    assert(s.len == CHIMERA_SID_MAX_LEN);
    assert(chimera_sid_to_str(&s, buf, sizeof(buf)) == (int) strlen(big));
    assert(strcmp(buf, big) == 0);

    /* One more sub-authority than the cap. */
    snprintf(big + pos, sizeof(big) - pos, "-7");
    assert(chimera_sid_from_str(&s, big) == -1);

    TEST_PASS("15 sub-authorities round-trip, 16 are rejected");
} /* test_max_sub_auths */

/* Malformed strings and too-small output buffers are rejected. */
static void
test_bad_input(void)
{
    struct chimera_sid s;
    char               tiny[4];
    uint8_t            binsmall[8];

    assert(chimera_sid_from_str(&s, "") == -1);
    assert(chimera_sid_from_str(&s, "X-1-5-21") == -1);
    assert(chimera_sid_from_str(&s, "S-1") == -1);
    assert(chimera_sid_from_str(&s, "S-1-5-abc") == -1);
    assert(chimera_sid_from_str(&s, "S-1-5-21-") == -1);

    assert(chimera_sid_from_str(&s, "S-1-5-21-1-2-3-1001") == 0);
    assert(chimera_sid_to_str(&s, tiny, sizeof(tiny)) == -1);
    assert(chimera_sid_str_to_bin("S-1-5-21-1-2-3-1001", binsmall,
                                  sizeof(binsmall)) == -1);

    TEST_PASS("malformed strings and small buffers are rejected");
} /* test_bad_input */

/* Zero length is the absent state; equal compares length and bytes. */
static void
test_absent_and_equal(void)
{
    struct chimera_sid a, b, none;

    memset(&none, 0, sizeof(none));
    assert(!chimera_sid_present(&none));
    assert(!chimera_sid_present(NULL));

    assert(chimera_sid_from_str(&a, "S-1-5-21-1-2-3-1001") == 0);
    assert(chimera_sid_from_str(&b, "S-1-5-21-1-2-3-1002") == 0);
    assert(!chimera_sid_equal(&a, &b));
    assert(chimera_sid_equal(&a, &a));
    assert(!chimera_sid_equal(&a, &none));
    assert(!chimera_sid_equal(&a, NULL));
    assert(!chimera_sid_equal(NULL, NULL));

    TEST_PASS("len 0 is the absent state; equality compares bytes");
} /* test_absent_and_equal */

int
main(
    int    argc,
    char **argv)
{
    test_str_roundtrip();
    test_bin_roundtrip();
    test_max_sub_auths();
    test_bad_input();
    test_absent_and_equal();

    fprintf(stderr, "All SID codec tests passed\n");
    return 0;
} /* main */
