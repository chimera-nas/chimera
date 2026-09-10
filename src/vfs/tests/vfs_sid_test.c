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

/* A length that cannot describe a SID is not a SID: the presence predicate,
 * and everything built on it, treats it as absent.  That is what bounds every
 * consumer that copies `len` bytes -- the serializer, the SMB emitter, the
 * backends -- whoever built the struct and however badly. */
static void
test_len_invariant(void)
{
    struct chimera_sid   s, t;
    char                 buf[CHIMERA_SID_STR_MAX];
    static const uint8_t bad[] = { 1, 2, 3, 4, 5, 6, 7, 69, 100, 200, 255 };

    assert(chimera_sid_from_str(&s, "S-1-5-21-1-2-3-1001") == 0);

    for (unsigned i = 0; i < sizeof(bad); i++) {
        t     = s;
        t.len = bad[i];
        assert(!chimera_sid_present(&t));
        assert(!chimera_sid_equal(&t, &t));
        assert(!chimera_sid_equal(&t, &s));
        assert(chimera_sid_to_str(&t, buf, sizeof(buf)) == -1);
    }

    /* The bounds themselves are present: the 8-byte header alone (a SID with
     * no sub-authorities) and the 15-sub-authority maximum. */
    t     = s;
    t.len = CHIMERA_SID_MIN_LEN;
    assert(chimera_sid_present(&t));
    t.len = CHIMERA_SID_MAX_LEN;
    assert(chimera_sid_present(&t));

    TEST_PASS("out-of-range lengths are absent; the bounds are present");
} /* test_len_invariant */

/* The owner/group pair record the native backends persist (cairn and diskfs
 * share this codec): every case leaves both outputs fully defined. */
static void
test_pair_codec(void)
{
    struct chimera_sid owner, group, o2, g2, none;
    uint8_t            rec[CHIMERA_SID_PAIR_MAX];
    uint8_t            big[CHIMERA_SID_PAIR_MAX + 16];
    int                len;

    memset(&none, 0, sizeof(none));
    assert(chimera_sid_from_str(&owner, "S-1-5-21-1-2-3-1001") == 0);
    assert(chimera_sid_from_str(&group, "S-1-5-21-1-2-3-513") == 0);

    /* Both present: round trip byte for byte. */
    len = chimera_sid_pair_encode(&owner, &group, rec, sizeof(rec));
    assert(len == 2 + (int) owner.len + (int) group.len);
    memset(&o2, 0xa5, sizeof(o2));
    memset(&g2, 0xa5, sizeof(g2));
    assert(chimera_sid_pair_decode(rec, len, &o2, &g2) == 0);
    assert(memcmp(&o2, &owner, sizeof(owner)) == 0);
    assert(memcmp(&g2, &group, sizeof(group)) == 0);

    /* Owner only: the record ends with a zero group length. */
    len = chimera_sid_pair_encode(&owner, NULL, rec, sizeof(rec));
    assert(len == 2 + (int) owner.len);
    assert(rec[len - 1] == 0);
    assert(chimera_sid_pair_decode(rec, len, &o2, &g2) == 0);
    assert(memcmp(&o2, &owner, sizeof(owner)) == 0);
    assert(memcmp(&g2, &none, sizeof(none)) == 0);

    /* Owner only, record cut right after the owner (no group length byte). */
    assert(chimera_sid_pair_decode(rec, len - 1, &o2, &g2) == 0);
    assert(memcmp(&o2, &owner, sizeof(owner)) == 0);
    assert(memcmp(&g2, &none, sizeof(none)) == 0);

    /* Group only: a zero owner length, then the group. */
    len = chimera_sid_pair_encode(&none, &group, rec, sizeof(rec));
    assert(len == 2 + (int) group.len);
    assert(rec[0] == 0);
    assert(chimera_sid_pair_decode(rec, len, &o2, &g2) == 0);
    assert(memcmp(&o2, &none, sizeof(none)) == 0);
    assert(memcmp(&g2, &group, sizeof(group)) == 0);

    /* Neither present: nothing to store.  Too small a buffer: refused. */
    assert(chimera_sid_pair_encode(NULL, NULL, rec, sizeof(rec)) == 0);
    assert(chimera_sid_pair_encode(&none, &none, rec, sizeof(rec)) == 0);
    assert(chimera_sid_pair_encode(&owner, &group, rec, 1) == -1);
    assert(chimera_sid_pair_encode(&owner, &group, rec,
                                   1 + owner.len + group.len) == -1);

    /* Absent record: both outputs absent, not an error. */
    memset(&o2, 0xa5, sizeof(o2));
    memset(&g2, 0xa5, sizeof(g2));
    assert(chimera_sid_pair_decode(NULL, 0, &o2, &g2) == 0);
    assert(memcmp(&o2, &none, sizeof(none)) == 0);
    assert(memcmp(&g2, &none, sizeof(none)) == 0);
    rec[0] = 0;
    assert(chimera_sid_pair_decode(rec, 1, &o2, &g2) == 0);
    assert(memcmp(&o2, &none, sizeof(none)) == 0);

    /* Corrupt owner, valid group: the group is still reachable and comes
    * back; the owner is absent, and the record is reported malformed. */
    len    = chimera_sid_pair_encode(&owner, &group, rec, sizeof(rec));
    rec[2] = CHIMERA_SID_MAX_SUB_AUTHS + 1; /* owner's sub-authority count */
    memset(&o2, 0xa5, sizeof(o2));
    assert(chimera_sid_pair_decode(rec, len, &o2, &g2) == -1);
    assert(memcmp(&o2, &none, sizeof(none)) == 0);
    assert(memcmp(&g2, &group, sizeof(group)) == 0);

    /* Owner length past the end of the record: nothing is reachable. */
    len    = chimera_sid_pair_encode(&owner, &group, rec, sizeof(rec));
    rec[0] = (uint8_t) (len + 5);
    memset(&g2, 0xa5, sizeof(g2));
    assert(chimera_sid_pair_decode(rec, len, &o2, &g2) == -1);
    assert(memcmp(&o2, &none, sizeof(none)) == 0);
    assert(memcmp(&g2, &none, sizeof(none)) == 0);

    /* Trailing bytes after the group are ignored. */
    len = chimera_sid_pair_encode(&owner, &group, big, sizeof(big));
    memset(big + len, 0xee, sizeof(big) - len);
    assert(chimera_sid_pair_decode(big, (int) sizeof(big), &o2, &g2) == 0);
    assert(memcmp(&o2, &owner, sizeof(owner)) == 0);
    assert(memcmp(&g2, &group, sizeof(group)) == 0);

    TEST_PASS("owner/group pair record round-trips and stays defined");
} /* test_pair_codec */

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
    test_len_invariant();
    test_pair_codec();

    fprintf(stderr, "All SID codec tests passed\n");
    return 0;
} /* main */
