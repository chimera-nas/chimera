// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <string.h>
#include <grp.h>
#undef NDEBUG
#include <assert.h>

#include "vfs/vfs_idmap.h"

#define TEST_PASS(name) fprintf(stderr, "  PASS: %s\n", name)

/* Special-who and numeric who strings round-trip without a domain. */
static void
test_who_roundtrip(void)
{
    char                     buf[CHIMERA_IDMAP_WHO_MAX];
    struct chimera_principal p, q;
    int                      len;

    /* OWNER@ */
    p   = chimera_idmap_special_principal(CHIMERA_WHO_OWNER);
    len = chimera_idmap_principal_to_who(&p, NULL, buf, sizeof(buf));
    assert(len == 6 && strcmp(buf, "OWNER@") == 0);
    assert(chimera_idmap_who_to_principal(buf, len, 0, NULL, &q) == 0);
    assert(q.type == CHIMERA_PRINCIPAL_SPECIAL && q.special == CHIMERA_WHO_OWNER);

    /* EVERYONE@ */
    p   = chimera_idmap_special_principal(CHIMERA_WHO_EVERYONE);
    len = chimera_idmap_principal_to_who(&p, NULL, buf, sizeof(buf));
    assert(strcmp(buf, "EVERYONE@") == 0);
    assert(chimera_idmap_who_to_principal(buf, len, 0, NULL, &q) == 0);
    assert(q.special == CHIMERA_WHO_EVERYONE);

    /* numeric user with no domain -> numeric string */
    p   = chimera_idmap_uid_principal(1000);
    len = chimera_idmap_principal_to_who(&p, NULL, buf, sizeof(buf));
    assert(strcmp(buf, "1000") == 0);
    assert(chimera_idmap_who_to_principal(buf, len, 0, NULL, &q) == 0);
    assert(q.type == CHIMERA_PRINCIPAL_USER && q.id == 1000);

    /* numeric group, is_group decode */
    p   = chimera_idmap_gid_principal(2000);
    len = chimera_idmap_principal_to_who(&p, NULL, buf, sizeof(buf));
    assert(strcmp(buf, "2000") == 0);
    assert(chimera_idmap_who_to_principal(buf, len, 1, NULL, &q) == 0);
    assert(q.type == CHIMERA_PRINCIPAL_GROUP && q.id == 2000);

    TEST_PASS("NFSv4 who strings round-trip (special + numeric)");
} /* test_who_roundtrip */

/* SID encoding for special-whos and numeric ids. */
static void
test_sid_roundtrip(void)
{
    char                     buf[CHIMERA_IDMAP_SID_MAX];
    struct chimera_principal p, q;

    p = chimera_idmap_special_principal(CHIMERA_WHO_EVERYONE);
    assert(chimera_idmap_principal_to_sid(&p, buf, sizeof(buf)) > 0);
    assert(strcmp(buf, "S-1-1-0") == 0);
    assert(chimera_idmap_sid_to_principal(buf, &q) == 0);
    assert(q.type == CHIMERA_PRINCIPAL_SPECIAL && q.special == CHIMERA_WHO_EVERYONE);

    /* CREATOR_OWNER (the inheritance template) owns S-1-3-0; OWNER@ has no
     * standalone SID -- the SD emitter substitutes the concrete owner SID. */
    p = chimera_idmap_special_principal(CHIMERA_WHO_CREATOR_OWNER);
    assert(chimera_idmap_principal_to_sid(&p, buf, sizeof(buf)) > 0);
    assert(strcmp(buf, "S-1-3-0") == 0);
    assert(chimera_idmap_sid_to_principal(buf, &q) == 0);
    assert(q.special == CHIMERA_WHO_CREATOR_OWNER);

    p = chimera_idmap_special_principal(CHIMERA_WHO_OWNER);
    assert(chimera_idmap_principal_to_sid(&p, buf, sizeof(buf)) < 0);

    /* uid/gid SIDs use the modefromsid scheme (S-1-5-88-1/2-<id>), matching the
     * owner/group SIDs the SMB security-descriptor emitter writes. */
    p = chimera_idmap_uid_principal(1234);
    chimera_idmap_principal_to_sid(&p, buf, sizeof(buf));
    assert(strcmp(buf, "S-1-5-88-1-1234") == 0);
    assert(chimera_idmap_sid_to_principal(buf, &q) == 0);
    assert(q.type == CHIMERA_PRINCIPAL_USER && q.id == 1234);

    p = chimera_idmap_gid_principal(5678);
    chimera_idmap_principal_to_sid(&p, buf, sizeof(buf));
    assert(strcmp(buf, "S-1-5-88-2-5678") == 0);
    assert(chimera_idmap_sid_to_principal(buf, &q) == 0);
    assert(q.type == CHIMERA_PRINCIPAL_GROUP && q.id == 5678);

    /* interop: the algorithmic S-1-22 user/group SIDs still decode. */
    assert(chimera_idmap_sid_to_principal("S-1-22-1-100", &q) == 0);
    assert(q.type == CHIMERA_PRINCIPAL_USER && q.id == 100);
    assert(chimera_idmap_sid_to_principal("S-1-22-2-200", &q) == 0);
    assert(q.type == CHIMERA_PRINCIPAL_GROUP && q.id == 200);

    /* unrecognised SID is rejected */
    assert(chimera_idmap_sid_to_principal("S-1-5-21-1-2-3-4", &q) == -1);

    TEST_PASS("Windows SID strings round-trip (special + numeric + interop)");
} /* test_sid_roundtrip */

/* Buffers that are too small are rejected, not overrun. */
static void
test_buffer_limits(void)
{
    char                     buf[4];
    struct chimera_principal p = chimera_idmap_special_principal(CHIMERA_WHO_EVERYONE);

    assert(chimera_idmap_principal_to_who(&p, NULL, buf, sizeof(buf)) == -1);
    assert(chimera_idmap_principal_to_sid(&p, buf, sizeof(buf)) == -1);

    TEST_PASS("too-small buffers are rejected");
} /* test_buffer_limits */

/* Every byte of the struct must be zero: the reserved byte, and the whole
 * native SID including the bytes past its length. */
static void
assert_principal_defined(const struct chimera_principal *q)
{
    const uint8_t *b = (const uint8_t *) &q->sid;

    assert(q->reserved == 0);
    assert(!chimera_sid_present(&q->sid));
    for (unsigned i = 0; i < sizeof(q->sid); i++) {
        assert(b[i] == 0);
    }
} /* assert_principal_defined */

/* The decoder fully defines a principal on every path.  The NFSv4 SETATTR
 * decoder hands it space from a bump allocator that is never cleared, so a
 * principal that only had type/special/id assigned carried whatever the
 * previous RPC left in the native-SID tail -- and an ACE is stored and
 * compared by value. */
static void
test_principal_fully_defined(void)
{
    struct chimera_principal q, want;
    struct group            *gr = getgrgid(0);
    char                     gname[CHIMERA_IDMAP_WHO_MAX];

    /* Special who. */
    memset(&q, 0xa5, sizeof(q));
    assert(chimera_idmap_who_to_principal("OWNER@", 6, 0, NULL, &q) == 0);
    assert_principal_defined(&q);
    want = chimera_idmap_special_principal(CHIMERA_WHO_OWNER);
    assert(memcmp(&q, &want, sizeof(q)) == 0);

    /* Well-known SID string naming a special. */
    memset(&q, 0xa5, sizeof(q));
    assert(chimera_idmap_who_to_principal("S-1-3-0", 7, 0, NULL, &q) == 0);
    assert_principal_defined(&q);
    want = chimera_idmap_special_principal(CHIMERA_WHO_CREATOR_OWNER);
    assert(memcmp(&q, &want, sizeof(q)) == 0);

    /* Numeric user and group. */
    memset(&q, 0xa5, sizeof(q));
    assert(chimera_idmap_who_to_principal("1000", 4, 0, NULL, &q) == 0);
    assert_principal_defined(&q);
    want = chimera_idmap_uid_principal(1000);
    assert(memcmp(&q, &want, sizeof(q)) == 0);

    memset(&q, 0xa5, sizeof(q));
    assert(chimera_idmap_who_to_principal("2000", 4, 1, NULL, &q) == 0);
    assert_principal_defined(&q);
    want = chimera_idmap_gid_principal(2000);
    assert(memcmp(&q, &want, sizeof(q)) == 0);

    /* name@domain through nsswitch: root is uid 0 everywhere; the group
     * named for gid 0 is looked up so the test does not assume its name. */
    memset(&q, 0xa5, sizeof(q));
    assert(chimera_idmap_who_to_principal("root@example.com", 16, 0, NULL,
                                          &q) == 0);
    assert_principal_defined(&q);
    want = chimera_idmap_uid_principal(0);
    assert(memcmp(&q, &want, sizeof(q)) == 0);

    if (gr) {
        int n = snprintf(gname, sizeof(gname), "%s@example.com", gr->gr_name);

        memset(&q, 0xa5, sizeof(q));
        assert(chimera_idmap_who_to_principal(gname, n, 1, NULL, &q) == 0);
        assert_principal_defined(&q);
        want = chimera_idmap_gid_principal(0);
        assert(memcmp(&q, &want, sizeof(q)) == 0);
    }

    /* Failure leaves nothing of the caller's storage either. */
    memset(&q, 0xa5, sizeof(q));
    assert(chimera_idmap_who_to_principal("nosuchuser_chimera@dom", 22, 0,
                                          NULL, &q) == -1);
    assert_principal_defined(&q);
    assert(q.type == 0 && q.special == 0 && q.id == 0);

    TEST_PASS("decoded principals are fully defined on every path");
} /* test_principal_fully_defined */

/* An opaque native-SID principal has no NFSv4 name and no algorithmic SID:
 * both encoders refuse it, so the NFSv4 ACL emitter drops the ACE instead of
 * naming uid 0 -- root -- and nothing invents an S-1-5-88-2-0 for it.  An
 * unknown type (a corrupt on-disk blob) is nameless the same way. */
static void
test_sid_principal_unnamed(void)
{
    struct chimera_principal p;
    char                     buf[CHIMERA_IDMAP_WHO_MAX];

    memset(&p, 0, sizeof(p));
    p.type = CHIMERA_PRINCIPAL_SID;
    assert(chimera_sid_from_str(&p.sid, "S-1-5-21-1-2-3-4") == 0);

    assert(chimera_idmap_principal_to_who(&p, NULL, buf, sizeof(buf)) == -1);
    assert(chimera_idmap_principal_to_who(&p, "example.com", buf,
                                          sizeof(buf)) == -1);
    assert(chimera_idmap_principal_to_sid(&p, buf, sizeof(buf)) == -1);

    p.type = 200;
    assert(chimera_idmap_principal_to_who(&p, NULL, buf, sizeof(buf)) == -1);
    assert(chimera_idmap_principal_to_sid(&p, buf, sizeof(buf)) == -1);

    TEST_PASS("an opaque SID principal is nameless to NFSv4 and the idmap");
} /* test_sid_principal_unnamed */

int
main(
    int    argc,
    char **argv)
{
    test_who_roundtrip();
    test_sid_roundtrip();
    test_buffer_limits();
    test_principal_fully_defined();
    test_sid_principal_unnamed();

    fprintf(stderr, "All idmap tests passed\n");
    return 0;
} /* main */
