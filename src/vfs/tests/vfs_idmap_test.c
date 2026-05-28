// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <string.h>
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

int
main(
    int    argc,
    char **argv)
{
    test_who_roundtrip();
    test_sid_roundtrip();
    test_buffer_limits();

    fprintf(stderr, "All idmap tests passed\n");
    return 0;
} /* main */
