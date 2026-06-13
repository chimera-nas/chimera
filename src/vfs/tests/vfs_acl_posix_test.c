// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#undef NDEBUG
#include <assert.h>

#include "vfs/vfs_acl.h"
#include "vfs/vfs_acl_posix.h"

#define TEST_PASS(name) fprintf(stderr, "  PASS: %s\n", name)

#define ACL_BUF(name, n)                                              \
        uint8_t name ## _storage[sizeof(struct chimera_acl) +         \
                                 (n) * sizeof(struct chimera_ace)];   \
        struct chimera_acl *name = (struct chimera_acl *) name ## _storage

#define MAXE 64

/*
 * The keystone property: mode -> canonical -> POSIX.1e -> canonical must project
 * back to the same 9-bit mode for every one of the 512 permission patterns.
 * This exercises the deny-aware special-who projection in both directions and
 * the mask folding, and proves a SETACL of a GETACL'd mode-only object is
 * idempotent.
 */
static void
test_mode_roundtrip_through_posix(void)
{
    for (uint32_t mode = 0; mode <= 0777; mode++) {
        ACL_BUF(c1, MAXE);
        ACL_BUF(c2, MAXE);
        struct chimera_posix_acl_entry pa[MAXE], pd[MAXE];
        uint32_t                       na = 0, nd = 0;
        int                            n;

        n = chimera_acl_from_mode(mode, c1, MAXE);
        assert(n >= 3);

        n = chimera_acl_to_posix(c1, 1000, 2000, 0, pa, MAXE, &na,
                                 pd, MAXE, &nd);
        assert(n == 0);
        /* A mode-only ACL has no named entries, so no mask is emitted:
         * USER_OBJ, GROUP_OBJ, OTHER -- exactly three. */
        assert(na == 3);
        assert(nd == 0);

        n = chimera_acl_from_posix(pa, na, pd, nd, c2, MAXE);
        assert(n >= 3);

        assert(chimera_acl_to_mode(c2) == mode);
    }
    TEST_PASS("mode -> canonical -> POSIX -> canonical -> mode for all 0..0777");
} /* test_mode_roundtrip_through_posix */

/* Locate a POSIX entry of a given tag (ignoring the DEFAULT wire flag). */
static const struct chimera_posix_acl_entry *
find_entry(
    const struct chimera_posix_acl_entry *e,
    uint32_t                              n,
    uint16_t                              tag,
    uint32_t                              id,
    int                                   match_id)
{
    for (uint32_t i = 0; i < n; i++) {
        if ((e[i].e_tag & ~CHIMERA_NFS_ACL_DEFAULT) == tag &&
            (!match_id || e[i].e_id == id)) {
            return &e[i];
        }
    }
    return NULL;
} /* find_entry */

/*
 * A named user with explicit perms must survive canonical -> POSIX, appear in
 * the mask, and come back as a USER-principal ALLOW ACE.
 */
static void
test_named_user_preserved(void)
{
    ACL_BUF(c1, MAXE);
    ACL_BUF(c2, MAXE);
    struct chimera_posix_acl_entry        pa[MAXE], pd[MAXE];
    uint32_t                              na = 0, nd = 0;
    const struct chimera_posix_acl_entry *e;
    int                                   n, found;

    /* 0640 base + a named user 1005 granted r-x. */
    n = chimera_acl_from_mode(0640, c1, MAXE);
    assert(n >= 3);
    c1->aces[n].type        = CHIMERA_ACE_ALLOWED;
    c1->aces[n].flags       = 0;
    c1->aces[n].access_mask = CHIMERA_ACE_PERM_R | CHIMERA_ACE_PERM_X;
    c1->aces[n].who.type    = CHIMERA_PRINCIPAL_USER;
    c1->aces[n].who.special = 0;
    c1->aces[n].who.id      = 1005;
    n++;
    c1->num_aces = n;

    n = chimera_acl_to_posix(c1, 1000, 2000, 0, pa, MAXE, &na, pd, MAXE, &nd);
    assert(n == 0);

    e = find_entry(pa, na, CHIMERA_POSIX_ACL_USER, 1005, 1);
    assert(e != NULL);
    assert(e->e_perm == (CHIMERA_POSIX_ACL_READ | CHIMERA_POSIX_ACL_EXECUTE));

    /* The mask must include the named user's permissions (r-x). */
    e = find_entry(pa, na, CHIMERA_POSIX_ACL_MASK, 0, 0);
    assert(e != NULL);
    assert((e->e_perm & (CHIMERA_POSIX_ACL_READ | CHIMERA_POSIX_ACL_EXECUTE)) ==
           (CHIMERA_POSIX_ACL_READ | CHIMERA_POSIX_ACL_EXECUTE));

    /* Round-trips back to a USER-principal ALLOW ACE with READ_DATA|EXECUTE. */
    n = chimera_acl_from_posix(pa, na, pd, nd, c2, MAXE);
    assert(n >= 3);
    found = 0;
    for (int i = 0; i < c2->num_aces; i++) {
        if (c2->aces[i].who.type == CHIMERA_PRINCIPAL_USER &&
            c2->aces[i].who.id == 1005) {
            assert(c2->aces[i].type == CHIMERA_ACE_ALLOWED);
            assert(c2->aces[i].access_mask & CHIMERA_ACE_READ_DATA);
            assert(c2->aces[i].access_mask & CHIMERA_ACE_EXECUTE);
            assert(!(c2->aces[i].access_mask & CHIMERA_ACE_WRITE_DATA));
            found = 1;
        }
    }
    assert(found);
    TEST_PASS("named user preserved through canonical<->POSIX");
} /* test_named_user_preserved */

/*
 * The POSIX mask must clamp a named group's effective permissions when the
 * group is fed back into the canonical model.
 */
static void
test_mask_clamps_group(void)
{
    ACL_BUF(c, MAXE);
    struct chimera_posix_acl_entry pa[8];
    uint32_t                       n;
    int                            rc, found;

    /* user::rwx, group::rwx (named 2001), mask::r--, other::--- :
    * the group's effective perms are clamped to r by the mask. */
    pa[0] = (struct chimera_posix_acl_entry) { CHIMERA_POSIX_ACL_USER_OBJ, 0x7, 1000 };
    pa[1] = (struct chimera_posix_acl_entry) { CHIMERA_POSIX_ACL_GROUP_OBJ, 0x7, 2000 };
    pa[2] = (struct chimera_posix_acl_entry) { CHIMERA_POSIX_ACL_GROUP, 0x7, 2001 };
    pa[3] = (struct chimera_posix_acl_entry) { CHIMERA_POSIX_ACL_MASK, CHIMERA_POSIX_ACL_READ, 0 };
    pa[4] = (struct chimera_posix_acl_entry) { CHIMERA_POSIX_ACL_OTHER, 0, 0 };
    n     = 5;

    rc = chimera_acl_from_posix(pa, n, NULL, 0, c, MAXE);
    assert(rc >= 3);

    found = 0;
    for (int i = 0; i < c->num_aces; i++) {
        if (c->aces[i].who.type == CHIMERA_PRINCIPAL_GROUP &&
            c->aces[i].who.id == 2001) {
            /* masked to read only: no write/execute granted. */
            assert(c->aces[i].access_mask & CHIMERA_ACE_READ_DATA);
            assert(!(c->aces[i].access_mask & CHIMERA_ACE_WRITE_DATA));
            assert(!(c->aces[i].access_mask & CHIMERA_ACE_EXECUTE));
            found = 1;
        }
    }
    assert(found);
    TEST_PASS("POSIX mask clamps named-group effective perms");
} /* test_mask_clamps_group */

/*
 * A POSIX default ACL must map to inheritable canonical templates and project
 * back to an equivalent default ACL.
 */
static void
test_default_acl_roundtrip(void)
{
    ACL_BUF(c, MAXE);
    struct chimera_posix_acl_entry        pa[4], pd_in[4], pa_out[MAXE], pd_out[MAXE];
    uint32_t                              na = 0, nd = 0;
    const struct chimera_posix_acl_entry *e;
    int                                   rc, inheritable = 0;

    /* Minimal access ACL + a default ACL granting rwx/r-x/r--. */
    pa[0] = (struct chimera_posix_acl_entry) { CHIMERA_POSIX_ACL_USER_OBJ, 0x7, 1000 };
    pa[1] = (struct chimera_posix_acl_entry) { CHIMERA_POSIX_ACL_GROUP_OBJ, 0x5, 2000 };
    pa[2] = (struct chimera_posix_acl_entry) { CHIMERA_POSIX_ACL_OTHER, 0x4, 0 };

    pd_in[0] = (struct chimera_posix_acl_entry) { CHIMERA_POSIX_ACL_USER_OBJ, 0x7, 1000 };
    pd_in[1] = (struct chimera_posix_acl_entry) { CHIMERA_POSIX_ACL_GROUP_OBJ, 0x5, 2000 };
    pd_in[2] = (struct chimera_posix_acl_entry) { CHIMERA_POSIX_ACL_OTHER, 0x4, 0 };

    rc = chimera_acl_from_posix(pa, 3, pd_in, 3, c, MAXE);
    assert(rc >= 3);

    /* The canonical ACL must carry inheritable INHERIT_ONLY templates. */
    for (int i = 0; i < c->num_aces; i++) {
        if (c->aces[i].flags & CHIMERA_ACE_FLAG_INHERIT_ONLY) {
            assert(c->aces[i].flags & CHIMERA_ACE_FLAG_FILE_INHERIT);
            assert(c->aces[i].flags & CHIMERA_ACE_FLAG_DIR_INHERIT);
            inheritable++;
        }
    }
    assert(inheritable >= 3);

    /* Project back as a directory: a default ACL must reappear. */
    rc = chimera_acl_to_posix(c, 1000, 2000, 1, pa_out, MAXE, &na,
                              pd_out, MAXE, &nd);
    assert(rc == 0);
    assert(nd >= 3);

    e = find_entry(pd_out, nd, CHIMERA_POSIX_ACL_USER_OBJ, 0, 0);
    assert(e && e->e_perm == 0x7);
    e = find_entry(pd_out, nd, CHIMERA_POSIX_ACL_GROUP_OBJ, 0, 0);
    assert(e && e->e_perm == 0x5);
    e = find_entry(pd_out, nd, CHIMERA_POSIX_ACL_OTHER, 0, 0);
    assert(e && e->e_perm == 0x4);

    /* A non-directory projection must emit no default entries. */
    na = nd = 0;
    rc = chimera_acl_to_posix(c, 1000, 2000, 0, pa_out, MAXE, &na,
                              pd_out, MAXE, &nd);
    assert(rc == 0);
    assert(nd == 0);

    TEST_PASS("POSIX default ACL <-> inheritable canonical templates");
} /* test_default_acl_roundtrip */

/*
 * USER_OBJ/GROUP_OBJ must carry the owner uid/gid in e_id (as Linux/Solaris
 * encode them); MASK and OTHER carry 0.
 */
static void
test_owner_ids_in_entries(void)
{
    ACL_BUF(c, MAXE);
    struct chimera_posix_acl_entry        pa[MAXE], pd[MAXE];
    uint32_t                              na = 0, nd = 0;
    const struct chimera_posix_acl_entry *e;

    chimera_acl_from_mode(0644, c, MAXE);
    chimera_acl_to_posix(c, 4242, 5252, 0, pa, MAXE, &na, pd, MAXE, &nd);

    e = find_entry(pa, na, CHIMERA_POSIX_ACL_USER_OBJ, 4242, 1);
    assert(e != NULL);
    e = find_entry(pa, na, CHIMERA_POSIX_ACL_GROUP_OBJ, 5252, 1);
    assert(e != NULL);
    e = find_entry(pa, na, CHIMERA_POSIX_ACL_OTHER, 0, 1);
    assert(e != NULL);
    TEST_PASS("owner/group ids encoded in USER_OBJ/GROUP_OBJ e_id");
} /* test_owner_ids_in_entries */

int
main(
    int    argc,
    char **argv)
{
    fprintf(stderr, "vfs_acl_posix_test:\n");
    test_mode_roundtrip_through_posix();
    test_named_user_preserved();
    test_mask_clamps_group();
    test_default_acl_roundtrip();
    test_owner_ids_in_entries();
    fprintf(stderr, "All POSIX.1e ACL translation tests passed.\n");
    return 0;
} /* main */
