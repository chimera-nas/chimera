// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#undef NDEBUG
#include <assert.h>

#include "vfs/vfs.h"
#include "vfs/vfs_acl.h"
#include "vfs/vfs_acl_serialize.h"
#include "vfs/vfs_access.h"
#include "vfs/vfs_attrs.h"
#include "vfs/vfs_cred.h"

#define TEST_PASS(name) fprintf(stderr, "  PASS: %s\n", name)

#define ACL_BUF(name, n)                                              \
        uint8_t name ## _storage[sizeof(struct chimera_acl) +         \
                                 (n) * sizeof(struct chimera_ace)];   \
        struct chimera_acl *name = (struct chimera_acl *) name ## _storage

static struct chimera_vfs_cred
mkcred(
    uint32_t uid,
    uint32_t gid)
{
    struct chimera_vfs_cred c;

    chimera_vfs_cred_init_unix(&c, uid, gid, 0, NULL);
    return c;
} /* mkcred */

/*
 * mode -> ACL -> mode must round-trip exactly for all 512 permission patterns.
 */
static void
test_mode_roundtrip(void)
{
    for (uint32_t mode = 0; mode <= 0777; mode++) {
        ACL_BUF(acl, 8);
        int      n = chimera_acl_from_mode(mode, acl, 8);
        uint32_t out;

        assert(n >= 3);
        out = chimera_acl_to_mode(acl);
        assert(out == mode);
    }
    TEST_PASS("mode<->ACL round-trips for all 0..0777");
} /* test_mode_roundtrip */

/*
 * Cumulative-grant trap: mode 0604 (owner rw, group ---, other r).  The group
 * member must NOT inherit the EVERYONE@ read grant.
 */
static void
test_cumulative_deny(void)
{
    ACL_BUF(acl, 8);
    struct chimera_vfs_cred owner  = mkcred(1000, 2000);
    struct chimera_vfs_cred grpmbr = mkcred(1001, 2000);
    struct chimera_vfs_cred other  = mkcred(1002, 2002);
    uint32_t                g;

    chimera_acl_from_mode(0604, acl, 8);

    /* owner can read+write */
    g = chimera_acl_access_check(acl, 0604, 1000, 2000, &owner,
                                 CHIMERA_ACE_READ_DATA | CHIMERA_ACE_WRITE_DATA, 0);
    assert(g == (CHIMERA_ACE_READ_DATA | CHIMERA_ACE_WRITE_DATA));

    /* group member: owning group has no bits -> read must be denied even though
     * EVERYONE@ grants read */
    g = chimera_acl_access_check(acl, 0604, 1000, 2000, &grpmbr,
                                 CHIMERA_ACE_READ_DATA, 0);
    assert(g == 0);

    /* an unrelated user falls through to EVERYONE@ and may read */
    g = chimera_acl_access_check(acl, 0604, 1000, 2000, &other,
                                 CHIMERA_ACE_READ_DATA, 0);
    assert(g == CHIMERA_ACE_READ_DATA);

    TEST_PASS("cumulative-grant DENY prevents group over-grant (0604)");
} /* test_cumulative_deny */

/* NULL ACL falls back to mode-bit evaluation. */
static void
test_null_acl_modecheck(void)
{
    struct chimera_vfs_cred owner = mkcred(1000, 2000);
    struct chimera_vfs_cred other = mkcred(1002, 2002);
    uint32_t                g;

    g = chimera_acl_access_check(NULL, 0600, 1000, 2000, &owner,
                                 CHIMERA_ACE_WRITE_DATA, 0);
    assert(g == CHIMERA_ACE_WRITE_DATA);

    g = chimera_acl_access_check(NULL, 0600, 1000, 2000, &other,
                                 CHIMERA_ACE_READ_DATA, 0);
    assert(g == 0);

    TEST_PASS("NULL ACL falls back to mode bits");
} /* test_null_acl_modecheck */

/* Root bypasses ACL evaluation. */
static void
test_root_bypass(void)
{
    ACL_BUF(acl, 8);
    struct chimera_vfs_cred root = mkcred(0, 0);
    uint32_t                req  = CHIMERA_ACE_READ_DATA | CHIMERA_ACE_WRITE_DATA;
    uint32_t                g;

    chimera_acl_from_mode(0000, acl, 8);
    g = chimera_acl_access_check(acl, 0000, 1000, 2000, &root, req, 0);
    assert(g == req);

    TEST_PASS("root bypasses ACL");
} /* test_root_bypass */

/* An explicit DENY ACE before an ALLOW wins for the overlapping bits. */
static void
test_explicit_deny_precedence(void)
{
    ACL_BUF(acl, 4);
    struct chimera_vfs_cred user = mkcred(1500, 3000);
    uint32_t                g;

    acl->ctrl_flags = 0;
    acl->num_aces   = 2;

    acl->aces[0].type        = CHIMERA_ACE_DENIED;
    acl->aces[0].flags       = 0;
    acl->aces[0].access_mask = CHIMERA_ACE_WRITE_DATA;
    acl->aces[0].who.type    = CHIMERA_PRINCIPAL_USER;
    acl->aces[0].who.id      = 1500;

    acl->aces[1].type        = CHIMERA_ACE_ALLOWED;
    acl->aces[1].flags       = 0;
    acl->aces[1].access_mask = CHIMERA_ACE_READ_DATA | CHIMERA_ACE_WRITE_DATA;
    acl->aces[1].who.type    = CHIMERA_PRINCIPAL_USER;
    acl->aces[1].who.id      = 1500;

    g = chimera_acl_access_check(acl, 0, 1, 1, &user,
                                 CHIMERA_ACE_READ_DATA | CHIMERA_ACE_WRITE_DATA, 0);
    assert(g == CHIMERA_ACE_READ_DATA); /* write denied, read allowed */

    TEST_PASS("explicit ordered DENY beats later ALLOW");
} /* test_explicit_deny_precedence */

/* chmod preserves named ACEs and regenerates special-who from the new mode. */
static void
test_chmod_preserves_named(void)
{
    ACL_BUF(in, 8);
    ACL_BUF(out, 16);
    struct chimera_vfs_cred named = mkcred(4000, 5000);
    int                     n;
    uint32_t                g;

    /* Build: named-user 4000 ALLOW write, then mode 0700 ACL. */
    in->ctrl_flags          = 0;
    in->aces[0].type        = CHIMERA_ACE_ALLOWED;
    in->aces[0].flags       = 0;
    in->aces[0].access_mask = CHIMERA_ACE_WRITE_DATA;
    in->aces[0].who.type    = CHIMERA_PRINCIPAL_USER;
    in->aces[0].who.id      = 4000;
    in->num_aces            = 1;

    n = chimera_acl_chmod(in, 0640, out, 16);
    assert(n > 1);

    /* named user still has its explicit write grant */
    g = chimera_acl_access_check(out, 0640, 1, 2, &named,
                                 CHIMERA_ACE_WRITE_DATA, 0);
    assert(g == CHIMERA_ACE_WRITE_DATA);

    /* and the new mode is reflected */
    assert(chimera_acl_to_mode(out) == 0640);

    TEST_PASS("chmod preserves named ACEs, regenerates special-who");
} /* test_chmod_preserves_named */

/* Inheritance: a FILE_INHERIT ACE lands as an effective ACE on a new file. */
static void
test_inherit_file(void)
{
    ACL_BUF(parent, 4);
    ACL_BUF(child, 8);
    struct chimera_vfs_cred user = mkcred(7000, 8000);
    int                     n;
    uint32_t                g;

    /* Auto-inherited parent: the materialised child ACEs are marked INHERITED. */
    parent->ctrl_flags    = CHIMERA_ACL_CTRL_AUTO_INHERITED;
    parent->aces[0].type  = CHIMERA_ACE_ALLOWED;
    parent->aces[0].flags = CHIMERA_ACE_FLAG_FILE_INHERIT |
        CHIMERA_ACE_FLAG_DIR_INHERIT;
    parent->aces[0].access_mask = CHIMERA_ACE_READ_DATA;
    parent->aces[0].who.type    = CHIMERA_PRINCIPAL_USER;
    parent->aces[0].who.id      = 7000;
    parent->num_aces            = 1;

    n = chimera_acl_inherit(parent, 0 /* file */, 0644, child, 8);
    assert(n == 1);
    /* On a file the inheritance flags are stripped; INHERITED is set because the
     * parent DACL is in auto-inherit mode. */
    assert(!(child->aces[0].flags & CHIMERA_ACE_FLAG_INHERIT_MASK));
    assert(child->aces[0].flags & CHIMERA_ACE_FLAG_INHERITED);

    g = chimera_acl_access_check(child, 0, 1, 2, &user,
                                 CHIMERA_ACE_READ_DATA, 0);
    assert(g == CHIMERA_ACE_READ_DATA);

    TEST_PASS("FILE_INHERIT ACE becomes effective on a new file");
} /* test_inherit_file */

/* No inheritable ACEs -> inherit yields nothing (the caller seeds a default). */
static void
test_inherit_fallback(void)
{
    ACL_BUF(parent, 4);
    ACL_BUF(child, 8);
    int n;

    /* parent ACE is CONTAINER_INHERIT only; a new file inherits nothing. */
    parent->ctrl_flags          = 0;
    parent->aces[0].type        = CHIMERA_ACE_ALLOWED;
    parent->aces[0].flags       = CHIMERA_ACE_FLAG_DIR_INHERIT;
    parent->aces[0].access_mask = CHIMERA_ACE_READ_DATA;
    parent->aces[0].who.type    = CHIMERA_PRINCIPAL_SPECIAL;
    parent->aces[0].who.special = CHIMERA_WHO_EVERYONE;
    parent->num_aces            = 1;

    /* chimera_acl_inherit returns 0 when nothing is inheritable; the create
     * path is responsible for seeding a default ACL in that case. */
    n = chimera_acl_inherit(parent, 0 /* file */, 0640, child, 8);
    assert(n == 0);

    TEST_PASS("inherit yields no ACEs when nothing is inheritable");
} /* test_inherit_fallback */

/* Serialize -> deserialize must reproduce the ACL byte-for-byte.  Zero the
 * backing storage first: struct chimera_ace/chimera_principal carry interior
 * padding that field assignments don't write, so a raw memcmp would otherwise
 * compare uninitialised stack bytes (which differ under -O2). */
static void
test_serialize_roundtrip(void)
{
    ACL_BUF(acl, 8);
    ACL_BUF(back, 8);
    uint8_t buf[256];
    int     len, n;

    memset(acl_storage, 0, sizeof(acl_storage));
    memset(back_storage, 0, sizeof(back_storage));

    chimera_acl_from_mode(0751, acl, 8);
    acl->ctrl_flags = CHIMERA_ACL_CTRL_PROTECTED;

    len = chimera_acl_serialize(acl, buf, sizeof(buf));
    assert(len > 0);
    assert((size_t) len == chimera_acl_serialized_size(acl));

    n = chimera_acl_deserialize(buf, len, back, 8);
    assert(n == acl->num_aces);
    assert(back->ctrl_flags == acl->ctrl_flags);
    assert(memcmp(acl->aces, back->aces,
                  acl->num_aces * sizeof(struct chimera_ace)) == 0);

    /* too-small output buffer is rejected */
    assert(chimera_acl_deserialize(buf, len, back, 1) == -1);
    /* truncated input is rejected */
    assert(chimera_acl_deserialize(buf, 3, back, 8) == -1);

    TEST_PASS("serialize/deserialize round-trips, rejects bad input");
} /* test_serialize_roundtrip */

/* The central VFS gate: who must the engine enforce, and does it allow/deny. */
static void
test_gate(void)
{
    struct chimera_vfs_cred  owner = mkcred(1000, 2000);
    struct chimera_vfs_cred  other = mkcred(1002, 2002);
    struct chimera_vfs_cred  root  = mkcred(0, 0);
    struct chimera_vfs_cred  none;
    struct chimera_vfs_attrs attr;

    /* gate_needed: a backend that delegates DAC is never engine-enforced. */
    assert(chimera_vfs_gate_needed(CHIMERA_VFS_CAP_DELEGATES_DAC, &owner) == 0);
    /* nor are exempt credentials, even on an engine-authoritative backend. */
    chimera_vfs_cred_init_unix(&none, 0, 0, 0, NULL);
    none.flavor = CHIMERA_VFS_AUTH_NONE;
    assert(chimera_vfs_gate_needed(0, &none) == 0);
    assert(chimera_vfs_gate_needed(0, &root) == 0);
    /* a non-root identity on a non-delegating backend must be enforced. */
    assert(chimera_vfs_gate_needed(0, &owner) == 1);

    /* gate: an empty requirement is always allowed (no-op mutation). */
    memset(&attr, 0, sizeof(attr));
    attr.va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID;
    attr.va_mode     = 0700;
    attr.va_uid      = 1000;
    attr.va_gid      = 2000;
    assert(chimera_vfs_gate(&attr, &owner, 0) == CHIMERA_VFS_OK);

    /* the owner holds WRITE_ACL implicitly (may rewrite the DACL / chmod) ... */
    assert(chimera_vfs_gate(&attr, &owner, CHIMERA_ACE_WRITE_ACL) == CHIMERA_VFS_OK);
    /* ... but NOT WRITE_OWNER without an explicit grant (no implicit chown). */
    assert(chimera_vfs_gate(&attr, &owner, CHIMERA_ACE_WRITE_OWNER) == CHIMERA_VFS_EACCES);
    /* a non-owner with no ACE gets neither. */
    assert(chimera_vfs_gate(&attr, &other, CHIMERA_ACE_WRITE_ACL) == CHIMERA_VFS_EACCES);

    TEST_PASS("gate: enforcement scoping + owner-implicit WRITE_ACL, not WRITE_OWNER");
} /* test_gate */

int
main(
    int    argc,
    char **argv)
{
    test_mode_roundtrip();
    test_cumulative_deny();
    test_null_acl_modecheck();
    test_root_bypass();
    test_explicit_deny_precedence();
    test_chmod_preserves_named();
    test_inherit_file();
    test_inherit_fallback();
    test_serialize_roundtrip();
    test_gate();

    fprintf(stderr, "All ACL engine tests passed\n");
    return 0;
} /* main */
