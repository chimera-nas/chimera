// SPDX-FileCopyrightText: 2006-2012 Pawel Jakub Dawidek <pawel@dawidek.net>
// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: BSD-2-Clause
//
// Derived from the pjdfstest POSIX filesystem test suite
// (https://github.com/pjd/pjdfstest) by Pawel Jakub Dawidek.  These are C
// reimplementations of the upstream shell test cases, run against the Chimera
// POSIX client, and are distributed under pjdfstest's original 2-clause BSD
// license.

/* Ported from pjdfstest tests/granular/03.t:
 * "NFSv4 granular permissions checking - DELETE and DELETE_CHILD".
 *
 * NFSv4 removal: a name may be removed if the parent directory grants the caller
 * WRITE_DATA/DELETE_CHILD.  A DELETE_CHILD::deny on the parent vetoes removal of
 * a child the caller could otherwise delete; a DELETE_CHILD::allow on an
 * otherwise-unwritable directory permits it.  This exercises
 * chimera_vfs_delete_allowed / the VFS-core delete gate via unlink.
 *
 * Scope / Chimera deviations from the FreeBSD:ZFS upstream:
 *   - Upstream returns EPERM when removal is vetoed by an explicit
 *     DELETE_CHILD::deny ACE.  Chimera's delete gate reports EACCES uniformly
 *     for "removal not authorized" (the enforcement -- removal is blocked -- is
 *     identical; only the errno class differs), so the deny case below asserts
 *     EACCES.
 *   - The per-object DELETE-grant override (a DELETE::allow ACE on the *child*
 *     letting a caller remove it regardless of the parent's rights) is an
 *     NFSv4/NT concept that Chimera applies only to ACL-flavored (SMB / AUTH_
 *     ATTR) callers; a POSIX (AUTH_UNIX) client -- which this harness is -- gets
 *     directory-based POSIX deletion.  Those subtests are therefore omitted. */

#include "../../pjd_common.h"

int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);

    char *n0 = pjd_namegen();
    char *n2 = pjd_namegen();

    EXPECT(0, pjd_mkdir(n2, 0755));
    pjd_cd(n2);

    /* Unlink denied on a directory the caller cannot write. */
    EXPECT(0, pjd_create(n0, 0644));
    pjd_set_user(65534, 65534);
    EXPECT(EACCES, pjd_unlink(n0));
    pjd_set_root();

    /* WRITE_DATA on the parent permits unlink. */
    {
        struct chimera_ace a = pjd_ace_user(CHIMERA_ACE_ALLOWED, 65534,
                                            CHIMERA_ACE_WRITE_DATA);
        EXPECT(0, pjd_prependacl(".", &a));
    }
    pjd_set_user(65534, 65534);
    EXPECT(0, pjd_unlink(n0));
    pjd_set_root();

    /* DELETE_CHILD::deny on the parent vetoes unlink even with WRITE_DATA. */
    EXPECT(0, pjd_create(n0, 0644));
    {
        struct chimera_ace a = pjd_ace_user(CHIMERA_ACE_DENIED, 65534,
                                            CHIMERA_ACE_DELETE_CHILD);
        EXPECT(0, pjd_prependacl(".", &a));
    }
    pjd_set_user(65534, 65534);
    EXPECT(EACCES, pjd_unlink(n0));
    pjd_set_root();
    EXPECT(0, pjd_unlink(n0));

    /* DELETE_CHILD::allow on an otherwise-unwritable directory permits unlink. */
    EXPECT(0, pjd_create(n0, 0644));
    {
        /* Reset the parent ACL: deny WRITE_DATA but allow DELETE_CHILD. */
        struct chimera_ace dc = pjd_ace_user(CHIMERA_ACE_ALLOWED, 65534,
                                             CHIMERA_ACE_DELETE_CHILD);
        struct chimera_ace wd = pjd_ace_user(CHIMERA_ACE_DENIED, 65534,
                                             CHIMERA_ACE_WRITE_DATA);
        EXPECT(0, pjd_prependacl(".", &wd));
        EXPECT(0, pjd_prependacl(".", &dc));
    }
    pjd_set_user(65534, 65534);
    EXPECT(0, pjd_unlink(n0));
    pjd_set_root();

    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n2));

    return pjd_end();
} /* main */
