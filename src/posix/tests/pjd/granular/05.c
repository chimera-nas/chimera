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

/* Ported from pjdfstest tests/granular/05.t:
 * "NFSv4 granular permissions checking - DELETE and DELETE_CHILD with
 * directories".
 *
 * The DELETE_CHILD removal rules of granular/03.t applied to subdirectories
 * removed with rmdir: removal is governed by the *parent* directory's
 * WRITE_DATA/DELETE_CHILD, not by anything inside the subdirectory.
 *
 * Same Chimera scope/deviation notes as granular/03.c apply: the DELETE_CHILD::
 * deny veto reports EACCES (not the upstream EPERM), and the per-object
 * DELETE-grant override is ACL-flavored-only and omitted for this POSIX
 * client. */

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

    /* rmdir denied on a directory the caller cannot write. */
    EXPECT(0, pjd_mkdir(n0, 0755));
    pjd_set_user(65534, 65534);
    EXPECT(EACCES, pjd_rmdir(n0));
    pjd_set_root();

    /* WRITE_DATA on the parent permits rmdir. */
    {
        struct chimera_ace a = pjd_ace_user(CHIMERA_ACE_ALLOWED, 65534,
                                            CHIMERA_ACE_WRITE_DATA);
        EXPECT(0, pjd_prependacl(".", &a));
    }
    pjd_set_user(65534, 65534);
    EXPECT(0, pjd_rmdir(n0));
    pjd_set_root();

    /* DELETE_CHILD::deny on the parent vetoes rmdir even with WRITE_DATA. */
    EXPECT(0, pjd_mkdir(n0, 0755));
    {
        struct chimera_ace a = pjd_ace_user(CHIMERA_ACE_DENIED, 65534,
                                            CHIMERA_ACE_DELETE_CHILD);
        EXPECT(0, pjd_prependacl(".", &a));
    }
    pjd_set_user(65534, 65534);
    EXPECT(EACCES, pjd_rmdir(n0));
    pjd_set_root();
    EXPECT(0, pjd_rmdir(n0));

    /* DELETE_CHILD::allow on an unwritable directory permits rmdir. */
    EXPECT(0, pjd_mkdir(n0, 0755));
    {
        struct chimera_ace dc = pjd_ace_user(CHIMERA_ACE_ALLOWED, 65534,
                                             CHIMERA_ACE_DELETE_CHILD);
        struct chimera_ace wd = pjd_ace_user(CHIMERA_ACE_DENIED, 65534,
                                             CHIMERA_ACE_WRITE_DATA);
        EXPECT(0, pjd_prependacl(".", &wd));
        EXPECT(0, pjd_prependacl(".", &dc));
    }
    pjd_set_user(65534, 65534);
    EXPECT(0, pjd_rmdir(n0));
    pjd_set_root();

    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n2));

    return pjd_end();
} /* main */
