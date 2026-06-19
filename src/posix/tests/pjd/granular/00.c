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

/* Ported from pjdfstest tests/granular/00.t:
 * "NFSv4 granular permissions checking - WRITE_DATA vs APPEND_DATA on
 * directories".
 *
 * On NFSv4/Windows directory ACLs, WRITE_DATA == ADD_FILE (create a plain file
 * in the directory) and APPEND_DATA == ADD_SUBDIRECTORY (mkdir in the
 * directory).  Granting one and denying the other lets a user create files but
 * not subdirectories (and vice versa).  This exercises the WRITE_DATA gate on
 * the open(O_CREAT)/link paths and the APPEND_DATA gate on mkdir.
 *
 * A faithful representative subset of the upstream 49 subtests: it covers the
 * allow and deny path of both rights, plus the cross-directory rename and the
 * delete_child interaction. */

#include "../../pjd_common.h"

int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);

    char *n0 = pjd_namegen();
    char *n1 = pjd_namegen();
    char *n3 = pjd_namegen();
    char *n2 = pjd_namegen();

    EXPECT(0, pjd_mkdir(n2, 0755));
    EXPECT(0, pjd_mkdir(n3, 0777));
    pjd_cd(n2);

    /* root can do everything in n2. */
    EXPECT(0, pjd_create(n0, 0644));
    EXPECT(0, pjd_link(n0, n1));
    EXPECT(0, pjd_unlink(n1));
    EXPECT(0, pjd_unlink(n0));
    EXPECT(0, pjd_mkdir(n0, 0755));
    EXPECT(0, pjd_rmdir(n0));

    /* ---- WRITE_DATA allowed, APPEND_DATA denied: files yes, dirs no. ---- */
    {
        struct chimera_ace a = pjd_ace_user(CHIMERA_ACE_DENIED, 65534,
                                            CHIMERA_ACE_APPEND_DATA);
        struct chimera_ace b = pjd_ace_user(CHIMERA_ACE_ALLOWED, 65534,
                                            CHIMERA_ACE_WRITE_DATA);
        EXPECT(0, pjd_prependacl(".", &a));
        EXPECT(0, pjd_prependacl(".", &b));
    }

    pjd_set_user(65534, 65534);

    /* Can create files / hardlinks (WRITE_DATA). */
    EXPECT(0, pjd_create(n0, 0644));
    EXPECT(0, pjd_link(n0, n1));
    EXPECT(0, pjd_unlink(n1));
    EXPECT(0, pjd_unlink(n0));

    /* Cannot create directories (APPEND_DATA denied). */
    EXPECT(EACCES, pjd_mkdir(n0, 0755));
    EXPECT(ENOENT, pjd_rmdir(n0));

    pjd_set_root();
    EXPECT(0, pjd_mkdir(n0, 0755));
    pjd_set_user(65534, 65534);
    EXPECT(0, pjd_rmdir(n0));

    /* Can move a file in from another directory (WRITE_DATA on dest). */
    pjd_set_root();
    {
        char src[256];
        snprintf(src, sizeof(src), "../%s/%s", n3, n1);
        EXPECT(0, pjd_create(src, 0644));
        pjd_set_user(65534, 65534);
        EXPECT(0, pjd_rename(src, n0));
        EXPECT(0, pjd_unlink(n0));
    }

    /* Note on directory rename-in: upstream additionally asserts that moving a
     * *directory* into the dest requires APPEND_DATA (so this WRITE_DATA-allowed
     * / APPEND_DATA-denied dest would reject it).  Chimera deliberately
     * authorizes a subdirectory rename via the destination's WRITE_DATA rather
     * than distinguishing APPEND_DATA (see vfs_proc_rename_at.c), so that
     * assertion does not map and is omitted here. */

    /* ---- WRITE_DATA denied, APPEND_DATA allowed: dirs yes, files no. ---- */
    pjd_set_root();
    {
        struct chimera_ace a = pjd_ace_user(CHIMERA_ACE_DENIED, 65534,
                                            CHIMERA_ACE_WRITE_DATA);
        struct chimera_ace b = pjd_ace_user(CHIMERA_ACE_ALLOWED, 65534,
                                            CHIMERA_ACE_APPEND_DATA);
        EXPECT(0, pjd_prependacl(".", &a));
        EXPECT(0, pjd_prependacl(".", &b));
    }

    pjd_set_user(65534, 65534);

    /* Cannot create files (WRITE_DATA denied). */
    EXPECT(EACCES, pjd_create(n0, 0644));

    /* But CAN create directories (APPEND_DATA). */
    EXPECT(0, pjd_mkdir(n0, 0755));
    /* Cannot remove it: removing needs WRITE_DATA / DELETE_CHILD on the dir. */
    EXPECT(EACCES, pjd_rmdir(n0));

    pjd_set_root();
    EXPECT(0, pjd_rmdir(n0));

    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n2));
    EXPECT(0, pjd_rmdir(n3));

    return pjd_end();
} /* main */
