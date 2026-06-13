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

/* Ported from pjdfstest tests/rename/04.t: EACCES when a component of either path prefix denies search. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen(), *n1 = pjd_namegen(), *n2 = pjd_namegen(), *n3 = pjd_namegen();
    char  src[256], dst[256];
    EXPECT(0, pjd_mkdir(n0, 0755));
    pjd_cd(n0);
    EXPECT(0, pjd_mkdir(n1, 0755));
    EXPECT(0, pjd_chown(n1, 65534, 65534));
    snprintf(src, sizeof(src), "%s/%s", n1, n2);
    snprintf(dst, sizeof(dst), "%s/%s", n1, n3);
    pjd_set_user(65534, 65534);
    EXPECT(0, pjd_create(src, 0644));
    EXPECT(0, pjd_rename(src, dst));
    EXPECT(0, pjd_rename(dst, src));
    pjd_set_root();
    EXPECT(0, pjd_chmod(n1, 0644));   /* no search */
    pjd_set_user(65534, 65534);
    EXPECT(EACCES, pjd_rename(src, dst));
    pjd_set_root();
    EXPECT(0, pjd_chmod(n1, 0755));
    pjd_set_user(65534, 65534);
    EXPECT(0, pjd_unlink(src));
    pjd_set_root();
    EXPECT(0, pjd_rmdir(n1));
    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n0));
    return pjd_end();
} /* main */
