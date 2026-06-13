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

/* Ported from pjdfstest tests/truncate/06.t:
 * truncate returns EACCES if the named file is not writable by the user. */

#include "../../pjd_common.h"

int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);

    char *n0 = pjd_namegen();
    char *n1 = pjd_namegen();

    EXPECT(0, pjd_mkdir(n0, 0755));
    pjd_cd(n0);

    EXPECT(0, pjd_create(n1, 0644));

    /* Not the owner, no write bit for others -> EACCES. */
    pjd_set_user(65534, 65534);
    EXPECT(EACCES, pjd_truncate(n1, 123));
    pjd_set_root();

    /* Owner, but file mode 0444 (no write) -> EACCES. */
    EXPECT(0, pjd_chown(n1, 65534, 65534));
    EXPECT(0, pjd_chmod(n1, 0444));
    pjd_set_user(65534, 65534);
    EXPECT(EACCES, pjd_truncate(n1, 123));
    pjd_set_root();

    EXPECT(0, pjd_unlink(n1));
    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n0));

    return pjd_end();
} /* main */
