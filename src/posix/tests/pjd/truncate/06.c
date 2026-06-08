// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

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
