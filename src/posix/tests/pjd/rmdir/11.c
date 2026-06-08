// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/rmdir/11.t:
 * rmdir returns EACCES or EPERM if the containing directory is sticky and the
 * caller owns neither the containing directory nor the directory to remove. */

#include "../../pjd_common.h"

static void
expect_acces_or_eperm(const char *path)
{
    int rc = pjd_rmdir(path);
    int e  = (rc < 0) ? errno : 0;

    PJD_CHECK(e == EACCES || e == EPERM,
              "sticky rmdir %s -> %s (want EACCES/EPERM)", path, strerror(e));
} /* expect_acces_or_eperm */

int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);

    char *n0 = pjd_namegen();
    char *n1 = pjd_namegen();
    char *n2 = pjd_namegen();
    char  n0n1[256];

    EXPECT(0, pjd_mkdir(n2, 0755));
    pjd_cd(n2);

    EXPECT(0, pjd_mkdir(n0, 0755));
    EXPECT(0, pjd_chown(n0, 65534, 65534));
    EXPECT(0, pjd_chmod(n0, 01777));   /* sticky */

    snprintf(n0n1, sizeof(n0n1), "%s/%s", n0, n1);

    /* Caller owns both the sticky dir and the dir to remove -> allowed. */
    pjd_set_user(65534, 65534);
    EXPECT(0, pjd_mkdir(n0n1, 0755));
    EXPECT(0, pjd_rmdir(n0n1));
    pjd_set_root();

    /* Caller owns the dir to remove but not the sticky dir -> allowed. */
    for (int k = 0; k < 2; k++) {
        int id = k == 0 ? 0 : 65533;
        EXPECT(0, pjd_chown(n0, id, id));
        EXPECT(0, pjd_create_file_owned(PJD_FT_DIR, n0n1, 65534, 65534));
        pjd_set_user(65534, 65534);
        EXPECT(0, pjd_rmdir(n0n1));
        pjd_set_root();
    }

    /* Caller owns the sticky dir but not the dir to remove -> allowed. */
    EXPECT(0, pjd_chown(n0, 65534, 65534));
    for (int k = 0; k < 2; k++) {
        int id = k == 0 ? 0 : 65533;
        EXPECT(0, pjd_create_file_owned(PJD_FT_DIR, n0n1, id, id));
        pjd_set_user(65534, 65534);
        EXPECT(0, pjd_rmdir(n0n1));
        pjd_set_root();
    }

    /* Caller owns neither -> EACCES/EPERM. */
    for (int k = 0; k < 2; k++) {
        int id = k == 0 ? 0 : 65533;
        EXPECT(0, pjd_chown(n0, id, id));
        EXPECT(0, pjd_create_file_owned(PJD_FT_DIR, n0n1, id, id));
        pjd_set_user(65534, 65534);
        expect_acces_or_eperm(n0n1);
        pjd_set_root();
        EXPECT(0, pjd_rmdir(n0n1));
    }

    EXPECT(0, pjd_rmdir(n0));
    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n2));

    return pjd_end();
} /* main */
