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

/* Ported from pjdfstest tests/chown/07.t:
 * chown returns EPERM if the effective user is not the super-user and either is
 * not the file owner, or attempts to change the owner (uid) or to a group it
 * does not belong to. */

#include "../../pjd_common.h"

static const enum pjd_ftype types[] = {
    PJD_FT_REGULAR, PJD_FT_DIR,    PJD_FT_FIFO,
    PJD_FT_BLOCK,   PJD_FT_CHAR,   PJD_FT_SOCKET,  PJD_FT_SYMLINK,
};

int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);

    char *n0 = pjd_namegen();
    char *n1 = pjd_namegen();
    char *n2 = pjd_namegen();
    char *n3 = pjd_namegen();
    char  n1n2[256], n1n3[256];

    EXPECT(0, pjd_mkdir(n0, 0755));
    pjd_cd(n0);
    EXPECT(0, pjd_mkdir(n1, 0755));
    EXPECT(0, pjd_chown(n1, 65534, 65534));

    snprintf(n1n2, sizeof(n1n2), "%s/%s", n1, n2);
    snprintf(n1n3, sizeof(n1n3), "%s/%s", n1, n3);

    for (unsigned i = 0; i < sizeof(types) / sizeof(types[0]); i++) {
        enum pjd_ftype t = types[i];

        if (t != PJD_FT_SYMLINK) {
            EXPECT(0, pjd_create_file_owned(t, n1n2, 65534, 65534));

            pjd_set_user(65534, 65534);
            EXPECT(EPERM, pjd_chown(n1n2, 65533, 65533));
            pjd_set_root();
            pjd_set_user(65533, 65533);
            EXPECT(EPERM, pjd_chown(n1n2, 65534, 65534));
            EXPECT(EPERM, pjd_chown(n1n2, 65533, 65533));
            pjd_set_root();
            pjd_set_user(65534, 65534);
            EXPECT(EPERM, pjd_chown(n1n2, (uid_t) -1, 65533));
            pjd_set_root();

            /* chown through a symlink to the target. */
            pjd_set_user(65534, 65534);
            EXPECT(0, pjd_symlink(n2, n1n3));
            pjd_set_root();
            pjd_set_user(65534, 65534);
            EXPECT(EPERM, pjd_chown(n1n3, 65533, 65533));
            pjd_set_root();
            pjd_set_user(65533, 65533);
            EXPECT(EPERM, pjd_chown(n1n3, 65534, 65534));
            EXPECT(EPERM, pjd_chown(n1n3, 65533, 65533));
            pjd_set_root();
            pjd_set_user(65534, 65534);
            EXPECT(EPERM, pjd_chown(n1n3, (uid_t) -1, 65533));
            pjd_set_root();
            EXPECT(0, pjd_unlink(n1n3));

            EXPECT(0, pjd_remove_file(t, n1n2));
        }

        EXPECT(0, pjd_create_file_owned(t, n1n2, 65534, 65534));
        pjd_set_user(65534, 65534);
        EXPECT(EPERM, pjd_lchown(n1n2, 65533, 65533));
        pjd_set_root();
        pjd_set_user(65533, 65533);
        EXPECT(EPERM, pjd_lchown(n1n2, 65534, 65534));
        EXPECT(EPERM, pjd_lchown(n1n2, 65533, 65533));
        pjd_set_root();
        pjd_set_user(65534, 65534);
        EXPECT(EPERM, pjd_lchown(n1n2, (uid_t) -1, 65533));
        pjd_set_root();
        EXPECT(0, pjd_remove_file(t, n1n2));
    }

    EXPECT(0, pjd_rmdir(n1));
    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n0));

    return pjd_end();
} /* main */
