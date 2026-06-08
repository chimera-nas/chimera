// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/chmod/00.t:
* "chmod changes permission" - across every file type, chmod following a
* symlink, ctime updates on success / no update on failure, and S_ISGID
* clearing when a non-owner chmods a file whose group it does not match.
* (lchmod is FreeBSD-only and omitted.) */

#include "../../pjd_common.h"

static const enum pjd_ftype types[] = {
    PJD_FT_REGULAR, PJD_FT_DIR,    PJD_FT_FIFO,
    PJD_FT_BLOCK,   PJD_FT_CHAR,   PJD_FT_SOCKET,
    PJD_FT_SYMLINK,
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

    EXPECT(0, pjd_mkdir(n2, 0755));
    pjd_cd(n2);

    for (unsigned i = 0; i < sizeof(types) / sizeof(types[0]); i++) {
        enum pjd_ftype t = types[i];

        if (t != PJD_FT_SYMLINK) {
            EXPECT(0, pjd_create_file(t, n0));
            EXPECT(0, pjd_chmod(n0, 0111));
            EXPECT_EQ(0111, pjd_stat_mode(n0));

            /* chmod through a symlink affects the target, not the link. */
            EXPECT(0, pjd_symlink(n0, n1));
            int linkmode = pjd_lstat_mode(n1);
            EXPECT(0, pjd_chmod(n1, 0222));
            EXPECT_EQ(0222, pjd_stat_mode(n1));
            EXPECT_EQ(0222, pjd_stat_mode(n0));
            EXPECT_EQ(linkmode, pjd_lstat_mode(n1));
            EXPECT(0, pjd_unlink(n1));

            EXPECT(0, pjd_remove_file(t, n0));
        }
    }

    /* Successful chmod updates ctime. */
    for (unsigned i = 0; i < sizeof(types) / sizeof(types[0]); i++) {
        enum pjd_ftype  t = types[i];
        struct timespec c1, c2;

        if (t == PJD_FT_SYMLINK) {
            continue;
        }

        EXPECT(0, pjd_create_file(t, n0));
        pjd_lstat_ctime(n0, &c1);
        pjd_settle();
        EXPECT(0, pjd_chmod(n0, 0111));
        pjd_lstat_ctime(n0, &c2);
        PJD_CHECK(pjd_timespec_lt(&c1, &c2), "ctime advanced after chmod (type %d)", t);
        EXPECT(0, pjd_remove_file(t, n0));
    }

    /* Unsuccessful chmod (EPERM as non-owner) does not update ctime. */
    for (unsigned i = 0; i < sizeof(types) / sizeof(types[0]); i++) {
        enum pjd_ftype  t = types[i];
        struct timespec c1, c2;

        if (t == PJD_FT_SYMLINK) {
            continue;
        }

        EXPECT(0, pjd_create_file(t, n0));
        pjd_lstat_ctime(n0, &c1);
        pjd_settle();
        pjd_set_user(65534, 65534);
        EXPECT(EPERM, pjd_chmod(n0, 0111));
        pjd_set_root();
        pjd_lstat_ctime(n0, &c2);
        PJD_CHECK(c1.tv_sec == c2.tv_sec && c1.tv_nsec == c2.tv_nsec,
                  "ctime unchanged after failed chmod (type %d)", t);
        EXPECT(0, pjd_remove_file(t, n0));
    }

    /* POSIX: when a non-privileged process whose effective gid does not match
     * the file's gid (and the file is a regular file) chmods it, S_ISGID is
     * cleared on success. */
    EXPECT(0, pjd_create(n0, 0755));
    EXPECT(0, pjd_chown(n0, 65534, 65534));

    pjd_set_user(65534, 65534);
    EXPECT(0, pjd_chmod(n0, 02755));
    pjd_set_root();
    EXPECT_EQ(02755, pjd_stat_mode(n0));

    pjd_set_user(65534, 65534);
    EXPECT(0, pjd_chmod(n0, 0755));
    pjd_set_root();
    EXPECT_EQ(0755, pjd_stat_mode(n0));

    /* gid 65533 != file gid 65534 -> Linux clears S_ISGID and succeeds. */
    pjd_set_user(65534, 65533);
    EXPECT(0, pjd_chmod(n0, 02755));
    pjd_set_root();
    EXPECT_EQ(0755, pjd_stat_mode(n0));

    EXPECT(0, pjd_unlink(n0));

    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n2));

    return pjd_end();
} /* main */
