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

/* Ported from pjdfstest tests/unlink/00.t:
 * unlink removes files of every (non-directory) type; a successful unlink that
 * leaves another link updates the surviving link's ctime and the containing
 * directory's mtime/ctime; a permission-denied unlink does not. */

#include "../../pjd_common.h"

static const enum pjd_ftype types[] = {
    PJD_FT_REGULAR, PJD_FT_FIFO,      PJD_FT_BLOCK,
    PJD_FT_CHAR,    PJD_FT_SOCKET,    PJD_FT_SYMLINK,
};

/* Types that support hard links (not symlinks/dirs). */
static const enum pjd_ftype link_types[] = {
    PJD_FT_REGULAR, PJD_FT_FIFO, PJD_FT_BLOCK, PJD_FT_CHAR, PJD_FT_SOCKET,
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
    char  st;

    EXPECT(0, pjd_mkdir(n2, 0755));
    pjd_cd(n2);

    /* Each type can be created and unlinked. */
    for (unsigned i = 0; i < sizeof(types) / sizeof(types[0]); i++) {
        struct stat junk;
        EXPECT(0, pjd_create_file(types[i], n0));
        EXPECT(0, pjd_unlink(n0));
        EXPECT(ENOENT, pjd_lstat(n0, &junk));
    }
    (void) st;

    /* A successful unlink of one link updates the survivor's ctime. */
    for (unsigned i = 0; i < sizeof(link_types) / sizeof(link_types[0]); i++) {
        struct timespec c1, c2;
        EXPECT(0, pjd_create_file(link_types[i], n0));
        EXPECT(0, pjd_link(n0, n1));
        pjd_lstat_ctime(n0, &c1);
        pjd_settle();
        EXPECT(0, pjd_unlink(n1));
        pjd_lstat_ctime(n0, &c2);
        PJD_CHECK(pjd_timespec_lt(&c1, &c2), "survivor ctime advanced (type %d)", link_types[i]);
        EXPECT(0, pjd_unlink(n0));
    }

    /* A permission-denied unlink (no write on the parent) leaves ctime alone. */
    for (unsigned i = 0; i < sizeof(types) / sizeof(types[0]); i++) {
        struct timespec c1, c2;
        if (types[i] == PJD_FT_SYMLINK) {
            continue;
        }
        EXPECT(0, pjd_create_file(types[i], n0));
        pjd_lstat_ctime(n0, &c1);
        pjd_settle();
        pjd_set_user(65534, 65534);
        EXPECT(EACCES, pjd_unlink(n0));
        pjd_set_root();
        pjd_lstat_ctime(n0, &c2);
        PJD_CHECK(c1.tv_sec == c2.tv_sec && c1.tv_nsec == c2.tv_nsec,
                  "ctime unchanged after failed unlink (type %d)", types[i]);
        EXPECT(0, pjd_unlink(n0));
    }

    /* Unlinking an entry updates the containing directory's mtime/ctime. */
    for (unsigned i = 0; i < sizeof(types) / sizeof(types[0]); i++) {
        struct timespec t0, t1;
        char            n0n1[256];
        EXPECT(0, pjd_mkdir(n0, 0755));
        snprintf(n0n1, sizeof(n0n1), "%s/%s", n0, n1);
        EXPECT(0, pjd_create_file(types[i], n0n1));
        pjd_stat_ctime(n0, &t0);
        pjd_settle();
        EXPECT(0, pjd_unlink(n0n1));
        pjd_stat_mtime(n0, &t1);
        PJD_CHECK(pjd_timespec_lt(&t0, &t1), "parent mtime advanced (type %d)", types[i]);
        pjd_stat_ctime(n0, &t1);
        PJD_CHECK(pjd_timespec_lt(&t0, &t1), "parent ctime advanced (type %d)", types[i]);
        EXPECT(0, pjd_rmdir(n0));
    }

    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n2));

    return pjd_end();
} /* main */
