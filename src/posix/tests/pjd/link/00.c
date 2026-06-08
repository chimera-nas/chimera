// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/link/00.t:
 * link creates hard links - link count tracks across names, metadata is shared
 * by all links, and removing names decrements the count until the inode is gone. */

#include "../../pjd_common.h"

static const enum pjd_ftype types[] = {
    PJD_FT_REGULAR, PJD_FT_FIFO, PJD_FT_BLOCK, PJD_FT_CHAR, PJD_FT_SOCKET,
};

static mode_t
type_fmt(enum pjd_ftype t)
{
    switch (t) {
        case PJD_FT_REGULAR: return S_IFREG;
        case PJD_FT_FIFO:    return S_IFIFO;
        case PJD_FT_BLOCK:   return S_IFBLK;
        case PJD_FT_CHAR:    return S_IFCHR;
        case PJD_FT_SOCKET:  return S_IFSOCK;
        default:             return 0;
    } /* switch */
} /* type_fmt */

static void
check(
    const char *name,
    mode_t      fmt,
    int         mode,
    long        nlink,
    long        uid,
    long        gid)
{
    struct stat st;

    if (pjd_lstat(name, &st) != 0) {
        PJD_CHECK(0, "lstat %s", name);
        return;
    }
    PJD_CHECK((st.st_mode & S_IFMT) == fmt, "%s type", name);
    if (mode >= 0) {
        EXPECT_EQ(mode, st.st_mode & 07777);
    }
    EXPECT_EQ(nlink, (long) st.st_nlink);
    if (uid >= 0) {
        EXPECT_EQ(uid, (long) st.st_uid);
    }
    if (gid >= 0) {
        EXPECT_EQ(gid, (long) st.st_gid);
    }
} /* check */

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

    EXPECT(0, pjd_mkdir(n3, 0755));
    pjd_cd(n3);

    for (unsigned i = 0; i < sizeof(types) / sizeof(types[0]); i++) {
        mode_t fmt = type_fmt(types[i]);

        EXPECT(0, pjd_create_file(types[i], n0));
        check(n0, fmt, -1, 1, -1, -1);

        EXPECT(0, pjd_link(n0, n1));
        check(n0, fmt, -1, 2, -1, -1);
        check(n1, fmt, -1, 2, -1, -1);

        EXPECT(0, pjd_link(n1, n2));
        check(n0, fmt, -1, 3, -1, -1);
        check(n2, fmt, -1, 3, -1, -1);

        /* Metadata is shared across all links. */
        EXPECT(0, pjd_chmod(n1, 0201));
        EXPECT(0, pjd_chown(n1, 65534, 65533));
        check(n0, fmt, 0201, 3, 65534, 65533);
        check(n1, fmt, 0201, 3, 65534, 65533);
        check(n2, fmt, 0201, 3, 65534, 65533);

        EXPECT(0, pjd_unlink(n0));
        check(n1, fmt, 0201, 2, 65534, 65533);
        check(n2, fmt, 0201, 2, 65534, 65533);

        EXPECT(0, pjd_unlink(n2));
        check(n1, fmt, 0201, 1, 65534, 65533);

        EXPECT(0, pjd_unlink(n1));
    }

    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n3));

    return pjd_end();
} /* main */
