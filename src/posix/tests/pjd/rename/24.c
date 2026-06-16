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

/* Ported from pjdfstest tests/rename/24.t: rename of a directory updates its
 * ".." link and both parents' link counts. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *sp = pjd_namegen(), *dp = pjd_namegen(), *src = pjd_namegen(), *dst = pjd_namegen();
    char  sp_src[256], sp_src_dd[256], dp_dst[256], dp_dst_dd[256];

    snprintf(sp_src, sizeof(sp_src), "%s/%s", sp, src);
    snprintf(sp_src_dd, sizeof(sp_src_dd), "%s/%s/..", sp, src);
    snprintf(dp_dst, sizeof(dp_dst), "%s/%s", dp, dst);
    snprintf(dp_dst_dd, sizeof(dp_dst_dd), "%s/%s/..", dp, dst);

    EXPECT(0, pjd_mkdir(sp, 0755));
    EXPECT(0, pjd_mkdir(dp, 0755));
    EXPECT(0, pjd_mkdir(sp_src, 0755));

    /* Initial conditions: src_parent has 3 links (., its own .., src/..),
     * dst_parent has 2 (., ..). */
    EXPECT_EQ(3, pjd_stat_nlink(sp));
    EXPECT_EQ(2, pjd_stat_nlink(dp));

    /* src/.. resolves to src_parent. */
    EXPECT_EQ(pjd_lstat_inode(sp), pjd_lstat_inode(sp_src_dd));

    EXPECT(0, pjd_rename(sp_src, dp_dst));

    /* After the move: src_parent loses a link, dst_parent gains one. */
    EXPECT_EQ(2, pjd_stat_nlink(sp));
    EXPECT_EQ(3, pjd_stat_nlink(dp));

    /* dst/.. now resolves to dst_parent. */
    EXPECT_EQ(pjd_lstat_inode(dp), pjd_lstat_inode(dp_dst_dd));

    EXPECT(0, pjd_rmdir(dp_dst));
    EXPECT(0, pjd_rmdir(dp));
    EXPECT(0, pjd_rmdir(sp));
    return pjd_end();
} /* main */
