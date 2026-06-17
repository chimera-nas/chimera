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

/* Ported from pjdfstest tests/granular/04.t:
 * "NFSv4 granular permissions checking - ACL_WRITE_OWNER".
 *
 * WRITE_OWNER lets a non-owner change the file's group (chgrp) -- but only to a
 * group the caller belongs to -- and, where the platform permits, change the
 * owner (chown) to the caller's own uid.  Without WRITE_OWNER (or ownership) a
 * non-owner chgrp/chown fails with EPERM.  This exercises the WRITE_OWNER ACE
 * path of the VFS setattr gate. */

#include "../../pjd_common.h"

int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);

    char    *n0 = pjd_namegen();
    char    *n2 = pjd_namegen();
    uint32_t gids[2];

    EXPECT(0, pjd_mkdir(n2, 0755));
    pjd_cd(n2);

    /* ---- WRITE_OWNER permits setting gid to our own only. ---- */
    EXPECT(0, pjd_create(n0, 0644));
    EXPECT_EQ(0, pjd_lstat_uid(n0));
    EXPECT_EQ(0, pjd_lstat_gid(n0));

    /* Non-owner, no WRITE_OWNER: chgrp denied. */
    gids[0] = 65532;
    gids[1] = 65531;
    pjd_set_user_groups(65534, 65532, 2, gids);
    EXPECT(EPERM, pjd_chown(n0, (uid_t) -1, 65532));
    pjd_set_root();
    EXPECT_EQ(0, pjd_lstat_uid(n0));
    EXPECT_EQ(0, pjd_lstat_gid(n0));

    /* Grant WRITE_OWNER to 65534. */
    {
        struct chimera_ace a = pjd_ace_user(CHIMERA_ACE_ALLOWED, 65534,
                                            CHIMERA_ACE_WRITE_OWNER);
        EXPECT(0, pjd_prependacl(n0, &a));
    }

    /* chgrp to a group we are NOT a member of (65530): denied. */
    pjd_set_user_groups(65534, 65532, 2, gids);
    EXPECT(EPERM, pjd_chown(n0, (uid_t) -1, 65530));
    pjd_set_root();
    EXPECT_EQ(0, pjd_lstat_uid(n0));
    EXPECT_EQ(0, pjd_lstat_gid(n0));

    /* chgrp to a group we ARE a member of (65532): allowed. */
    pjd_set_user_groups(65534, 65532, 2, gids);
    EXPECT(0, pjd_chown(n0, (uid_t) -1, 65532));
    pjd_set_root();
    EXPECT_EQ(0, pjd_lstat_uid(n0));
    EXPECT_EQ(65532, pjd_lstat_gid(n0));

    EXPECT(0, pjd_unlink(n0));

    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n2));

    return pjd_end();
} /* main */
