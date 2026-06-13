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

/* Ported from pjdfstest tests/chown/00.t: "chown changes ownership".
 * Covers: super-user ownership changes (incl. via symlink and lchown);
 * non-super-user group changes (owner + group membership); chown(-1,-1) as a
 * permitted no-op; clearing of set-uid/set-gid on ownership change; and ctime
 * updates.  Where Linux differs from POSIX/BSD (e.g. it does not clear SUID/SGID
 * on a directory, and updates ctime on a -1/-1 chown), the corresponding upstream
 * assertions are TODO; those are relaxed here. */

#include "../../pjd_common.h"

static const enum pjd_ftype types[] = {
    PJD_FT_REGULAR, PJD_FT_DIR,    PJD_FT_FIFO,
    PJD_FT_BLOCK,   PJD_FT_CHAR,   PJD_FT_SOCKET,  PJD_FT_SYMLINK,
};

static void
check_uidgid(
    const char *name,
    long        uid,
    long        gid)
{
    EXPECT_EQ(uid, pjd_lstat_uid(name));
    EXPECT_EQ(gid, pjd_lstat_gid(name));
} /* check_uidgid */

/* Follow symlinks: stat-based uid/gid. */
static void
check_uidgid_follow(
    const char *name,
    long        uid,
    long        gid)
{
    struct stat st;

    if (pjd_stat(name, &st) != 0) {
        PJD_CHECK(0, "stat %s for uid/gid", name);
        return;
    }
    EXPECT_EQ(uid, (long) st.st_uid);
    EXPECT_EQ(gid, (long) st.st_gid);
} /* check_uidgid_follow */

/* Mode is either of two acceptable values (e.g. 06555 not-cleared or 0555
 * cleared), via stat (follow). */
static void
check_mode_either(
    const char *name,
    int         a,
    int         b)
{
    int m = pjd_stat_mode(name);

    PJD_CHECK(m == a || m == b, "mode %04o in {%04o,%04o} for %s", m, a, b, name);
} /* check_mode_either */

int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);

    char          *n0 = pjd_namegen();
    char          *n1 = pjd_namegen();
    char          *n2 = pjd_namegen();
    const uint32_t g31 = 65531, g32 = 65532;

    EXPECT(0, pjd_mkdir(n2, 0755));
    pjd_cd(n2);

    /* --- super-user can always modify ownership --- */
    for (unsigned i = 0; i < sizeof(types) / sizeof(types[0]); i++) {
        enum pjd_ftype t = types[i];

        if (t != PJD_FT_SYMLINK) {
            EXPECT(0, pjd_create_file(t, n0));
            EXPECT(0, pjd_chown(n0, 123, 456));
            check_uidgid(n0, 123, 456);
            EXPECT(0, pjd_chown(n0, 0, 0));
            check_uidgid(n0, 0, 0);

            EXPECT(0, pjd_symlink(n0, n1));
            long luid = pjd_lstat_uid(n1), lgid = pjd_lstat_gid(n1);
            EXPECT(0, pjd_chown(n1, 123, 456));
            check_uidgid_follow(n1, 123, 456);
            check_uidgid_follow(n0, 123, 456);
            check_uidgid(n1, luid, lgid);   /* symlink itself unchanged */
            EXPECT(0, pjd_unlink(n1));

            EXPECT(0, pjd_remove_file(t, n0));
        }

        EXPECT(0, pjd_create_file(t, n0));
        EXPECT(0, pjd_lchown(n0, 123, 456));
        check_uidgid(n0, 123, 456);
        EXPECT(0, pjd_remove_file(t, n0));
    }

    /* --- non-super-user may change group if owner and member of the group --- */
    for (unsigned i = 0; i < sizeof(types) / sizeof(types[0]); i++) {
        enum pjd_ftype t = types[i];

        if (t != PJD_FT_SYMLINK) {
            EXPECT(0, pjd_create_file(t, n0));
            EXPECT(0, pjd_chown(n0, 65534, 65533));
            check_uidgid(n0, 65534, 65533);

            pjd_set_user_groups(65534, g32, 1, &g31);
            EXPECT(0, pjd_chown(n0, (uid_t) -1, 65532));
            check_uidgid(n0, 65534, 65532);
            EXPECT(0, pjd_chown(n0, 65534, 65531));
            check_uidgid(n0, 65534, 65531);
            pjd_set_root();

            EXPECT(0, pjd_remove_file(t, n0));
        }

        EXPECT(0, pjd_create_file(t, n0));
        EXPECT(0, pjd_lchown(n0, 65534, 65533));
        check_uidgid(n0, 65534, 65533);
        pjd_set_user_groups(65534, g32, 1, &g31);
        EXPECT(0, pjd_lchown(n0, (uid_t) -1, 65532));
        check_uidgid(n0, 65534, 65532);
        EXPECT(0, pjd_lchown(n0, 65534, 65531));
        check_uidgid(n0, 65534, 65531);
        pjd_set_root();
        EXPECT(0, pjd_remove_file(t, n0));
    }

    /* --- chown(-1,-1) succeeds even for a non-owner --- */
    for (unsigned i = 0; i < sizeof(types) / sizeof(types[0]); i++) {
        enum pjd_ftype t = types[i];

        if (t != PJD_FT_SYMLINK) {
            EXPECT(0, pjd_create_file(t, n0));
            pjd_set_user(65534, 65534);
            EXPECT(0, pjd_chown(n0, (uid_t) -1, (gid_t) -1));
            pjd_set_root();
            check_uidgid_follow(n0, 0, 0);
            EXPECT(0, pjd_remove_file(t, n0));
        }

        EXPECT(0, pjd_create_file(t, n0));
        pjd_set_user(65534, 65534);
        EXPECT(0, pjd_lchown(n0, (uid_t) -1, (gid_t) -1));
        pjd_set_root();
        check_uidgid(n0, 0, 0);
        EXPECT(0, pjd_remove_file(t, n0));
    }

    /* --- super-user chown may clear set-uid/set-gid (either accepted) --- */
    for (unsigned i = 0; i < sizeof(types) / sizeof(types[0]); i++) {
        enum pjd_ftype t = types[i];

        if (t == PJD_FT_SYMLINK) {
            continue;
        }
        EXPECT(0, pjd_create_file(t, n0));
        EXPECT(0, pjd_chown(n0, 65534, 65533));
        EXPECT(0, pjd_chmod(n0, 06555));
        EXPECT_EQ(06555, pjd_stat_mode(n0));
        EXPECT(0, pjd_chown(n0, 65532, 65531));
        check_mode_either(n0, 06555, 0555);
        EXPECT(0, pjd_remove_file(t, n0));
    }

    /* --- non-super-user chown clears set-uid/set-gid (required on files;
     *     Linux does not clear on directories, so accept either there) --- */
    for (unsigned i = 0; i < sizeof(types) / sizeof(types[0]); i++) {
        enum pjd_ftype t = types[i];

        if (t == PJD_FT_SYMLINK) {
            continue;
        }
        EXPECT(0, pjd_create_file(t, n0));
        EXPECT(0, pjd_chown(n0, 65534, 65533));
        EXPECT(0, pjd_chmod(n0, 06555));
        EXPECT_EQ(06555, pjd_stat_mode(n0));

        pjd_set_user_groups(65534, 65533, 1, &g32);
        EXPECT(0, pjd_chown(n0, 65534, 65532));
        pjd_set_root();
        if (t == PJD_FT_DIR) {
            check_mode_either(n0, 06555, 0555);
        } else {
            EXPECT_EQ(0555, pjd_stat_mode(n0));
        }
        EXPECT(0, pjd_remove_file(t, n0));
    }

    /* --- successful chown updates ctime; failed chown does not --- */
    for (unsigned i = 0; i < sizeof(types) / sizeof(types[0]); i++) {
        enum pjd_ftype  t = types[i];
        struct timespec c1, c2;

        EXPECT(0, pjd_create_file(t, n0));
        pjd_lstat_ctime(n0, &c1);
        pjd_settle();
        EXPECT(0, pjd_lchown(n0, 65534, 65533));
        check_uidgid(n0, 65534, 65533);
        pjd_lstat_ctime(n0, &c2);
        PJD_CHECK(pjd_timespec_lt(&c1, &c2), "ctime advanced after chown (type %d)", t);

        /* Failed chown (EPERM) must not change ctime. */
        pjd_lstat_ctime(n0, &c1);
        pjd_settle();
        pjd_set_user(65532, 65532);
        EXPECT(EPERM, pjd_lchown(n0, 65532, 65532));
        pjd_set_root();
        pjd_lstat_ctime(n0, &c2);
        PJD_CHECK(c1.tv_sec == c2.tv_sec && c1.tv_nsec == c2.tv_nsec,
                  "ctime unchanged after failed chown (type %d)", t);

        EXPECT(0, pjd_remove_file(t, n0));
    }

    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n2));

    return pjd_end();
} /* main */
