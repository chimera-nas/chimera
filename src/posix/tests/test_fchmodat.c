// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <fcntl.h>
#include "posix_test_common.h"

#ifndef AT_FDCWD
#define AT_FDCWD            -100
#endif /* ifndef AT_FDCWD */

#ifndef AT_SYMLINK_NOFOLLOW
#define AT_SYMLINK_NOFOLLOW 0x100
#endif /* ifndef AT_SYMLINK_NOFOLLOW */

/* fchmodat relative to a real directory descriptor, through an interior
 * component, and with AT_SYMLINK_NOFOLLOW leaving a symlink's target alone. */
static void
test_real_dirfd(struct posix_test_env *env)
{
    struct stat st;
    int         dfd, fd, rc;

    rc = chimera_posix_mkdir("/test/fchmodat_dir", 0755);
    if (rc != 0) {
        fprintf(stderr, "mkdir fchmodat_dir failed: %s\n", strerror(errno));
        posix_test_fail(env);
    }

    rc = chimera_posix_mkdir("/test/fchmodat_dir/sub", 0755);
    if (rc != 0) {
        fprintf(stderr, "mkdir fchmodat_dir/sub failed: %s\n", strerror(errno));
        posix_test_fail(env);
    }

    fd = chimera_posix_open("/test/fchmodat_dir/sub/file", O_CREAT | O_RDWR, 0644);
    if (fd < 0) {
        fprintf(stderr, "create fchmodat_dir/sub/file failed: %s\n", strerror(errno));
        posix_test_fail(env);
    }
    chimera_posix_close(fd);

    rc = chimera_posix_symlink("sub/file", "/test/fchmodat_dir/link");
    if (rc != 0) {
        fprintf(stderr, "symlink failed: %s\n", strerror(errno));
        posix_test_fail(env);
    }

    dfd = chimera_posix_open("/test/fchmodat_dir", O_RDONLY);
    if (dfd < 0) {
        fprintf(stderr, "open fchmodat_dir failed: %s\n", strerror(errno));
        posix_test_fail(env);
    }

    rc = chimera_posix_fchmodat(dfd, "sub/file", 0600, 0);
    if (rc != 0) {
        fprintf(stderr, "fchmodat(dfd, sub/file) failed: %s\n", strerror(errno));
        posix_test_fail(env);
    }

    rc = chimera_posix_stat("/test/fchmodat_dir/sub/file", &st);
    if (rc != 0 || (st.st_mode & 0777) != 0600) {
        fprintf(stderr, "fchmodat(dfd, sub/file): expected 0600, got %03o (%s)\n",
                st.st_mode & 0777, strerror(errno));
        posix_test_fail(env);
    }

    /* Through the link: the target changes. */
    rc = chimera_posix_fchmodat(dfd, "link", 0640, 0);
    if (rc != 0) {
        fprintf(stderr, "fchmodat(dfd, link) failed: %s\n", strerror(errno));
        posix_test_fail(env);
    }

    rc = chimera_posix_stat("/test/fchmodat_dir/sub/file", &st);
    if (rc != 0 || (st.st_mode & 0777) != 0640) {
        fprintf(stderr, "fchmodat(dfd, link): expected 0640 on the target, got %03o\n",
                st.st_mode & 0777);
        posix_test_fail(env);
    }

    /* NOFOLLOW: whatever the backend does with a symlink's own mode, the
     * target must be left alone. */
    rc = chimera_posix_fchmodat(dfd, "link", 0600, AT_SYMLINK_NOFOLLOW);
    (void) rc;

    rc = chimera_posix_stat("/test/fchmodat_dir/sub/file", &st);
    if (rc != 0 || (st.st_mode & 0777) != 0640) {
        fprintf(stderr, "fchmodat(dfd, link, NOFOLLOW) changed the target: got %03o\n",
                st.st_mode & 0777);
        posix_test_fail(env);
    }

    chimera_posix_close(dfd);

    chimera_posix_unlink("/test/fchmodat_dir/link");
    chimera_posix_unlink("/test/fchmodat_dir/sub/file");
    chimera_posix_rmdir("/test/fchmodat_dir/sub");
    chimera_posix_rmdir("/test/fchmodat_dir");
} /* test_real_dirfd */

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   fd;
    int                   rc;
    chimera_posix_stat_t  st;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Testing fchmodat...\n");

    // Create test file with mode 0644
    fd = chimera_posix_open("/test/fchmodat_test", O_CREAT | O_RDWR, 0644);

    if (fd < 0) {
        fprintf(stderr, "Failed to create test file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    chimera_posix_close(fd);

    // Test fchmodat with AT_FDCWD
    rc = chimera_posix_fchmodat(AT_FDCWD, "/test/fchmodat_test", 0700, 0);

    if (rc != 0) {
        fprintf(stderr, "fchmodat with AT_FDCWD failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Verify the mode changed
    rc = chimera_posix_stat("/test/fchmodat_test", &st);

    if (rc != 0) {
        fprintf(stderr, "stat failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if ((st.st_mode & 0777) != 0700) {
        fprintf(stderr, "fchmodat: expected mode 0700, got %03o\n", st.st_mode & 0777);
        posix_test_fail(&env);
    }

    test_real_dirfd(&env);

    /* Exercise the real-dirfd branch, which must preserve the requested mode
     * across OPEN and SETATTR rather than passing creation attributes. */
    int             dirfd = chimera_posix_open("/test", O_RDONLY | O_DIRECTORY, 0);
    if (dirfd < 0 || chimera_posix_fchmodat(dirfd, "fchmodat_test", 0601, 0) ||
        chimera_posix_stat("/test/fchmodat_test", &st) || (st.st_mode & 0777) != 0601) {
        fprintf(stderr, "dirfd fchmodat failed to apply mode: %s\n", strerror(errno));
        posix_test_fail(&env);
    }
    struct timespec times[2] = { { .tv_sec = 1700000000 }, { .tv_sec = 1700000123 } };
    if (chimera_posix_utimensat(dirfd, "fchmodat_test", times, 0) ||
        chimera_posix_stat("/test/fchmodat_test", &st) || st.st_mtime != times[1].tv_sec) {
        fprintf(stderr, "dirfd utimensat failed to apply requested time\n");
        posix_test_fail(&env);
    }
    times[0].tv_nsec = times[1].tv_nsec = UTIME_OMIT;
    if (chimera_posix_utimensat(dirfd, "fchmodat_test", times, 0) ||
        chimera_posix_stat("/test/fchmodat_test", &st) || st.st_mtime != 1700000123) {
        fprintf(stderr, "both-omitted utimensat changed attributes\n");
        posix_test_fail(&env);
    }
    fd    = chimera_posix_open("/test/fchmodat_test", O_RDONLY, 0);
    errno = 0;
    if (fd < 0 || chimera_posix_utimensat(fd, "child", times, 0) != -1 || errno != ENOTDIR) {
        fprintf(stderr, "utimensat non-directory dirfd did not veto resolution\n");
        posix_test_fail(&env);
    }
    chimera_posix_close(fd);
    chimera_posix_close(dirfd);

    fprintf(stderr, "fchmodat test passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
} /* main */
