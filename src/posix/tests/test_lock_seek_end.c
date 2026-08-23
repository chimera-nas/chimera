// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * SEEK_END byte-range locks against a claim-arbitrating backend.
 *
 * A SEEK_END range is the one shape the local claim core cannot arbitrate:
 * resolving the offset here would race the file's size, so the geometry is
 * handed to the backend to resolve atomically with the operation.  That
 * makes the backend the whole answer for these, and this test pins the
 * three things that follow from it -- the lock is granted, the matching
 * unlock is expressible (it names the range, since this side never learned
 * the absolute one), and a range resolving before byte 0 is rejected.
 *
 * memfs-only: it is the backend that arbitrates ranges in-process, and
 * these assertions are about one process's view.
 */

#include "posix_test_common.h"

static struct flock
lock_desc(
    short type,
    int   whence,
    off_t start,
    off_t len)
{
    struct flock fl;

    memset(&fl, 0, sizeof(fl));
    fl.l_type   = type;
    fl.l_whence = whence;
    fl.l_start  = start;
    fl.l_len    = len;
    return fl;
} /* lock_desc */

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    struct flock          fl;
    int                   fd;
    int                   rc;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);
    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fd = chimera_posix_open("/test/seek_end_file",
                            O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (fd < 0) {
        fprintf(stderr, "Failed to create test file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    /* Give the file a size the EOF-relative ranges below resolve against. */
    if (chimera_posix_write(fd, "01234567890123456789", 20) != 20) {
        fprintf(stderr, "Failed to size the test file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    /* The last ten bytes, named relative to EOF. */
    fl = lock_desc(F_WRLCK, SEEK_END, -10, 10);
    rc = chimera_posix_fcntl(fd, F_SETLK, &fl);
    if (rc != 0) {
        fprintf(stderr, "SEEK_END F_SETLK failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    /* The unlock names the same EOF-relative range rather than a token,
     * and must be carried out rather than refused. */
    fl = lock_desc(F_UNLCK, SEEK_END, -10, 10);
    rc = chimera_posix_fcntl(fd, F_SETLK, &fl);
    if (rc != 0) {
        fprintf(stderr, "SEEK_END F_UNLCK failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    /* Having been released, the range is grantable again -- which is the
     * observable difference between an unlock that happened and one that
     * was quietly dropped. */
    fl = lock_desc(F_WRLCK, SEEK_END, -10, 10);
    rc = chimera_posix_fcntl(fd, F_SETLK, &fl);
    if (rc != 0) {
        fprintf(stderr, "SEEK_END re-lock after unlock failed: %s\n",
                strerror(errno));
        posix_test_fail(&env);
    }
    fl = lock_desc(F_UNLCK, SEEK_END, -10, 10);
    chimera_posix_fcntl(fd, F_SETLK, &fl);

    /* A range that resolves before the start of the file is invalid, and
     * only the side that knows the size can say so. */
    fl = lock_desc(F_WRLCK, SEEK_END, -100, 10);
    rc = chimera_posix_fcntl(fd, F_SETLK, &fl);
    if (rc == 0 || errno != EINVAL) {
        fprintf(stderr,
                "SEEK_END before file start: rc=%d errno=%s, expected EINVAL\n",
                rc, strerror(errno));
        posix_test_fail(&env);
    }

    chimera_posix_close(fd);

    posix_test_success(&env);
    return 0;
} /* main */
