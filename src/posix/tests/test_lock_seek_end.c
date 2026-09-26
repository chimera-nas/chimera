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
 * Runs against memfs and host backends, using independent semantic owners
 * to observe actual backend coverage within one process.
 */

#include "posix_test_common.h"

static struct flock
lock_desc(
    short         type,
    int           whence,
    chimera_off_t start,
    chimera_off_t len)
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

    /* Invalid EOF-relative unlocks fail even when this owner holds nothing. */
    fl    = lock_desc(F_UNLCK, SEEK_END, -100, 10);
    errno = 0;
    if (chimera_posix_fcntl(fd, F_SETLK, &fl) != -1 || errno != EINVAL) {
        fprintf(stderr, "unheld invalid SEEK_END unlock was not rejected\n");
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

    /* Finish the default process owner's lifetime before switching to
     * synthetic owners. Range unlock alone need not retire its backend
     * anchor, and test teardown removes the filesystem before domain shutdown. */
    int      default_owner_fd = chimera_posix_dup(fd);
    if (default_owner_fd < 0 || chimera_posix_close(default_owner_fd)) {
        fprintf(stderr, "default owner close cleanup failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    /* Backend-only tokens have process/file ownership, not OFD lifetime:
     * closing a different descriptor must release an EOF-relative grant. */
    uint64_t owner_a = 0xa1, owner_b = 0xb2;
    chimera_posix_set_lock_owner(&owner_a);
    /* Mixing absolute and EOF-relative geometry must not leave stale local
     * coverage after the backend has split or downgraded the lock. */
    fl = lock_desc(F_WRLCK, SEEK_SET, 0, 10);
    if (chimera_posix_fcntl(fd, F_SETLK, &fl)) {
        posix_test_fail(&env);
    }
    fl = lock_desc(F_UNLCK, SEEK_END, -18, 2); /* [2,4) */
    if (chimera_posix_fcntl(fd, F_SETLK, &fl)) {
        posix_test_fail(&env);
    }
    chimera_posix_set_lock_owner(&owner_b);
    fl = lock_desc(F_WRLCK, SEEK_SET, 2, 2);
    if (chimera_posix_fcntl(fd, F_GETLK, &fl) || fl.l_type != F_UNLCK) {
        fprintf(stderr, "EOF-relative unlock left stale absolute coverage\n");
        posix_test_fail(&env);
    }
    fl = lock_desc(F_WRLCK, SEEK_SET, 0, 2);
    if (chimera_posix_fcntl(fd, F_GETLK, &fl) || fl.l_type != F_WRLCK) {
        fprintf(stderr, "EOF-relative unlock lost outside coverage\n");
        posix_test_fail(&env);
    }
    chimera_posix_set_lock_owner(&owner_a);
    fl = lock_desc(F_RDLCK, SEEK_END, -16, 2); /* [4,6) */
    if (chimera_posix_fcntl(fd, F_SETLK, &fl)) {
        posix_test_fail(&env);
    }
    chimera_posix_set_lock_owner(&owner_b);
    fl = lock_desc(F_RDLCK, SEEK_SET, 4, 2);
    if (chimera_posix_fcntl(fd, F_SETLK, &fl)) {
        fprintf(stderr, "EOF-relative downgrade retained a stale write lock\n");
        posix_test_fail(&env);
    }
    fl = lock_desc(F_UNLCK, SEEK_SET, 4, 2);
    if (chimera_posix_fcntl(fd, F_SETLK, &fl)) {
        posix_test_fail(&env);
    }
    fl = lock_desc(F_WRLCK, SEEK_SET, 4, 2);
    if (chimera_posix_fcntl(fd, F_GETLK, &fl) || fl.l_type != F_RDLCK) {
        fprintf(stderr, "EOF-relative downgrade lost the remaining read lock\n");
        posix_test_fail(&env);
    }
    chimera_posix_set_lock_owner(&owner_a);
    int other = chimera_posix_open("/test/seek_end_file", O_RDWR, 0);
    if (other < 0) {
        posix_test_fail(&env);
    }
    fl = lock_desc(F_WRLCK, SEEK_END, -10, 10);
    if (chimera_posix_fcntl(fd, F_SETLK, &fl) || chimera_posix_close(other)) {
        fprintf(stderr, "SEEK_END grant/other-description close failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }
    chimera_posix_set_lock_owner(&owner_b);
    fl = lock_desc(F_WRLCK, SEEK_END, -10, 10);
    if (chimera_posix_fcntl(fd, F_GETLK, &fl) || fl.l_type != F_UNLCK) {
        fprintf(stderr, "other-description close retained SEEK_END token\n");
        posix_test_fail(&env);
    }
    fl = lock_desc(F_WRLCK, SEEK_END, -10, 10);
    if (chimera_posix_fcntl(fd, F_SETLK, &fl)) {
        fprintf(stderr, "released SEEK_END range cannot be reacquired: %s\n", strerror(errno));
        posix_test_fail(&env);
    }
    if (chimera_posix_close(fd)) {
        fprintf(stderr, "owner B close cleanup failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);
    return 0;
} /* main */
