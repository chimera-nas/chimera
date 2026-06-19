// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Cross-mount link(2)/rename(2) must fail with EXDEV: a hard link or rename
 * cannot span file systems.  The primary backend is mounted at /test; a second,
 * independent memfs instance is mounted at /test2 so the two have distinct mount
 * ids.  Operations within a single mount must still succeed.
 *
 * Models pjdfstest link/14 and rename/15, which use a second mounted file
 * system; here a second VFS mount provides the distinct device.
 */
#include "posix_test_common.h"

static int failures;

static void
check(
    int         cond,
    const char *msg)
{
    static int n;

    n++;
    if (cond) {
        fprintf(stderr, "ok %d - %s\n", n, msg);
    } else {
        fprintf(stderr, "not ok %d - %s\n", n, msg);
        failures++;
    }
} /* check */

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   fd, rc;

    posix_test_init(&env, argv, argc);

    if (posix_test_mount(&env) != 0) {
        fprintf(stderr, "Failed to mount primary test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    /* A second, independent memfs instance -> a distinct mount id from /test. */
    rc = chimera_posix_mount("/test2", "memfs", "/");
    if (rc != 0) {
        fprintf(stderr, "Failed to mount second module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    /* Fixtures: a file and a directory on /test, a directory on /test2. */
    fd = chimera_posix_open("/test/file", O_CREAT | O_RDWR, 0644);
    check(fd >= 0, "create /test/file");
    if (fd >= 0) {
        chimera_posix_close(fd);
    }
    check(chimera_posix_mkdir("/test/dir", 0755) == 0, "mkdir /test/dir");
    check(chimera_posix_mkdir("/test2/dir", 0755) == 0, "mkdir /test2/dir");

    /* Cross-mount hard link -> EXDEV. */
    errno = 0;
    rc    = chimera_posix_link("/test/file", "/test2/link");
    check(rc != 0 && errno == EXDEV, "cross-mount link -> EXDEV");

    /* Cross-mount rename of a file -> EXDEV. */
    errno = 0;
    rc    = chimera_posix_rename("/test/file", "/test2/moved");
    check(rc != 0 && errno == EXDEV, "cross-mount rename(file) -> EXDEV");

    /* Cross-mount rename of a directory -> EXDEV. */
    errno = 0;
    rc    = chimera_posix_rename("/test/dir", "/test2/moveddir");
    check(rc != 0 && errno == EXDEV, "cross-mount rename(dir) -> EXDEV");

    /* Same-mount operations still succeed. */
    check(chimera_posix_link("/test/file", "/test/link2") == 0,
          "same-mount link succeeds");
    check(chimera_posix_rename("/test/link2", "/test/renamed") == 0,
          "same-mount rename succeeds");
    check(chimera_posix_rename("/test/dir", "/test/dir2") == 0,
          "same-mount rename(dir) succeeds");

    /* Cleanup. */
    chimera_posix_unlink("/test/file");
    chimera_posix_unlink("/test/renamed");
    chimera_posix_rmdir("/test/dir2");
    chimera_posix_rmdir("/test2/dir");

    if (failures) {
        posix_test_fail(&env);
    }
    posix_test_success(&env);
    return 0;
} /* main */
