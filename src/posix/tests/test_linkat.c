// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <fcntl.h>
#include "posix_test_common.h"

#ifndef AT_FDCWD
#define AT_FDCWD -100
#endif /* ifndef AT_FDCWD */

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   rc;
    int                   fd;
    struct stat           st1, st2;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Testing linkat...\n");

    // Create a file
    fd = chimera_posix_openat(AT_FDCWD, "/test/linkat_src.txt", O_CREAT | O_RDWR, 0644);
    if (fd < 0) {
        fprintf(stderr, "Failed to create source file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }
    chimera_posix_close(fd);

    // Create a hard link
    rc = chimera_posix_linkat(AT_FDCWD, "/test/linkat_src.txt",
                              AT_FDCWD, "/test/linkat_dst.txt", 0);
    if (rc != 0) {
        fprintf(stderr, "linkat failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Verify both exist and have same inode
    rc = chimera_posix_fstatat(AT_FDCWD, "/test/linkat_src.txt", &st1, 0);
    if (rc != 0) {
        fprintf(stderr, "fstatat on src failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    rc = chimera_posix_fstatat(AT_FDCWD, "/test/linkat_dst.txt", &st2, 0);
    if (rc != 0) {
        fprintf(stderr, "fstatat on dst failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if (st1.st_ino != st2.st_ino) {
        fprintf(stderr, "Hard link inodes don't match\n");
        posix_test_fail(&env);
    }

    // Cleanup
    chimera_posix_unlinkat(AT_FDCWD, "/test/linkat_src.txt", 0);
    chimera_posix_unlinkat(AT_FDCWD, "/test/linkat_dst.txt", 0);

    fprintf(stderr, "linkat tests passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
} /* main */
