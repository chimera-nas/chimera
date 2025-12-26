// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "posix_test_common.h"

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   fd;
    int                   rc;
    struct stat           st1, st2;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fd = chimera_posix_open("/test/testfile", O_CREAT | O_RDWR, 0644);

    if (fd < 0) {
        fprintf(stderr, "Failed to create test file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    chimera_posix_close(fd);

    rc = chimera_posix_link("/test/testfile", "/test/hardlink");

    if (rc != 0) {
        fprintf(stderr, "Failed to create hard link: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Created hard link successfully\n");

    rc = chimera_posix_stat("/test/testfile", &st1);

    if (rc != 0) {
        fprintf(stderr, "Failed to stat original file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    rc = chimera_posix_stat("/test/hardlink", &st2);

    if (rc != 0) {
        fprintf(stderr, "Failed to stat hard link: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if (st1.st_ino != st2.st_ino) {
        fprintf(stderr, "Hard link has different inode: %lu vs %lu\n",
                (unsigned long) st1.st_ino, (unsigned long) st2.st_ino);
        posix_test_fail(&env);
    }

    if (st1.st_nlink < 2 || st2.st_nlink < 2) {
        fprintf(stderr, "Link count should be at least 2: %lu, %lu\n",
                (unsigned long) st1.st_nlink, (unsigned long) st2.st_nlink);
        posix_test_fail(&env);
    }

    fprintf(stderr, "Hard link verified: same inode %lu, nlink=%lu\n",
            (unsigned long) st1.st_ino, (unsigned long) st1.st_nlink);

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
} /* main */
