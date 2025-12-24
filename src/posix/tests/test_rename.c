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
    struct stat           st;

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

    rc = chimera_posix_rename("/test/testfile", "/test/renamedfile");

    if (rc != 0) {
        fprintf(stderr, "Failed to rename file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Renamed file successfully\n");

    rc = chimera_posix_stat("/test/testfile", &st);

    if (rc == 0) {
        fprintf(stderr, "Old file name still exists after rename\n");
        posix_test_fail(&env);
    }

    rc = chimera_posix_stat("/test/renamedfile", &st);

    if (rc != 0) {
        fprintf(stderr, "Failed to stat renamed file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Verified rename: old name gone, new name exists\n");

    fd = chimera_posix_open("/test/renamedfile", O_RDONLY, 0);

    if (fd < 0) {
        fprintf(stderr, "Failed to open renamed file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Opened renamed file successfully\n");

    chimera_posix_close(fd);

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
} /* main */
