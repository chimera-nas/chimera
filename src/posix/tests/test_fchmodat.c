// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

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
    int                   fd;
    int                   rc;
    struct stat           st;

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

    fprintf(stderr, "fchmodat test passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
} /* main */
