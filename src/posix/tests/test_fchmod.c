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

    fprintf(stderr, "Testing fchmod...\n");

    // Create test file with mode 0644
    fd = chimera_posix_open("/test/fchmod_test", O_CREAT | O_RDWR, 0644);

    if (fd < 0) {
        fprintf(stderr, "Failed to create test file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Change mode to 0755 using fchmod
    rc = chimera_posix_fchmod(fd, 0755);

    if (rc != 0) {
        fprintf(stderr, "fchmod failed: %s\n", strerror(errno));
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    // Verify the mode changed using fstat
    rc = chimera_posix_fstat(fd, &st);

    if (rc != 0) {
        fprintf(stderr, "fstat failed: %s\n", strerror(errno));
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    if ((st.st_mode & 0777) != 0755) {
        fprintf(stderr, "fchmod: expected mode 0755, got %03o\n", st.st_mode & 0777);
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    chimera_posix_close(fd);

    fprintf(stderr, "fchmod test passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
}
