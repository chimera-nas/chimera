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

    fprintf(stderr, "Testing chmod...\n");

    // Create test file with mode 0644
    fd = chimera_posix_open("/test/chmod_test", O_CREAT | O_RDWR, 0644);

    if (fd < 0) {
        fprintf(stderr, "Failed to create test file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    chimera_posix_close(fd);

    // Change mode to 0755
    rc = chimera_posix_chmod("/test/chmod_test", 0755);

    if (rc != 0) {
        fprintf(stderr, "chmod failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Verify the mode changed
    rc = chimera_posix_stat("/test/chmod_test", &st);

    if (rc != 0) {
        fprintf(stderr, "stat failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if ((st.st_mode & 0777) != 0755) {
        fprintf(stderr, "chmod: expected mode 0755, got %03o\n", st.st_mode & 0777);
        posix_test_fail(&env);
    }

    fprintf(stderr, "chmod test passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
}
