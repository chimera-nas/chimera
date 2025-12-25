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
    const char           *test_data = "Hello, World! This is test data.";
    size_t                data_len;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Testing fdatasync...\n");

    data_len = strlen(test_data);

    // Create test file
    fd = chimera_posix_open("/test/fdatasync_test", O_CREAT | O_RDWR | O_TRUNC, 0644);

    if (fd < 0) {
        fprintf(stderr, "Failed to create test file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Write some data
    if (chimera_posix_write(fd, test_data, data_len) != (ssize_t) data_len) {
        fprintf(stderr, "Failed to write test data: %s\n", strerror(errno));
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    // Test fdatasync
    rc = chimera_posix_fdatasync(fd);

    if (rc != 0) {
        fprintf(stderr, "fdatasync failed: %s\n", strerror(errno));
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    fprintf(stderr, "fdatasync on open file passed\n");

    // Write more data and fdatasync again
    if (chimera_posix_write(fd, test_data, data_len) != (ssize_t) data_len) {
        fprintf(stderr, "Failed to write more test data: %s\n", strerror(errno));
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    rc = chimera_posix_fdatasync(fd);

    if (rc != 0) {
        fprintf(stderr, "second fdatasync failed: %s\n", strerror(errno));
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    fprintf(stderr, "second fdatasync passed\n");

    chimera_posix_close(fd);

    fprintf(stderr, "fdatasync test passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
}
