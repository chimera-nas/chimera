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
    int                   fd1, fd2, fd3;
    int                   rc;
    char                  buf[64];
    ssize_t               nread;
    const char           *test_data = "Hello, World!";
    size_t                data_len;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Testing dup...\n");

    data_len = strlen(test_data);

    // Create test file
    fd1 = chimera_posix_open("/test/dup_test", O_CREAT | O_RDWR | O_TRUNC, 0644);

    if (fd1 < 0) {
        fprintf(stderr, "Failed to create test file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Write some data
    if (chimera_posix_write(fd1, test_data, data_len) != (ssize_t) data_len) {
        fprintf(stderr, "Failed to write test data: %s\n", strerror(errno));
        chimera_posix_close(fd1);
        posix_test_fail(&env);
    }

    // Test dup
    fd2 = chimera_posix_dup(fd1);

    if (fd2 < 0) {
        fprintf(stderr, "dup failed: %s\n", strerror(errno));
        chimera_posix_close(fd1);
        posix_test_fail(&env);
    }

    if (fd2 == fd1) {
        fprintf(stderr, "dup returned same fd: %d\n", fd2);
        chimera_posix_close(fd1);
        posix_test_fail(&env);
    }

    fprintf(stderr, "dup created new fd: %d (original: %d)\n", fd2, fd1);

    // Seek to beginning using the duplicated fd
    if (chimera_posix_lseek(fd2, 0, SEEK_SET) != 0) {
        fprintf(stderr, "lseek on dup'd fd failed: %s\n", strerror(errno));
        chimera_posix_close(fd1);
        chimera_posix_close(fd2);
        posix_test_fail(&env);
    }

    // Read using the duplicated fd
    nread = chimera_posix_read(fd2, buf, data_len);

    if (nread != (ssize_t) data_len) {
        fprintf(stderr, "read on dup'd fd failed: %s\n", strerror(errno));
        chimera_posix_close(fd1);
        chimera_posix_close(fd2);
        posix_test_fail(&env);
    }

    buf[nread] = '\0';

    if (strcmp(buf, test_data) != 0) {
        fprintf(stderr, "Data mismatch: expected '%s', got '%s'\n", test_data, buf);
        chimera_posix_close(fd1);
        chimera_posix_close(fd2);
        posix_test_fail(&env);
    }

    fprintf(stderr, "Read via dup'd fd succeeded\n");

    // Close original fd - dup'd fd should still work
    chimera_posix_close(fd1);

    // Seek and read using dup'd fd after closing original
    if (chimera_posix_lseek(fd2, 0, SEEK_SET) != 0) {
        fprintf(stderr, "lseek after close original failed: %s\n", strerror(errno));
        chimera_posix_close(fd2);
        posix_test_fail(&env);
    }

    nread = chimera_posix_read(fd2, buf, data_len);

    if (nread != (ssize_t) data_len) {
        fprintf(stderr, "read after close original failed: %s\n", strerror(errno));
        chimera_posix_close(fd2);
        posix_test_fail(&env);
    }

    buf[nread] = '\0';

    if (strcmp(buf, test_data) != 0) {
        fprintf(stderr, "Data mismatch after close: expected '%s', got '%s'\n", test_data, buf);
        chimera_posix_close(fd2);
        posix_test_fail(&env);
    }

    fprintf(stderr, "dup'd fd works after closing original\n");

    // Test dup2 - use a lower fd number to ensure we're within max_fds
    fprintf(stderr, "Testing dup2...\n");

    // Use a low fd number that's definitely within range
    int target_fd = 50;

    fd3 = chimera_posix_dup2(fd2, target_fd);

    if (fd3 < 0) {
        fprintf(stderr, "dup2 failed: %s\n", strerror(errno));
        chimera_posix_close(fd2);
        posix_test_fail(&env);
    }

    if (fd3 != target_fd) {
        fprintf(stderr, "dup2 returned wrong fd: expected %d, got %d\n", target_fd, fd3);
        chimera_posix_close(fd2);
        chimera_posix_close(fd3);
        posix_test_fail(&env);
    }

    fprintf(stderr, "dup2 created fd at specific number: %d\n", fd3);

    // Test dup2 with same fd (should just return the fd)
    rc = chimera_posix_dup2(fd2, fd2);

    if (rc != fd2) {
        fprintf(stderr, "dup2 same fd failed: expected %d, got %d\n", fd2, rc);
        chimera_posix_close(fd2);
        chimera_posix_close(fd3);
        posix_test_fail(&env);
    }

    fprintf(stderr, "dup2 with same fd returned fd correctly\n");

    chimera_posix_close(fd2);
    chimera_posix_close(fd3);

    fprintf(stderr, "dup/dup2 test passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
}
