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
    const char           *test_data = "Hello, World! This is test data.";
    size_t                data_len;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Testing truncate...\n");

    data_len = strlen(test_data);

    // Create test file with initial content
    fd = chimera_posix_open("/test/truncate_test", O_CREAT | O_RDWR | O_TRUNC, 0644);

    if (fd < 0) {
        fprintf(stderr, "Failed to create test file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if (chimera_posix_write(fd, test_data, data_len) != (ssize_t) data_len) {
        fprintf(stderr, "Failed to write test data: %s\n", strerror(errno));
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    chimera_posix_close(fd);

    // Verify initial size
    rc = chimera_posix_stat("/test/truncate_test", &st);

    if (rc != 0) {
        fprintf(stderr, "stat failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if ((size_t) st.st_size != data_len) {
        fprintf(stderr, "Initial size wrong: expected %zu, got %lld\n",
                data_len, (long long) st.st_size);
        posix_test_fail(&env);
    }

    // Test truncate to smaller size
    fprintf(stderr, "Testing truncate to smaller size...\n");
    rc = chimera_posix_truncate("/test/truncate_test", 10);

    if (rc != 0) {
        fprintf(stderr, "truncate to 10 failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    rc = chimera_posix_stat("/test/truncate_test", &st);

    if (rc != 0) {
        fprintf(stderr, "stat after truncate failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if (st.st_size != 10) {
        fprintf(stderr, "Size after truncate wrong: expected 10, got %lld\n",
                (long long) st.st_size);
        posix_test_fail(&env);
    }

    fprintf(stderr, "truncate to smaller size passed\n");

    // Test truncate to larger size (extends with zeros)
    fprintf(stderr, "Testing truncate to larger size...\n");
    rc = chimera_posix_truncate("/test/truncate_test", 100);

    if (rc != 0) {
        fprintf(stderr, "truncate to 100 failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    rc = chimera_posix_stat("/test/truncate_test", &st);

    if (rc != 0) {
        fprintf(stderr, "stat after extend failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if (st.st_size != 100) {
        fprintf(stderr, "Size after extend wrong: expected 100, got %lld\n",
                (long long) st.st_size);
        posix_test_fail(&env);
    }

    fprintf(stderr, "truncate to larger size passed\n");

    // Test truncate to zero
    fprintf(stderr, "Testing truncate to zero...\n");
    rc = chimera_posix_truncate("/test/truncate_test", 0);

    if (rc != 0) {
        fprintf(stderr, "truncate to 0 failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    rc = chimera_posix_stat("/test/truncate_test", &st);

    if (rc != 0) {
        fprintf(stderr, "stat after truncate to 0 failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if (st.st_size != 0) {
        fprintf(stderr, "Size after truncate to 0 wrong: expected 0, got %lld\n",
                (long long) st.st_size);
        posix_test_fail(&env);
    }

    fprintf(stderr, "truncate to zero passed\n");

    fprintf(stderr, "truncate test passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
} /* main */
