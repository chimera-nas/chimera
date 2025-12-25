// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Test for pread, pwrite, readv, writev, preadv, pwritev, preadv2, pwritev2

#include <sys/uio.h>
#include "posix_test_common.h"

#define TEST_DATA     "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
#define TEST_DATA_LEN 36

static void
test_pread_pwrite(int fd)
{
    char    buf[64];
    ssize_t ret;

    fprintf(stderr, "Testing pread/pwrite...\n");

    // Write at specific offsets using pwrite
    ret = chimera_posix_pwrite(fd, "HELLO", 5, 0);
    if (ret != 5) {
        fprintf(stderr, "pwrite at offset 0 failed: %zd\n", ret);
        exit(1);
    }

    ret = chimera_posix_pwrite(fd, "WORLD", 5, 10);
    if (ret != 5) {
        fprintf(stderr, "pwrite at offset 10 failed: %zd\n", ret);
        exit(1);
    }

    ret = chimera_posix_pwrite(fd, "-----", 5, 5);
    if (ret != 5) {
        fprintf(stderr, "pwrite at offset 5 failed: %zd\n", ret);
        exit(1);
    }

    // Read back using pread at specific offsets
    memset(buf, 0, sizeof(buf));
    ret = chimera_posix_pread(fd, buf, 5, 0);
    if (ret != 5 || memcmp(buf, "HELLO", 5) != 0) {
        fprintf(stderr, "pread at offset 0 failed: %zd, got '%.*s'\n", ret, 5, buf);
        exit(1);
    }

    memset(buf, 0, sizeof(buf));
    ret = chimera_posix_pread(fd, buf, 5, 10);
    if (ret != 5 || memcmp(buf, "WORLD", 5) != 0) {
        fprintf(stderr, "pread at offset 10 failed: %zd, got '%.*s'\n", ret, 5, buf);
        exit(1);
    }

    memset(buf, 0, sizeof(buf));
    ret = chimera_posix_pread(fd, buf, 15, 0);
    if (ret != 15 || memcmp(buf, "HELLO-----WORLD", 15) != 0) {
        fprintf(stderr, "pread full content failed: %zd, got '%.*s'\n", ret, 15, buf);
        exit(1);
    }

    // Verify pread doesn't change file offset - write sequentially then pread
    chimera_posix_lseek(fd, 0, SEEK_SET);
    chimera_posix_write(fd, "X", 1);  // offset now at 1

    ret = chimera_posix_pread(fd, buf, 1, 5);  // pread at offset 5
    if (ret != 1 || buf[0] != '-') {
        fprintf(stderr, "pread didn't read correct data: got '%c'\n", buf[0]);
        exit(1);
    }

    // Write at current offset (should be 1, not 5)
    ret = chimera_posix_write(fd, "Y", 1);
    if (ret != 1) {
        fprintf(stderr, "write after pread failed\n");
        exit(1);
    }

    // Verify: should now have "XY" at start
    memset(buf, 0, sizeof(buf));
    ret = chimera_posix_pread(fd, buf, 2, 0);
    if (ret != 2 || memcmp(buf, "XY", 2) != 0) {
        fprintf(stderr, "pread offset preservation failed: got '%.*s'\n", 2, buf);
        exit(1);
    }

    fprintf(stderr, "pread/pwrite tests passed\n");
} /* test_pread_pwrite */

static void
test_readv_writev(int fd)
{
    struct iovec iov[3];
    char         buf1[10], buf2[10], buf3[10];
    ssize_t      ret;

    fprintf(stderr, "Testing readv/writev...\n");

    // Reset file
    chimera_posix_lseek(fd, 0, SEEK_SET);

    // Setup write iovecs
    char         data1[] = "AAA";
    char         data2[] = "BBBBB";
    char         data3[] = "CC";

    iov[0].iov_base = data1;
    iov[0].iov_len  = 3;
    iov[1].iov_base = data2;
    iov[1].iov_len  = 5;
    iov[2].iov_base = data3;
    iov[2].iov_len  = 2;

    ret = chimera_posix_writev(fd, iov, 3);
    if (ret != 10) {
        fprintf(stderr, "writev failed: expected 10, got %zd\n", ret);
        exit(1);
    }

    // Reset to beginning
    chimera_posix_lseek(fd, 0, SEEK_SET);

    // Setup read iovecs
    memset(buf1, 0, sizeof(buf1));
    memset(buf2, 0, sizeof(buf2));
    memset(buf3, 0, sizeof(buf3));

    iov[0].iov_base = buf1;
    iov[0].iov_len  = 3;
    iov[1].iov_base = buf2;
    iov[1].iov_len  = 5;
    iov[2].iov_base = buf3;
    iov[2].iov_len  = 2;

    ret = chimera_posix_readv(fd, iov, 3);
    if (ret != 10) {
        fprintf(stderr, "readv failed: expected 10, got %zd\n", ret);
        exit(1);
    }

    if (memcmp(buf1, "AAA", 3) != 0 ||
        memcmp(buf2, "BBBBB", 5) != 0 ||
        memcmp(buf3, "CC", 2) != 0) {
        fprintf(stderr, "readv data mismatch: '%.*s' '%.*s' '%.*s'\n",
                3, buf1, 5, buf2, 2, buf3);
        exit(1);
    }

    fprintf(stderr, "readv/writev tests passed\n");
} /* test_readv_writev */

static void
test_preadv_pwritev(int fd)
{
    struct iovec iov[2];
    char         buf1[10], buf2[10];
    ssize_t      ret;

    fprintf(stderr, "Testing preadv/pwritev...\n");

    // Write at specific offset using pwritev
    char         data1[] = "111";
    char         data2[] = "222";

    iov[0].iov_base = data1;
    iov[0].iov_len  = 3;
    iov[1].iov_base = data2;
    iov[1].iov_len  = 3;

    ret = chimera_posix_pwritev(fd, iov, 2, 20);
    if (ret != 6) {
        fprintf(stderr, "pwritev failed: expected 6, got %zd\n", ret);
        exit(1);
    }

    // Read back using preadv
    memset(buf1, 0, sizeof(buf1));
    memset(buf2, 0, sizeof(buf2));

    iov[0].iov_base = buf1;
    iov[0].iov_len  = 3;
    iov[1].iov_base = buf2;
    iov[1].iov_len  = 3;

    ret = chimera_posix_preadv(fd, iov, 2, 20);
    if (ret != 6) {
        fprintf(stderr, "preadv failed: expected 6, got %zd\n", ret);
        exit(1);
    }

    if (memcmp(buf1, "111", 3) != 0 || memcmp(buf2, "222", 3) != 0) {
        fprintf(stderr, "preadv data mismatch: '%.*s' '%.*s'\n", 3, buf1, 3, buf2);
        exit(1);
    }

    // Verify preadv/pwritev don't change file offset
    off_t pos_before = chimera_posix_lseek(fd, 0, SEEK_CUR);

    ret = chimera_posix_preadv(fd, iov, 2, 20);

    off_t pos_after = chimera_posix_lseek(fd, 0, SEEK_CUR);

    if (pos_before != pos_after) {
        fprintf(stderr, "preadv changed file offset: %ld -> %ld\n",
                (long) pos_before, (long) pos_after);
        exit(1);
    }

    fprintf(stderr, "preadv/pwritev tests passed\n");
} /* test_preadv_pwritev */

static void
test_preadv2_pwritev2(int fd)
{
    struct iovec iov[2];
    char         buf1[10], buf2[10];
    ssize_t      ret;

    fprintf(stderr, "Testing preadv2/pwritev2...\n");

    // Write at specific offset using pwritev2 (with flags=0)
    char         data1[] = "XXX";
    char         data2[] = "YYY";

    iov[0].iov_base = data1;
    iov[0].iov_len  = 3;
    iov[1].iov_base = data2;
    iov[1].iov_len  = 3;

    ret = chimera_posix_pwritev2(fd, iov, 2, 30, 0);
    if (ret != 6) {
        fprintf(stderr, "pwritev2 failed: expected 6, got %zd\n", ret);
        exit(1);
    }

    // Read back using preadv2
    memset(buf1, 0, sizeof(buf1));
    memset(buf2, 0, sizeof(buf2));

    iov[0].iov_base = buf1;
    iov[0].iov_len  = 3;
    iov[1].iov_base = buf2;
    iov[1].iov_len  = 3;

    ret = chimera_posix_preadv2(fd, iov, 2, 30, 0);
    if (ret != 6) {
        fprintf(stderr, "preadv2 failed: expected 6, got %zd\n", ret);
        exit(1);
    }

    if (memcmp(buf1, "XXX", 3) != 0 || memcmp(buf2, "YYY", 3) != 0) {
        fprintf(stderr, "preadv2 data mismatch: '%.*s' '%.*s'\n", 3, buf1, 3, buf2);
        exit(1);
    }

    // Test with 64-bit variants
    char data3[] = "ZZZ";
    char data4[] = "WWW";

    iov[0].iov_base = data3;
    iov[0].iov_len  = 3;
    iov[1].iov_base = data4;
    iov[1].iov_len  = 3;

    ret = chimera_posix_pwritev64v2(fd, iov, 2, 40, 0);
    if (ret != 6) {
        fprintf(stderr, "pwritev64v2 failed: expected 6, got %zd\n", ret);
        exit(1);
    }

    memset(buf1, 0, sizeof(buf1));
    memset(buf2, 0, sizeof(buf2));

    iov[0].iov_base = buf1;
    iov[0].iov_len  = 3;
    iov[1].iov_base = buf2;
    iov[1].iov_len  = 3;

    ret = chimera_posix_preadv64v2(fd, iov, 2, 40, 0);
    if (ret != 6) {
        fprintf(stderr, "preadv64v2 failed: expected 6, got %zd\n", ret);
        exit(1);
    }

    if (memcmp(buf1, "ZZZ", 3) != 0 || memcmp(buf2, "WWW", 3) != 0) {
        fprintf(stderr, "preadv64v2 data mismatch: '%.*s' '%.*s'\n", 3, buf1, 3, buf2);
        exit(1);
    }

    fprintf(stderr, "preadv2/pwritev2 tests passed\n");
} /* test_preadv2_pwritev2 */

static void
test_64bit_variants(int fd)
{
    char    buf[10];
    ssize_t ret;

    fprintf(stderr, "Testing 64-bit variants...\n");

    // Test pread64/pwrite64
    ret = chimera_posix_pwrite64(fd, "64BIT", 5, 50);
    if (ret != 5) {
        fprintf(stderr, "pwrite64 failed: %zd\n", ret);
        exit(1);
    }

    memset(buf, 0, sizeof(buf));
    ret = chimera_posix_pread64(fd, buf, 5, 50);
    if (ret != 5 || memcmp(buf, "64BIT", 5) != 0) {
        fprintf(stderr, "pread64 failed: %zd, got '%.*s'\n", ret, 5, buf);
        exit(1);
    }

    // Test preadv64/pwritev64
    struct iovec iov[1];
    char         data[] = "VEC64";

    iov[0].iov_base = data;
    iov[0].iov_len  = 5;

    ret = chimera_posix_pwritev64(fd, iov, 1, 60);
    if (ret != 5) {
        fprintf(stderr, "pwritev64 failed: %zd\n", ret);
        exit(1);
    }

    memset(buf, 0, sizeof(buf));
    iov[0].iov_base = buf;
    iov[0].iov_len  = 5;

    ret = chimera_posix_preadv64(fd, iov, 1, 60);
    if (ret != 5 || memcmp(buf, "VEC64", 5) != 0) {
        fprintf(stderr, "preadv64 failed: %zd, got '%.*s'\n", ret, 5, buf);
        exit(1);
    }

    fprintf(stderr, "64-bit variants tests passed\n");
} /* test_64bit_variants */

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   fd;
    int                   rc;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Create test file
    fd = chimera_posix_open("/test/pio_test", O_CREAT | O_RDWR | O_TRUNC, 0644);

    if (fd < 0) {
        fprintf(stderr, "Failed to create test file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Run all tests
    test_pread_pwrite(fd);
    test_readv_writev(fd);
    test_preadv_pwritev(fd);
    test_preadv2_pwritev2(fd);
    test_64bit_variants(fd);

    fprintf(stderr, "All positional I/O tests passed!\n");

    chimera_posix_close(fd);

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
} /* main */
