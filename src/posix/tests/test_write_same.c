// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Tests for chimera_posix_write_same (NFSv4.2 WRITE_SAME / Application Data
 * Block expansion).  memfs and diskfs advertise CHIMERA_VFS_CAP_WRITE_SAME; the
 * other backends return ENOTSUP at the cap gate, in which case the test only
 * asserts the clean ENOTSUP path. */

#include "posix_test_common.h"

static struct posix_test_env *g_env;

static const char             PATTERN[] = "WXYZ"; /* 4-byte pattern */
#define PATLEN     4
#define RELOFF     8
#define BLOCK_SIZE 512
#define BLOCK_CNT  16
#define TOTAL      (BLOCK_SIZE * BLOCK_CNT)

static void
die(
    const char *what,
    ssize_t     n)
{
    fprintf(stderr, "%s: ret=%zd errno=%s\n", what, n, strerror(errno));
    posix_test_fail(g_env);
} /* die */

/* Expected byte at absolute file offset `off` for the ADB written at adb_off. */
static unsigned char
expected_byte(
    uint64_t off,
    uint64_t adb_off)
{
    uint64_t rel = off - adb_off;
    uint32_t pos = rel % BLOCK_SIZE;

    if (pos >= RELOFF && pos < RELOFF + PATLEN) {
        return (unsigned char) PATTERN[pos - RELOFF];
    }
    return 0;
} /* expected_byte */

static void
verify_pattern(
    int      fd,
    uint64_t adb_off)
{
    unsigned char buf[TOTAL];
    ssize_t       n;

    n = chimera_posix_pread(fd, buf, TOTAL, (off_t) adb_off);
    if (n != TOTAL) {
        die("pread verify", n);
    }
    for (uint64_t i = 0; i < TOTAL; i++) {
        if (buf[i] != expected_byte(adb_off + i, adb_off)) {
            fprintf(stderr, "pattern mismatch at off %llu: got %02x want %02x\n",
                    (unsigned long long) (adb_off + i), buf[i],
                    expected_byte(adb_off + i, adb_off));
            posix_test_fail(g_env);
        }
    }
} /* verify_pattern */

/* Returns 1 if WRITE_SAME is supported, 0 if ENOTSUP. */
static int
probe_support(void)
{
    int     fd;
    ssize_t n;

    fd = chimera_posix_open("/test/ws_probe", O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (fd < 0) {
        die("probe open", fd);
    }

    n = chimera_posix_write_same(fd, 0, BLOCK_SIZE, BLOCK_CNT, PATTERN, PATLEN,
                                 RELOFF);
    chimera_posix_close(fd);

    if (n == TOTAL) {
        return 1;
    }
    if (n == -1 && (errno == ENOTSUP || errno == EOPNOTSUPP)) {
        return 0;
    }
    fprintf(stderr, "probe write_same unexpected: ret=%zd errno=%s\n",
            n, strerror(errno));
    posix_test_fail(g_env);
    return 0; /* unreachable */
} /* probe_support */

static void
test_basic_pattern(void)
{
    int     fd;
    ssize_t n;

    fprintf(stderr, "Testing write_same fixed pattern into a fresh file...\n");

    fd = chimera_posix_open("/test/ws_basic", O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (fd < 0) {
        die("open basic", fd);
    }

    n = chimera_posix_write_same(fd, 0, BLOCK_SIZE, BLOCK_CNT, PATTERN, PATLEN,
                                 RELOFF);
    if (n != TOTAL) {
        die("write_same basic", n);
    }

    verify_pattern(fd, 0);
    chimera_posix_close(fd);
    fprintf(stderr, "  basic passed\n");
} /* test_basic_pattern */

static void
test_offset(void)
{
    int      fd;
    ssize_t  n;
    uint64_t adb_off = 4096;

    fprintf(stderr, "Testing write_same at a non-zero offset...\n");

    fd = chimera_posix_open("/test/ws_off", O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (fd < 0) {
        die("open off", fd);
    }

    n = chimera_posix_write_same(fd, (off_t) adb_off, BLOCK_SIZE, BLOCK_CNT,
                                 PATTERN, PATLEN, RELOFF);
    if (n != TOTAL) {
        die("write_same off", n);
    }

    verify_pattern(fd, adb_off);
    chimera_posix_close(fd);
    fprintf(stderr, "  offset passed\n");
} /* test_offset */

static void
test_overwrite(void)
{
    char    junk[TOTAL];
    int     fd;
    ssize_t n;

    fprintf(stderr, "Testing write_same overwriting existing data...\n");

    memset(junk, 'X', sizeof(junk));

    fd = chimera_posix_open("/test/ws_over", O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (fd < 0) {
        die("open over", fd);
    }

    n = chimera_posix_pwrite(fd, junk, TOTAL, 0);
    if (n != TOTAL) {
        die("pwrite junk", n);
    }

    n = chimera_posix_write_same(fd, 0, BLOCK_SIZE, BLOCK_CNT, PATTERN, PATLEN,
                                 RELOFF);
    if (n != TOTAL) {
        die("write_same over", n);
    }

    /* The old 'X' data must be fully replaced by the pattern. */
    verify_pattern(fd, 0);
    chimera_posix_close(fd);
    fprintf(stderr, "  overwrite passed\n");
} /* test_overwrite */

static void
test_zero_pattern(void)
{
    char    junk[TOTAL];
    char    buf[TOTAL];
    int     fd;
    ssize_t n;

    fprintf(stderr, "Testing write_same zero pattern (reads back as zeros)...\n");

    memset(junk, 'Q', sizeof(junk));

    fd = chimera_posix_open("/test/ws_zero", O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (fd < 0) {
        die("open zero", fd);
    }

    n = chimera_posix_pwrite(fd, junk, TOTAL, 0);
    if (n != TOTAL) {
        die("pwrite zero-junk", n);
    }

    /* pattern_len 0 -> blocks are all zero. */
    n = chimera_posix_write_same(fd, 0, BLOCK_SIZE, BLOCK_CNT, NULL, 0, 0);
    if (n != TOTAL) {
        die("write_same zero", n);
    }

    n = chimera_posix_pread(fd, buf, TOTAL, 0);
    if (n != TOTAL) {
        die("pread zero", n);
    }
    for (int i = 0; i < TOTAL; i++) {
        if (buf[i] != 0) {
            fprintf(stderr, "zero-pattern: nonzero byte %02x at %d\n",
                    (unsigned char) buf[i], i);
            posix_test_fail(g_env);
        }
    }

    chimera_posix_close(fd);
    fprintf(stderr, "  zero-pattern passed\n");
} /* test_zero_pattern */

static void
test_unsupported(void)
{
    int     fd;
    ssize_t n;

    fprintf(stderr, "Testing write_same surfaces ENOTSUP cleanly...\n");

    fd = chimera_posix_open("/test/ws_unsup", O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (fd < 0) {
        die("open unsup", fd);
    }

    errno = 0;
    n     = chimera_posix_write_same(fd, 0, BLOCK_SIZE, BLOCK_CNT, PATTERN,
                                     PATLEN, RELOFF);
    if (n != -1 || (errno != ENOTSUP && errno != EOPNOTSUPP)) {
        fprintf(stderr, "expected ENOTSUP/EOPNOTSUPP, got n=%zd errno=%s\n",
                n, strerror(errno));
        posix_test_fail(g_env);
    }

    chimera_posix_close(fd);
    fprintf(stderr, "  ENOTSUP path passed\n");
} /* test_unsupported */

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   rc;

    g_env = &env;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);
    if (rc != 0) {
        fprintf(stderr, "Failed to mount: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if (probe_support()) {
        fprintf(stderr, "Backend '%s' supports write_same - running full tests\n",
                env.backend);
        test_basic_pattern();
        test_offset();
        test_overwrite();
        test_zero_pattern();
    } else {
        fprintf(stderr, "Backend '%s' does not advertise CAP_WRITE_SAME - "
                "verifying ENOTSUP path\n", env.backend);
        test_unsupported();
    }

    fprintf(stderr, "All write_same tests passed!\n");

    rc = posix_test_umount();
    if (rc != 0) {
        fprintf(stderr, "Failed to unmount: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);
    return 0;
} /* main */
