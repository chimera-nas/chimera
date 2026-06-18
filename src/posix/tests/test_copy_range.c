// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Tests for chimera_posix_copy_file_range and chimera_posix_clone_file_range.
 *
 * Some backends don't advertise CHIMERA_VFS_CAP_COPY_RANGE / _CLONE_RANGE
 * (diskfs, cairn, the nfs proxy used by nfs3/nfs3rdma tests). On those the
 * VFS layer returns ENOTSUP at the cap gate. The test probes for support
 * up front and only asserts behavior the backend actually offers.
 */

#include "posix_test_common.h"

#define PATTERN_LEN 4096

static struct posix_test_env *g_env;

static void
die(
    const char *what,
    ssize_t     n)
{
    fprintf(stderr, "%s: ret=%zd errno=%s\n", what, n, strerror(errno));
    posix_test_fail(g_env);
} /* die */

static void
fill_pattern(
    char  *buf,
    size_t len,
    char   seed)
{
    for (size_t i = 0; i < len; i++) {
        buf[i] = (char) (seed + (char) (i & 0x3f));
    }
} /* fill_pattern */

/* Returns 1 if the backend supports copy_file_range, 0 if ENOTSUP. */
static int
probe_copy_support(void)
{
    int     fd1, fd2;
    ssize_t n;

    fd1 = chimera_posix_open("/test/probe_a", O_CREAT | O_RDWR | O_TRUNC, 0644);
    fd2 = chimera_posix_open("/test/probe_b", O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (fd1 < 0 || fd2 < 0) {
        die("probe open", -1);
    }

    n = chimera_posix_pwrite(fd1, "X", 1, 0);
    if (n != 1) {
        die("probe pwrite", n);
    }

    n = chimera_posix_copy_file_range(fd1, NULL, fd2, NULL, 1, 0);

    chimera_posix_close(fd1);
    chimera_posix_close(fd2);

    if (n == 1) {
        return 1;
    }
    if (n == -1 && (errno == ENOTSUP || errno == EOPNOTSUPP)) {
        return 0;
    }
    fprintf(stderr, "probe copy_file_range unexpected: ret=%zd errno=%s\n",
            n, strerror(errno));
    posix_test_fail(g_env);
    return 0; /* unreachable */
} /* probe_copy_support */

static void
test_basic_copy(void)
{
    char    src_buf[PATTERN_LEN];
    char    verify[PATTERN_LEN];
    int     src_fd, dst_fd;
    ssize_t n;

    fprintf(stderr, "Testing chimera_posix_copy_file_range basic...\n");

    fill_pattern(src_buf, PATTERN_LEN, 'a');

    src_fd = chimera_posix_open("/test/copy_src", O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (src_fd < 0) {
        die("open src", src_fd);
    }
    dst_fd = chimera_posix_open("/test/copy_dst", O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (dst_fd < 0) {
        die("open dst", dst_fd);
    }

    n = chimera_posix_pwrite(src_fd, src_buf, PATTERN_LEN, 0);
    if (n != PATTERN_LEN) {
        die("pwrite src", n);
    }

    n = chimera_posix_copy_file_range(src_fd, NULL, dst_fd, NULL, PATTERN_LEN, 0);
    if (n != PATTERN_LEN) {
        die("copy_file_range basic", n);
    }

    n = chimera_posix_pread(dst_fd, verify, PATTERN_LEN, 0);
    if (n != PATTERN_LEN || memcmp(src_buf, verify, PATTERN_LEN) != 0) {
        die("dst content mismatch", n);
    }

    chimera_posix_close(src_fd);
    chimera_posix_close(dst_fd);

    fprintf(stderr, "  basic passed\n");
} /* test_basic_copy */

static void
test_copy_with_offsets(void)
{
    char    src_buf[PATTERN_LEN];
    char    verify[PATTERN_LEN];
    int     src_fd, dst_fd;
    off_t   src_off = 1024;
    off_t   dst_off = 2048;
    ssize_t n;

    fprintf(stderr, "Testing copy_file_range with explicit offsets...\n");

    fill_pattern(src_buf, PATTERN_LEN, 'b');

    src_fd = chimera_posix_open("/test/copy_off_src", O_CREAT | O_RDWR | O_TRUNC, 0644);
    dst_fd = chimera_posix_open("/test/copy_off_dst", O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (src_fd < 0 || dst_fd < 0) {
        die("open offsets", -1);
    }

    n = chimera_posix_pwrite(src_fd, src_buf, PATTERN_LEN, 0);
    if (n != PATTERN_LEN) {
        die("pwrite offsets", n);
    }

    n = chimera_posix_copy_file_range(src_fd, &src_off, dst_fd, &dst_off, 2048, 0);
    if (n != 2048) {
        die("copy_file_range offsets", n);
    }
    if (src_off != 1024 + 2048 || dst_off != 2048 + 2048) {
        fprintf(stderr, "offsets not advanced: src_off=%lld dst_off=%lld\n",
                (long long) src_off, (long long) dst_off);
        posix_test_fail(g_env);
    }

    n = chimera_posix_pread(dst_fd, verify, 2048, 2048);
    if (n != 2048 || memcmp(verify, src_buf + 1024, 2048) != 0) {
        die("offset content mismatch", n);
    }

    chimera_posix_close(src_fd);
    chimera_posix_close(dst_fd);

    fprintf(stderr, "  offsets passed\n");
} /* test_copy_with_offsets */

static void
test_copy_zero_length(void)
{
    int     fd1, fd2;
    ssize_t n;

    fprintf(stderr, "Testing copy_file_range with len=0...\n");

    fd1 = chimera_posix_open("/test/copy_zero_a", O_CREAT | O_RDWR | O_TRUNC, 0644);
    fd2 = chimera_posix_open("/test/copy_zero_b", O_CREAT | O_RDWR | O_TRUNC, 0644);

    n = chimera_posix_copy_file_range(fd1, NULL, fd2, NULL, 0, 0);
    if (n != 0) {
        die("copy len=0", n);
    }

    chimera_posix_close(fd1);
    chimera_posix_close(fd2);

    fprintf(stderr, "  zero-length passed\n");
} /* test_copy_zero_length */

static void
test_copy_short_at_eof(void)
{
    char    src_buf[1024];
    char    verify[2048];
    int     src_fd, dst_fd;
    ssize_t n;

    fprintf(stderr, "Testing copy_file_range short read at EOF...\n");

    fill_pattern(src_buf, 1024, 'c');

    src_fd = chimera_posix_open("/test/copy_short_src", O_CREAT | O_RDWR | O_TRUNC, 0644);
    dst_fd = chimera_posix_open("/test/copy_short_dst", O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (src_fd < 0 || dst_fd < 0) {
        die("open short", -1);
    }

    n = chimera_posix_pwrite(src_fd, src_buf, 1024, 0);
    if (n != 1024) {
        die("pwrite short", n);
    }

    n = chimera_posix_copy_file_range(src_fd, NULL, dst_fd, NULL, 2048, 0);
    if (n != 1024) {
        die("short-at-EOF expected 1024", n);
    }

    n = chimera_posix_pread(dst_fd, verify, 1024, 0);
    if (n != 1024 || memcmp(verify, src_buf, 1024) != 0) {
        die("short-at-EOF content mismatch", n);
    }

    chimera_posix_close(src_fd);
    chimera_posix_close(dst_fd);

    fprintf(stderr, "  short-at-EOF passed\n");
} /* test_copy_short_at_eof */

static void
test_copy_invalid_flags(void)
{
    int     fd1, fd2;
    ssize_t n;

    fprintf(stderr, "Testing copy_file_range with invalid flags...\n");

    fd1 = chimera_posix_open("/test/copy_inv_a", O_CREAT | O_RDWR | O_TRUNC, 0644);
    fd2 = chimera_posix_open("/test/copy_inv_b", O_CREAT | O_RDWR | O_TRUNC, 0644);

    errno = 0;
    n     = chimera_posix_copy_file_range(fd1, NULL, fd2, NULL, 16, 0x1);
    if (n != -1 || errno != EINVAL) {
        fprintf(stderr, "expected EINVAL for nonzero flags, got n=%zd errno=%s\n",
                n, strerror(errno));
        posix_test_fail(g_env);
    }

    chimera_posix_close(fd1);
    chimera_posix_close(fd2);

    fprintf(stderr, "  invalid-flags passed\n");
} /* test_copy_invalid_flags */

/* Clone is block-aligned on memfs and on filesystems with reflink support.
 * memfs's default block size is 64 KiB and the linux backend's underlying
 * fs typically requires 4 KiB alignment, so 64 KiB safely lands on a real
 * block boundary on every supporting backend without needing to ask. */
#define CLONE_PROBE_LEN (64 * 1024)

/* Returns 1 if the backend supports clone_file_range on a block-aligned
 * range, 0 if it surfaces ENOTSUP. Other errors fail the test. */
static int
probe_clone_support(void)
{
    int     fd1, fd2, rc;
    char   *src_buf;
    ssize_t n;

    src_buf = malloc(CLONE_PROBE_LEN);
    if (!src_buf) {
        die("probe_clone malloc", -1);
    }
    memset(src_buf, 'Z', CLONE_PROBE_LEN);

    fd1 = chimera_posix_open("/test/probe_clone_a", O_CREAT | O_RDWR | O_TRUNC, 0644);
    fd2 = chimera_posix_open("/test/probe_clone_b", O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (fd1 < 0 || fd2 < 0) {
        die("probe_clone open", -1);
    }

    n = chimera_posix_pwrite(fd1, src_buf, CLONE_PROBE_LEN, 0);
    if (n != CLONE_PROBE_LEN) {
        die("probe_clone pwrite", n);
    }

    rc = chimera_posix_clone_file_range(fd2, 0, fd1, 0, CLONE_PROBE_LEN);

    chimera_posix_close(fd1);
    chimera_posix_close(fd2);
    free(src_buf);

    if (rc == 0) {
        return 1;
    }
    if (rc == -1 && (errno == ENOTSUP || errno == EOPNOTSUPP)) {
        return 0;
    }
    fprintf(stderr, "probe clone unexpected: rc=%d errno=%s\n",
            rc, strerror(errno));
    posix_test_fail(g_env);
    return 0; /* unreachable */
} /* probe_clone_support */

static void
test_clone_content(void)
{
    char   *src_buf;
    char   *overwrite;
    char   *verify;
    int     src_fd, dst_fd, rc;
    ssize_t n;

    fprintf(stderr, "Testing clone_file_range content...\n");

    src_buf   = malloc(CLONE_PROBE_LEN);
    overwrite = malloc(CLONE_PROBE_LEN);
    verify    = malloc(CLONE_PROBE_LEN);
    if (!src_buf || !overwrite || !verify) {
        die("clone malloc", -1);
    }

    fill_pattern(src_buf, CLONE_PROBE_LEN, 'd');
    fill_pattern(overwrite, CLONE_PROBE_LEN, 'e');

    src_fd = chimera_posix_open("/test/clone_src", O_CREAT | O_RDWR | O_TRUNC, 0644);
    dst_fd = chimera_posix_open("/test/clone_dst", O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (src_fd < 0 || dst_fd < 0) {
        die("clone open", -1);
    }

    n = chimera_posix_pwrite(src_fd, src_buf, CLONE_PROBE_LEN, 0);
    if (n != CLONE_PROBE_LEN) {
        die("clone pwrite", n);
    }

    rc = chimera_posix_clone_file_range(dst_fd, 0, src_fd, 0, CLONE_PROBE_LEN);
    if (rc != 0) {
        die("clone_file_range", rc);
    }

    n = chimera_posix_pread(dst_fd, verify, CLONE_PROBE_LEN, 0);
    if (n != CLONE_PROBE_LEN || memcmp(src_buf, verify, CLONE_PROBE_LEN) != 0) {
        die("clone content mismatch", n);
    }

    /* COW: write to dst must not disturb src */
    n = chimera_posix_pwrite(dst_fd, overwrite, CLONE_PROBE_LEN, 0);
    if (n != CLONE_PROBE_LEN) {
        die("clone pwrite dst", n);
    }

    n = chimera_posix_pread(src_fd, verify, CLONE_PROBE_LEN, 0);
    if (n != CLONE_PROBE_LEN || memcmp(src_buf, verify, CLONE_PROBE_LEN) != 0) {
        die("clone src disturbed after dst write", n);
    }

    chimera_posix_close(src_fd);
    chimera_posix_close(dst_fd);
    free(src_buf);
    free(overwrite);
    free(verify);

    fprintf(stderr, "  clone content + COW passed\n");
} /* test_clone_content */

/* Clone, then unlink the source: the destination must keep the (now sole-owned)
 * data, and a write to it must still succeed.  Exercises the refcount-decrement
 * free path on the unlinked source's shared extents. */
static void
test_clone_unlink(void)
{
    char   *src_buf;
    char   *over;
    char   *verify;
    int     src_fd, dst_fd, rc;
    ssize_t n;

    fprintf(stderr, "Testing clone then unlink source (refcount free path)...\n");

    src_buf = malloc(CLONE_PROBE_LEN);
    over    = malloc(CLONE_PROBE_LEN);
    verify  = malloc(CLONE_PROBE_LEN);
    if (!src_buf || !over || !verify) {
        die("clone_unlink malloc", -1);
    }
    fill_pattern(src_buf, CLONE_PROBE_LEN, 'm');
    fill_pattern(over, CLONE_PROBE_LEN, 'n');

    src_fd = chimera_posix_open("/test/clone_ul_src", O_CREAT | O_RDWR | O_TRUNC, 0644);
    dst_fd = chimera_posix_open("/test/clone_ul_dst", O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (src_fd < 0 || dst_fd < 0) {
        die("clone_unlink open", -1);
    }

    n = chimera_posix_pwrite(src_fd, src_buf, CLONE_PROBE_LEN, 0);
    if (n != CLONE_PROBE_LEN) {
        die("clone_unlink pwrite", n);
    }

    rc = chimera_posix_clone_file_range(dst_fd, 0, src_fd, 0, CLONE_PROBE_LEN);
    if (rc != 0) {
        die("clone_unlink clone", rc);
    }

    /* Unlink + close the source; its shared extents must decrement, not free. */
    chimera_posix_close(src_fd);
    if (chimera_posix_unlink("/test/clone_ul_src") != 0) {
        die("clone_unlink unlink", -1);
    }

    /* Destination still reads the cloned data... */
    n = chimera_posix_pread(dst_fd, verify, CLONE_PROBE_LEN, 0);
    if (n != CLONE_PROBE_LEN || memcmp(src_buf, verify, CLONE_PROBE_LEN) != 0) {
        die("clone_unlink dst data lost after src unlink", n);
    }

    /* ...and remains writable (it is now the sole owner). */
    n = chimera_posix_pwrite(dst_fd, over, CLONE_PROBE_LEN, 0);
    if (n != CLONE_PROBE_LEN) {
        die("clone_unlink dst rewrite", n);
    }
    n = chimera_posix_pread(dst_fd, verify, CLONE_PROBE_LEN, 0);
    if (n != CLONE_PROBE_LEN || memcmp(over, verify, CLONE_PROBE_LEN) != 0) {
        die("clone_unlink dst rewrite mismatch", n);
    }

    chimera_posix_close(dst_fd);
    free(src_buf);
    free(over);
    free(verify);

    fprintf(stderr, "  clone-then-unlink passed\n");
} /* test_clone_unlink */

static void
test_clone_unsupported(void)
{
    int fd1, fd2, rc;

    fprintf(stderr, "Testing clone_file_range surfaces ENOTSUP cleanly...\n");

    fd1 = chimera_posix_open("/test/clone_unsup_a", O_CREAT | O_RDWR | O_TRUNC, 0644);
    fd2 = chimera_posix_open("/test/clone_unsup_b", O_CREAT | O_RDWR | O_TRUNC, 0644);

    errno = 0;
    rc    = chimera_posix_clone_file_range(fd2, 0, fd1, 0, 4096);
    if (rc != -1 || (errno != ENOTSUP && errno != EOPNOTSUPP)) {
        fprintf(stderr, "expected ENOTSUP/EOPNOTSUPP, got rc=%d errno=%s\n",
                rc, strerror(errno));
        posix_test_fail(g_env);
    }

    chimera_posix_close(fd1);
    chimera_posix_close(fd2);

    fprintf(stderr, "  ENOTSUP path passed\n");
} /* test_clone_unsupported */

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   rc;
    int                   copy_supported;

    g_env = &env;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);
    if (rc != 0) {
        fprintf(stderr, "Failed to mount: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    copy_supported = probe_copy_support();

    if (copy_supported) {
        fprintf(stderr, "Backend '%s' supports copy_file_range - running full tests\n",
                env.backend);
        test_basic_copy();
        test_copy_with_offsets();
        test_copy_zero_length();
        test_copy_short_at_eof();
    } else {
        fprintf(stderr, "Backend '%s' does not advertise CAP_COPY_RANGE - "
                "skipping copy-content tests\n", env.backend);
    }

    /* flag-validation runs regardless of copy support */
    test_copy_invalid_flags();

    if (probe_clone_support()) {
        fprintf(stderr, "Backend '%s' supports clone_file_range - validating content\n",
                env.backend);
        test_clone_content();
        test_clone_unlink();
    } else {
        fprintf(stderr, "Backend '%s' does not advertise CAP_CLONE_RANGE - "
                "verifying ENOTSUP path\n", env.backend);
        test_clone_unsupported();
    }

    fprintf(stderr, "All copy_file_range/clone_file_range tests passed!\n");

    rc = posix_test_umount();
    if (rc != 0) {
        fprintf(stderr, "Failed to unmount: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);
    return 0;
} /* main */
