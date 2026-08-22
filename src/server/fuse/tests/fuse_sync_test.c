// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Synchronous cross-mount coherence (coherence=sync, the default).
 *
 * argv[1]/argv[2] are two FUSE mounts of one share with 60-SECOND attr and
 * entry timeouts.  Unlike fuse_coherence_test (which polls, proving the
 * asynchronous invalidation pushes arrive), every assertion here is a
 * SINGLE syscall issued after the mutating call on the other mount has
 * returned: the sync contract is that a mutation does not return anywhere
 * until every peer kernel's caches for the affected state are gone, so
 * there is nothing to wait for.  Any need to poll is a failure.
 *
 * The tail scenarios churn one directory (and one file) from both mounts
 * concurrently and assert wall-clock sanity: the in-flight-detection
 * softening must resolve the cross-kernel lock cycles, not the multi-second
 * gate/break deadlines.
 */

#define _GNU_SOURCE 1

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/wait.h>

static int failures;

#define CHECK(cond, ...) \
        do { \
            if (!(cond)) { \
                printf("FAIL: " __VA_ARGS__); \
                printf(" (errno %d %s)\n", errno, strerror(errno)); \
                failures++; \
            } else { \
                printf("ok:   " __VA_ARGS__); printf("\n"); \
            } \
        } while (0)

static int
write_file(
    const char *path,
    const char *data)
{
    int fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    int rc;

    if (fd < 0) {
        return -1;
    }

    rc = write(fd, data, strlen(data));
    close(fd);

    return rc == (int) strlen(data) ? 0 : -1;
} /* write_file */

/* Prime B's kernel attr cache hard: the first stat may be served
 * uncached (coverage begins at first touch), the second replies under a
 * held grant and carries the full 60s TTL. */
static void
prime_stat(const char *path)
{
    struct stat st;

    (void) stat(path, &st);
    (void) stat(path, &st);
} /* prime_stat */

static double
now_s(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
} /* now_s */

/* Churn worker: count create/unlink cycles of distinct names in `dir`. */
static int
churn_names(
    const char *dir,
    const char *tag,
    int         count)
{
    char path[512];
    int  i;

    for (i = 0; i < count; i++) {
        snprintf(path, sizeof(path), "%s/churn_%s_%d", dir, tag, i);
        if (write_file(path, "x") != 0) {
            return -1;
        }
        if (unlink(path) != 0) {
            return -1;
        }
    }

    return 0;
} /* churn_names */

/* Churn worker: rewrite one shared file `count` times. */
static int
churn_writes(
    const char *path,
    int         count)
{
    int  fd, i;
    char buf[64];

    fd = open(path, O_WRONLY);
    if (fd < 0) {
        return -1;
    }

    for (i = 0; i < count; i++) {
        int n = snprintf(buf, sizeof(buf), "gen-%d\n", i);
        if (pwrite(fd, buf, n, 0) != n) {
            close(fd);
            return -1;
        }
    }

    close(fd);
    return 0;
} /* churn_writes */

int
main(
    int   argc,
    char *argv[])
{
    char        a[512], b[512], b2[512];
    char        buf[64];
    struct stat st;
    int         fd, n;

    setvbuf(stdout, NULL, _IONBF, 0);

    if (argc < 3) {
        fprintf(stderr, "usage: fuse_sync_test <mount_a> <mount_b>\n");
        return 1;
    }

    /* --- data + size: B's very next stat/read after A's write --- */

    snprintf(a, sizeof(a), "%s/s_file", argv[1]);
    snprintf(b, sizeof(b), "%s/s_file", argv[2]);

    CHECK(write_file(a, "one") == 0, "A creates the file");

    prime_stat(b);
    fd = open(b, O_RDONLY);
    CHECK(fd >= 0, "B opens and primes its page cache");
    n = read(fd, buf, sizeof(buf));
    CHECK(n == 3 && memcmp(buf, "one", 3) == 0, "B reads the original data");
    close(fd);
    CHECK(stat(b, &st) == 0 && st.st_size == 3, "B caches size 3");

    CHECK(write_file(a, "two-is-longer") == 0, "A rewrites with a new size");

    CHECK(stat(b, &st) == 0 && st.st_size == 13,
          "B's IMMEDIATE next stat sees the new size (no polling)");

    fd = open(b, O_RDONLY);
    n  = fd >= 0 ? read(fd, buf, sizeof(buf)) : -1;
    CHECK(n == 13 && memcmp(buf, "two-is-longer", 13) == 0,
          "B's immediate read sees the new data");
    if (fd >= 0) {
        close(fd);
    }

    /* --- metadata: B's very next stat after A's chmod --- */

    prime_stat(b);
    CHECK(chmod(a, 0640) == 0, "A chmods to 640");
    CHECK(stat(b, &st) == 0 && (st.st_mode & 07777) == 0640,
          "B's immediate stat sees mode 640");

    /* --- unlink: B's very next stat is ENOENT --- */

    prime_stat(b);
    CHECK(unlink(a) == 0, "A unlinks the file");
    errno = 0;
    CHECK(stat(b, &st) != 0 && errno == ENOENT,
          "B's immediate stat is ENOENT");

    /* --- negative dentry: cached miss killed by A's create --- */

    snprintf(a, sizeof(a), "%s/s_ghost", argv[1]);
    snprintf(b, sizeof(b), "%s/s_ghost", argv[2]);

    errno = 0;
    CHECK(stat(b, &st) != 0 && errno == ENOENT, "B misses the ghost");
    errno = 0;
    CHECK(stat(b, &st) != 0 && errno == ENOENT,
          "B misses again (negative dentry now cached)");

    CHECK(write_file(a, "boo") == 0, "A creates the ghost");

    CHECK(stat(b, &st) == 0 && st.st_size == 3,
          "B's immediate stat finds it through the cached negative");

    CHECK(unlink(a) == 0, "ghost cleanup");

    /* --- rename: both names correct on B's very next stats --- */

    snprintf(a, sizeof(a), "%s/s_old", argv[1]);
    snprintf(b, sizeof(b), "%s/s_old", argv[2]);
    snprintf(b2, sizeof(b2), "%s/s_new", argv[2]);

    CHECK(write_file(a, "payload") == 0, "A creates the rename source");
    prime_stat(b);
    /* Prime a negative dentry for the destination name too. */
    errno = 0;
    (void) stat(b2, &st);
    (void) stat(b2, &st);

    {
        char a2[512];

        snprintf(a2, sizeof(a2), "%s/s_new", argv[1]);
        CHECK(rename(a, a2) == 0, "A renames it");
    }

    errno = 0;
    CHECK(stat(b, &st) != 0 && errno == ENOENT,
          "B's immediate stat: old name gone");
    CHECK(stat(b2, &st) == 0 && st.st_size == 7,
          "B's immediate stat: new name resolves");
    CHECK(unlink(b2) == 0, "rename cleanup via B");

    /* --- directory attrs: B's next stat after A creates inside --- */

    snprintf(a, sizeof(a), "%s/s_dir", argv[1]);
    snprintf(b, sizeof(b), "%s/s_dir", argv[2]);

    CHECK(mkdir(a, 0755) == 0, "A makes a directory");
    prime_stat(b);
    CHECK(stat(b, &st) == 0, "B primes the directory's attrs");

    struct timespec old_mtime = st.st_mtim;

    usleep(20000); /* mtime granularity, not polling */

    snprintf(b2, sizeof(b2), "%s/s_dir/inner", argv[1]);
    CHECK(write_file(b2, "i") == 0, "A creates a file inside it");

    CHECK(stat(b, &st) == 0 &&
          (st.st_mtim.tv_sec != old_mtime.tv_sec ||
           st.st_mtim.tv_nsec != old_mtime.tv_nsec),
          "B's immediate stat sees the directory's mtime move");

    /* --- concurrent same-dir churn from both mounts: the cross-kernel
     * gate cycle must resolve via in-flight detection, not the 5s
     * deadline.  100 gated ops at deadline pace would take minutes. --- */

    {
        double elapsed, start = now_s();
        pid_t  pa, pb;
        int    stat_a = -1, stat_b = -1;

        snprintf(a, sizeof(a), "%s/s_dir", argv[1]);
        snprintf(b, sizeof(b), "%s/s_dir", argv[2]);

        pa = fork();
        if (pa == 0) {
            _exit(churn_names(a, "a", 25) == 0 ? 0 : 1);
        }
        pb = fork();
        if (pb == 0) {
            _exit(churn_names(b, "b", 25) == 0 ? 0 : 1);
        }

        waitpid(pa, &stat_a, 0);
        waitpid(pb, &stat_b, 0);
        elapsed = now_s() - start;

        CHECK(WIFEXITED(stat_a) && WEXITSTATUS(stat_a) == 0 &&
              WIFEXITED(stat_b) && WEXITSTATUS(stat_b) == 0,
              "concurrent same-dir churn from both mounts succeeds");
        CHECK(elapsed < 20.0,
              "same-dir churn finished in %.1fs (deadline-grind would be minutes)",
              elapsed);
    }

    /* --- concurrent same-file writes from both mounts: the write/write
     * invalidation cycle must resolve via the write-inflight deferral. --- */

    {
        double elapsed, start;
        pid_t  pa, pb;
        int    stat_a = -1, stat_b = -1;

        snprintf(a, sizeof(a), "%s/s_wfile", argv[1]);
        snprintf(b, sizeof(b), "%s/s_wfile", argv[2]);

        CHECK(write_file(a, "seed") == 0, "A seeds the contended file");
        prime_stat(b);

        start = now_s();

        pa = fork();
        if (pa == 0) {
            _exit(churn_writes(a, 25) == 0 ? 0 : 1);
        }
        pb = fork();
        if (pb == 0) {
            _exit(churn_writes(b, 25) == 0 ? 0 : 1);
        }

        waitpid(pa, &stat_a, 0);
        waitpid(pb, &stat_b, 0);
        elapsed = now_s() - start;

        CHECK(WIFEXITED(stat_a) && WEXITSTATUS(stat_a) == 0 &&
              WIFEXITED(stat_b) && WEXITSTATUS(stat_b) == 0,
              "concurrent same-file writes from both mounts succeed");
        CHECK(elapsed < 20.0,
              "same-file write churn finished in %.1fs",
              elapsed);

        /* After the dust settles the writers are gone: one more foreign
         * write then a single foreign stat must be exact again. */
        CHECK(write_file(a, "final-state!") == 0, "A writes the final state");
        CHECK(stat(b, &st) == 0 && st.st_size == 12,
              "B's immediate stat sees the final size");
    }

    printf("\n%s (%d failures)\n", failures ? "FAILED" : "PASSED", failures);

    return failures != 0;
} /* main */
