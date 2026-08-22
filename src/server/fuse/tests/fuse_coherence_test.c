// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Cross-mount cache coherence.  argv[1] and argv[2] are two FUSE mounts of
 * the same share whose kernel attribute and entry caches are configured
 * with 60-SECOND timeouts, so any prompt visibility of mount-A changes on
 * mount B can only come from the daemon's invalidation pushes
 * (NOTIFY_INVAL_INODE from lease breaks, NOTIFY_INVAL_ENTRY from directory
 * watches) -- never from timeout expiry.  Each assertion polls up to five
 * seconds; without invalidation these fail hard against the 60s TTLs.
 */

#define _GNU_SOURCE 1

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>

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

#define POLL_LIMIT_US (5 * 1000 * 1000)
#define POLL_STEP_US  (50 * 1000)

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

static long
poll_for_size(
    const char *path,
    off_t       want)
{
    struct stat st;
    long        waited = 0;

    for (;;) {
        if (stat(path, &st) == 0 && st.st_size == want) {
            return waited;
        }

        if (waited >= POLL_LIMIT_US) {
            return -1;
        }

        usleep(POLL_STEP_US);
        waited += POLL_STEP_US;
    }
} /* poll_for_size */

static long
poll_for_enoent(const char *path)
{
    struct stat st;
    long        waited = 0;

    for (;;) {
        if (stat(path, &st) != 0 && errno == ENOENT) {
            return waited;
        }

        if (waited >= POLL_LIMIT_US) {
            return -1;
        }

        usleep(POLL_STEP_US);
        waited += POLL_STEP_US;
    }
} /* poll_for_enoent */

static long
poll_for_mode(
    const char *path,
    mode_t      want)
{
    struct stat st;
    long        waited = 0;

    for (;;) {
        if (stat(path, &st) == 0 && (st.st_mode & 07777) == want) {
            return waited;
        }

        if (waited >= POLL_LIMIT_US) {
            return -1;
        }

        usleep(POLL_STEP_US);
        waited += POLL_STEP_US;
    }
} /* poll_for_mode */

static long
poll_for_mtime_change(
    const char            *path,
    const struct timespec *old)
{
    struct stat st;
    long        waited = 0;

    for (;;) {
        if (stat(path, &st) == 0 &&
            (st.st_mtim.tv_sec != old->tv_sec ||
             st.st_mtim.tv_nsec != old->tv_nsec)) {
            return waited;
        }

        if (waited >= POLL_LIMIT_US) {
            return -1;
        }

        usleep(POLL_STEP_US);
        waited += POLL_STEP_US;
    }
} /* poll_for_mtime_change */

int
main(
    int   argc,
    char *argv[])
{
    char        a[512], b[512], b2[512];
    char        buf[64];
    struct stat st;
    long        waited;
    int         fd, n;

    if (argc < 3) {
        fprintf(stderr, "usage: fuse_coherence_test <mount_a> <mount_b>\n");
        return 1;
    }

    /* --- data + attribute invalidation on a file B has cached --- */

    snprintf(a, sizeof(a), "%s/coh_file", argv[1]);
    snprintf(b, sizeof(b), "%s/coh_file", argv[2]);

    CHECK(write_file(a, "one") == 0, "A creates the file");

    /* Prime B's kernel caches: dentry, attributes, and pages. */
    fd = open(b, O_RDONLY);
    CHECK(fd >= 0, "B opens the file");

    n = read(fd, buf, sizeof(buf));
    CHECK(n == 3 && memcmp(buf, "one", 3) == 0, "B reads the original data");
    CHECK(fstat(fd, &st) == 0 && st.st_size == 3, "B caches size 3");
    close(fd);

    CHECK(write_file(a, "two-is-longer") == 0, "A rewrites with a new size");

    waited = poll_for_size(b, 13);
    CHECK(waited >= 0,
          "B sees the new size in %ld ms despite a 60s attr TTL",
          waited / 1000);

    fd = open(b, O_RDONLY);
    n  = fd >= 0 ? read(fd, buf, sizeof(buf)) : -1;
    CHECK(n == 13 && memcmp(buf, "two-is-longer", 13) == 0,
          "B reads the new data, not its cached pages");
    if (fd >= 0) {
        close(fd);
    }

    /* --- entry invalidation: unlink seen through the other mount --- */

    CHECK(unlink(a) == 0, "A unlinks the file");

    waited = poll_for_enoent(b);
    CHECK(waited >= 0,
          "B gets ENOENT in %ld ms despite a 60s entry TTL",
          waited / 1000);

    /* --- entry invalidation: rename --- */

    snprintf(a, sizeof(a), "%s/coh_old", argv[1]);
    snprintf(b, sizeof(b), "%s/coh_old", argv[2]);
    snprintf(b2, sizeof(b2), "%s/coh_new", argv[2]);

    CHECK(write_file(a, "payload") == 0, "A creates the rename source");
    CHECK(stat(b, &st) == 0, "B caches the source dentry");

    snprintf(b2, sizeof(b2), "%s/coh_new", argv[1]);
    CHECK(rename(a, b2) == 0, "A renames it");

    waited = poll_for_enoent(b);
    CHECK(waited >= 0,
          "B sees the old name gone in %ld ms",
          waited / 1000);

    snprintf(b2, sizeof(b2), "%s/coh_new", argv[2]);
    CHECK(stat(b2, &st) == 0 && st.st_size == 7, "B resolves the new name");

    CHECK(unlink(b2) == 0, "cleanup via B");

    /* --- metadata-only change: chmod via A seen by B (open-primed) --- */

    snprintf(a, sizeof(a), "%s/coh_meta", argv[1]);
    snprintf(b, sizeof(b), "%s/coh_meta", argv[2]);

    CHECK(write_file(a, "m") == 0 && chmod(a, 0644) == 0,
          "A creates the metadata file");

    fd = open(b, O_RDONLY);
    CHECK(fd >= 0 && fstat(fd, &st) == 0 && (st.st_mode & 07777) == 0644,
          "B opens and caches mode 644");
    if (fd >= 0) {
        close(fd);
    }

    CHECK(chmod(a, 0640) == 0, "A chmods to 640");

    waited = poll_for_mode(b, 0640);
    CHECK(waited >= 0,
          "B sees the new mode in %ld ms despite a 60s attr TTL",
          waited / 1000);

    /* --- stat-only file: B never opens it --- */

    snprintf(a, sizeof(a), "%s/coh_statonly", argv[1]);
    snprintf(b, sizeof(b), "%s/coh_statonly", argv[2]);

    CHECK(write_file(a, "s") == 0, "A creates the stat-only file");

    CHECK(stat(b, &st) == 0 && st.st_size == 1,
          "B primes its attr cache with a bare stat");

    CHECK(write_file(a, "sss") == 0, "A rewrites it");

    waited = poll_for_size(b, 3);
    CHECK(waited >= 0,
          "B's bare stat sees the new size in %ld ms",
          waited / 1000);

    CHECK(chmod(a, 0600) == 0, "A chmods the stat-only file");

    waited = poll_for_mode(b, 0600);
    CHECK(waited >= 0,
          "B's bare stat sees the new mode in %ld ms",
          waited / 1000);

    /* --- directory attributes track foreign namespace changes --- */

    snprintf(a, sizeof(a), "%s/coh_dir", argv[1]);
    snprintf(b, sizeof(b), "%s/coh_dir", argv[2]);

    CHECK(mkdir(a, 0755) == 0, "A makes a directory");

    CHECK(stat(b, &st) == 0, "B primes the directory's attrs");

    struct timespec old_mtime = st.st_mtim;

    usleep(20000); /* ensure a distinguishable mtime */

    /* mkdir rather than a plain file create: VFS-core emits namespace
     * events for mkdir/remove/rename/link, and it is the event that
     * carries the invalidation. */
    snprintf(b2, sizeof(b2), "%s/coh_dir/inner", argv[1]);
    CHECK(mkdir(b2, 0755) == 0, "A makes a directory inside it");

    waited = poll_for_mtime_change(b, &old_mtime);
    CHECK(waited >= 0,
          "B sees the directory's mtime move in %ld ms",
          waited / 1000);

    printf("\n%s (%d failures)\n", failures ? "FAILED" : "PASSED", failures);

    return failures != 0;
} /* main */
