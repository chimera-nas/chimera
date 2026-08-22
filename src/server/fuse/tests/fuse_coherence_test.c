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

    printf("\n%s (%d failures)\n", failures ? "FAILED" : "PASSED", failures);

    return failures != 0;
} /* main */
