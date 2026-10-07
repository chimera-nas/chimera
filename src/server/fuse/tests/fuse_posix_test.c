// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * POSIX semantics against a live FUSE mountpoint (argv[1]), asserted with
 * plain syscalls and exact errnos.  Runs under fuse_posix_test.sh, which
 * stands up the daemon and the mount.  No chimera libraries: everything
 * observable here traveled through the kernel FUSE protocol.
 */

#define _GNU_SOURCE 1

#include "common/test_host.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#include "common/platform.h"
#else  /* ifdef _WIN32 */
#include <unistd.h>
#endif /* ifdef _WIN32 */
#include <fcntl.h>
#include <errno.h>
#include "common/dirent.h"
#include <sys/stat.h>
#ifdef _WIN32
#include "common/platform.h"
#endif /* ifdef _WIN32 */
#include <sys/statvfs.h>
#include <sys/xattr.h>
#ifndef _WIN32
#include <grp.h>
#include <sys/wait.h>
#endif /* ifndef _WIN32 */

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

/*
 * POSIX rights retention: I/O rights bind when the file is OPENED, so a
 * descriptor already open for writing keeps working across a chmod that
 * would deny a fresh open.  Path-based truncate(2) is the contrast case
 * -- it re-checks and must fail.  Nothing in the tree covered this pair,
 * which is how a FUSE descriptor came to lose its write right to a later
 * chmod (the grant was re-derived at first I/O from the current mode).
 * Runs only as an unprivileged user: root bypasses the check entirely,
 * so as root this would pass no matter what the server did.  Run as root,
 * main() runs it in a child that drops to FUSE_TEST_UNPRIV_UID, which needs
 * an allow_other mount.  have_private says main() left a root-owned 0700
 * directory "private" here for the non-owner case.
 */
static void
unprivileged_checks(int have_private)
{
    struct stat    st;
    DIR           *dirp;
    struct dirent *de;
    int            count;

    int            rfd = open("retain", O_CREAT | O_RDWR, 0644);

    CHECK(rfd >= 0, "rights retention: create");

    if (rfd >= 0) {
        CHECK(chmod("retain", 0444) == 0, "rights retention: chmod 0444");

        CHECK(ftruncate(rfd, 4096) == 0,
              "ftruncate through a writable fd survives chmod");
        CHECK(pwrite(rfd, "z", 1, 0) == 1,
              "write through a writable fd survives chmod");

        CHECK(truncate("retain", 0) < 0 && errno == EACCES,
              "truncate(path) still denied after chmod");

        close(rfd);
    }
    CHECK(unlink("retain") == 0, "rights retention: cleanup");

    /* The same for a directory stream: opendir(3) binds the right to
     * list it, so readdir keeps working after a chmod that would refuse
     * a fresh opendir. */
    CHECK(chimera_test_mkdir("retaindir", 0755) == 0 &&
          close(open("retaindir/f", O_CREAT | O_WRONLY, 0644)) == 0,
          "directory rights retention: create");
    dirp = opendir("retaindir");
    CHECK(dirp != NULL, "directory rights retention: opendir");
    if (dirp) {
        CHECK(chmod("retaindir", 0300) == 0,
              "directory rights retention: chmod 0300");

        count = 0;
        errno = 0;
        while ((de = readdir(dirp)) != NULL) {
            if (strcmp(de->d_name, "f") == 0) {
                count++;
            }
        }
        CHECK(errno == 0 && count == 1,
              "readdir through an open stream survives chmod (errno %d)",
              errno);
        closedir(dirp);

        dirp = opendir("retaindir");
        CHECK(dirp == NULL && errno == EACCES,
              "opendir still denied after chmod");
        if (dirp) {
            closedir(dirp);
        }
        CHECK(chmod("retaindir", 0755) == 0,
              "directory rights retention: restore");
    }
    CHECK(unlink("retaindir/f") == 0 && rmdir("retaindir") == 0,
          "directory rights retention: cleanup");

    /* Read without search: the owner of a 0644 directory (rw-) may list
     * it but not reach its entries.  READDIRPLUS must not hand the
     * kernel the entries' attributes and nodeids: on a
     * no_default_permissions mount with cached entries
     * (coherence=ttl) it would serve a later stat from them and never
     * send the lookup that checks search.  A default mount has the
     * kernel check search itself. */
    CHECK(chimera_test_mkdir("nosearch", 0755) == 0 &&
          close(open("nosearch/f", O_CREAT | O_WRONLY, 0644)) == 0,
          "read without search: create");
    CHECK(chmod("nosearch", 0644) == 0, "read without search: chmod 0644");
    dirp = opendir("nosearch");
    CHECK(dirp != NULL, "read without search: opendir");
    if (dirp) {
        int dtype = -1;

        count = 0;
        while ((de = readdir(dirp)) != NULL) {
            if (strcmp(de->d_name, "f") == 0) {
                count++;
                dtype = de->d_type;
            }
        }
        CHECK(count == 1, "read without search: readdir lists the names");
        /* getdents(2) gives d_type with read alone; only the attributes
         * and the handle are search's to withhold. */
        CHECK(dtype == DT_REG,
              "read without search: the entry keeps its d_type (%d)", dtype);
        closedir(dirp);
    }
    CHECK(stat("nosearch/f", &st) < 0 && errno == EACCES,
          "stat of an entry still denied after readdir");
    CHECK(chmod("nosearch", 0755) == 0 &&
          unlink("nosearch/f") == 0 && rmdir("nosearch") == 0,
          "read without search: cleanup");

    /* A directory the caller may not read: opendir is refused, by chimera
     * itself on a no_default_permissions mount.  On a passthrough backend the
     * by-handle open beneath it checks nothing, so only the engine's own
     * check stands in the way. */
    if (have_private) {
        errno = 0;
        dirp  = opendir("private");
        CHECK(dirp == NULL && errno == EACCES,
              "opendir of another user's 0700 directory denied");
        if (dirp) {
            closedir(dirp);
        }
    }
} /* unprivileged_checks */

#ifndef _WIN32
/*
 * Run unprivileged_checks() as `uid` from a root test run, in a scratch
 * directory that uid owns, beside a root-owned 0700 directory for the
 * non-owner case.  The mount must allow other users (allow_other).
 * Returns the child's failure count, or -1 if it could not run.
 */
static int
unprivileged_as(int uid)
{
    pid_t pid;
    int   wstatus;

    if (chimera_test_mkdir("unpriv", 0755) != 0 ||
        chown("unpriv", uid, uid) != 0 ||
        chimera_test_mkdir("unpriv/private", 0700) != 0) {
        return -1;
    }

    fflush(stdout);
    pid = fork();
    if (pid < 0) {
        return -1;
    }

    if (pid == 0) {
        failures = 0;
        if (setgroups(0, NULL) != 0 || setgid(uid) != 0 || setuid(uid) != 0 ||
            chdir("unpriv") != 0) {
            printf("FAIL: could not become uid %d (errno %d %s)\n",
                   uid, errno, strerror(errno));
            _exit(1);
        }
        unprivileged_checks(1);
        fflush(stdout);
        _exit(failures > 255 ? 255 : failures);
    }

    if (waitpid(pid, &wstatus, 0) != pid || !WIFEXITED(wstatus)) {
        return -1;
    }

    CHECK(rmdir("unpriv/private") == 0 && rmdir("unpriv") == 0,
          "unprivileged checks: cleanup");

    return WEXITSTATUS(wstatus);
} /* unprivileged_as */
#endif /* ifndef _WIN32 */


/* The permission checks: run as the invoking user when unprivileged, else as
 * FUSE_TEST_UNPRIV_UID, else skipped (root bypasses DAC). */
static void
access_checks(void)
{
    const char *uid = getenv("FUSE_TEST_UNPRIV_UID");

    if (geteuid() != 0) {
        unprivileged_checks(0);
#ifndef _WIN32
    } else if (uid) {
        CHECK(unprivileged_as(atoi(uid)) == 0,
              "unprivileged checks as uid %s", uid);
#endif /* ifndef _WIN32 */
    } else {
        printf("skip: rights retention (running as root, DAC bypassed)\n");
    }
} /* access_checks */

int
main(
    int   argc,
    char *argv[])
{
    struct stat     st, st2;
    struct statvfs  stv;
    struct timespec times[2];
    char            buf[65536], buf2[65536];
    ssize_t         n;
    int             fd, fd2, rc, i;

    if (argc < 2 || chdir(argv[1]) != 0) {
        fprintf(stderr, "usage: fuse_posix_test <mountpoint>\n");
        return 1;
    }

    /* FUSE_TEST_ACCESS_ONLY: just the permission checks, for a backend whose
     * cell exists to cover how it enforces access. */
    if (getenv("FUSE_TEST_ACCESS_ONLY")) {
        access_checks();
        printf("\n%s (%d failures)\n", failures ? "FAILED" : "PASSED", failures);
        return failures != 0;
    }

    /* --- O_CREAT|O_EXCL --- */

    fd = open("excl", O_CREAT | O_EXCL | O_RDWR, 0644);
    CHECK(fd >= 0, "exclusive create");

    fd2 = open("excl", O_CREAT | O_EXCL | O_RDWR, 0644);
    CHECK(fd2 < 0 && errno == EEXIST, "second exclusive create fails EEXIST");

    /* --- pwrite/pread at offsets, fstat coherence --- */

    memset(buf, 'a', sizeof(buf));
    n = pwrite(fd, buf, sizeof(buf), 0);
    CHECK(n == sizeof(buf), "64KB pwrite");

    n = pwrite(fd, "XY", 2, 100);
    CHECK(n == 2, "overwrite at offset");

    rc = fd >= 0 ? fstat(fd, &st) : -1;
    CHECK(rc == 0 && st.st_size == sizeof(buf), "fstat size after writes");

    n = pread(fd, buf2, 4, 99);
    CHECK(n == 4 && buf2[0] == 'a' && buf2[1] == 'X' && buf2[2] == 'Y' &&
          buf2[3] == 'a', "pread sees the overwrite");

    /* --- O_APPEND interleaved with pwrite --- */

    fd2 = open("excl", O_WRONLY | O_APPEND);
    CHECK(fd2 >= 0, "open O_APPEND");

    n = write(fd2, "tail", 4);
    CHECK(n == 4, "append write");

    rc = fd2 >= 0 ? fstat(fd2, &st) : -1;
    CHECK(rc == 0 && st.st_size == sizeof(buf) + 4, "append landed at EOF");

    close(fd2);

    /* --- ftruncate both directions --- */

    rc = fd >= 0 ? ftruncate(fd, 1000) : -1;
    CHECK(rc == 0 && fstat(fd, &st) == 0 && st.st_size == 1000,
          "ftruncate down");

    rc = fd >= 0 ? ftruncate(fd, 100000) : -1;
    CHECK(rc == 0 && fstat(fd, &st) == 0 && st.st_size == 100000,
          "ftruncate up");

    n = pread(fd, buf2, 4, 50000);
    CHECK(n == 4 && memcmp(buf2, "\0\0\0\0", 4) == 0,
          "hole reads as zeros");

    /* --- fsync / fdatasync --- */

    CHECK(fd >= 0 && fsync(fd) == 0, "fsync");
    CHECK(fd >= 0 && fdatasync(fd) == 0, "fdatasync");

    /* --- unlink while open --- */

    rc = unlink("excl");
    CHECK(rc == 0, "unlink while open");

    n = pwrite(fd, "still", 5, 0);
    CHECK(n == 5, "write to unlinked file");

    n = pread(fd, buf2, 5, 0);
    CHECK(n == 5 && memcmp(buf2, "still", 5) == 0, "read from unlinked file");

    close(fd);

    CHECK(stat("excl", &st) < 0 && errno == ENOENT,
          "unlinked name is gone");

    /* --- rename over an open target --- */

    fd = open("target", O_CREAT | O_RDWR, 0644);
    CHECK(fd >= 0 && write(fd, "old", 3) == 3, "create rename target");

    fd2 = open("source", O_CREAT | O_RDWR, 0644);
    CHECK(fd2 >= 0 && write(fd2, "new", 3) == 3, "create rename source");
    close(fd2);

    rc = rename("source", "target");
    CHECK(rc == 0, "rename over open target");

    n = pread(fd, buf2, 3, 0);
    CHECK(n == 3 && memcmp(buf2, "old", 3) == 0,
          "open fd still reads the replaced file");

    /*
     * The rename unlinked the target, so the inode this descriptor still
     * names has no links left.  Its file handle does not change, which is
     * what makes this worth asserting: a cached attribute keyed by that
     * handle survives the rename and keeps reporting the pre-rename link
     * count unless the rename invalidates it.
     */
    rc = fd >= 0 ? fstat(fd, &st) : -1;
    CHECK(rc == 0 && st.st_nlink == 0,
          "replaced inode reports nlink 0 through the surviving fd (%lu)",
          rc == 0 ? (unsigned long) st.st_nlink : 0UL);

    close(fd);
    unlink("target");

    /* --- directories: ENOTEMPTY, ENOENT --- */

    CHECK(chimera_test_mkdir("d", 0755) == 0, "mkdir");
    CHECK(chimera_test_mkdir("d/e", 0755) == 0, "nested mkdir");
    CHECK(rmdir("d") < 0 && errno == ENOTEMPTY, "rmdir non-empty ENOTEMPTY");
    CHECK(rmdir("d/e") == 0 && rmdir("d") == 0, "rmdir bottom-up");
    CHECK(unlink("d") < 0 && errno == ENOENT, "unlink missing ENOENT");
    CHECK(open("d/x", O_RDONLY) < 0 && errno == ENOENT,
          "open under missing dir ENOENT");

    /* --- symlink / link / st_nlink --- */

    fd = open("base", O_CREAT | O_WRONLY, 0644);
    CHECK(fd >= 0 && write(fd, "z", 1) == 1, "create link base");
    close(fd);

    CHECK(symlink("base", "slink") == 0, "symlink");

    n = readlink("slink", buf2, sizeof(buf2));
    CHECK(n == 4 && memcmp(buf2, "base", 4) == 0, "readlink");

    rc = lstat("slink", &st);
    CHECK(rc == 0 && S_ISLNK(st.st_mode), "lstat sees the link itself");

    CHECK(link("base", "blink") == 0, "hardlink");

    rc = stat("base", &st);
    CHECK(rc == 0 && st.st_nlink == 2, "st_nlink after hardlink");

    rc = stat("blink", &st2);
    CHECK(rc == 0 && st.st_ino == st2.st_ino, "hardlink shares the inode");

    unlink("slink");
    unlink("blink");

    /* --- utimensat --- */

    times[0].tv_sec  = 1000000;
    times[0].tv_nsec = 0;
    times[1].tv_sec  = 2000000;
    times[1].tv_nsec = 500;

    rc = utimensat(AT_FDCWD, "base", times, 0);
    CHECK(rc == 0, "utimensat explicit times");

    rc = stat("base", &st);
    CHECK(rc == 0 && st.st_atim.tv_sec == 1000000 &&
          st.st_mtim.tv_sec == 2000000 && st.st_mtim.tv_nsec == 500,
          "explicit timestamps round-trip");

    /* --- chmod/chown via path --- */

    CHECK(chmod("base", 0604) == 0 && stat("base", &st) == 0 &&
          (st.st_mode & 07777) == 0604, "chmod");

    /* --- xattrs through the f* variants --- */

    fd = open("base", O_RDWR);

    rc = fsetxattr(fd, "user.test", "value1", 6, 0);

    if (rc < 0 && errno == ENOTSUP) {
        printf("ok:   xattrs unsupported by backend (ENOTSUP passthrough)\n");
    } else {
        CHECK(rc == 0, "fsetxattr create");

        n = fgetxattr(fd, "user.test", buf2, sizeof(buf2));
        CHECK(n == 6 && memcmp(buf2, "value1", 6) == 0, "fgetxattr value");

        n = fgetxattr(fd, "user.test", NULL, 0);
        CHECK(n == 6, "fgetxattr size probe");

        rc = fsetxattr(fd, "user.test", "v2", 2, XATTR_CREATE);
        CHECK(rc < 0 && errno == EEXIST, "XATTR_CREATE on existing EEXIST");

        rc = fsetxattr(fd, "user.test", "v2", 2, XATTR_REPLACE);
        CHECK(rc == 0, "XATTR_REPLACE");

        n = flistxattr(fd, buf2, sizeof(buf2));
        CHECK(n >= 10 && memmem(buf2, n, "user.test", 10) != NULL,
              "flistxattr contains the name");

        rc = fremovexattr(fd, "user.test");
        CHECK(rc == 0, "fremovexattr");

        n = fgetxattr(fd, "user.test", buf2, sizeof(buf2));
        CHECK(n < 0 && errno == ENODATA, "removed xattr is gone");
    }

    close(fd);
    unlink("base");

    /* --- statvfs --- */

    rc = statvfs(".", &stv);
    CHECK(rc == 0 && stv.f_bsize > 0 && stv.f_namemax >= 255, "statvfs");

    /* --- readdir past one FUSE reply buffer --- */

    CHECK(chimera_test_mkdir("many", 0755) == 0, "mkdir for large readdir");

    for (i = 0; i < 2000; i++) {
        char name[64];
        snprintf(name, sizeof(name), "many/entry-%04d-padded-name-%04d", i, i);
        fd = open(name, O_CREAT | O_WRONLY, 0644);
        if (fd < 0) {
            break;
        }
        close(fd);
    }
    CHECK(i == 2000, "created 2000 entries");

    DIR           *dirp = opendir("many");
    CHECK(dirp != NULL, "opendir");

    int            count = 0;
    struct dirent *de;

    while (dirp && (de = readdir(dirp)) != NULL) {
        if (de->d_name[0] != '.') {
            count++;
        }
    }
    if (dirp) {
        closedir(dirp);
    }

    CHECK(count == 2000, "readdir returned all entries (%d)", count);

    access_checks();

    printf("\n%s (%d failures)\n", failures ? "FAILED" : "PASSED", failures);

    return failures != 0;
} /* main */
