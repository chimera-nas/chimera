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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/xattr.h>

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

    rc = fstat(fd, &st);
    CHECK(rc == 0 && st.st_size == sizeof(buf), "fstat size after writes");

    n = pread(fd, buf2, 4, 99);
    CHECK(n == 4 && buf2[0] == 'a' && buf2[1] == 'X' && buf2[2] == 'Y' &&
          buf2[3] == 'a', "pread sees the overwrite");

    /* --- O_APPEND interleaved with pwrite --- */

    fd2 = open("excl", O_WRONLY | O_APPEND);
    CHECK(fd2 >= 0, "open O_APPEND");

    n = write(fd2, "tail", 4);
    CHECK(n == 4, "append write");

    rc = fstat(fd2, &st);
    CHECK(rc == 0 && st.st_size == sizeof(buf) + 4, "append landed at EOF");

    close(fd2);

    /* --- ftruncate both directions --- */

    rc = ftruncate(fd, 1000);
    CHECK(rc == 0 && fstat(fd, &st) == 0 && st.st_size == 1000,
          "ftruncate down");

    rc = ftruncate(fd, 100000);
    CHECK(rc == 0 && fstat(fd, &st) == 0 && st.st_size == 100000,
          "ftruncate up");

    n = pread(fd, buf2, 4, 50000);
    CHECK(n == 4 && memcmp(buf2, "\0\0\0\0", 4) == 0,
          "hole reads as zeros");

    /* --- fsync / fdatasync --- */

    CHECK(fsync(fd) == 0, "fsync");
    CHECK(fdatasync(fd) == 0, "fdatasync");

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

    close(fd);
    unlink("target");

    /* --- directories: ENOTEMPTY, ENOENT --- */

    CHECK(mkdir("d", 0755) == 0, "mkdir");
    CHECK(mkdir("d/e", 0755) == 0, "nested mkdir");
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

    CHECK(mkdir("many", 0755) == 0, "mkdir for large readdir");

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

    while ((de = readdir(dirp)) != NULL) {
        if (de->d_name[0] != '.') {
            count++;
        }
    }
    closedir(dirp);

    CHECK(count == 2000, "readdir returned all entries (%d)", count);

    printf("\n%s (%d failures)\n", failures ? "FAILED" : "PASSED", failures);

    return failures != 0;
} /* main */
