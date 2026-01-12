// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Test holey file that walks around file size edges: 2GB and 4GB
// Based on cthon/special/bigfile2.c from Connectathon 2004
//
// Tests that the platform supports files with offsets bigger than 31 bits

#define _LARGEFILE64_SOURCE 1

#include "cthon_common.h"

#define HIGH_WORD(n) ((unsigned int)(((unsigned int)((n) >> 32)) & 0xffffffff))
#define LOW_WORD(n)  ((unsigned int)((unsigned int)(n) & 0xffffffff))

static char *filename;

static void
check_around(int fd, off_t where)
{
    char          buf;
    int           i;
    off_t         start = where - 2;
    int           numbytes = 5;
    struct stat   statbuf;
    char          basechar = '0';

    if (chimera_posix_lseek(fd, start, SEEK_SET) < 0) {
        fprintf(stderr, "can't do initial seek to 0x%x%08x: %s\n",
                HIGH_WORD(start), LOW_WORD(start), strerror(errno));
        exit(1);
    }

    for (i = 0; i < numbytes; i++) {
        buf = basechar + i;
        if (chimera_posix_write(fd, &buf, 1) < 0) {
            fprintf(stderr, "can't write at 0x%x%08x: %s\n",
                    HIGH_WORD(start + i), LOW_WORD(start + i),
                    strerror(errno));
            exit(1);
        }
        if (chimera_posix_fstat(fd, &statbuf) < 0) {
            fprintf(stderr, "can't stat %s: %s\n", filename, strerror(errno));
            exit(1);
        }
        if (statbuf.st_size != start + i + 1) {
            fprintf(stderr, "expected size 0x%x%08x, got 0x%x%08x\n",
                    HIGH_WORD(start + i + 1), LOW_WORD(start + i + 1),
                    HIGH_WORD(statbuf.st_size), LOW_WORD(statbuf.st_size));
            exit(1);
        }
    }

    for (i = 0; i < numbytes; i++) {
        if (chimera_posix_lseek(fd, start + i, SEEK_SET) < 0) {
            fprintf(stderr, "can't seek to 0x%x%08x to reread: %s\n",
                    HIGH_WORD(start + i), LOW_WORD(start + i),
                    strerror(errno));
            exit(1);
        }
        if (chimera_posix_read(fd, &buf, 1) < 0) {
            fprintf(stderr, "can't read at offset 0x%x%08x: %s\n",
                    HIGH_WORD(start + i), LOW_WORD(start + i),
                    strerror(errno));
            exit(1);
        }
        if (buf != basechar + i) {
            fprintf(stderr, "expected '%c', got '%c' at 0x%x%08x\n",
                    basechar + i, buf,
                    HIGH_WORD(start + i), LOW_WORD(start + i));
            exit(1);
        }
    }
}

int
main(int argc, char **argv)
{
    struct posix_test_env env;
    int                   rc;
    int                   opt;
    char                  str[MAXPATHLEN];
    int                   fd;
    int                   oflags;

    cthon_Myname = "cthon_special_bigfile2";

    posix_test_init(&env, argv, argc);

    optind = 1;
    while ((opt = getopt(argc, argv, "hb:")) != -1) {
        switch (opt) {
            case 'b': break;
            default: break;
        }
    }

    rc = posix_test_mount(&env);
    if (rc != 0) {
        fprintf(stderr, "Failed to mount: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    cthon_testdir(NULL);

    fprintf(stdout, "%s: large file (2GB/4GB boundary) test\n", cthon_Myname);

    snprintf(str, sizeof(str), "%s/bigfile2", cthon_getcwd());
    filename = str;

    oflags = O_RDWR | O_CREAT | O_TRUNC | O_SYNC;
#ifdef O_LARGEFILE
    oflags |= O_LARGEFILE;
#endif

    fd = chimera_posix_open(str, oflags, 0666);
    if (fd < 0) {
        cthon_error("can't open %s", str);
        posix_test_fail(&env);
    }

    // Test around 2GB boundary
    fprintf(stdout, "\tTesting around 2GB boundary...\n");
    check_around(fd, ((off_t)0x7fffffff) + 1);

    if (chimera_posix_ftruncate(fd, 0) < 0) {
        cthon_error("can't truncate %s", str);
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    // Test around 4GB boundary
    fprintf(stdout, "\tTesting around 4GB boundary...\n");
    check_around(fd, ((off_t)(0xffffffffU)) + 1);

    chimera_posix_close(fd);
    chimera_posix_unlink(str);

    fprintf(stdout, "\tLarge file test passed\n");

    cthon_complete();
    posix_test_umount();
    posix_test_success(&env);
    return 0;
}
